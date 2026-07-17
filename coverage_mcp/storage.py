from __future__ import annotations

import csv
import json
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Queue
from typing import Any

import duckdb

from coverage_mcp.contracts import MAX_SUMMARY_LINES, MAX_TIMEOUT_SECONDS, MIN_SUMMARY_LINES, MIN_TIMEOUT_SECONDS
from coverage_mcp.git_utils import inspect_git, merge_base
from coverage_mcp.models import CoverageReport
from coverage_mcp.parsers import parse_coverage_report

DEFAULT_RUN_RETENTION = 100
COMMAND_DURATION_SAMPLE_LIMIT = 20


def row_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, row, strict=True))


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def minute_bucket(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


class CoverageStore:
    def __init__(self, db_path: str | Path, *, run_retention: int = DEFAULT_RUN_RETENTION) -> None:
        if run_retention < 1:
            raise ValueError("run_retention must be at least 1")
        self.db_path = Path(db_path).expanduser()
        self.run_retention = run_retention
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_dir = self.db_path.parent / "runs"
        self._conn = duckdb.connect(self.db_path.as_posix())
        self._lock = threading.RLock()
        self._run_lock = threading.Lock()
        self._process_lock = threading.RLock()
        self._active_processes: dict[str, subprocess.Popen[str]] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._job_queue: Queue[str | None] = Queue()
        self._closing = threading.Event()
        self._init_schema()
        queued_jobs = self._recover_run_jobs()
        self._prune_run_history()
        self._refresh_all_command_duration_stats()
        self._worker = threading.Thread(
            target=self._run_worker,
            name="coverage-mcp-runner",
            daemon=True,
        )
        self._worker.start()
        for run_id in queued_jobs:
            self._cancel_event(run_id)
            self._job_queue.put(run_id)

    def close(self) -> None:
        self._closing.set()
        self._job_queue.put(None)
        self._worker.join()
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    minute_bucket TIMESTAMP NOT NULL,
                    repo_path VARCHAR NOT NULL,
                    repo_key VARCHAR NOT NULL,
                    branch VARCHAR,
                    commit_sha VARCHAR,
                    base_ref VARCHAR,
                    suite VARCHAR NOT NULL,
                    format VARCHAR NOT NULL,
                    report_path VARCHAR NOT NULL,
                    warnings VARCHAR NOT NULL,
                    metadata VARCHAR NOT NULL,
                    total_lines INTEGER NOT NULL,
                    covered_lines INTEGER NOT NULL,
                    total_branches INTEGER NOT NULL,
                    covered_branches INTEGER NOT NULL,
                    total_functions INTEGER NOT NULL,
                    covered_functions INTEGER NOT NULL,
                    total_regions INTEGER NOT NULL,
                    covered_regions INTEGER NOT NULL,
                    line_rate DOUBLE,
                    branch_rate DOUBLE,
                    function_rate DOUBLE,
                    region_rate DOUBLE
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    snapshot_id VARCHAR NOT NULL,
                    file_path VARCHAR NOT NULL,
                    total_lines INTEGER NOT NULL,
                    covered_lines INTEGER NOT NULL,
                    total_branches INTEGER NOT NULL,
                    covered_branches INTEGER NOT NULL,
                    total_functions INTEGER NOT NULL,
                    covered_functions INTEGER NOT NULL,
                    total_regions INTEGER NOT NULL,
                    covered_regions INTEGER NOT NULL,
                    line_rate DOUBLE,
                    branch_rate DOUBLE,
                    function_rate DOUBLE,
                    region_rate DOUBLE,
                    raw_metrics VARCHAR NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lines (
                    snapshot_id VARCHAR NOT NULL,
                    file_path VARCHAR NOT NULL,
                    line_number INTEGER NOT NULL,
                    hits INTEGER NOT NULL,
                    covered BOOLEAN NOT NULL,
                    count_line BOOLEAN NOT NULL,
                    total_branches INTEGER NOT NULL,
                    covered_branches INTEGER NOT NULL,
                    total_functions INTEGER NOT NULL,
                    covered_functions INTEGER NOT NULL,
                    details VARCHAR NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worktrees (
                    id VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    name VARCHAR,
                    path VARCHAR NOT NULL,
                    repo_path VARCHAR NOT NULL,
                    repo_key VARCHAR NOT NULL,
                    branch VARCHAR,
                    head_sha VARCHAR,
                    base_ref VARCHAR NOT NULL,
                    base_sha VARCHAR,
                    baseline_snapshot_id VARCHAR
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS registered_commands (
                    id VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    name VARCHAR NOT NULL,
                    command VARCHAR NOT NULL,
                    cwd VARCHAR NOT NULL,
                    repo_path VARCHAR NOT NULL,
                    repo_key VARCHAR NOT NULL,
                    branch VARCHAR,
                    commit_sha VARCHAR,
                    shell VARCHAR NOT NULL,
                    approved_by VARCHAR NOT NULL,
                    approval_note VARCHAR NOT NULL,
                    artifact_specs VARCHAR NOT NULL,
                    enabled BOOLEAN NOT NULL,
                    duration_estimate_ms INTEGER,
                    duration_p90_ms INTEGER,
                    duration_sample_count INTEGER NOT NULL DEFAULT 0,
                    duration_stats_updated_at TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id VARCHAR PRIMARY KEY,
                    command_id VARCHAR NOT NULL,
                    command_name VARCHAR NOT NULL,
                    command VARCHAR NOT NULL,
                    idempotency_key VARCHAR,
                    cwd VARCHAR NOT NULL,
                    repo_path VARCHAR NOT NULL,
                    repo_key VARCHAR NOT NULL,
                    branch VARCHAR,
                    commit_sha VARCHAR,
                    started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    exit_code INTEGER,
                    status VARCHAR NOT NULL,
                    stdout_path VARCHAR NOT NULL,
                    stderr_path VARCHAR NOT NULL,
                    parsed_summary VARCHAR NOT NULL,
                    artifact_paths VARCHAR NOT NULL,
                    queued_at TIMESTAMP,
                    queue_duration_ms INTEGER,
                    cancellation_requested_at TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_artifacts (
                    run_id VARCHAR NOT NULL,
                    kind VARCHAR NOT NULL,
                    path VARCHAR NOT NULL,
                    exists BOOLEAN NOT NULL,
                    size_bytes BIGINT,
                    coverage_format VARCHAR,
                    suite VARCHAR,
                    modified_by_run BOOLEAN NOT NULL DEFAULT false,
                    ingest_status VARCHAR,
                    snapshot_id VARCHAR,
                    ingest_error VARCHAR
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_jobs (
                    id VARCHAR PRIMARY KEY,
                    command_id VARCHAR NOT NULL,
                    command_name VARCHAR NOT NULL,
                    command VARCHAR NOT NULL,
                    idempotency_key VARCHAR,
                    cwd VARCHAR NOT NULL,
                    repo_path VARCHAR NOT NULL,
                    repo_key VARCHAR NOT NULL,
                    branch VARCHAR,
                    commit_sha VARCHAR,
                    queued_at TIMESTAMP NOT NULL,
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    timeout_seconds INTEGER,
                    max_summary_lines INTEGER NOT NULL,
                    status VARCHAR NOT NULL,
                    stdout_path VARCHAR NOT NULL,
                    stderr_path VARCHAR NOT NULL,
                    error VARCHAR NOT NULL,
                    cancellation_requested_at TIMESTAMP
                )
                """
            )
            self._migrate_schema()
            for statement in [
                "CREATE INDEX IF NOT EXISTS idx_snapshots_repo_time ON snapshots(repo_key, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_commit ON snapshots(repo_key, commit_sha)",
                "CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id)",
                "CREATE INDEX IF NOT EXISTS idx_lines_lookup ON lines(snapshot_id, file_path, line_number)",
                "CREATE INDEX IF NOT EXISTS idx_worktrees_repo ON worktrees(repo_key, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_registered_commands_name ON registered_commands(name, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_runs_command_time ON runs(command_id, started_at)",
                "CREATE INDEX IF NOT EXISTS idx_run_artifacts_kind ON run_artifacts(kind)",
                "CREATE INDEX IF NOT EXISTS idx_run_jobs_status_time ON run_jobs(status, queued_at)",
                "CREATE INDEX IF NOT EXISTS idx_runs_idempotency ON runs(command_id, idempotency_key)",
                "CREATE INDEX IF NOT EXISTS idx_run_jobs_idempotency ON run_jobs(command_id, idempotency_key)",
            ]:
                with suppress(duckdb.Error):
                    self._conn.execute(statement)

    def _migrate_schema(self) -> None:
        line_columns = {row[1] for row in self._conn.execute("PRAGMA table_info('lines')").fetchall()}
        if "count_line" not in line_columns:
            self._conn.execute("ALTER TABLE lines ADD COLUMN count_line BOOLEAN DEFAULT true")
        for table in ("snapshots", "files"):
            columns = {row[1] for row in self._conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
            for name, definition in (
                ("total_regions", "INTEGER DEFAULT 0"),
                ("covered_regions", "INTEGER DEFAULT 0"),
                ("region_rate", "DOUBLE"),
            ):
                if name not in columns:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        for table, additions in (
            (
                "registered_commands",
                (
                    ("duration_estimate_ms", "INTEGER"),
                    ("duration_p90_ms", "INTEGER"),
                    ("duration_sample_count", "INTEGER DEFAULT 0"),
                    ("duration_stats_updated_at", "TIMESTAMP"),
                ),
            ),
            (
                "runs",
                (
                    ("queued_at", "TIMESTAMP"),
                    ("queue_duration_ms", "INTEGER"),
                    ("idempotency_key", "VARCHAR"),
                    ("cancellation_requested_at", "TIMESTAMP"),
                ),
            ),
            (
                "run_jobs",
                (
                    ("idempotency_key", "VARCHAR"),
                    ("cancellation_requested_at", "TIMESTAMP"),
                ),
            ),
            (
                "run_artifacts",
                (
                    ("coverage_format", "VARCHAR"),
                    ("suite", "VARCHAR"),
                    ("modified_by_run", "BOOLEAN DEFAULT false"),
                    ("ingest_status", "VARCHAR"),
                    ("snapshot_id", "VARCHAR"),
                    ("ingest_error", "VARCHAR"),
                ),
            ),
        ):
            columns = {row[1] for row in self._conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
            for name, definition in additions:
                if name not in columns:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def register_command(
        self,
        *,
        name: str,
        command: str,
        cwd: str | None = None,
        shell: str = "/bin/bash",
        artifact_paths: dict[str, Any] | None = None,
        human_approved: bool,
        approved_by: str,
        approval_note: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if not human_approved:
            raise ValueError("human_approved must be true to register a command")
        name = name.strip()
        command = command.strip()
        approved_by = approved_by.strip()
        approval_note = approval_note.strip()
        if not name:
            raise ValueError("command name is required")
        if not command:
            raise ValueError("command is required")
        if not approved_by:
            raise ValueError("approved_by is required")
        if not approval_note:
            raise ValueError("approval_note is required")
        cwd_path = Path(cwd or ".").expanduser().resolve()
        if not cwd_path.exists() or not cwd_path.is_dir():
            raise ValueError(f"cwd does not exist or is not a directory: {cwd_path}")
        git = inspect_git(cwd_path.as_posix())
        command_id = str(uuid.uuid4())
        specs = normalize_artifact_specs(artifact_paths or {})
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO registered_commands (
                    id, created_at, name, command, cwd, repo_path, repo_key, branch,
                    commit_sha, shell, approved_by, approval_note, artifact_specs, enabled,
                    duration_estimate_ms, duration_p90_ms, duration_sample_count,
                    duration_stats_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    command_id,
                    utcnow(),
                    name,
                    command,
                    cwd_path.as_posix(),
                    git.repo_path,
                    git.repo_key,
                    git.branch,
                    git.commit_sha,
                    shell,
                    approved_by,
                    approval_note,
                    json.dumps(specs),
                    enabled,
                    None,
                    None,
                    0,
                    None,
                ],
            )
        return self.registered_command(command_id)

    def registered_command(self, command_ref: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM registered_commands WHERE id = ?",
                [command_ref],
            ).fetchone()
            if row is None:
                row = self._conn.execute(
                    """
                    SELECT * FROM registered_commands
                    WHERE name = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    [command_ref],
                ).fetchone()
            if row is None:
                raise KeyError(f"registered command not found: {command_ref}")
            columns = [column[0] for column in self._conn.description]
        return self._with_topology(self._decode_json_fields(row_dict(columns, row), ["artifact_specs"]))

    def list_registered_commands(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM registered_commands
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [
            self._with_topology(self._decode_json_fields(row_dict(columns, row), ["artifact_specs"])) for row in rows
        ]

    def run_command_profiled(
        self,
        command_ref: str,
        *,
        max_summary_lines: int = 80,
        timeout_seconds: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        job = self.submit_command_profiled(
            command_ref,
            max_summary_lines=max_summary_lines,
            timeout_seconds=timeout_seconds,
            idempotency_key=idempotency_key,
        )
        result = self.wait_for_run(job["id"], max_summary_lines=max_summary_lines)
        result["submission_reused"] = job["submission_reused"]
        return result

    def submit_command_profiled(
        self,
        command_ref: str,
        *,
        max_summary_lines: int = 80,
        timeout_seconds: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        validate_run_limits(max_summary_lines=max_summary_lines, timeout_seconds=timeout_seconds)
        if self._closing.is_set():
            raise RuntimeError("coverage store is shutting down")
        registered = self.registered_command(command_ref)
        if not registered.get("enabled", True):
            raise ValueError(f"registered command is disabled: {command_ref}")
        if idempotency_key is not None:
            idempotency_key = idempotency_key.strip()
            if not idempotency_key:
                raise ValueError("idempotency_key must not be blank")
            if len(idempotency_key) > 200:
                raise ValueError("idempotency_key must not exceed 200 characters")
            existing_run_id = self._idempotent_run_id(registered["id"], idempotency_key)
            if existing_run_id is not None:
                result = self.run_result(existing_run_id, max_summary_lines=max_summary_lines)
                result["submission_reused"] = True
                return result

        run_id = str(uuid.uuid4())
        run_path = self.run_dir / run_id
        run_path.mkdir(parents=True, exist_ok=True)
        stdout_path = run_path / "stdout.log"
        stderr_path = run_path / "stderr.log"
        cwd = registered["cwd"]
        git = inspect_git(cwd)
        queued_at = utcnow()
        stdout_path.touch()
        stderr_path.touch()
        with self._lock:
            existing_run_id = self._idempotent_run_id(registered["id"], idempotency_key)
            if existing_run_id is not None:
                self._remove_managed_run_logs([run_id])
                result = self.run_result(existing_run_id, max_summary_lines=max_summary_lines)
                result["submission_reused"] = True
                return result
            self._conn.execute(
                """
                INSERT INTO run_jobs (
                    id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key,
                    branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds,
                    max_summary_lines, status, stdout_path, stderr_path, error,
                    cancellation_requested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    registered["id"],
                    registered["name"],
                    registered["command"],
                    idempotency_key,
                    cwd,
                    git.repo_path,
                    git.repo_key,
                    git.branch,
                    git.commit_sha,
                    queued_at,
                    None,
                    None,
                    timeout_seconds,
                    max(max_summary_lines, 1),
                    "queued",
                    stdout_path.as_posix(),
                    stderr_path.as_posix(),
                    "",
                    None,
                ],
            )
        self._cancel_event(run_id)
        self._job_queue.put(run_id)
        result = self.run_result(run_id, max_summary_lines=max_summary_lines)
        result["submission_reused"] = False
        return result

    def _idempotent_run_id(self, command_id: str, idempotency_key: str | None) -> str | None:
        if idempotency_key is None:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id FROM run_jobs
                WHERE command_id = ? AND idempotency_key = ?
                UNION ALL
                SELECT id FROM runs
                WHERE command_id = ? AND idempotency_key = ?
                LIMIT 1
                """,
                [command_id, idempotency_key, command_id, idempotency_key],
            ).fetchone()
        return str(row[0]) if row is not None else None

    def wait_for_run(self, run_id: str, *, max_summary_lines: int = 80) -> dict[str, Any]:
        while True:
            result = self.run_result(run_id, max_summary_lines=max_summary_lines)
            if result["terminal"]:
                if result["status"] == "internal_error":
                    raise RuntimeError(result.get("error") or "run worker failed")
                return result
            time.sleep(0.02)

    def cancel_run(self, run_id: str, *, max_summary_lines: int = 80) -> dict[str, Any]:
        validate_run_limits(max_summary_lines=max_summary_lines)
        requested_at = utcnow()
        command_id: str | None = None
        with self._lock:
            row = self._conn.execute("SELECT status, command_id FROM run_jobs WHERE id = ?", [run_id]).fetchone()
            if row is None:
                completed = self._conn.execute("SELECT status FROM runs WHERE id = ?", [run_id]).fetchone()
                if completed is None:
                    raise KeyError(f"run not found: {run_id}")
                if completed[0] == "cancelled":
                    return self.run_result(run_id, max_summary_lines=max_summary_lines)
                raise ValueError(f"run is already terminal: {completed[0]}")

            status, command_id = str(row[0]), str(row[1])
            if status == "cancelled":
                return self.run_result(run_id, max_summary_lines=max_summary_lines)
            if status not in {"queued", "running"}:
                raise ValueError(f"run is already terminal: {status}")
            if status == "queued":
                self._conn.execute(
                    """
                    UPDATE run_jobs
                    SET status = 'cancelled', ended_at = ?, cancellation_requested_at = ?,
                        error = 'Run cancelled before execution.'
                    WHERE id = ? AND status = 'queued'
                    """,
                    [requested_at, requested_at, run_id],
                )
            else:
                self._conn.execute(
                    """
                    UPDATE run_jobs
                    SET cancellation_requested_at = ?, error = 'Cancellation requested.'
                    WHERE id = ? AND status = 'running'
                    """,
                    [requested_at, run_id],
                )

        cancel_event = self._cancel_event(run_id)
        cancel_event.set()
        with self._process_lock:
            process = self._active_processes.get(run_id)
        if process is not None:
            self._signal_process_group(process, signal.SIGTERM)
        if status == "queued" and command_id is not None:
            self._prune_run_history(command_id)
        return self.run_result(run_id, max_summary_lines=max_summary_lines)

    def list_run_queue(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM run_jobs
                WHERE status IN ('queued', 'running')
                ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, queued_at
                LIMIT ?
                """,
                [min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._job_from_row(columns, row) for row in rows]

    def _run_worker(self) -> None:
        while True:
            run_id = self._job_queue.get()
            try:
                if run_id is None:
                    return
                if self._closing.is_set():
                    continue
                try:
                    self._execute_run_job(run_id)
                except Exception as exc:
                    self._mark_job_error(run_id, exc)
            finally:
                if run_id is not None:
                    self._forget_runtime_state(run_id)
                self._job_queue.task_done()

    def _execute_run_job(self, run_id: str) -> None:
        with self._run_lock:
            job = self._run_job(run_id)
            if job is None or job["status"] != "queued":
                return
            registered = self.registered_command(job["command_id"])
            started_at = utcnow()
            with self._lock:
                current = self._conn.execute("SELECT status FROM run_jobs WHERE id = ?", [run_id]).fetchone()
                if current is None or current[0] != "queued":
                    return
                self._conn.execute(
                    "UPDATE run_jobs SET status = 'running', started_at = ?, error = '' WHERE id = ?",
                    [started_at, run_id],
                )

            start = time.monotonic()
            exit_code: int | None = None
            status = "failed"
            error = ""
            timed_out = False
            stdout_path = Path(job["stdout_path"])
            stderr_path = Path(job["stderr_path"])
            cancel_event = self._cancel_event(run_id)
            process: subprocess.Popen[str] | None = None
            artifact_states_before = self._artifact_file_states(registered["artifact_specs"], job["cwd"])
            try:
                with (
                    stdout_path.open("w", encoding="utf-8", errors="replace") as stdout,
                    stderr_path.open("w", encoding="utf-8", errors="replace") as stderr,
                ):
                    process = subprocess.Popen(
                        job["command"],
                        shell=True,
                        cwd=job["cwd"],
                        executable=registered["shell"],
                        stdout=stdout,
                        stderr=stderr,
                        text=True,
                        start_new_session=True,
                    )
                    self._register_process(run_id, process)
                    exit_code, timed_out = self._wait_for_process(
                        process,
                        cancel_event=cancel_event,
                        timeout_seconds=job["timeout_seconds"],
                    )
                if timed_out:
                    status = "timeout"
                    with stderr_path.open("a", encoding="utf-8", errors="replace") as stderr:
                        stderr.write(f"\nCommand timed out after {job['timeout_seconds']} seconds.\n")
                elif cancel_event.is_set():
                    status = "cancelled"
                    error = "Run cancelled by request."
                    with stderr_path.open("a", encoding="utf-8", errors="replace") as stderr:
                        stderr.write("\nRun cancelled by request.\n")
                else:
                    status = "passed" if exit_code == 0 else "failed"
            except Exception as exc:
                status = "failed"
                error = f"{type(exc).__name__}: {exc}"
                with stderr_path.open("a", encoding="utf-8", errors="replace") as stderr:
                    stderr.write(f"\nCommand execution failed: {error}\n")
            finally:
                if process is not None and process.poll() is None:
                    with suppress(OSError):
                        self._signal_process_group(process, signal.SIGKILL)
                    with suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=0.5)
                self._unregister_process(run_id, process)

            ended_at = utcnow()
            duration_ms = int((time.monotonic() - start) * 1000)
            latest_job = self._run_job(run_id)
            if latest_job is not None:
                job = latest_job
            self._finalize_run_job(
                job=job,
                registered=registered,
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=duration_ms,
                exit_code=exit_code,
                status=status,
                error=error,
                artifact_states_before=artifact_states_before,
            )

    def _cancel_event(self, run_id: str) -> threading.Event:
        with self._process_lock:
            return self._cancel_events.setdefault(run_id, threading.Event())

    def _register_process(self, run_id: str, process: subprocess.Popen[str]) -> None:
        with self._process_lock:
            self._active_processes[run_id] = process

    def _unregister_process(self, run_id: str, process: subprocess.Popen[str] | None) -> None:
        with self._process_lock:
            if process is not None and self._active_processes.get(run_id) is process:
                self._active_processes.pop(run_id, None)

    def _forget_runtime_state(self, run_id: str) -> None:
        with self._process_lock:
            self._active_processes.pop(run_id, None)
            self._cancel_events.pop(run_id, None)

    def _wait_for_process(
        self,
        process: subprocess.Popen[str],
        *,
        cancel_event: threading.Event,
        timeout_seconds: int | None,
    ) -> tuple[int | None, bool]:
        started = time.monotonic()
        termination_started: float | None = None
        timed_out = False
        while True:
            return_code = process.poll()
            if return_code is not None:
                if termination_started is not None:
                    self._signal_process_group(process, signal.SIGKILL)
                return return_code, timed_out

            now = time.monotonic()
            if termination_started is None and cancel_event.is_set():
                self._signal_process_group(process, signal.SIGTERM)
                termination_started = now
            elif termination_started is None and timeout_seconds is not None and now - started >= timeout_seconds:
                timed_out = True
                self._signal_process_group(process, signal.SIGTERM)
                termination_started = now
            elif termination_started is not None and now - termination_started >= 2:
                self._signal_process_group(process, signal.SIGKILL)
            time.sleep(0.02)

    def _signal_process_group(self, process: subprocess.Popen[str], selected_signal: signal.Signals) -> None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, selected_signal)

    def _finalize_run_job(
        self,
        *,
        job: dict[str, Any],
        registered: dict[str, Any],
        started_at: datetime,
        ended_at: datetime,
        duration_ms: int,
        exit_code: int | None,
        status: str,
        error: str,
        artifact_states_before: dict[str, tuple[Any, ...] | None],
    ) -> None:
        run_id = job["id"]
        stdout_path = Path(job["stdout_path"])
        stderr_path = Path(job["stderr_path"])
        artifact_paths = self._collect_run_artifacts(
            registered["artifact_specs"],
            job["cwd"],
            previous_states=artifact_states_before,
        )
        self._auto_ingest_run_artifacts(
            artifact_paths,
            job=job,
            command_name=registered["name"],
            eligible=status in {"passed", "failed"} and exit_code is not None,
        )
        summary = summarize_run_logs(
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            exit_code=exit_code,
            status=status,
            duration_ms=duration_ms,
            max_summary_lines=job["max_summary_lines"],
        )
        if error:
            summary["execution_error"] = error
        queued_at = parse_datetime(job["queued_at"])
        queue_duration_ms = int((started_at - (queued_at or started_at)).total_seconds() * 1000)
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs (
                        id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key,
                        branch, commit_sha, started_at, ended_at, duration_ms, exit_code,
                        status, stdout_path, stderr_path, parsed_summary, artifact_paths,
                        queued_at, queue_duration_ms, cancellation_requested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        job["id"],
                        job["command_id"],
                        job["command_name"],
                        job["command"],
                        job["idempotency_key"],
                        job["cwd"],
                        job["repo_path"],
                        job["repo_key"],
                        job["branch"],
                        job["commit_sha"],
                        started_at,
                        ended_at,
                        duration_ms,
                        exit_code,
                        status,
                        stdout_path.as_posix(),
                        stderr_path.as_posix(),
                        json.dumps(summary),
                        json.dumps(artifact_paths),
                        queued_at,
                        max(queue_duration_ms, 0),
                        parse_datetime(job.get("cancellation_requested_at")),
                    ],
                )
                if artifact_paths:
                    self._conn.executemany(
                        """
                        INSERT INTO run_artifacts (
                            run_id, kind, path, exists, size_bytes, coverage_format, suite,
                            modified_by_run, ingest_status, snapshot_id, ingest_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            [
                                run_id,
                                artifact["kind"],
                                artifact["path"],
                                artifact["exists"],
                                artifact["size_bytes"],
                                artifact["coverage_format"],
                                artifact["suite"],
                                artifact["modified_by_run"],
                                artifact["ingest_status"],
                                artifact["snapshot_id"],
                                artifact["ingest_error"],
                            ]
                            for artifact in artifact_paths
                        ],
                    )
                self._conn.execute("DELETE FROM run_jobs WHERE id = ?", [run_id])
                if status in {"passed", "failed"} and exit_code is not None:
                    self._refresh_command_duration_stats(job["command_id"])
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self._prune_run_history(job["command_id"])

    def _refresh_all_command_duration_stats(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                WITH ranked_runs AS (
                    SELECT command_id, duration_ms, ended_at,
                           row_number() OVER (
                               PARTITION BY command_id
                               ORDER BY ended_at DESC, id DESC
                           ) AS recency
                    FROM runs
                    WHERE status IN ('passed', 'failed')
                      AND exit_code IS NOT NULL
                ), duration_stats AS (
                    SELECT command_id, median(duration_ms) AS estimate_ms,
                           quantile_cont(duration_ms, 0.9) AS p90_ms,
                           count(*) AS sample_count, max(ended_at) AS stats_updated_at
                    FROM ranked_runs
                    WHERE recency <= ?
                    GROUP BY command_id
                )
                UPDATE registered_commands AS commands
                SET duration_estimate_ms = round(stats.estimate_ms),
                    duration_p90_ms = round(stats.p90_ms),
                    duration_sample_count = stats.sample_count,
                    duration_stats_updated_at = stats.stats_updated_at
                FROM duration_stats AS stats
                WHERE commands.id = stats.command_id
                """,
                [COMMAND_DURATION_SAMPLE_LIMIT],
            )

    def _refresh_command_duration_stats(self, command_id: str) -> None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT median(duration_ms), quantile_cont(duration_ms, 0.9), count(*), max(ended_at)
                FROM (
                    SELECT duration_ms, ended_at
                    FROM runs
                    WHERE command_id = ?
                      AND status IN ('passed', 'failed')
                      AND exit_code IS NOT NULL
                    ORDER BY ended_at DESC, id DESC
                    LIMIT ?
                ) recent_runs
                """,
                [command_id, COMMAND_DURATION_SAMPLE_LIMIT],
            ).fetchone()
            if row is None or not row[2]:
                estimate_ms = None
                p90_ms = None
                sample_count = 0
                stats_updated_at = None
            else:
                estimate_ms = round(float(row[0]))
                p90_ms = round(float(row[1]))
                sample_count = int(row[2])
                stats_updated_at = row[3]
            self._conn.execute(
                """
                UPDATE registered_commands
                SET duration_estimate_ms = ?, duration_p90_ms = ?,
                    duration_sample_count = ?, duration_stats_updated_at = ?
                WHERE id = ?
                """,
                [
                    estimate_ms,
                    p90_ms,
                    sample_count,
                    stats_updated_at,
                    command_id,
                ],
            )

    def _mark_job_error(self, run_id: str, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        ended_at = utcnow()
        job = None
        with suppress(OSError):
            job = self._run_job(run_id)
            if job is not None:
                with Path(job["stderr_path"]).open("a", encoding="utf-8", errors="replace") as stderr:
                    stderr.write(f"\nRun worker failed: {message}\n")
        with self._lock:
            self._conn.execute(
                """
                UPDATE run_jobs
                SET status = 'internal_error', ended_at = ?, error = ?
                WHERE id = ?
                """,
                [ended_at, message, run_id],
            )
        if job is not None:
            self._prune_run_history(job["command_id"])

    def _recover_run_jobs(self) -> list[str]:
        recovered_at = utcnow()
        with self._lock:
            self._conn.execute(
                """
                UPDATE run_jobs
                SET status = 'interrupted', ended_at = ?,
                    error = 'Coverage MCP restarted while this command was running.'
                WHERE status = 'running'
                """,
                [recovered_at],
            )
            rows = self._conn.execute("SELECT id FROM run_jobs WHERE status = 'queued' ORDER BY queued_at").fetchall()
        return [str(row[0]) for row in rows]

    def _prune_run_history(self, command_id: str | None = None) -> int:
        if command_id is None:
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT command_id FROM runs
                    UNION
                    SELECT command_id FROM run_jobs
                    WHERE status NOT IN ('queued', 'running')
                    """
                ).fetchall()
            return sum(self._prune_run_history(str(row[0])) for row in rows)

        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, record_kind
                FROM (
                    SELECT id, 'run' AS record_kind, ended_at AS terminal_at
                    FROM runs
                    WHERE command_id = ?
                    UNION ALL
                    SELECT id, 'job' AS record_kind, COALESCE(ended_at, queued_at) AS terminal_at
                    FROM run_jobs
                    WHERE command_id = ? AND status NOT IN ('queued', 'running')
                ) terminal_runs
                ORDER BY terminal_at DESC, id DESC
                """,
                [command_id, command_id],
            ).fetchall()
            stale = [(str(row[0]), str(row[1])) for row in rows[self.run_retention :]]
            if not stale:
                return 0

            run_ids = [[run_id] for run_id, kind in stale if kind == "run"]
            job_ids = [[run_id] for run_id, kind in stale if kind == "job"]
            self._conn.execute("BEGIN")
            try:
                if run_ids:
                    self._conn.executemany("DELETE FROM run_artifacts WHERE run_id = ?", run_ids)
                    self._conn.executemany("DELETE FROM runs WHERE id = ?", run_ids)
                if job_ids:
                    self._conn.executemany("DELETE FROM run_jobs WHERE id = ?", job_ids)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        self._remove_managed_run_logs([run_id for run_id, _kind in stale])
        return len(stale)

    def _remove_managed_run_logs(self, run_ids: list[str]) -> None:
        root = self.run_dir.resolve()
        for run_id in run_ids:
            run_path = (root / run_id).resolve()
            if run_path.parent != root:
                continue
            for name in ("stdout.log", "stderr.log"):
                with suppress(OSError):
                    (run_path / name).unlink()
            with suppress(OSError):
                run_path.rmdir()

    def run_result(self, run_id: str, *, max_summary_lines: int = 80) -> dict[str, Any]:
        validate_run_limits(max_summary_lines=max_summary_lines)
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE id = ?", [run_id]).fetchone()
            if row is not None:
                columns = [column[0] for column in self._conn.description]
                run = self._run_from_row(columns, row)
            else:
                row = self._conn.execute("SELECT * FROM run_jobs WHERE id = ?", [run_id]).fetchone()
                if row is None:
                    raise KeyError(f"run not found: {run_id}")
                columns = [column[0] for column in self._conn.description]
                job = self._serialize(row_dict(columns, row))
                run = None
        if run is None:
            return self._job_response(job, max_summary_lines=max_summary_lines)
        summary = dict(run["parsed_summary"])
        excerpts = summary.get("excerpts")
        if isinstance(excerpts, list):
            limit = max(max_summary_lines, 1)
            summary["excerpts"] = excerpts[:limit]
            summary["truncated"] = bool(summary.get("truncated") or len(excerpts) > limit)
        run["parsed_summary"] = summary
        run["terminal"] = True
        run["poll_after_ms"] = None
        return run

    def run(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE id = ?", [run_id]).fetchone()
            if row is None:
                raise KeyError(f"run not found: {run_id}")
            columns = [column[0] for column in self._conn.description]
        return self._run_from_row(columns, row)

    def latest_run(self, command_ref: str | None = None) -> dict[str, Any] | None:
        job = self._latest_run_job(command_ref)
        args: list[Any] = []
        where = ""
        if command_ref:
            try:
                command = self.registered_command(command_ref)
            except KeyError:
                command = None
            if command:
                where = "WHERE command_id = ?"
                args.append(command["id"])
            else:
                where = "WHERE command_name = ?"
                args.append(command_ref)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT * FROM runs
                {where}
                ORDER BY started_at DESC
                LIMIT 1
                """,
                args,
            ).fetchone()
            if row is None:
                return job
            columns = [column[0] for column in self._conn.description]
        completed = self._run_from_row(columns, row)
        if job is not None and str(job["queued_at"]) > str(completed["started_at"]):
            return job
        return completed

    def _latest_run_job(self, command_ref: str | None) -> dict[str, Any] | None:
        args: list[Any] = []
        where = ""
        if command_ref:
            try:
                command = self.registered_command(command_ref)
            except KeyError:
                command = None
            if command:
                where = "WHERE command_id = ?"
                args.append(command["id"])
            else:
                where = "WHERE command_name = ?"
                args.append(command_ref)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT * FROM run_jobs
                {where}
                ORDER BY queued_at DESC
                LIMIT 1
                """,
                args,
            ).fetchone()
            if row is None:
                return None
            columns = [column[0] for column in self._conn.description]
        return self._job_from_row(columns, row)

    def _run_job(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM run_jobs WHERE id = ?", [run_id]).fetchone()
            if row is None:
                return None
            columns = [column[0] for column in self._conn.description]
        return self._serialize(row_dict(columns, row))

    def _job_from_row(self, columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        return self._job_response(self._serialize(row_dict(columns, row)))

    def _command_duration_stats(self, command_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT duration_estimate_ms, duration_p90_ms, duration_sample_count,
                       duration_stats_updated_at
                FROM registered_commands
                WHERE id = ?
                """,
                [command_id],
            ).fetchone()
        if row is None:
            return {
                "duration_estimate_ms": None,
                "duration_p90_ms": None,
                "duration_sample_count": 0,
                "duration_stats_updated_at": None,
            }
        return self._serialize(
            {
                "duration_estimate_ms": row[0],
                "duration_p90_ms": row[1],
                "duration_sample_count": row[2],
                "duration_stats_updated_at": row[3],
            }
        )

    def _queue_wait_estimate_ms(self, run_id: str, now: datetime) -> tuple[int | None, str | None]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT j.id, j.status, j.started_at, c.duration_estimate_ms
                FROM run_jobs j
                JOIN registered_commands c ON c.id = j.command_id
                WHERE j.status IN ('running', 'queued')
                ORDER BY CASE j.status WHEN 'running' THEN 0 ELSE 1 END,
                         j.queued_at, j.id
                """
            ).fetchall()
        wait_ms = 0
        history_missing = False
        for queued_id, status, started_at, estimate_ms in rows:
            if str(queued_id) == run_id:
                if history_missing:
                    return None, "queue_history_incomplete"
                return wait_ms, None
            if estimate_ms is None:
                history_missing = True
                continue
            remaining_ms = int(estimate_ms)
            if status == "running":
                started = parse_datetime(started_at)
                elapsed_ms = int((now - (started or now)).total_seconds() * 1000)
                remaining_ms = max(remaining_ms - elapsed_ms, 0)
            wait_ms += remaining_ms
        return None, "run_state_changed"

    def _job_eta_fields(
        self,
        job: dict[str, Any],
        *,
        status: str,
        now: datetime,
        duration_ms: int,
    ) -> dict[str, Any]:
        stats = self._command_duration_stats(str(job["command_id"]))
        fields: dict[str, Any] = {
            **stats,
            "duration_estimate_window": COMMAND_DURATION_SAMPLE_LIMIT,
            "eta_seconds": None,
            "eta": None,
            "estimated_start_at": None,
            "estimated_completion_at": None,
            "queue_wait_estimate_seconds": None,
            "estimate_overrun_seconds": 0,
            "eta_unavailable_reason": None,
        }
        if status not in {"queued", "running"}:
            fields.update(
                {
                    "eta_seconds": 0,
                    "eta": format_duration(0),
                    "estimated_start_at": job.get("started_at"),
                    "estimated_completion_at": job.get("ended_at"),
                    "queue_wait_estimate_seconds": 0,
                }
            )
            return fields

        estimate_ms = stats["duration_estimate_ms"]
        if status == "running":
            fields["estimated_start_at"] = job.get("started_at")
            fields["queue_wait_estimate_seconds"] = 0
            if estimate_ms is None:
                fields["eta_unavailable_reason"] = "no_command_history"
                return fields
            remaining_ms = max(int(estimate_ms) - duration_ms, 0)
            eta_seconds = milliseconds_to_seconds(remaining_ms)
            started_at = parse_datetime(job.get("started_at")) or now
            fields.update(
                {
                    "eta_seconds": eta_seconds,
                    "eta": format_duration(eta_seconds),
                    "estimated_completion_at": serialize_datetime(
                        started_at + timedelta(milliseconds=int(estimate_ms))
                    ),
                    "estimate_overrun_seconds": milliseconds_to_seconds(max(duration_ms - int(estimate_ms), 0)),
                }
            )
            return fields

        queue_wait_ms, queue_error = self._queue_wait_estimate_ms(str(job["id"]), now)
        if queue_wait_ms is not None:
            fields["queue_wait_estimate_seconds"] = milliseconds_to_seconds(queue_wait_ms)
            estimated_start = now + timedelta(milliseconds=queue_wait_ms)
            fields["estimated_start_at"] = serialize_datetime(estimated_start)
        if estimate_ms is None:
            fields["eta_unavailable_reason"] = "no_command_history"
            return fields
        if queue_wait_ms is None:
            fields["eta_unavailable_reason"] = queue_error
            return fields
        eta_ms = queue_wait_ms + int(estimate_ms)
        eta_seconds = milliseconds_to_seconds(eta_ms)
        fields.update(
            {
                "eta_seconds": eta_seconds,
                "eta": format_duration(eta_seconds),
                "estimated_completion_at": serialize_datetime(now + timedelta(milliseconds=eta_ms)),
            }
        )
        return fields

    def _job_response(self, job: dict[str, Any], *, max_summary_lines: int | None = None) -> dict[str, Any]:
        status = str(job["status"])
        terminal = status not in {"queued", "running"}
        started_at = parse_datetime(job.get("started_at"))
        queued_at = parse_datetime(job.get("queued_at"))
        ended_at = parse_datetime(job.get("ended_at"))
        now = utcnow()
        duration_ms = int(((ended_at or now) - (started_at or queued_at or now)).total_seconds() * 1000)
        result = {
            **job,
            "duration_ms": max(duration_ms, 0),
            "exit_code": None,
            "artifact_paths": [],
            "coverage_ingest": self._job_coverage_ingest(job, terminal=terminal),
            "terminal": terminal,
            "poll_after_ms": None if terminal else 1000,
            "execution_mode": "background",
            "cancellation_requested": job.get("cancellation_requested_at") is not None,
        }
        result.update(self._job_eta_fields(job, status=status, now=now, duration_ms=result["duration_ms"]))
        if terminal:
            result["parsed_summary"] = summarize_run_logs(
                stdout_path=Path(job["stdout_path"]),
                stderr_path=Path(job["stderr_path"]),
                exit_code=None,
                status=status,
                duration_ms=result["duration_ms"],
                max_summary_lines=max(max_summary_lines or job["max_summary_lines"], 1),
            )
        else:
            result["parsed_summary"] = {
                "status": status,
                "exit_code": None,
                "duration_ms": result["duration_ms"],
                "stdout_line_count": None,
                "stderr_line_count": None,
                "counters": {},
                "excerpts": [],
                "truncated": False,
                "stdout_path": job["stdout_path"],
                "stderr_path": job["stderr_path"],
                "summary_deferred": True,
            }
        if status == "queued":
            with self._lock:
                position = self._conn.execute(
                    """
                    SELECT count(*) FROM run_jobs
                    WHERE status = 'queued'
                      AND (queued_at < ? OR (queued_at = ? AND id <= ?))
                    """,
                    [job["queued_at"], job["queued_at"], job["id"]],
                ).fetchone()
            result["queue_position"] = int(position[0]) if position else 1
            self._with_relative_age(result, "queued_at", prefix="queued")
        elif status == "running":
            result["queue_position"] = 0
            self._with_relative_age(result, "started_at", prefix="running")
        else:
            result["queue_position"] = None
            self._with_relative_age(result, "ended_at")
        return self._with_topology(result)

    def _job_coverage_ingest(self, job: dict[str, Any], *, terminal: bool) -> dict[str, Any]:
        registered = self.registered_command(str(job["command_id"]))
        artifacts = [
            {
                "coverage_format": spec.get("coverage_format"),
                "ingest_status": "skipped_run_status" if terminal else None,
                "snapshot_id": None,
            }
            for spec in registered["artifact_specs"]
            if spec.get("coverage_format")
        ]
        return summarize_coverage_ingest(artifacts)

    def latest_artifact(self, *, command_ref: str | None = None, kind: str) -> dict[str, Any] | None:
        args: list[Any] = [kind]
        command_filter = ""
        if command_ref:
            try:
                command = self.registered_command(command_ref)
            except KeyError:
                command = None
            if command:
                command_filter = "AND r.command_id = ?"
                args.append(command["id"])
            else:
                command_filter = "AND r.command_name = ?"
                args.append(command_ref)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT a.run_id, a.kind, a.path, a.exists, a.size_bytes,
                       a.coverage_format, a.suite, a.modified_by_run,
                       a.ingest_status, a.snapshot_id, a.ingest_error,
                       r.command_id, r.command_name, r.repo_key, r.repo_path,
                       r.started_at, r.ended_at, r.status, r.exit_code
                FROM run_artifacts a
                JOIN runs r ON r.id = a.run_id
                WHERE a.kind = ?
                  {command_filter}
                ORDER BY r.started_at DESC
                LIMIT 1
                """,
                args,
            ).fetchone()
            if row is None:
                return None
            columns = [column[0] for column in self._conn.description]
        return self._with_relative_age(
            self._with_topology(self._serialize(row_dict(columns, row))),
            "ended_at",
            prefix="run",
        )

    def object_topology(self, object_kind: str, object_ref: str) -> dict[str, Any]:
        kind = object_kind.strip().lower().replace("-", "_")
        if kind in {"project", "repo", "repository"}:
            for project in self.projects(limit=1000):
                if object_ref in {project.get("repo_key"), project.get("repo_path")}:
                    return {"object_kind": "project", "object_ref": object_ref, "topology": project["topology"]}
            raise KeyError(f"project not found: {object_ref}")
        if kind in {"command", "registered_command", "test_command"}:
            command = self.registered_command(object_ref)
            return {"object_kind": "registered_command", "object_ref": object_ref, "topology": command["topology"]}
        if kind == "run":
            run = self.run_result(object_ref)
            return {"object_kind": "run", "object_ref": object_ref, "topology": run["topology"]}
        if kind in {"snapshot", "coverage_snapshot"}:
            snapshot = self.snapshot(object_ref)
            return {"object_kind": "coverage_snapshot", "object_ref": object_ref, "topology": snapshot["topology"]}
        if kind == "worktree":
            worktree = self.worktree(object_ref)
            return {"object_kind": "worktree", "object_ref": object_ref, "topology": worktree["topology"]}
        raise ValueError(f"unsupported topology object kind: {object_kind}")

    def _artifact_file_states(
        self,
        artifact_specs: list[dict[str, Any]],
        cwd: str,
    ) -> dict[str, tuple[Any, ...] | None]:
        states: dict[str, tuple[Any, ...] | None] = {}
        for spec in artifact_specs:
            path = self._artifact_path(spec, cwd)
            if path is not None:
                states[str(spec["kind"])] = self._artifact_file_state(path)
        return states

    @staticmethod
    def _artifact_path(spec: dict[str, Any], cwd: str) -> Path | None:
        raw_path = str(spec.get("path", "")).strip()
        if not raw_path:
            return None
        path = Path(raw_path).expanduser()
        return path if path.is_absolute() else Path(cwd) / path

    @staticmethod
    def _artifact_file_state(path: Path) -> tuple[Any, ...] | None:
        try:
            if not path.exists():
                return None
            if not path.is_file():
                return ("not_file",)
            metadata = path.stat()
        except OSError:
            return None
        return ("file", metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns, metadata.st_ino)

    def _collect_run_artifacts(
        self,
        artifact_specs: list[dict[str, Any]],
        cwd: str,
        *,
        previous_states: dict[str, tuple[Any, ...] | None] | None = None,
    ) -> list[dict[str, Any]]:
        artifacts: list[dict[str, Any]] = []
        for spec in artifact_specs:
            path = self._artifact_path(spec, cwd)
            if path is None:
                continue
            kind = str(spec["kind"])
            state = self._artifact_file_state(path)
            size_bytes = state[1] if state is not None and state[0] == "file" else None
            modified_by_run = previous_states is not None and state is not None and state != previous_states.get(kind)
            artifacts.append(
                {
                    "kind": kind,
                    "path": path.as_posix(),
                    "exists": state is not None,
                    "size_bytes": size_bytes,
                    "required": bool(spec.get("required", False)),
                    "coverage_format": spec.get("coverage_format"),
                    "suite": spec.get("suite"),
                    "modified_by_run": modified_by_run,
                    "ingest_status": None,
                    "snapshot_id": None,
                    "ingest_error": None,
                }
            )
        return artifacts

    def _auto_ingest_run_artifacts(
        self,
        artifacts: list[dict[str, Any]],
        *,
        job: dict[str, Any],
        command_name: str,
        eligible: bool,
    ) -> None:
        for artifact in artifacts:
            coverage_format = artifact.get("coverage_format")
            if not coverage_format:
                continue
            artifact["suite"] = artifact.get("suite") or command_name
            if not eligible:
                artifact["ingest_status"] = "skipped_run_status"
                artifact["ingest_error"] = "run did not complete with a process exit code"
                continue
            if not artifact["exists"]:
                artifact["ingest_status"] = "missing"
                artifact["ingest_error"] = "coverage artifact does not exist"
                continue
            if artifact["size_bytes"] is None:
                artifact["ingest_status"] = "failed"
                artifact["ingest_error"] = "coverage artifact is not a regular file"
                continue
            if not artifact["modified_by_run"]:
                artifact["ingest_status"] = "skipped_stale"
                artifact["ingest_error"] = "coverage artifact was not created or modified by this run"
                continue
            try:
                snapshot = self.ingest_report(
                    artifact["path"],
                    format=str(coverage_format),
                    repo_path=job["repo_path"],
                    branch=job.get("branch"),
                    commit_sha=job.get("commit_sha"),
                    suite=str(artifact["suite"]),
                )
            except Exception as exc:
                artifact["ingest_status"] = "failed"
                artifact["ingest_error"] = f"{type(exc).__name__}: {exc}"[:1000]
            else:
                artifact["ingest_status"] = "ingested"
                artifact["snapshot_id"] = snapshot["id"]

    def ingest_report(
        self,
        report_path: str,
        *,
        format: str = "auto",
        repo_path: str | None = None,
        branch: str | None = None,
        commit_sha: str | None = None,
        base_ref: str | None = None,
        suite: str = "default",
    ) -> dict[str, Any]:
        git = inspect_git(repo_path or str(Path(report_path).expanduser().parent))
        selected_repo_path = repo_path or git.repo_path
        git_for_repo = inspect_git(selected_repo_path)
        branch = branch or git_for_repo.branch
        commit_sha = commit_sha or git_for_repo.commit_sha
        report = parse_coverage_report(report_path, format=format, repo_path=selected_repo_path)
        snapshot_id = self.store_report(
            report,
            repo_path=git_for_repo.repo_path,
            repo_key=git_for_repo.repo_key,
            branch=branch,
            commit_sha=commit_sha,
            base_ref=base_ref,
            suite=suite,
        )
        return self.snapshot(snapshot_id)

    def store_report(
        self,
        report: CoverageReport,
        *,
        repo_path: str,
        repo_key: str,
        branch: str | None,
        commit_sha: str | None,
        base_ref: str | None,
        suite: str,
    ) -> str:
        snapshot_id = str(uuid.uuid4())
        now = utcnow()
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    """
                    INSERT INTO snapshots (
                        id, created_at, minute_bucket, repo_path, repo_key, branch, commit_sha, base_ref,
                        suite, format, report_path, warnings, metadata,
                        total_lines, covered_lines, total_branches, covered_branches,
                        total_functions, covered_functions, total_regions, covered_regions,
                        line_rate, branch_rate, function_rate, region_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        snapshot_id,
                        now,
                        minute_bucket(now),
                        repo_path,
                        repo_key,
                        branch,
                        commit_sha,
                        base_ref,
                        suite,
                        report.format,
                        report.report_path,
                        json.dumps(report.warnings),
                        json.dumps(report.metadata),
                        report.total_lines,
                        report.covered_lines,
                        report.total_branches,
                        report.covered_branches,
                        report.total_functions,
                        report.covered_functions,
                        report.total_regions,
                        report.covered_regions,
                        report.line_rate,
                        report.branch_rate,
                        report.function_rate,
                        report.region_rate,
                    ],
                )
                if report.files:
                    self._copy_rows(
                        "files",
                        [
                            "snapshot_id",
                            "file_path",
                            "total_lines",
                            "covered_lines",
                            "total_branches",
                            "covered_branches",
                            "total_functions",
                            "covered_functions",
                            "total_regions",
                            "covered_regions",
                            "line_rate",
                            "branch_rate",
                            "function_rate",
                            "region_rate",
                            "raw_metrics",
                        ],
                        [
                            [
                                snapshot_id,
                                file.file_path,
                                file.total_lines,
                                file.covered_lines,
                                file.total_branches,
                                file.covered_branches,
                                file.total_functions,
                                file.covered_functions,
                                file.total_regions,
                                file.covered_regions,
                                file.line_rate,
                                file.branch_rate,
                                file.function_rate,
                                file.region_rate,
                                json.dumps(file.raw_metrics),
                            ]
                            for file in report.files
                        ],
                    )
                if report.lines:
                    self._copy_rows(
                        "lines",
                        [
                            "snapshot_id",
                            "file_path",
                            "line_number",
                            "hits",
                            "covered",
                            "count_line",
                            "total_branches",
                            "covered_branches",
                            "total_functions",
                            "covered_functions",
                            "details",
                        ],
                        [
                            [
                                snapshot_id,
                                line.file_path,
                                line.line_number,
                                line.hits,
                                line.covered,
                                line.count_line,
                                line.total_branches,
                                line.covered_branches,
                                line.total_functions,
                                line.covered_functions,
                                json.dumps(line.details),
                            ]
                            for line in report.lines
                        ],
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return snapshot_id

    def _copy_rows(self, table: str, columns: list[str], rows: list[list[Any]]) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix=f"coverage-mcp-{table}-",
            suffix=".csv",
            dir=self.db_path.parent,
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            csv.writer(stream, lineterminator="\n").writerows(rows)
        try:
            sql_path = temporary_path.as_posix().replace("'", "''")
            column_sql = ", ".join(columns)
            self._conn.execute(f"COPY {table} ({column_sql}) FROM '{sql_path}' (FORMAT CSV, HEADER false, NULL '')")
        finally:
            temporary_path.unlink(missing_ok=True)

    def register_worktree(self, path: str, *, base_ref: str, name: str | None = None) -> dict[str, Any]:
        git = inspect_git(path)
        base_sha = merge_base(git.repo_path, base_ref) if git.commit_sha else None
        baseline_snapshot_id = self.find_baseline_snapshot(
            repo_key=git.repo_key,
            base_ref=base_ref,
            base_sha=base_sha,
        )
        worktree_id = str(uuid.uuid4())
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO worktrees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    worktree_id,
                    utcnow(),
                    name,
                    git.path,
                    git.repo_path,
                    git.repo_key,
                    git.branch,
                    git.commit_sha,
                    base_ref,
                    base_sha,
                    baseline_snapshot_id,
                ],
            )
        return self.worktree(worktree_id)

    def find_baseline_snapshot(
        self,
        *,
        repo_key: str,
        base_ref: str,
        base_sha: str | None = None,
        suite: str | None = None,
        before: datetime | None = None,
    ) -> str | None:
        def find(filters: list[str], args: list[Any]) -> str | None:
            if suite:
                filters.append("suite = ?")
                args.append(suite)
            if before:
                filters.append("created_at <= ?")
                args.append(before)
            row = self._conn.execute(
                f"""
                SELECT id FROM snapshots
                WHERE {" AND ".join(filters)}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                args,
            ).fetchone()
            return str(row[0]) if row else None

        with self._lock:
            if base_sha:
                snapshot_id = find(["repo_key = ?", "commit_sha = ?"], [repo_key, base_sha])
                if snapshot_id:
                    return snapshot_id
            return find(["repo_key = ?", "branch = ?"], [repo_key, base_ref])

    def list_snapshots(
        self,
        *,
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        args: list[Any] = []
        if repo_path:
            repo_key = inspect_git(repo_path).repo_key
            filters.append("repo_key = ?")
            args.append(repo_key)
        if branch:
            filters.append("branch = ?")
            args.append(branch)
        if suite:
            filters.append("suite = ?")
            args.append(suite)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM snapshots
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [*args, min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._snapshot_from_row(columns, row) for row in rows]

    def snapshot(self, snapshot_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM snapshots WHERE id = ?", [snapshot_id]).fetchone()
            if row is None:
                raise KeyError(f"snapshot not found: {snapshot_id}")
            columns = [column[0] for column in self._conn.description]
        return self._snapshot_from_row(columns, row)

    def latest_snapshot(
        self,
        *,
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
    ) -> dict[str, Any] | None:
        snapshots = self.list_snapshots(repo_path=repo_path, branch=branch, suite=suite, limit=1)
        return snapshots[0] if snapshots else None

    def list_worktrees(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM worktrees
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._worktree_from_row(columns, row) for row in rows]

    def projects(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                WITH project_sources AS (
                    SELECT repo_key, repo_path FROM snapshots
                    UNION ALL
                    SELECT repo_key, repo_path FROM registered_commands
                    UNION ALL
                    SELECT repo_key, repo_path FROM runs
                ),
                project_keys AS (
                    SELECT repo_key, any_value(repo_path) AS repo_path
                    FROM project_sources
                    GROUP BY repo_key
                ),
                latest AS (
                    SELECT *,
                           row_number() OVER (PARTITION BY repo_key ORDER BY created_at DESC) AS rn
                    FROM snapshots
                ),
                snapshot_aggregate AS (
                    SELECT repo_key,
                           count(*) AS snapshot_count,
                           count(DISTINCT branch) AS branch_count,
                           min(created_at) AS first_snapshot_at,
                           max(created_at) AS latest_snapshot_at
                    FROM snapshots
                    GROUP BY repo_key
                ),
                command_aggregate AS (
                    SELECT repo_key,
                           count(*) AS command_count,
                           max(created_at) AS latest_command_at
                    FROM registered_commands
                    GROUP BY repo_key
                ),
                run_aggregate AS (
                    SELECT repo_key,
                           count(*) AS run_count,
                           max(ended_at) AS latest_run_at
                    FROM runs
                    GROUP BY repo_key
                )
                SELECT
                    p.repo_key,
                    p.repo_path,
                    COALESCE(a.snapshot_count, 0) AS snapshot_count,
                    COALESCE(a.branch_count, 0) AS branch_count,
                    COALESCE(c.command_count, 0) AS command_count,
                    COALESCE(r.run_count, 0) AS run_count,
                    a.first_snapshot_at,
                    a.latest_snapshot_at,
                    c.latest_command_at,
                    r.latest_run_at,
                    l.id AS latest_snapshot_id,
                    l.branch AS latest_branch,
                    l.commit_sha AS latest_commit_sha,
                    l.suite AS latest_suite,
                    l.format AS latest_format,
                    l.total_lines,
                    l.covered_lines,
                    l.line_rate,
                    l.total_branches,
                    l.covered_branches,
                    l.branch_rate,
                    l.total_functions,
                    l.covered_functions,
                    l.function_rate,
                    l.total_regions,
                    l.covered_regions,
                    l.region_rate,
                    l.warnings
                FROM project_keys p
                LEFT JOIN snapshot_aggregate a ON a.repo_key = p.repo_key
                LEFT JOIN command_aggregate c ON c.repo_key = p.repo_key
                LEFT JOIN run_aggregate r ON r.repo_key = p.repo_key
                LEFT JOIN latest l ON l.repo_key = p.repo_key AND l.rn = 1
                ORDER BY GREATEST(
                    COALESCE(a.latest_snapshot_at, TIMESTAMP '1970-01-01'),
                    COALESCE(c.latest_command_at, TIMESTAMP '1970-01-01'),
                    COALESCE(r.latest_run_at, TIMESTAMP '1970-01-01')
                ) DESC
                LIMIT ?
                """,
                [min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        projects = []
        for row in rows:
            project = self._with_topology(self._decode_json_fields(row_dict(columns, row), ["warnings"]))
            self._with_relative_age(project, "latest_snapshot_at", prefix="latest_snapshot")
            self._with_relative_age(project, "latest_run_at", prefix="latest_run")
            projects.append(project)
        return projects

    def worktree(self, worktree_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM worktrees WHERE id = ?", [worktree_id]).fetchone()
            if row is None:
                raise KeyError(f"worktree not found: {worktree_id}")
            columns = [column[0] for column in self._conn.description]
        return self._worktree_from_row(columns, row)

    def files(self, snapshot_id: str, *, limit: int = 1000, offset: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM files
                WHERE snapshot_id = ?
                ORDER BY line_rate ASC NULLS LAST, total_lines DESC, file_path ASC
                LIMIT ? OFFSET ?
                """,
                [snapshot_id, min(max(limit, 1), 5000), max(offset, 0)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._decode_json_fields(row_dict(columns, row), ["raw_metrics"]) for row in rows]

    def file_coverage(self, snapshot_id: str, file_path: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM files
                WHERE snapshot_id = ? AND file_path = ?
                """,
                [snapshot_id, file_path],
            ).fetchone()
            if row is None:
                raise KeyError(f"file not found in snapshot: {file_path}")
            columns = [column[0] for column in self._conn.description]
        return self._decode_json_fields(row_dict(columns, row), ["raw_metrics"])

    def lines(self, snapshot_id: str, file_path: str, *, limit: int = 5000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM lines
                WHERE snapshot_id = ? AND file_path = ?
                ORDER BY line_number
                LIMIT ?
                """,
                [snapshot_id, file_path, min(max(limit, 1), 20000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._decode_json_fields(row_dict(columns, row), ["details"]) for row in rows]

    def trend(
        self,
        *,
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        file_path: str | None = None,
        worktree_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        args: list[Any] = []
        worktree = self.worktree(worktree_id) if worktree_id else None
        if worktree:
            filters.extend(["s.repo_key = ?", "s.repo_path = ?", "s.created_at >= ?"])
            args.extend([worktree["repo_key"], worktree["repo_path"], worktree["created_at"]])
            if not branch:
                branch = worktree.get("branch")
        if repo_path:
            filters.append("s.repo_key = ?")
            args.append(inspect_git(repo_path).repo_key)
        if branch:
            filters.append("s.branch = ?")
            args.append(branch)
        if suite:
            filters.append("s.suite = ?")
            args.append(suite)
        if file_path:
            filters.append("f.file_path = ?")
            args.append(file_path)
            source = """
                SELECT s.id, s.created_at, s.minute_bucket, s.branch, s.commit_sha, s.suite,
                       f.file_path, f.total_lines, f.covered_lines, f.line_rate,
                       f.total_branches, f.covered_branches, f.branch_rate,
                       f.total_functions, f.covered_functions, f.function_rate,
                       f.total_regions, f.covered_regions, f.region_rate
                FROM snapshots s
                JOIN files f ON f.snapshot_id = s.id
            """
        else:
            source = """
                SELECT s.id, s.created_at, s.minute_bucket, s.branch, s.commit_sha, s.suite,
                       NULL AS file_path, s.total_lines, s.covered_lines, s.line_rate,
                       s.total_branches, s.covered_branches, s.branch_rate,
                       s.total_functions, s.covered_functions, s.function_rate,
                       s.total_regions, s.covered_regions, s.region_rate
                FROM snapshots s
            """
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._lock:
            rows = self._conn.execute(
                f"""
                {source}
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [*args, min(max(limit, 1), 2000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._serialize(row_dict(columns, row)) for row in reversed(rows)]

    def worktree_progress(
        self,
        worktree_id: str,
        *,
        suite: str | None = None,
        file_path: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        worktree = self.worktree(worktree_id)
        baseline_snapshot_id = worktree.get("baseline_snapshot_id")
        if not baseline_snapshot_id:
            raise KeyError(f"worktree has no baseline snapshot: {worktree_id}")
        baseline_snapshot = self.snapshot(baseline_snapshot_id)
        latest_points = self.trend(worktree_id=worktree_id, limit=1) if suite is None else []
        selected_suite = suite or (latest_points[-1]["suite"] if latest_points else baseline_snapshot["suite"])
        baseline_snapshot_id = self._worktree_baseline_snapshot_id(worktree, selected_suite)
        baseline = self._trend_point(baseline_snapshot_id, file_path=file_path)
        points = self.trend(
            branch=worktree.get("branch"),
            suite=selected_suite,
            file_path=file_path,
            worktree_id=worktree_id,
            limit=limit,
        )
        current = points[-1] if points else None
        metrics = ("line_rate", "branch_rate", "function_rate", "region_rate")
        deltas = {
            metric: _delta(
                current.get(metric) if current else None,
                baseline.get(metric),
            )
            for metric in metrics
        }
        return {
            "worktree": worktree,
            "suite": selected_suite,
            "file_path": file_path,
            "baseline": baseline,
            "current": current,
            "deltas": deltas,
            "points": [{**baseline, "point_kind": "baseline"}]
            + [{**point, "point_kind": "worktree"} for point in points if point["id"] != baseline["id"]],
        }

    def _worktree_baseline_snapshot_id(self, worktree: dict[str, Any], suite: str) -> str:
        stored_id = worktree.get("baseline_snapshot_id")
        if not stored_id:
            raise KeyError(f"worktree has no baseline snapshot: {worktree['id']}")
        stored = self.snapshot(stored_id)
        if stored["suite"] == suite:
            return str(stored_id)
        created_at = datetime.fromisoformat(str(worktree["created_at"]).removesuffix("Z"))
        snapshot_id = self.find_baseline_snapshot(
            repo_key=str(worktree["repo_key"]),
            base_ref=str(worktree["base_ref"]),
            base_sha=worktree.get("base_sha"),
            suite=suite,
            before=created_at,
        )
        if not snapshot_id:
            raise KeyError(f"worktree has no frozen baseline snapshot for suite {suite}: {worktree['id']}")
        return snapshot_id

    def _trend_point(self, snapshot_id: str, *, file_path: str | None = None) -> dict[str, Any]:
        snapshot = self.snapshot(snapshot_id)
        if file_path:
            file = self.file_coverage(snapshot_id, file_path)
            return {
                "id": snapshot["id"],
                "created_at": snapshot["created_at"],
                "minute_bucket": snapshot["minute_bucket"],
                "branch": snapshot["branch"],
                "commit_sha": snapshot["commit_sha"],
                "suite": snapshot["suite"],
                "file_path": file_path,
                "total_lines": file["total_lines"],
                "covered_lines": file["covered_lines"],
                "line_rate": file["line_rate"],
                "total_branches": file["total_branches"],
                "covered_branches": file["covered_branches"],
                "branch_rate": file["branch_rate"],
                "total_functions": file["total_functions"],
                "covered_functions": file["covered_functions"],
                "function_rate": file["function_rate"],
                "total_regions": file["total_regions"],
                "covered_regions": file["covered_regions"],
                "region_rate": file["region_rate"],
            }
        keys = (
            "id",
            "created_at",
            "minute_bucket",
            "branch",
            "commit_sha",
            "suite",
            "total_lines",
            "covered_lines",
            "line_rate",
            "total_branches",
            "covered_branches",
            "branch_rate",
            "total_functions",
            "covered_functions",
            "function_rate",
            "total_regions",
            "covered_regions",
            "region_rate",
        )
        return {**{key: snapshot[key] for key in keys}, "file_path": None}

    def compare(
        self,
        *,
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_limit: int = 100,
        line_limit: int = 500,
    ) -> dict[str, Any]:
        current = self.snapshot(snapshot_id)
        baseline = self.snapshot(baseline_snapshot_id)
        with self._lock:
            rows = self._conn.execute(
                """
                WITH b AS (
                    SELECT * FROM files WHERE snapshot_id = ?
                ),
                c AS (
                    SELECT * FROM files WHERE snapshot_id = ?
                )
                SELECT
                    COALESCE(c.file_path, b.file_path) AS file_path,
                    b.total_lines AS baseline_total_lines,
                    c.total_lines AS current_total_lines,
                    b.covered_lines AS baseline_covered_lines,
                    c.covered_lines AS current_covered_lines,
                    b.line_rate AS baseline_line_rate,
                    c.line_rate AS current_line_rate,
                    COALESCE(c.line_rate, 0) - COALESCE(b.line_rate, 0) AS line_rate_delta,
                    b.total_branches AS baseline_total_branches,
                    c.total_branches AS current_total_branches,
                    b.covered_branches AS baseline_covered_branches,
                    c.covered_branches AS current_covered_branches,
                    b.branch_rate AS baseline_branch_rate,
                    c.branch_rate AS current_branch_rate,
                    COALESCE(c.branch_rate, 0) - COALESCE(b.branch_rate, 0) AS branch_rate_delta,
                    b.total_functions AS baseline_total_functions,
                    c.total_functions AS current_total_functions,
                    b.covered_functions AS baseline_covered_functions,
                    c.covered_functions AS current_covered_functions,
                    b.function_rate AS baseline_function_rate,
                    c.function_rate AS current_function_rate,
                    COALESCE(c.function_rate, 0) - COALESCE(b.function_rate, 0) AS function_rate_delta,
                    b.total_regions AS baseline_total_regions,
                    c.total_regions AS current_total_regions,
                    b.covered_regions AS baseline_covered_regions,
                    c.covered_regions AS current_covered_regions,
                    b.region_rate AS baseline_region_rate,
                    c.region_rate AS current_region_rate,
                    COALESCE(c.region_rate, 0) - COALESCE(b.region_rate, 0) AS region_rate_delta
                FROM b
                FULL OUTER JOIN c
                  ON c.file_path = b.file_path
                ORDER BY line_rate_delta ASC, file_path ASC
                LIMIT ?
                """,
                [baseline_snapshot_id, snapshot_id, min(max(file_limit, 1), 1000)],
            ).fetchall()
            file_columns = [column[0] for column in self._conn.description]

        changed_lines = self.changed_lines(
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            limit=line_limit,
        )
        return {
            "baseline": baseline,
            "current": current,
            "overall": {
                "line_rate_delta": _delta(current.get("line_rate"), baseline.get("line_rate")),
                "covered_lines_delta": current["covered_lines"] - baseline["covered_lines"],
                "total_lines_delta": current["total_lines"] - baseline["total_lines"],
                "branch_rate_delta": _delta(current.get("branch_rate"), baseline.get("branch_rate")),
                "covered_branches_delta": current["covered_branches"] - baseline["covered_branches"],
                "total_branches_delta": current["total_branches"] - baseline["total_branches"],
                "function_rate_delta": _delta(current.get("function_rate"), baseline.get("function_rate")),
                "covered_functions_delta": current["covered_functions"] - baseline["covered_functions"],
                "total_functions_delta": current["total_functions"] - baseline["total_functions"],
                "region_rate_delta": _delta(current.get("region_rate"), baseline.get("region_rate")),
                "covered_regions_delta": current["covered_regions"] - baseline["covered_regions"],
                "total_regions_delta": current["total_regions"] - baseline["total_regions"],
            },
            "files": [self._serialize(row_dict(file_columns, row)) for row in rows],
            "changed_lines": changed_lines,
        }

    def insights(
        self,
        *,
        snapshot_id: str,
        baseline_snapshot_id: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        limit = min(max(limit, 1), 50)
        snapshot = self.snapshot(snapshot_id)
        items: list[dict[str, Any]] = []

        for warning in snapshot.get("warnings", []):
            items.append(
                {
                    "severity": "info",
                    "category": "parser-warning",
                    "title": "Coverage format has lossy detail",
                    "detail": warning,
                    "snapshot_id": snapshot_id,
                }
            )

        zero_files = self._file_query(
            """
            SELECT file_path, total_lines, covered_lines, line_rate,
                   total_lines - covered_lines AS uncovered_lines
            FROM files
            WHERE snapshot_id = ? AND total_lines > 0 AND covered_lines = 0
            ORDER BY total_lines DESC, file_path
            LIMIT ?
            """,
            [snapshot_id, limit],
        )
        for file in zero_files:
            items.append(
                {
                    "severity": "high" if file["total_lines"] >= 20 else "medium",
                    "category": "zero-coverage-file",
                    "title": "File has no covered lines",
                    "detail": f"{file['file_path']} has 0/{file['total_lines']} covered lines.",
                    "file_path": file["file_path"],
                    "uncovered_lines": file["uncovered_lines"],
                    "line_rate": file["line_rate"],
                }
            )

        low_files = self._file_query(
            """
            SELECT file_path, total_lines, covered_lines, line_rate,
                   total_lines - covered_lines AS uncovered_lines
            FROM files
            WHERE snapshot_id = ?
              AND total_lines >= 5
              AND covered_lines > 0
              AND line_rate < 0.6
            ORDER BY uncovered_lines DESC, line_rate ASC, file_path
            LIMIT ?
            """,
            [snapshot_id, limit],
        )
        for file in low_files:
            items.append(
                {
                    "severity": "medium",
                    "category": "low-line-coverage",
                    "title": "File has low line coverage",
                    "detail": (
                        f"{file['file_path']} is {percent(file['line_rate'])} covered "
                        f"with {file['uncovered_lines']} uncovered lines."
                    ),
                    "file_path": file["file_path"],
                    "uncovered_lines": file["uncovered_lines"],
                    "line_rate": file["line_rate"],
                }
            )

        branch_files = self._file_query(
            """
            SELECT file_path, total_branches, covered_branches, branch_rate,
                   total_branches - covered_branches AS uncovered_branches
            FROM files
            WHERE snapshot_id = ?
              AND total_branches >= 2
              AND (branch_rate IS NULL OR branch_rate < 0.7)
            ORDER BY uncovered_branches DESC, branch_rate ASC NULLS FIRST, file_path
            LIMIT ?
            """,
            [snapshot_id, limit],
        )
        for file in branch_files:
            items.append(
                {
                    "severity": "medium",
                    "category": "low-branch-coverage",
                    "title": "Branch coverage needs attention",
                    "detail": (
                        f"{file['file_path']} covers {file['covered_branches']}/{file['total_branches']} branches."
                    ),
                    "file_path": file["file_path"],
                    "uncovered_branches": file["uncovered_branches"],
                    "branch_rate": file["branch_rate"],
                }
            )

        comparison = None
        if baseline_snapshot_id:
            comparison = self.compare(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                file_limit=limit,
                line_limit=limit * 20,
            )
            overall = comparison["overall"]
            if overall.get("line_rate_delta") is not None and overall["line_rate_delta"] < 0:
                items.append(
                    {
                        "severity": "high",
                        "category": "overall-regression",
                        "title": "Overall line coverage regressed",
                        "detail": f"Line coverage changed by {percent_delta(overall['line_rate_delta'])}.",
                        "line_rate_delta": overall["line_rate_delta"],
                        "covered_lines_delta": overall["covered_lines_delta"],
                    }
                )
            for file in comparison["files"][:limit]:
                delta = file.get("line_rate_delta")
                if delta is not None and delta < 0:
                    items.append(
                        {
                            "severity": "high" if delta <= -0.05 else "medium",
                            "category": "file-regression",
                            "title": "File coverage regressed",
                            "detail": f"{file['file_path']} changed by {percent_delta(delta)}.",
                            "file_path": file["file_path"],
                            "line_rate_delta": delta,
                        }
                    )
            regressed_lines = [line for line in comparison["changed_lines"] if line.get("status") == "regressed"][
                :limit
            ]
            for line in regressed_lines:
                items.append(
                    {
                        "severity": "high",
                        "category": "line-regression",
                        "title": "Line became uncovered",
                        "detail": f"{line['file_path']}:{line['line_number']} was covered and is now missed.",
                        "file_path": line["file_path"],
                        "line_number": line["line_number"],
                    }
                )

        items.sort(key=_insight_sort_key)
        return {
            "snapshot": snapshot,
            "baseline": comparison["baseline"] if comparison else None,
            "summary": {
                "item_count": len(items),
                "high_count": sum(1 for item in items if item["severity"] == "high"),
                "medium_count": sum(1 for item in items if item["severity"] == "medium"),
                "info_count": sum(1 for item in items if item["severity"] == "info"),
            },
            "items": items[: limit * 4],
        }

    def compare_worktree(
        self,
        worktree_id: str,
        *,
        snapshot_id: str | None = None,
        file_limit: int = 100,
        line_limit: int = 500,
    ) -> dict[str, Any]:
        worktree = self.worktree(worktree_id)
        current_id = snapshot_id
        if current_id is None:
            points = self.trend(worktree_id=worktree_id, limit=1)
            if not points:
                raise KeyError(f"no current snapshot found for worktree: {worktree_id}")
            current_id = points[-1]["id"]
        current = self.snapshot(current_id)
        baseline_snapshot_id = self._worktree_baseline_snapshot_id(worktree, current["suite"])
        comparison = self.compare(
            snapshot_id=current_id,
            baseline_snapshot_id=baseline_snapshot_id,
            file_limit=file_limit,
            line_limit=line_limit,
        )
        comparison["worktree"] = worktree
        return comparison

    def changed_lines(
        self,
        *,
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_path: str | None = None,
        only_regressions: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        filters = []
        args: list[Any] = [baseline_snapshot_id, snapshot_id]
        if file_path:
            filters.append("COALESCE(c.file_path, b.file_path) = ?")
            args.append(file_path)
        if only_regressions:
            filters.append("b.covered = true AND COALESCE(c.covered, false) = false")
        where_extra = " AND " + " AND ".join(filters) if filters else ""
        with self._lock:
            rows = self._conn.execute(
                f"""
                WITH b AS (
                    SELECT * FROM lines WHERE snapshot_id = ?
                ),
                c AS (
                    SELECT * FROM lines WHERE snapshot_id = ?
                )
                SELECT
                    COALESCE(c.file_path, b.file_path) AS file_path,
                    COALESCE(c.line_number, b.line_number) AS line_number,
                    b.covered AS baseline_covered,
                    c.covered AS current_covered,
                    b.hits AS baseline_hits,
                    c.hits AS current_hits,
                    b.total_branches AS baseline_total_branches,
                    c.total_branches AS current_total_branches,
                    b.covered_branches AS baseline_covered_branches,
                    c.covered_branches AS current_covered_branches,
                    CASE
                        WHEN b.line_number IS NULL THEN 'new'
                        WHEN c.line_number IS NULL THEN 'removed'
                        WHEN b.covered = true AND COALESCE(c.covered, false) = false THEN 'regressed'
                        WHEN COALESCE(b.covered, false) = false AND c.covered = true THEN 'improved'
                        ELSE 'changed'
                    END AS status
                FROM b
                FULL OUTER JOIN c
                  ON c.file_path = b.file_path
                 AND c.line_number = b.line_number
                WHERE (
                    COALESCE(b.covered, false) != COALESCE(c.covered, false)
                    OR COALESCE(b.hits, -1) != COALESCE(c.hits, -1)
                    OR COALESCE(b.covered_branches, -1) != COALESCE(c.covered_branches, -1)
                    OR COALESCE(b.total_branches, -1) != COALESCE(c.total_branches, -1)
                  )
                  {where_extra}
                ORDER BY
                    CASE status WHEN 'regressed' THEN 0 WHEN 'improved' THEN 1 ELSE 2 END,
                    file_path,
                    line_number
                LIMIT ?
                """,
                [*args, min(max(limit, 1), 5000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._serialize(row_dict(columns, row)) for row in rows]

    def _file_query(self, query: str, args: list[Any]) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(query, args).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._serialize(row_dict(columns, row)) for row in rows]

    def line_history(
        self,
        *,
        file_path: str,
        line_number: int,
        repo_path: str | None = None,
        branch: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters = ["l.file_path = ?", "l.line_number = ?"]
        args: list[Any] = [file_path, line_number]
        if repo_path:
            filters.append("s.repo_key = ?")
            args.append(inspect_git(repo_path).repo_key)
        if branch:
            filters.append("s.branch = ?")
            args.append(branch)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT s.id AS snapshot_id, s.created_at, s.branch, s.commit_sha, s.suite,
                       l.file_path, l.line_number, l.hits, l.covered,
                       l.total_branches, l.covered_branches
                FROM lines l
                JOIN snapshots s ON s.id = l.snapshot_id
                WHERE {" AND ".join(filters)}
                ORDER BY s.created_at DESC
                LIMIT ?
                """,
                [*args, min(max(limit, 1), 1000)],
            ).fetchall()
            columns = [column[0] for column in self._conn.description]
        return [self._serialize(row_dict(columns, row)) for row in reversed(rows)]

    def source_lines(
        self,
        *,
        snapshot_id: str,
        file_path: str,
        start: int,
        end: int,
    ) -> list[dict[str, Any]]:
        snapshot = self.snapshot(snapshot_id)
        repo = Path(snapshot["repo_path"]).resolve()
        source = (repo / file_path).resolve()
        if repo not in source.parents and source != repo:
            raise ValueError("file_path escapes repository root")
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(file_path)
        start = max(1, start)
        end = min(max(start, end), start + 199)
        result: list[dict[str, Any]] = []
        with source.open("r", encoding="utf-8", errors="replace") as handle:
            for index, text in enumerate(handle, start=1):
                if index < start:
                    continue
                if index > end:
                    break
                result.append({"line_number": index, "text": text.rstrip("\n")})
        return result

    def _snapshot_from_row(self, columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        snapshot = self._with_topology(self._decode_json_fields(row_dict(columns, row), ["warnings", "metadata"]))
        return self._with_relative_age(snapshot, "created_at")

    def _run_from_row(self, columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        run = self._with_topology(
            self._decode_json_fields(row_dict(columns, row), ["parsed_summary", "artifact_paths"])
        )
        run["coverage_ingest"] = summarize_coverage_ingest(run["artifact_paths"])
        run["terminal"] = True
        run["poll_after_ms"] = None
        run["queue_position"] = None
        run["execution_mode"] = "background"
        run["cancellation_requested"] = run.get("cancellation_requested_at") is not None
        run.update(
            self._job_eta_fields(
                run,
                status=str(run["status"]),
                now=utcnow(),
                duration_ms=int(run["duration_ms"]),
            )
        )
        return self._with_relative_age(run, "ended_at")

    def _worktree_from_row(self, columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        return self._with_topology(self._serialize(row_dict(columns, row)))

    def _decode_json_fields(self, value: dict[str, Any], fields: list[str]) -> dict[str, Any]:
        for field in fields:
            raw = value.get(field)
            if isinstance(raw, str):
                try:
                    value[field] = json.loads(raw)
                except json.JSONDecodeError:
                    value[field] = raw
        return self._serialize(value)

    def _serialize(self, value: dict[str, Any]) -> dict[str, Any]:
        serialized: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(item, datetime):
                serialized[key] = item.isoformat() + "Z"
            else:
                serialized[key] = item
        return serialized

    def _with_relative_age(
        self,
        value: dict[str, Any],
        timestamp_field: str,
        *,
        prefix: str = "",
    ) -> dict[str, Any]:
        relative = relative_age(value.get(timestamp_field))
        if relative is None:
            return value
        age_seconds, age = relative
        field_prefix = f"{prefix}_" if prefix else ""
        value[f"{field_prefix}age_seconds"] = age_seconds
        value[f"{field_prefix}age"] = age
        return value

    def _with_topology(self, value: dict[str, Any]) -> dict[str, Any]:
        if "topology" not in value:
            topology = infer_topology(value)
            if topology:
                value["topology"] = topology
        return value


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return current - baseline


def validate_run_limits(*, max_summary_lines: int, timeout_seconds: int | None = None) -> None:
    if not MIN_SUMMARY_LINES <= max_summary_lines <= MAX_SUMMARY_LINES:
        raise ValueError(f"max_summary_lines must be between {MIN_SUMMARY_LINES} and {MAX_SUMMARY_LINES}")
    if timeout_seconds is not None and not MIN_TIMEOUT_SECONDS <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout_seconds must be between {MIN_TIMEOUT_SECONDS} and {MAX_TIMEOUT_SECONDS}")


def relative_age(
    value: datetime | str | None,
    *,
    now: datetime | None = None,
) -> tuple[int, str] | None:
    timestamp = parse_datetime(value)
    if timestamp is None:
        return None
    current = now or utcnow()
    if current.tzinfo is not None:
        current = current.astimezone(UTC).replace(tzinfo=None)
    seconds = max(0, int((current - timestamp).total_seconds()))
    return seconds, format_age(seconds)


def parse_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    timestamp = value
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(
                timestamp.removesuffix("Z") + ("+00:00" if timestamp.endswith("Z") else "")
            )
        except ValueError:
            return None
    if timestamp.tzinfo is not None:
        return timestamp.astimezone(UTC).replace(tzinfo=None)
    return timestamp


def format_age(seconds: int) -> str:
    remaining = max(0, seconds)
    parts: list[str] = []
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)):
        amount, remaining = divmod(remaining, size)
        if amount:
            suffix = "" if amount == 1 else "s"
            parts.append(f"{amount} {unit}{suffix}")
    if not parts:
        parts.append("0 seconds")
    return " ".join(parts) + " ago"


def format_duration(seconds: int) -> str:
    return format_age(seconds).removesuffix(" ago")


def milliseconds_to_seconds(milliseconds: int) -> int:
    return (max(milliseconds, 0) + 999) // 1000


def serialize_datetime(value: datetime) -> str:
    return value.isoformat() + "Z"


def infer_topology(value: dict[str, Any]) -> dict[str, Any] | None:
    if "repo_key" not in value and "repo_path" not in value:
        return None
    project = {
        "repo_key": value.get("repo_key"),
        "repo_path": value.get("repo_path"),
    }
    git = {
        "branch": value.get("branch") or value.get("latest_branch"),
        "commit_sha": value.get("commit_sha") or value.get("latest_commit_sha") or value.get("head_sha"),
    }

    if "stdout_path" in value and "stderr_path" in value and "command_id" in value:
        raw_artifacts = value.get("artifact_paths")
        artifacts: list[Any] = raw_artifacts if isinstance(raw_artifacts, list) else []
        return {
            "kind": "run",
            "project": project,
            "command": {
                "id": value.get("command_id"),
                "name": value.get("command_name"),
            },
            "run": {
                "id": value.get("id"),
                "status": value.get("status"),
                "cwd": value.get("cwd"),
                "started_at": value.get("started_at"),
                "ended_at": value.get("ended_at"),
                "exit_code": value.get("exit_code"),
            },
            "git": git,
            "artifacts": [
                {
                    "kind": artifact.get("kind"),
                    "path": artifact.get("path"),
                    "exists": artifact.get("exists"),
                    "coverage_format": artifact.get("coverage_format"),
                    "suite": artifact.get("suite"),
                    "ingest_status": artifact.get("ingest_status"),
                    "snapshot_id": artifact.get("snapshot_id"),
                }
                for artifact in artifacts
                if isinstance(artifact, dict)
            ],
        }

    if "approved_by" in value and "artifact_specs" in value and "command" in value:
        raw_specs = value.get("artifact_specs")
        specs: list[Any] = raw_specs if isinstance(raw_specs, list) else []
        return {
            "kind": "registered_command",
            "project": project,
            "command": {
                "id": value.get("id"),
                "name": value.get("name"),
                "cwd": value.get("cwd"),
                "enabled": value.get("enabled"),
            },
            "registration": {
                "approved_by": value.get("approved_by"),
                "created_at": value.get("created_at"),
                "branch": value.get("branch"),
                "commit_sha": value.get("commit_sha"),
            },
            "artifact_kinds": [spec.get("kind") for spec in specs if isinstance(spec, dict)],
        }

    if "latest_snapshot_id" in value and "snapshot_count" in value:
        return {
            "kind": "project",
            "project": {
                **project,
                "snapshot_count": value.get("snapshot_count"),
                "command_count": value.get("command_count"),
                "run_count": value.get("run_count"),
            },
            "latest_snapshot": {
                "id": value.get("latest_snapshot_id"),
                "branch": value.get("latest_branch"),
                "commit_sha": value.get("latest_commit_sha"),
                "suite": value.get("latest_suite"),
                "format": value.get("latest_format"),
            },
        }

    if "baseline_snapshot_id" in value and "base_ref" in value and "path" in value:
        return {
            "kind": "worktree",
            "project": project,
            "worktree": {
                "id": value.get("id"),
                "path": value.get("path"),
                "name": value.get("name"),
                "branch": value.get("branch"),
                "head_sha": value.get("head_sha"),
            },
            "baseline": {
                "base_ref": value.get("base_ref"),
                "base_sha": value.get("base_sha"),
                "snapshot_id": value.get("baseline_snapshot_id"),
            },
        }

    if "report_path" in value and "suite" in value and "format" in value and "total_lines" in value:
        return {
            "kind": "coverage_snapshot",
            "project": project,
            "snapshot": {
                "id": value.get("id"),
                "suite": value.get("suite"),
                "format": value.get("format"),
                "report_path": value.get("report_path"),
                "created_at": value.get("created_at"),
            },
            "git": git,
        }

    if "run_id" in value and "kind" in value and "path" in value and "command_id" in value:
        return {
            "kind": "run_artifact",
            "project": project,
            "command": {
                "id": value.get("command_id"),
                "name": value.get("command_name"),
            },
            "run": {
                "id": value.get("run_id"),
                "status": value.get("status"),
                "exit_code": value.get("exit_code"),
            },
            "artifact": {
                "kind": value.get("kind"),
                "path": value.get("path"),
                "exists": value.get("exists"),
                "size_bytes": value.get("size_bytes"),
            },
        }

    return None


def summarize_coverage_ingest(artifact_paths: list[dict[str, Any]]) -> dict[str, Any]:
    coverage_artifacts = [artifact for artifact in artifact_paths if artifact.get("coverage_format")]
    if not coverage_artifacts:
        return {
            "status": "not_configured",
            "configured_artifacts": 0,
            "ingested_artifacts": 0,
            "failed_artifacts": 0,
            "skipped_artifacts": 0,
            "snapshot_ids": [],
        }

    statuses = [
        "not_recorded" if "ingest_status" not in artifact else str(artifact.get("ingest_status") or "pending")
        for artifact in coverage_artifacts
    ]
    ingested = statuses.count("ingested")
    failed = sum(status in {"failed", "missing"} for status in statuses)
    skipped = sum(status.startswith("skipped_") for status in statuses)
    if ingested == len(statuses):
        status = "ingested"
    elif ingested:
        status = "partial"
    elif failed:
        status = "failed"
    elif statuses and all(item == "skipped_stale" for item in statuses):
        status = "skipped_stale"
    elif statuses and all(item == "skipped_run_status" for item in statuses):
        status = "skipped_run_status"
    elif statuses and all(item == "not_recorded" for item in statuses):
        status = "not_recorded"
    else:
        status = "pending"
    return {
        "status": status,
        "configured_artifacts": len(coverage_artifacts),
        "ingested_artifacts": ingested,
        "failed_artifacts": failed,
        "skipped_artifacts": skipped,
        "snapshot_ids": [artifact["snapshot_id"] for artifact in coverage_artifacts if artifact.get("snapshot_id")],
    }


def normalize_artifact_specs(artifact_paths: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for kind, value in artifact_paths.items():
        kind = str(kind).strip()
        if not kind:
            continue
        if isinstance(value, str):
            path = value
            required = False
            coverage_format = None
            suite = None
        elif isinstance(value, dict):
            path = str(value.get("path", "")).strip()
            required = bool(value.get("required", False))
            coverage_format = value.get("coverage_format") or value.get("format")
            raw_suite = value.get("suite")
            suite = str(raw_suite).strip() if raw_suite is not None else None
            if raw_suite is not None and not suite:
                raise ValueError(f"artifact spec for {kind} has a blank suite")
        else:
            raise ValueError(f"artifact spec for {kind} must be a path string or object")
        if not path:
            raise ValueError(f"artifact spec for {kind} is missing path")
        specs.append(
            {
                "kind": kind,
                "path": path,
                "required": required,
                "coverage_format": coverage_format,
                "suite": suite,
            }
        )
    return specs


def summarize_run_logs(
    *,
    stdout_path: Path,
    stderr_path: Path,
    exit_code: int | None,
    status: str,
    duration_ms: int,
    max_summary_lines: int,
) -> dict[str, Any]:
    stdout = profile_log(stdout_path, stream="stdout", max_lines=max_summary_lines)
    stderr = profile_log(stderr_path, stream="stderr", max_lines=max_summary_lines)
    excerpts = [*stderr["interesting"], *stdout["interesting"]]
    if len(excerpts) < max_summary_lines:
        tail_needed = max_summary_lines - len(excerpts)
        excerpts.extend([*stderr["tail"], *stdout["tail"]][:tail_needed])
    excerpts = excerpts[:max_summary_lines]
    counters = merge_counters(stdout["counters"], stderr["counters"])
    return {
        "status": status,
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "stdout_line_count": stdout["line_count"],
        "stderr_line_count": stderr["line_count"],
        "counters": counters,
        "excerpts": excerpts,
        "truncated": stdout["line_count"] + stderr["line_count"] > len(excerpts),
        "stdout_path": stdout_path.as_posix(),
        "stderr_path": stderr_path.as_posix(),
    }


def profile_log(path: Path, *, stream: str, max_lines: int) -> dict[str, Any]:
    interesting: list[dict[str, Any]] = []
    tail: deque[dict[str, Any]] = deque(maxlen=max_lines)
    counters: dict[str, int] = {}
    line_count = 0
    if not path.exists():
        return {"line_count": 0, "interesting": [], "tail": [], "counters": {}}
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, raw in enumerate(handle, start=1):
            line_count = line_number
            text = raw.rstrip("\n")
            update_log_counters(counters, text)
            item = {"stream": stream, "line_number": line_number, "text": text[:1000]}
            tail.append(item)
            if len(interesting) < max_lines and is_interesting_log_line(text):
                interesting.append(item)
    return {
        "line_count": line_count,
        "interesting": interesting,
        "tail": list(tail),
        "counters": counters,
    }


def update_log_counters(counters: dict[str, int], text: str) -> None:
    lowered = text.lower()
    for key in ["passed", "failed", "skipped", "error", "errors", "failure", "failures"]:
        match = re.search(rf"\b(\d+)\s+{key}\b", lowered)
        if match:
            counters[key] = counters.get(key, 0) + int(match.group(1))


def merge_counters(*items: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for counters in items:
        for key, value in counters.items():
            merged[key] = merged.get(key, 0) + value
    return merged


def is_interesting_log_line(text: str) -> bool:
    lowered = text.lower()
    needles = [
        "error",
        "failed",
        "failure",
        "panic",
        "traceback",
        "exception",
        "assert",
        "segmentation fault",
        "timeout",
        "fatal",
    ]
    return any(needle in lowered for needle in needles)


def percent(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value * 100:.1f}%"


def percent_delta(value: float | None) -> str:
    if value is None:
        return "unknown"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.1f} points"


def _insight_sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
    severity_rank = {"high": 0, "medium": 1, "info": 2}
    severity = item.get("severity")
    severity_key = severity if isinstance(severity, str) else ""
    return (
        severity_rank.get(severity_key, 3),
        str(item.get("category", "")),
        str(item.get("file_path", "")),
    )
