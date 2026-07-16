from __future__ import annotations

import json
import re
import subprocess
import threading
import time
import uuid
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from coverage_mcp.git_utils import inspect_git, merge_base
from coverage_mcp.models import CoverageReport
from coverage_mcp.parsers import parse_coverage_report


def row_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, row, strict=True))


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def minute_bucket(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


class CoverageStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_dir = self.db_path.parent / "runs"
        self._conn = duckdb.connect(self.db_path.as_posix())
        self._lock = threading.RLock()
        self._init_schema()

    def close(self) -> None:
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
                    line_rate DOUBLE,
                    branch_rate DOUBLE,
                    function_rate DOUBLE
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
                    line_rate DOUBLE,
                    branch_rate DOUBLE,
                    function_rate DOUBLE,
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
                    enabled BOOLEAN NOT NULL
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
                    artifact_paths VARCHAR NOT NULL
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
                    size_bytes BIGINT
                )
                """
            )
            for statement in [
                "CREATE INDEX IF NOT EXISTS idx_snapshots_repo_time ON snapshots(repo_key, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_commit ON snapshots(repo_key, commit_sha)",
                "CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id)",
                "CREATE INDEX IF NOT EXISTS idx_lines_lookup ON lines(snapshot_id, file_path, line_number)",
                "CREATE INDEX IF NOT EXISTS idx_worktrees_repo ON worktrees(repo_key, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_registered_commands_name ON registered_commands(name, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_runs_command_time ON runs(command_id, started_at)",
                "CREATE INDEX IF NOT EXISTS idx_run_artifacts_kind ON run_artifacts(kind)",
            ]:
                with suppress(duckdb.Error):
                    self._conn.execute(statement)
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        line_columns = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info('lines')").fetchall()
        }
        if "count_line" not in line_columns:
            self._conn.execute("ALTER TABLE lines ADD COLUMN count_line BOOLEAN DEFAULT true")

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
                INSERT INTO registered_commands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            self._with_topology(self._decode_json_fields(row_dict(columns, row), ["artifact_specs"]))
            for row in rows
        ]

    def run_command_profiled(
        self,
        command_ref: str,
        *,
        max_summary_lines: int = 80,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        registered = self.registered_command(command_ref)
        if not registered.get("enabled", True):
            raise ValueError(f"registered command is disabled: {command_ref}")

        run_id = str(uuid.uuid4())
        run_path = self.run_dir / run_id
        run_path.mkdir(parents=True, exist_ok=True)
        stdout_path = run_path / "stdout.log"
        stderr_path = run_path / "stderr.log"
        cwd = registered["cwd"]
        git = inspect_git(cwd)
        started_at = utcnow()
        start = time.monotonic()
        exit_code: int | None = None
        status = "failed"
        try:
            with stdout_path.open("w", encoding="utf-8", errors="replace") as stdout, stderr_path.open(
                "w",
                encoding="utf-8",
                errors="replace",
            ) as stderr:
                completed = subprocess.run(
                    registered["command"],
                    shell=True,
                    cwd=cwd,
                    executable=registered["shell"],
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                    timeout=timeout_seconds,
                    check=False,
                )
            exit_code = completed.returncode
            status = "passed" if exit_code == 0 else "failed"
        except subprocess.TimeoutExpired:
            status = "timeout"
            exit_code = None
            with stderr_path.open("a", encoding="utf-8", errors="replace") as stderr:
                stderr.write(f"\nCommand timed out after {timeout_seconds} seconds.\n")
        ended_at = utcnow()
        duration_ms = int((time.monotonic() - start) * 1000)
        artifact_paths = self._collect_run_artifacts(registered["artifact_specs"], cwd)
        summary = summarize_run_logs(
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            exit_code=exit_code,
            status=status,
            duration_ms=duration_ms,
            max_summary_lines=max(max_summary_lines, 1),
        )
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        registered["id"],
                        registered["name"],
                        registered["command"],
                        cwd,
                        git.repo_path,
                        git.repo_key,
                        git.branch,
                        git.commit_sha,
                        started_at,
                        ended_at,
                        duration_ms,
                        exit_code,
                        status,
                        stdout_path.as_posix(),
                        stderr_path.as_posix(),
                        json.dumps(summary),
                        json.dumps(artifact_paths),
                    ],
                )
                if artifact_paths:
                    self._conn.executemany(
                        """
                        INSERT INTO run_artifacts VALUES (?, ?, ?, ?, ?)
                        """,
                        [
                            [
                                run_id,
                                artifact["kind"],
                                artifact["path"],
                                artifact["exists"],
                                artifact["size_bytes"],
                            ]
                            for artifact in artifact_paths
                        ],
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.run_result(run_id, max_summary_lines=max_summary_lines)

    def run_result(self, run_id: str, *, max_summary_lines: int = 80) -> dict[str, Any]:
        run = self.run(run_id)
        run["parsed_summary"] = summarize_run_logs(
            stdout_path=Path(run["stdout_path"]),
            stderr_path=Path(run["stderr_path"]),
            exit_code=run["exit_code"],
            status=run["status"],
            duration_ms=run["duration_ms"],
            max_summary_lines=max(max_summary_lines, 1),
        )
        return run

    def run(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE id = ?", [run_id]).fetchone()
            if row is None:
                raise KeyError(f"run not found: {run_id}")
            columns = [column[0] for column in self._conn.description]
        return self._with_topology(
            self._decode_json_fields(row_dict(columns, row), ["parsed_summary", "artifact_paths"])
        )

    def latest_run(self, command_ref: str | None = None) -> dict[str, Any] | None:
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
                return None
            columns = [column[0] for column in self._conn.description]
        return self._with_topology(
            self._decode_json_fields(row_dict(columns, row), ["parsed_summary", "artifact_paths"])
        )

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
        return self._with_topology(self._serialize(row_dict(columns, row)))

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
            run = self.run(object_ref)
            return {"object_kind": "run", "object_ref": object_ref, "topology": run["topology"]}
        if kind in {"snapshot", "coverage_snapshot"}:
            snapshot = self.snapshot(object_ref)
            return {"object_kind": "coverage_snapshot", "object_ref": object_ref, "topology": snapshot["topology"]}
        if kind == "worktree":
            worktree = self.worktree(object_ref)
            return {"object_kind": "worktree", "object_ref": object_ref, "topology": worktree["topology"]}
        raise ValueError(f"unsupported topology object kind: {object_kind}")

    def _collect_run_artifacts(self, artifact_specs: list[dict[str, Any]], cwd: str) -> list[dict[str, Any]]:
        artifacts: list[dict[str, Any]] = []
        cwd_path = Path(cwd)
        for spec in artifact_specs:
            raw_path = str(spec.get("path", "")).strip()
            if not raw_path:
                continue
            path = Path(raw_path).expanduser()
            if not path.is_absolute():
                path = cwd_path / path
            exists = path.exists()
            artifacts.append(
                {
                    "kind": spec["kind"],
                    "path": path.as_posix(),
                    "exists": exists,
                    "size_bytes": path.stat().st_size if exists and path.is_file() else None,
                    "required": bool(spec.get("required", False)),
                    "coverage_format": spec.get("coverage_format"),
                }
            )
        return artifacts

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
                    INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        report.line_rate,
                        report.branch_rate,
                        report.function_rate,
                    ],
                )
                if report.files:
                    self._conn.executemany(
                        """
                        INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
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
                                file.line_rate,
                                file.branch_rate,
                                file.function_rate,
                                json.dumps(file.raw_metrics),
                            ]
                            for file in report.files
                        ],
                    )
                if report.lines:
                    self._conn.executemany(
                        """
                        INSERT INTO lines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
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
    ) -> str | None:
        with self._lock:
            if base_sha:
                row = self._conn.execute(
                    """
                    SELECT id FROM snapshots
                    WHERE repo_key = ? AND commit_sha = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    [repo_key, base_sha],
                ).fetchone()
                if row:
                    return str(row[0])
            row = self._conn.execute(
                """
                SELECT id FROM snapshots
                WHERE repo_key = ? AND branch = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [repo_key, base_ref],
            ).fetchone()
            return str(row[0]) if row else None

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
                           max(started_at) AS latest_run_at
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
        return [self._with_topology(self._decode_json_fields(row_dict(columns, row), ["warnings"])) for row in rows]

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
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        args: list[Any] = []
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
                       f.total_branches, f.covered_branches, f.branch_rate
                FROM snapshots s
                JOIN files f ON f.snapshot_id = s.id
            """
        else:
            source = """
                SELECT s.id, s.created_at, s.minute_bucket, s.branch, s.commit_sha, s.suite,
                       NULL AS file_path, s.total_lines, s.covered_lines, s.line_rate,
                       s.total_branches, s.covered_branches, s.branch_rate
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
                    COALESCE(c.branch_rate, 0) - COALESCE(b.branch_rate, 0) AS branch_rate_delta
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
                        f"{file['file_path']} covers {file['covered_branches']}/"
                        f"{file['total_branches']} branches."
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
            regressed_lines = [
                line
                for line in comparison["changed_lines"]
                if line.get("status") == "regressed"
            ][:limit]
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

    def compare_worktree(self, worktree_id: str, *, snapshot_id: str | None = None) -> dict[str, Any]:
        worktree = self.worktree(worktree_id)
        baseline_snapshot_id = worktree.get("baseline_snapshot_id")
        if not baseline_snapshot_id:
            raise KeyError(f"worktree has no baseline snapshot: {worktree_id}")
        current_id = snapshot_id
        if current_id is None:
            latest = self.latest_snapshot(repo_path=worktree["repo_path"], branch=worktree.get("branch"))
            if not latest:
                raise KeyError(f"no current snapshot found for worktree: {worktree_id}")
            current_id = latest["id"]
        comparison = self.compare(snapshot_id=current_id, baseline_snapshot_id=baseline_snapshot_id)
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
                WHERE {' AND '.join(filters)}
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
        return self._with_topology(self._decode_json_fields(row_dict(columns, row), ["warnings", "metadata"]))

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
        elif isinstance(value, dict):
            path = str(value.get("path", "")).strip()
            required = bool(value.get("required", False))
            coverage_format = value.get("coverage_format") or value.get("format")
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
