from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

import duckdb
import pytest

from coverage_mcp.models import CoverageReport, FileCoverage, LineCoverage
from coverage_mcp.storage import (
    CoverageStore,
    infer_topology,
    is_interesting_log_line,
    merge_counters,
    normalize_artifact_specs,
    percent,
    percent_delta,
    profile_log,
    summarize_run_logs,
    update_log_counters,
)


def make_lcov(path: Path, *, file_path: str = "src/a.py", hits: tuple[int, ...] = (1, 0)) -> None:
    rows = [f"DA:{index},{hit}" for index, hit in enumerate(hits, start=1)]
    path.write_text(f"TN:\nSF:{file_path}\n" + "\n".join(rows) + "\nend_of_record\n", encoding="utf-8")


def test_register_command_validation_and_disabled_run(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        kwargs = {
            "cwd": tmp_path.as_posix(),
            "human_approved": True,
            "approved_by": "tester",
            "approval_note": "approved",
        }
        for overrides, message in [
            ({"name": "", "command": "echo ok"}, "name"),
            ({"name": "x", "command": ""}, "command"),
            ({"name": "x", "command": "echo ok", "approved_by": ""}, "approved_by"),
            ({"name": "x", "command": "echo ok", "approval_note": ""}, "approval_note"),
            ({"name": "x", "command": "echo ok", "cwd": (tmp_path / "missing").as_posix()}, "cwd"),
        ]:
            data = {**kwargs, **overrides}
            with pytest.raises(ValueError, match=message):
                store.register_command(**data)

        disabled = store.register_command(
            name="disabled",
            command="echo ok",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved disabled command",
            enabled=False,
        )
        with pytest.raises(ValueError, match="disabled"):
            store.run_command_profiled(disabled["id"])
    finally:
        store.close()


def test_storage_query_edges_and_worktree_without_baseline(tmp_path):
    report = tmp_path / "coverage.lcov"
    make_lcov(report)
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        snapshot = store.ingest_report(
            report.as_posix(),
            format="lcov",
            repo_path=tmp_path.as_posix(),
            branch="main",
            commit_sha="abc",
            suite="unit",
        )
        assert store.list_snapshots(repo_path=tmp_path.as_posix(), branch="main", suite="unit")
        assert store.files(snapshot["id"], limit=0)
        assert store.lines(snapshot["id"], "src/a.py", limit=0)
        assert store.trend(repo_path=tmp_path.as_posix(), branch="main", suite="unit")
        assert store.line_history(file_path="src/a.py", line_number=1, repo_path=tmp_path.as_posix(), branch="main")
        assert store.changed_lines(
            snapshot_id=snapshot["id"],
            baseline_snapshot_id=snapshot["id"],
            only_regressions=True,
        ) == []

        worktree = store.register_worktree(tmp_path.as_posix(), base_ref="no-such-branch")
        with pytest.raises(KeyError, match="baseline"):
            store.compare_worktree(worktree["id"], snapshot_id=snapshot["id"])
        with pytest.raises(KeyError, match="file"):
            store.file_coverage(snapshot["id"], "missing.py")
        assert store.object_topology("project", tmp_path.as_posix())["topology"]["kind"] == "project"
        with pytest.raises(KeyError, match="project"):
            store.object_topology("project", "missing")
        with pytest.raises(ValueError, match="unsupported"):
            store.object_topology("unsupported", "x")
    finally:
        store.close()


def test_storage_insights_source_and_worktree_latest_paths(tmp_path):
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    (source_dir / "low.py").write_text("one\ntwo\nthree\nfour\nfive\n", encoding="utf-8")
    base = tmp_path / "base.lcov"
    current = tmp_path / "current.lcov"
    coveragepy = tmp_path / "coverage.json"
    base.write_text("TN:\nSF:src/low.py\nDA:1,1\nDA:2,1\nend_of_record\n", encoding="utf-8")
    current.write_text(
        "TN:\nSF:src/low.py\nDA:1,1\nDA:2,0\nDA:3,0\nDA:4,0\nDA:5,0\nend_of_record\n",
        encoding="utf-8",
    )
    coveragepy.write_text(
        '{"files":{"src/low.py":{"executed_lines":[1],"missing_lines":[2,3,4,5]}}}',
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        base_snapshot = store.ingest_report(
            base.as_posix(),
            format="lcov",
            repo_path=tmp_path.as_posix(),
            branch="main",
            commit_sha="base",
        )
        current_snapshot = store.ingest_report(
            current.as_posix(),
            format="lcov",
            repo_path=tmp_path.as_posix(),
            branch=None,
            commit_sha="head",
        )
        warning_snapshot = store.ingest_report(
            coveragepy.as_posix(),
            format="coveragepy",
            repo_path=tmp_path.as_posix(),
            branch="warning",
            commit_sha="warning",
        )
        worktree = store.register_worktree(tmp_path.as_posix(), base_ref="main")

        insights = store.insights(snapshot_id=current_snapshot["id"], baseline_snapshot_id=base_snapshot["id"])
        warning_insights = store.insights(snapshot_id=warning_snapshot["id"])
        comparison = store.compare_worktree(worktree["id"])
        worktree_topology = store.object_topology("worktree", worktree["id"])
        source = store.source_lines(snapshot_id=current_snapshot["id"], file_path="src/low.py", start=3, end=3)

        assert any(item["category"] == "low-line-coverage" for item in insights["items"])
        assert any(item["category"] == "parser-warning" for item in warning_insights["items"])
        assert comparison["current"]["id"] == warning_snapshot["id"]
        assert worktree_topology["topology"]["kind"] == "worktree"
        assert source == [{"line_number": 3, "text": "three"}]
        assert store._collect_run_artifacts([{"kind": "blank", "path": " "}], tmp_path.as_posix()) == []
    finally:
        store.close()


def test_run_latest_fallbacks_and_missing_artifacts(tmp_path):
    script = tmp_path / "run.py"
    script.write_text("print('1 passed')\n", encoding="utf-8")
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="suite",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            artifact_paths={
                "missing": {"path": "missing.txt", "required": True, "coverage_format": "custom"},
            },
            human_approved=True,
            approved_by="tester",
            approval_note="approved run fallback command",
        )
        run = store.run_command_profiled(command["name"])
        assert store.latest_run()["id"] == run["id"]
        assert store.latest_run(command_ref="suite")["id"] == run["id"]
        assert store.latest_run(command_ref="unknown") is None
        artifact = store.latest_artifact(command_ref="suite", kind="missing")
        assert artifact is not None
        assert artifact["exists"] is False
        assert store.latest_artifact(command_ref="unknown", kind="missing") is None
    finally:
        store.close()


def test_log_summary_and_topology_helpers(tmp_path):
    stdout = tmp_path / "stdout.log"
    stderr = tmp_path / "stderr.log"
    stdout.write_text("line\n2 passed\n3 skipped\n" + "x" * 1200 + "\n", encoding="utf-8")
    stderr.write_text("Traceback error\n1 failed\n", encoding="utf-8")

    counters: dict[str, int] = {}
    update_log_counters(counters, "4 errors")
    assert counters["errors"] == 4
    assert merge_counters({"passed": 1}, {"passed": 2}) == {"passed": 3}
    assert is_interesting_log_line("fatal panic")
    assert profile_log(tmp_path / "missing.log", stream="stdout", max_lines=2)["line_count"] == 0
    summary = summarize_run_logs(
        stdout_path=stdout,
        stderr_path=stderr,
        exit_code=1,
        status="failed",
        duration_ms=12,
        max_summary_lines=3,
    )
    assert summary["counters"]["passed"] == 2
    assert summary["counters"]["failed"] == 1
    assert len(summary["excerpts"]) == 3
    assert percent(None) == "unknown"
    assert percent(0.5) == "50.0%"
    assert percent_delta(None) == "unknown"
    assert percent_delta(0.125) == "+12.5 points"
    assert infer_topology({}) is None
    worktree_topology = infer_topology(
        {"repo_key": "r", "repo_path": "p", "baseline_snapshot_id": "s", "base_ref": "main", "path": "w"}
    )
    assert worktree_topology is not None
    assert worktree_topology["kind"] == "worktree"
    assert infer_topology({"repo_key": "r", "repo_path": "p"}) is None


def test_normalize_artifact_specs_edges():
    specs = normalize_artifact_specs({"": "ignored", "json": {"path": "a.json", "format": "custom"}, "txt": "a.txt"})
    assert [spec["kind"] for spec in specs] == ["json", "txt"]
    with pytest.raises(ValueError, match="must be"):
        normalize_artifact_specs({"bad": 1})
    with pytest.raises(ValueError, match="missing path"):
        normalize_artifact_specs({"bad": {"path": ""}})


def test_storage_rollbacks_are_triggered(monkeypatch, tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    report = CoverageReport(
        format="unit",
        report_path="report",
        files=[FileCoverage("a.py", total_lines=1, covered_lines=1)],
        lines=[LineCoverage("a.py", 1, hits=1, covered=True)],
    )
    try:
        original_conn = store._conn
        calls = {"insert": 0}

        class FailingConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            @property
            def description(self):
                return self.wrapped.description

            def execute(self, query, *args, **kwargs):
                if "INSERT INTO snapshots" in str(query):
                    calls["insert"] += 1
                    raise RuntimeError("insert failed")
                return self.wrapped.execute(query, *args, **kwargs)

            def executemany(self, query, *args, **kwargs):
                return self.wrapped.executemany(query, *args, **kwargs)

            def close(self):
                return self.wrapped.close()

        monkeypatch.setattr(store, "_conn", FailingConnection(original_conn))
        with pytest.raises(RuntimeError, match="insert failed"):
            store.store_report(
                report,
                repo_path=tmp_path.as_posix(),
                repo_key=tmp_path.as_posix(),
                branch="main",
                commit_sha="abc",
                base_ref=None,
                suite="unit",
            )
        assert calls["insert"] == 1
    finally:
        store.close()


def test_run_rollback_and_private_decode_paths(monkeypatch, tmp_path):
    script = tmp_path / "run.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="rollback-run",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved rollback run",
        )
        original_conn = store._conn

        class FailingRunConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            @property
            def description(self):
                return self.wrapped.description

            def execute(self, query, *args, **kwargs):
                if "INSERT INTO runs" in str(query):
                    raise RuntimeError("run insert failed")
                return self.wrapped.execute(query, *args, **kwargs)

            def executemany(self, query, *args, **kwargs):
                return self.wrapped.executemany(query, *args, **kwargs)

            def close(self):
                return self.wrapped.close()

        monkeypatch.setattr(store, "_conn", FailingRunConnection(original_conn))
        with pytest.raises(RuntimeError, match="run insert failed"):
            store.run_command_profiled(command["id"])
        assert store._decode_json_fields({"raw": "not json"}, ["raw"])["raw"] == "not json"
    finally:
        store.close()


def test_existing_schema_migration(tmp_path):
    db = tmp_path / "coverage.duckdb"
    import duckdb

    conn = duckdb.connect(db.as_posix())
    conn.execute(
        """
        CREATE TABLE lines (
            snapshot_id VARCHAR,
            file_path VARCHAR,
            line_number INTEGER,
            hits INTEGER,
            covered BOOLEAN,
            total_branches INTEGER,
            covered_branches INTEGER,
            total_functions INTEGER,
            covered_functions INTEGER,
            details VARCHAR
        )
        """
    )
    conn.close()
    store = CoverageStore(db)
    try:
        columns = {row[1] for row in store._conn.execute("PRAGMA table_info('lines')").fetchall()}
        assert "count_line" in columns
    finally:
        store.close()


def test_index_creation_failures_do_not_block_schema_init():
    class IndexFailingConnection:
        description = [("cid",), ("name",)]

        def __init__(self):
            self.index_failures = 0

        def execute(self, query, *args, **kwargs):
            if str(query).startswith("CREATE INDEX"):
                self.index_failures += 1
                raise duckdb.Error("index unavailable")
            return self

        def fetchall(self):
            return [(0, "count_line")]

    store = CoverageStore.__new__(CoverageStore)
    store._lock = threading.RLock()
    store._conn = IndexFailingConnection()

    CoverageStore._init_schema(store)

    assert store._conn.index_failures == 8


def test_git_metadata_is_captured_for_registered_commands(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    (repo / "run.py").write_text("print('ok')\n", encoding="utf-8")
    subprocess.run(["git", "add", "run.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="git-suite",
            command=f"{sys.executable} run.py",
            cwd=repo.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved git command",
        )
        assert command["branch"] == "main"
        assert command["commit_sha"]
    finally:
        store.close()


def test_worktree_compare_without_current_branch_snapshot(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    (repo / "src").mkdir()
    (repo / "src" / "a.py").write_text("one\n", encoding="utf-8")
    report = repo / "base.lcov"
    report.write_text("TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repo, check=True, capture_output=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(["git", "checkout", "-b", "feature"], cwd=repo, check=True, capture_output=True)
    (repo / "src" / "a.py").write_text("one\ntwo\n", encoding="utf-8")
    subprocess.run(["git", "commit", "-am", "feature"], cwd=repo, check=True, capture_output=True)

    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        store.ingest_report(
            report.as_posix(),
            format="lcov",
            repo_path=repo.as_posix(),
            branch="main",
            commit_sha=base_sha,
        )
        worktree = store.register_worktree(repo.as_posix(), base_ref="main")

        with pytest.raises(KeyError, match="no current snapshot"):
            store.compare_worktree(worktree["id"])
    finally:
        store.close()
