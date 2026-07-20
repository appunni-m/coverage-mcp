from __future__ import annotations

import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from coverage_mcp import storage as storage_module
from coverage_mcp.models import CoverageReport, FileCoverage, LineCoverage
from coverage_mcp.storage import COLLECTION_FETCH_LIMIT, CoverageStore, collection_query_limit
from coverage_mcp.storage_helpers import (
    bounded_log_text,
    compact_run_result,
    format_age,
    infer_topology,
    is_interesting_log_line,
    merge_counters,
    normalize_artifact_specs,
    normalize_log_queries,
    percent,
    percent_delta,
    profile_log,
    relative_age,
    search_log_file,
    summarize_coverage_ingest,
    summarize_run_logs,
    truncate_to_word_budget,
    update_log_counters,
)


def make_lcov(path: Path, *, file_path: str = "src/a.py", hits: tuple[int, ...] = (1, 0)) -> None:
    rows = [f"DA:{index},{hit}" for index, hit in enumerate(hits, start=1)]
    path.write_text(f"TN:\nSF:{file_path}\n" + "\n".join(rows) + "\nend_of_record\n", encoding="utf-8")


def test_collection_query_limit_uses_one_shared_overflow_ceiling():
    assert collection_query_limit(0) == 1
    assert collection_query_limit(42) == 42
    assert collection_query_limit(COLLECTION_FETCH_LIMIT + 100) == COLLECTION_FETCH_LIMIT


def test_file_gaps_groups_only_relevant_lines_and_paginates_ranges(tmp_path):
    file_path = "src/gaps.py"
    report = CoverageReport(
        format="synthetic",
        report_path=(tmp_path / "coverage.json").as_posix(),
        files=[
            FileCoverage(
                file_path=file_path,
                total_lines=5,
                covered_lines=1,
                total_branches=2,
                covered_branches=1,
                total_functions=1,
                covered_functions=0,
            )
        ],
        lines=[
            LineCoverage(file_path, 1),
            LineCoverage(file_path, 2),
            LineCoverage(file_path, 3, hits=1, covered=True, total_branches=2, covered_branches=1),
            LineCoverage(file_path, 4, total_functions=1),
            LineCoverage(file_path, 5),
        ],
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        snapshot_id = store.store_report(
            report,
            repo_path=tmp_path.as_posix(),
            repo_key=tmp_path.as_posix(),
            branch="main",
            commit_sha="abc",
            base_ref=None,
            suite="unit",
        )

        first = store.file_gaps(snapshot_id, file_path, max_ranges=2)
        assert first == {
            "total_relevant_lines": 5,
            "uncovered_line_count": 4,
            "partial_branch_line_count": 1,
            "uncovered_function_line_count": 1,
            "returned_range_count": 2,
            "truncated": True,
            "next_start_line": 4,
            "ranges": [
                {
                    "start_line": 1,
                    "end_line": 2,
                    "line_count": 2,
                    "reasons": ["uncovered"],
                    "missed_branches": 0,
                    "missed_functions": 0,
                },
                {
                    "start_line": 3,
                    "end_line": 3,
                    "line_count": 1,
                    "reasons": ["partial_branch"],
                    "missed_branches": 1,
                    "missed_functions": 0,
                },
            ],
        }
        continuation = store.file_gaps(snapshot_id, file_path, start_line=first["next_start_line"], max_ranges=1000)
        assert continuation["truncated"] is False
        assert continuation["next_start_line"] is None
        assert [gap["reasons"] for gap in continuation["ranges"]] == [
            ["uncovered", "uncovered_function"],
            ["uncovered"],
        ]
        selected = store.lines_in_ranges(
            snapshot_id,
            file_path,
            [
                {"start": 4, "end": 5},
                {"start": 2, "end": 3},
                {"start": 3, "end": 4},
                {"start": 2, "end": 2},
            ],
        )
        assert selected["requested_ranges"] == [{"start": 2, "end": 5}]
        assert selected["requested_line_count"] == 4
        assert selected["returned_line_count"] == 4
        assert selected["unrecorded_line_count"] == 0
        assert [line["line_number"] for line in selected["lines"]] == [2, 3, 4, 5]
        assert selected["lines"][1]["covered"] is True
        assert "details" not in selected["lines"][1]
        assert store.lines_in_ranges(snapshot_id, file_path, []) == {
            "requested_ranges": [],
            "requested_line_count": 0,
            "returned_line_count": 0,
            "unrecorded_line_count": 0,
            "lines": [],
        }
        store._conn.execute(
            "INSERT INTO lines SELECT * FROM lines WHERE snapshot_id = ? AND line_number = 3", [snapshot_id]
        )
        deduplicated = store.lines_in_ranges(snapshot_id, file_path, [{"start": 3, "end": 3}] * 10)
        assert deduplicated["requested_ranges"] == [{"start": 3, "end": 3}]
        assert deduplicated["returned_line_count"] == 1
        boundary = store.lines_in_ranges(
            snapshot_id,
            file_path,
            [{"start": 1, "end": 100}, {"start": 50, "end": 200}],
        )
        assert boundary["requested_ranges"] == [{"start": 1, "end": 200}]
        assert boundary["requested_line_count"] == 200
        assert boundary["returned_line_count"] == 5
        assert boundary["unrecorded_line_count"] == 195
        with pytest.raises(ValueError, match="positive"):
            store.lines_in_ranges(snapshot_id, file_path, [{"start": 0, "end": 1}])
        with pytest.raises(ValueError, match="end"):
            store.lines_in_ranges(snapshot_id, file_path, [{"start": 3, "end": 2}])
        with pytest.raises(ValueError, match="10 ranges"):
            store.lines_in_ranges(snapshot_id, file_path, [{"start": 1, "end": 1}] * 11)
        with pytest.raises(ValueError, match="200 lines"):
            store.lines_in_ranges(snapshot_id, file_path, [{"start": 1, "end": 201}])
    finally:
        store.close()


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
        assert (
            store.changed_lines(
                snapshot_id=snapshot["id"],
                baseline_snapshot_id=snapshot["id"],
                only_regressions=True,
            )
            == []
        )

        worktree = store.register_worktree(tmp_path.as_posix(), base_ref="no-such-branch")
        with pytest.raises(ValueError, match="predates"):
            store.compare_worktree(worktree["id"], snapshot_id=snapshot["id"])
        with pytest.raises(KeyError, match="baseline"):
            store.worktree_progress(worktree["id"])
        with pytest.raises(KeyError, match="file"):
            store.file_coverage(snapshot["id"], "missing.py")
        assert store.object_topology("project", tmp_path.as_posix())["topology"]["kind"] == "project"
        with pytest.raises(KeyError, match="project"):
            store.object_topology("project", "missing")
        with pytest.raises(ValueError, match="unsupported"):
            store.object_topology("unsupported", "x")
    finally:
        store.close()


def test_comparison_and_source_lineage_guards(monkeypatch, tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        snapshots = {
            "repo-a": {"id": "repo-a", "repo_key": "a", "repo_path": "/a", "suite": "unit"},
            "repo-b": {"id": "repo-b", "repo_key": "b", "repo_path": "/b", "suite": "unit"},
            "suite-b": {"id": "suite-b", "repo_key": "a", "repo_path": "/a", "suite": "integration"},
        }
        monkeypatch.setattr(store, "snapshot", lambda snapshot_id: snapshots[snapshot_id])
        with pytest.raises(ValueError, match="same repository"):
            store.compare(snapshot_id="repo-a", baseline_snapshot_id="repo-b")
        with pytest.raises(ValueError, match="same suite"):
            store.compare(snapshot_id="repo-a", baseline_snapshot_id="suite-b")
        with pytest.raises(ValueError, match="same repository"):
            store.changed_lines(snapshot_id="repo-a", baseline_snapshot_id="repo-b")
        with pytest.raises(ValueError, match="same suite"):
            store.changed_lines(snapshot_id="repo-a", baseline_snapshot_id="suite-b")
        monkeypatch.setattr(store, "snapshot", lambda _snapshot_id: snapshots["repo-a"])
        assert store.changed_lines(snapshot_id="repo-a", baseline_snapshot_id="repo-a", file_path="a.py") == []

        worktree = {
            "id": "worktree",
            "repo_key": "a",
            "path": "/worktree",
            "created_at": "2026-01-01T00:00:00Z",
        }
        monkeypatch.setattr(store, "worktree", lambda _worktree_id: worktree)
        monkeypatch.setattr(store, "snapshot", lambda _snapshot_id: snapshots["repo-a"])
        with pytest.raises(ValueError, match="selected worktree"):
            store.compare_worktree("worktree", snapshot_id="repo-a")
        monkeypatch.setattr(store, "trend", lambda **kwargs: [])
        with pytest.raises(KeyError, match="no current snapshot"):
            store.compare_worktree("worktree")
        with pytest.raises(KeyError, match="no baseline snapshot"):
            store._worktree_baseline_snapshot_id({**worktree, "baseline_snapshot_id": None}, "unit")

        monkeypatch.setattr(
            store,
            "snapshot",
            lambda _snapshot_id: {"repo_path": tmp_path.as_posix(), "suite": "unit"},
        )
        with pytest.raises(ValueError, match="escapes"):
            store.source_lines(snapshot_id="snapshot", file_path="../outside.py", start=1, end=1)
        with pytest.raises(FileNotFoundError):
            store.source_lines(snapshot_id="snapshot", file_path="missing.py", start=1, end=1)
    finally:
        store.close()


def test_trend_returns_all_available_coverage_dimensions(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        report = CoverageReport(
            format="llvm",
            report_path="coverage.json",
            files=[
                FileCoverage(
                    file_path="src/a.cc",
                    total_lines=10,
                    covered_lines=9,
                    total_branches=8,
                    covered_branches=6,
                    total_functions=4,
                    covered_functions=3,
                    total_regions=20,
                    covered_regions=17,
                )
            ],
            lines=[],
        )
        store.store_report(
            report,
            repo_path=tmp_path.as_posix(),
            repo_key=tmp_path.as_posix(),
            branch="main",
            commit_sha="abc",
            base_ref=None,
            suite="unit",
        )

        overall = store.trend(repo_path=tmp_path.as_posix())
        file_trend = store.trend(repo_path=tmp_path.as_posix(), file_path="src/a.cc")

        assert overall[0]["line_rate"] == pytest.approx(0.9)
        assert overall[0]["branch_rate"] == pytest.approx(0.75)
        assert overall[0]["function_rate"] == pytest.approx(0.75)
        assert overall[0]["region_rate"] == pytest.approx(0.85)
        assert file_trend[0]["total_regions"] == 20
        assert file_trend[0]["covered_regions"] == 17
    finally:
        store.close()


def test_bulk_copy_preserves_structured_data_and_removes_batches(tmp_path):
    file_path = 'src/a,"quoted".py'
    report = CoverageReport(
        format="test",
        report_path="coverage.json",
        files=[
            FileCoverage(
                file_path=file_path,
                total_lines=1,
                covered_lines=1,
                raw_metrics={"note": 'comma, quote " and\nnewline'},
            )
        ],
        lines=[
            LineCoverage(
                file_path=file_path,
                line_number=7,
                hits=3,
                covered=True,
                details={"condition": 'left, "right"\nnext'},
            )
        ],
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        snapshot_id = store.store_report(
            report,
            repo_path=tmp_path.as_posix(),
            repo_key=tmp_path.as_posix(),
            branch="main",
            commit_sha="abc",
            base_ref=None,
            suite="unit",
        )

        file = store.file_coverage(snapshot_id, file_path)
        lines = store.lines(snapshot_id, file_path)

        assert file["raw_metrics"] == {"note": 'comma, quote " and\nnewline'}
        assert file["branch_rate"] is None
        assert lines[0]["details"] == {"condition": 'left, "right"\nnext'}
        assert not list(tmp_path.glob("coverage-mcp-*.csv"))
    finally:
        store.close()


def test_worktree_progress_isolated_from_other_lineages(tmp_path):
    repo = tmp_path / "repo"
    worktree_path = tmp_path / "feature"
    other_path = tmp_path / "other"
    repo.mkdir()
    other_path.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    (repo / "file.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repo, check=True, capture_output=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        baseline_id = store.store_report(
            CoverageReport(
                format="llvm",
                report_path="base.json",
                files=[FileCoverage("src/a.cc", total_lines=10, covered_lines=7, total_branches=4, covered_branches=2)],
                lines=[],
            ),
            repo_path=repo.as_posix(),
            repo_key=repo.as_posix(),
            branch="main",
            commit_sha=base_sha,
            base_ref=None,
            suite="unit",
        )
        integration_baseline_id = store.store_report(
            CoverageReport(
                format="llvm",
                report_path="integration-base.json",
                files=[FileCoverage("src/a.cc", total_lines=10, covered_lines=5, total_branches=4, covered_branches=1)],
                lines=[],
            ),
            repo_path=repo.as_posix(),
            repo_key=repo.as_posix(),
            branch="main",
            commit_sha=base_sha,
            base_ref=None,
            suite="integration",
        )
        subprocess.run(
            ["git", "worktree", "add", "-b", "feature", worktree_path.as_posix(), "main"],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        worktree = store.register_worktree(worktree_path.as_posix(), base_ref="main", name="feature")
        store.store_report(
            CoverageReport(
                format="llvm",
                report_path="late-main.json",
                files=[
                    FileCoverage(
                        "src/a.cc",
                        total_lines=10,
                        covered_lines=10,
                        total_branches=4,
                        covered_branches=4,
                    )
                ],
                lines=[],
            ),
            repo_path=repo.as_posix(),
            repo_key=repo.as_posix(),
            branch="main",
            commit_sha=base_sha,
            base_ref=None,
            suite="unit",
        )
        current_id = store.store_report(
            CoverageReport(
                format="llvm",
                report_path="current.json",
                files=[FileCoverage("src/a.cc", total_lines=10, covered_lines=9, total_branches=4, covered_branches=3)],
                lines=[],
            ),
            repo_path=worktree_path.as_posix(),
            repo_key=repo.as_posix(),
            branch="feature",
            commit_sha="feature-head",
            base_ref="main",
            suite="unit",
        )
        store.store_report(
            CoverageReport(
                format="llvm",
                report_path="unrelated.json",
                files=[FileCoverage("src/a.cc", total_lines=10, covered_lines=1, total_branches=4, covered_branches=0)],
                lines=[],
            ),
            repo_path=other_path.as_posix(),
            repo_key=repo.as_posix(),
            branch="feature",
            commit_sha="other-head",
            base_ref="main",
            suite="unit",
        )

        progress = store.worktree_progress(worktree["id"], suite="unit")
        default_progress = store.worktree_progress(worktree["id"])
        integration_progress = store.worktree_progress(worktree["id"], suite="integration")
        file_progress = store.worktree_progress(worktree["id"], suite="unit", file_path="src/a.cc")
        comparison = store.compare_worktree(worktree["id"])

        assert worktree["baseline_snapshot_id"] == integration_baseline_id
        assert progress["baseline"]["id"] == baseline_id
        assert progress["current"]["id"] == current_id
        assert [point["id"] for point in progress["points"]] == [baseline_id, current_id]
        assert progress["deltas"]["line_rate"] == pytest.approx(0.2)
        assert progress["deltas"]["branch_rate"] == pytest.approx(0.25)
        assert default_progress["suite"] == "unit"
        assert default_progress["baseline"]["id"] == baseline_id
        assert integration_progress["baseline"]["id"] == integration_baseline_id
        assert integration_progress["current"] is None
        assert file_progress["baseline"]["file_path"] == "src/a.cc"
        assert file_progress["current"]["line_rate"] == pytest.approx(0.9)
        assert comparison["current"]["id"] == current_id
        assert comparison["baseline"]["id"] == baseline_id
        with pytest.raises(KeyError, match="suite missing"):
            store.worktree_progress(worktree["id"], suite="missing")
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
        worktree = store.register_worktree(tmp_path.as_posix(), base_ref="main")
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
    script.write_text("import time\ntime.sleep(0.1)\nprint('1 passed')\n", encoding="utf-8")
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
        assert store.run(run["id"])["id"] == run["id"]
        with pytest.raises(KeyError, match="run not found"):
            store.run("missing")
        assert store.latest_run()["id"] == run["id"]
        assert store.latest_run(command_ref="suite")["id"] == run["id"]
        assert store.latest_run(command_ref="unknown") is None
        pending = store.submit_command_profiled(command["id"])
        assert store.latest_run()["id"] == pending["id"]
        assert store.latest_run(command_ref="suite")["id"] == pending["id"]
        store.wait_for_run(pending["id"])
        assert store._run_job("missing") is None
        store._execute_run_job("missing")
        artifact = store.latest_artifact(command_ref="suite", kind="missing")
        assert artifact is not None
        assert artifact["exists"] is False
        assert store.latest_artifact(command_ref="unknown", kind="missing") is None
    finally:
        store.close()


def test_managed_run_reports_stale_missing_and_invalid_coverage_artifacts(tmp_path):
    make_lcov(tmp_path / "stale.lcov")
    (tmp_path / "coverage-dir").mkdir()
    script = tmp_path / "artifacts.py"
    script.write_text(
        "from pathlib import Path\nPath('invalid.xml').write_text('<coverage>')\nprint('1 passed')\n",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="artifact-check",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            artifact_paths={
                "stale": {"path": "stale.lcov", "coverage_format": "lcov"},
                "invalid": {"path": "invalid.xml", "coverage_format": "cobertura"},
                "missing": {"path": "missing.lcov", "coverage_format": "lcov", "required": True},
                "directory": {"path": "coverage-dir", "coverage_format": "lcov"},
            },
            human_approved=True,
            approved_by="tester",
            approval_note="approved coverage artifact state test",
        )

        run = store.run_command_profiled(command["id"])
        artifacts = {artifact["kind"]: artifact for artifact in run["artifact_paths"]}

        assert run["status"] == "passed"
        assert run["coverage_ingest"]["status"] == "failed"
        assert run["coverage_ingest"]["failed_artifacts"] == 3
        assert run["coverage_ingest"]["skipped_artifacts"] == 1
        assert artifacts["stale"]["ingest_status"] == "skipped_stale"
        assert artifacts["invalid"]["modified_by_run"] is True
        assert artifacts["invalid"]["ingest_status"] == "failed"
        assert "ParseError" in artifacts["invalid"]["ingest_error"]
        assert artifacts["invalid"]["suite"] == "artifact-check"
        assert artifacts["missing"]["ingest_status"] == "missing"
        assert artifacts["directory"]["ingest_status"] == "failed"
        assert store.list_snapshots(limit=10) == []

        pending = store._collect_run_artifacts(
            [{"kind": "coverage", "path": "stale.lcov", "coverage_format": "lcov"}],
            tmp_path.as_posix(),
            previous_states={"coverage": None},
        )
        store._auto_ingest_run_artifacts(
            pending,
            job={"repo_path": tmp_path.as_posix(), "branch": None, "commit_sha": None},
            command_name="cancelled-suite",
            eligible=False,
        )
        assert pending[0]["ingest_status"] == "skipped_run_status"
        assert summarize_coverage_ingest(pending)["status"] == "skipped_run_status"
    finally:
        store.close()


def test_run_queue_recovers_pending_jobs_and_marks_active_job_interrupted(tmp_path):
    db_path = tmp_path / "coverage.duckdb"
    store = CoverageStore(db_path, run_concurrency=1)
    command = store.register_command(
        name="recoverable-suite",
        command=f"{sys.executable} -c 'import time; time.sleep(1); print(\"1 passed\")'",
        cwd=tmp_path.as_posix(),
        human_approved=True,
        approved_by="tester",
        approval_note="approved queue recovery test",
    )
    active = store.submit_command_profiled(command["id"])
    for _ in range(100):
        active = store.run_result(active["id"])
        if active["status"] == "running":
            break
        time.sleep(0.01)
    assert active["status"] == "running"
    interrupted = store.submit_command_profiled(command["id"])
    stale_interrupted = store.submit_command_profiled(command["id"])
    pending = store.submit_command_profiled(command["id"])
    store.close()

    with pytest.raises(RuntimeError, match="shutting down"):
        store.submit_command_profiled(command["id"])

    conn = duckdb.connect(db_path.as_posix())
    conn.execute(
        "UPDATE run_jobs SET status = 'running', started_at = ? WHERE id = ?",
        [datetime.now(UTC), interrupted["id"]],
    )
    conn.execute(
        """
        UPDATE run_jobs
        SET status = 'interrupted', ended_at = ?, error = 'older interruption'
        WHERE id = ?
        """,
        [datetime.now(UTC) - timedelta(days=1), stale_interrupted["id"]],
    )
    conn.close()

    recovered = CoverageStore(db_path, run_retention=3, run_concurrency=1)
    try:
        interrupted_result = recovered.run_result(interrupted["id"])
        pending_result = recovered.wait_for_run(pending["id"])
        active_result = recovered.run_result(active["id"])

        assert interrupted_result["status"] == "interrupted"
        assert interrupted_result["terminal"] is True
        assert interrupted_result["queue_position"] is None
        assert "restarted" in interrupted_result["error"]
        with pytest.raises(ValueError, match="already terminal"):
            recovered.cancel_run(interrupted["id"])
        assert pending_result["status"] == "passed"
        assert pending_result["terminal"] is True
        assert pending_result["queue_duration_ms"] >= 0
        assert active_result["status"] == "passed"
        with pytest.raises(KeyError, match="run not found"):
            recovered.run_result(stale_interrupted["id"])
        assert not (tmp_path / "runs" / stale_interrupted["id"]).exists()
        assert recovered.list_run_queue() == []
    finally:
        recovered.close()


def test_run_execution_error_is_recorded_without_killing_worker(monkeypatch, tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="missing-shell",
            command="echo unreachable",
            cwd=tmp_path.as_posix(),
            shell=(tmp_path / "missing-shell").as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved execution failure test",
        )

        result = store.run_command_profiled(command["id"])

        assert result["status"] == "failed"
        assert result["exit_code"] is None
        assert "FileNotFoundError" in result["parsed_summary"]["execution_error"]

        started = store.register_command(
            name="failed-after-start",
            command=f"{sys.executable} -c 'import time; time.sleep(30)'",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved post-start cleanup test",
        )

        def fail_wait(*_args, **_kwargs):
            raise RuntimeError("wait failed")

        with monkeypatch.context() as patch:
            patch.setattr(store, "_wait_for_process", fail_wait)
            failed_after_start = store.run_command_profiled(started["id"])
        assert failed_after_start["status"] == "failed"
        assert "wait failed" in failed_after_start["parsed_summary"]["execution_error"]
        assert store._active_processes == {}
    finally:
        store.close()


def test_run_retention_is_count_based_and_isolated_per_command(monkeypatch, tmp_path):
    db_path = tmp_path / "coverage.duckdb"
    with pytest.raises(ValueError, match="at least 1"):
        CoverageStore(db_path, run_retention=0)
    with pytest.raises(ValueError, match="between 1 and 32"):
        CoverageStore(db_path, run_concurrency=0)
    with pytest.raises(ValueError, match="between 1 and 32"):
        CoverageStore(db_path, run_concurrency=33)

    script = tmp_path / "run.py"
    script.write_text(
        "from pathlib import Path\nPath('artifact.txt').write_text('ok')\nprint('1 passed')\n",
        encoding="utf-8",
    )
    store = CoverageStore(db_path, run_retention=10)
    primary = store.register_command(
        name="primary",
        command=f"{sys.executable} {script.name}",
        cwd=tmp_path.as_posix(),
        artifact_paths={"text": "artifact.txt"},
        human_approved=True,
        approved_by="tester",
        approval_note="approved retention test",
    )
    secondary = store.register_command(
        name="secondary",
        command=f"{sys.executable} {script.name}",
        cwd=tmp_path.as_posix(),
        artifact_paths={"text": "artifact.txt"},
        human_approved=True,
        approved_by="tester",
        approval_note="approved command isolation test",
    )
    primary_runs = [store.run_command_profiled(primary["id"]) for _ in range(3)]
    secondary_run = store.run_command_profiled(secondary["id"])
    oldest_log_dir = Path(primary_runs[0]["stdout_path"]).parent
    assert oldest_log_dir.exists()
    original_conn = store._conn

    class FailingRetentionConnection:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        @property
        def description(self):
            return self.wrapped.description

        def execute(self, query, *args, **kwargs):
            return self.wrapped.execute(query, *args, **kwargs)

        def executemany(self, query, *args, **kwargs):
            if "DELETE FROM runs" in str(query):
                raise RuntimeError("retention delete failed")
            return self.wrapped.executemany(query, *args, **kwargs)

        def close(self):
            return self.wrapped.close()

    store.run_retention = 2
    with monkeypatch.context() as patch:
        patch.setattr(store, "_conn", FailingRetentionConnection(original_conn))
        with pytest.raises(RuntimeError, match="retention delete failed"):
            store._prune_run_history(primary["id"])
        assert store.run_result(primary_runs[0]["id"])["status"] == "passed"
        assert store._conn.execute(
            "SELECT count(*) FROM run_artifacts WHERE run_id = ?",
            [primary_runs[0]["id"]],
        ).fetchone() == (1,)
    store.run_retention = 10
    store.close()

    retained = CoverageStore(db_path, run_retention=2)
    try:
        with pytest.raises(KeyError, match="run not found"):
            retained.run_result(primary_runs[0]["id"])
        assert retained.run_result(primary_runs[1]["id"])["status"] == "passed"
        assert retained.run_result(primary_runs[2]["id"])["status"] == "passed"
        assert retained.run_result(secondary_run["id"])["status"] == "passed"
        assert not oldest_log_dir.exists()
        assert retained._conn.execute(
            "SELECT count(*) FROM run_artifacts WHERE run_id = ?",
            [primary_runs[0]["id"]],
        ).fetchone() == (0,)
        assert (tmp_path / "artifact.txt").exists()
        assert retained._prune_run_history(primary["id"]) == 0

        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "stdout.log").write_text("keep", encoding="utf-8")
        retained._remove_managed_run_logs(["../outside"])
        assert (outside / "stdout.log").exists()
    finally:
        retained.close()


def test_command_duration_history_drives_running_and_queued_eta(tmp_path):
    script = tmp_path / "timed.py"
    script.write_text(
        """import sys
import time
from pathlib import Path

time.sleep(float(Path(sys.argv[1]).read_text()))
print("1 passed")
""",
        encoding="utf-8",
    )
    primary_delay = tmp_path / "primary-delay.txt"
    secondary_delay = tmp_path / "secondary-delay.txt"
    unknown_delay = tmp_path / "unknown-delay.txt"
    for path in (primary_delay, secondary_delay, unknown_delay):
        path.write_text("0.001", encoding="utf-8")

    store = CoverageStore(tmp_path / "coverage.duckdb", run_concurrency=1)
    try:
        primary = store.register_command(
            name="timed-primary",
            command=f"{sys.executable} {script.name} {primary_delay.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved primary ETA test command",
        )
        secondary = store.register_command(
            name="timed-secondary",
            command=f"{sys.executable} {script.name} {secondary_delay.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved secondary ETA test command",
        )
        unknown = store.register_command(
            name="timed-unknown",
            command=f"{sys.executable} {script.name} {unknown_delay.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved no-history ETA test command",
        )
        assert primary["duration_estimate_ms"] is None
        assert primary["duration_sample_count"] == 0
        assert store._command_duration_stats("missing")["duration_sample_count"] == 0
        store._refresh_command_duration_stats(unknown["id"])
        assert store.registered_command(unknown["id"])["duration_estimate_ms"] is None

        primary_runs = [store.run_command_profiled(primary["id"]) for _ in range(3)]
        secondary_run = store.run_command_profiled(secondary["id"])
        for run, duration_ms in zip(primary_runs, (100, 200, 1000), strict=True):
            store._conn.execute("UPDATE runs SET duration_ms = ? WHERE id = ?", [duration_ms, run["id"]])
        store._conn.execute("UPDATE runs SET duration_ms = 400 WHERE id = ?", [secondary_run["id"]])
        store._refresh_command_duration_stats(primary["id"])
        store._refresh_command_duration_stats(secondary["id"])

        learned = store.registered_command(primary["id"])
        assert learned["duration_estimate_ms"] == 200
        assert learned["duration_p90_ms"] == 840
        assert learned["duration_sample_count"] == 3
        assert learned["duration_stats_updated_at"]

        primary_delay.write_text("0.8", encoding="utf-8")
        secondary_delay.write_text("0.8", encoding="utf-8")
        running = store.submit_command_profiled(primary["id"])
        for _ in range(100):
            running = store.run_result(running["id"])
            if running["status"] == "running":
                break
            time.sleep(0.01)
        queued = store.submit_command_profiled(secondary["id"])
        queued = store.run_result(queued["id"])

        assert running["duration_estimate_ms"] == 200
        assert running["duration_p90_ms"] == 840
        assert running["eta_seconds"] is not None
        assert not running["eta"].endswith("ago")
        assert running["estimated_completion_at"].endswith("Z")
        assert queued["queue_position"] == 1
        assert queued["duration_estimate_ms"] == 400
        assert queued["queue_wait_estimate_seconds"] is not None
        assert queued["eta_seconds"] >= queued["queue_wait_estimate_seconds"]
        assert queued["estimated_start_at"].endswith("Z")
        assert queued["estimated_completion_at"].endswith("Z")

        time.sleep(0.25)
        overrun = store.run_result(running["id"])
        assert overrun["status"] == "running"
        assert overrun["eta_seconds"] == 0
        assert overrun["estimate_overrun_seconds"] >= 1
        store.cancel_run(queued["id"])
        cancelled = store.cancel_run(running["id"])
        if not cancelled["terminal"]:
            cancelled = store.wait_for_run(running["id"])
        assert cancelled["eta_seconds"] == 0
        assert cancelled["eta"] == "0 seconds"
        assert store.registered_command(primary["id"])["duration_sample_count"] == 3

        unknown_delay.write_text("0.8", encoding="utf-8")
        no_history = store.submit_command_profiled(unknown["id"])
        for _ in range(100):
            no_history = store.run_result(no_history["id"])
            if no_history["status"] == "running":
                break
            time.sleep(0.01)
        assert no_history["eta_seconds"] is None
        assert no_history["eta_unavailable_reason"] == "no_command_history"

        blocked_eta = store.submit_command_profiled(secondary["id"])
        blocked_eta = store.run_result(blocked_eta["id"])
        assert blocked_eta["eta_seconds"] is None
        assert blocked_eta["queue_wait_estimate_seconds"] is None
        assert blocked_eta["eta_unavailable_reason"] == "queue_history_incomplete"
        assert store._queue_wait_estimate_ms("missing", datetime.now(UTC).replace(tzinfo=None)) == (
            None,
            "run_state_changed",
        )
        store.cancel_run(blocked_eta["id"])
        store.cancel_run(no_history["id"])
        store.wait_for_run(no_history["id"])
        assert store.registered_command(unknown["id"])["duration_sample_count"] == 0
    finally:
        store.close()


def test_four_worker_pool_runs_in_parallel_and_estimates_by_lane(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="parallel-suite",
            command=f"{sys.executable} -c 'import time; time.sleep(2)'",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved four-worker concurrency test",
        )
        store._conn.execute(
            """
            UPDATE registered_commands
            SET duration_estimate_ms = 2000, duration_p90_ms = 2000,
                duration_sample_count = 3, duration_stats_updated_at = ?
            WHERE id = ?
            """,
            [datetime.now(UTC), command["id"]],
        )

        runs = [store.submit_command_profiled(command["id"], idempotency_key=f"parallel-{index}") for index in range(5)]
        for _ in range(200):
            queue = store.list_run_queue(limit=10)
            if sum(item["status"] == "running" for item in queue) == 4:
                break
            time.sleep(0.01)

        running = [item for item in queue if item["status"] == "running"]
        queued = [item for item in queue if item["status"] == "queued"]
        assert store.run_concurrency == 4
        assert len(store._workers) == 4
        assert len(running) == 4
        assert len(queued) == 1
        assert queued[0]["queue_position"] == 1
        assert 0 <= queued[0]["queue_wait_estimate_seconds"] <= 2
        assert queued[0]["eta_seconds"] <= 4

        store.cancel_run(queued[0]["id"])
        for item in running:
            store.cancel_run(item["id"])
        assert {store.wait_for_run(run["id"])["status"] for run in runs} == {"cancelled"}
    finally:
        store.close()


def test_parallel_queue_eta_handles_lane_scheduling_edges(monkeypatch, tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb", run_concurrency=2)
    now = datetime.now(UTC).replace(tzinfo=None)

    class QueueRowsConnection:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, _query):
            return self

        def fetchall(self):
            return self.rows

    def estimate(rows):
        with monkeypatch.context() as patch:
            patch.setattr(store, "_conn", QueueRowsConnection(rows))
            return store._queue_wait_estimate_ms("target", now)

    try:
        assert estimate(
            [
                ("running-1", "running", now, 1000),
                ("running-2", "running", now, 1000),
                ("running-3", "running", now, 1000),
                ("target", "queued", None, 500),
            ]
        ) == (None, "queue_history_incomplete")
        assert estimate(
            [
                ("running", "running", now, 1000),
                ("predecessor", "queued", None, 500),
                ("target", "queued", None, 500),
            ]
        ) == (500, None)
        assert estimate(
            [
                ("running-1", "running", now, 1000),
                ("running-2", "running", now, 2000),
                ("predecessor", "queued", None, 500),
                ("target", "queued", None, 500),
            ]
        ) == (1500, None)
        assert estimate(
            [
                ("running-1", "running", now, 1000),
                ("running-2", "running", now, 2000),
                ("predecessor", "queued", None, None),
                ("target", "queued", None, 500),
            ]
        ) == (None, "queue_history_incomplete")
    finally:
        store.close()


def test_run_submission_idempotency_is_atomic_and_command_scoped(monkeypatch, tmp_path):
    script = tmp_path / "run.py"
    script.write_text(
        """from pathlib import Path
import time
path = Path("executions.txt")
path.write_text(path.read_text() + "run\\n" if path.exists() else "run\\n")
time.sleep(0.1)
print("1 passed")
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        primary = store.register_command(
            name="idempotent-primary",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved idempotency concurrency test",
        )
        secondary = store.register_command(
            name="idempotent-secondary",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved command-scoped idempotency test",
        )

        with ThreadPoolExecutor(max_workers=5) as executor:
            submissions = list(
                executor.map(
                    lambda _index: store.submit_command_profiled(
                        primary["id"],
                        idempotency_key="  task-42  ",
                    ),
                    range(5),
                )
            )

        assert len({item["id"] for item in submissions}) == 1
        assert sum(not item["submission_reused"] for item in submissions) == 1
        primary_result = store.wait_for_run(submissions[0]["id"])
        assert primary_result["idempotency_key"] == "task-42"
        repeated = store.submit_command_profiled(primary["id"], idempotency_key="task-42")
        assert repeated["id"] == primary_result["id"]
        assert repeated["terminal"] is True
        assert repeated["submission_reused"] is True

        existing_dirs = set((tmp_path / "runs").iterdir())
        lookups = iter([None, primary_result["id"]])
        with monkeypatch.context() as patch:
            patch.setattr(store, "_idempotent_run_id", lambda _command_id, _key: next(lookups))
            raced = store.submit_command_profiled(primary["id"], idempotency_key="simulated-race")
        assert raced["id"] == primary_result["id"]
        assert raced["submission_reused"] is True
        assert set((tmp_path / "runs").iterdir()) == existing_dirs

        secondary_result = store.run_command_profiled(secondary["id"], idempotency_key="task-42")
        assert secondary_result["id"] != primary_result["id"]
        assert (tmp_path / "executions.txt").read_text(encoding="utf-8").splitlines() == ["run", "run"]

        with pytest.raises(ValueError, match="blank"):
            store.submit_command_profiled(primary["id"], idempotency_key=" ")
        with pytest.raises(ValueError, match="200"):
            store.submit_command_profiled(primary["id"], idempotency_key="x" * 201)
        with pytest.raises(ValueError, match="max_summary_lines"):
            store.submit_command_profiled(primary["id"], max_summary_lines=501)
        with pytest.raises(ValueError, match="timeout_seconds"):
            store.submit_command_profiled(primary["id"], timeout_seconds=0)
        with pytest.raises(ValueError, match="max_summary_lines"):
            store.run_result(primary_result["id"], max_summary_lines=0)
        with pytest.raises(ValueError, match="max_summary_lines"):
            store.cancel_run(primary_result["id"], max_summary_lines=501)
        with pytest.raises(KeyError, match="run not found"):
            store.search_run_logs("missing", "x")
        for kwargs, message in [
            ({"query": ""}, "query"),
            ({"query": "x", "stream": "invalid"}, "stream"),
            ({"query": "x", "context_lines": 11}, "context_lines"),
            ({"query": "x", "max_matches": 0}, "max_matches"),
            ({"query": "x", "max_words": 19}, "max_words"),
        ]:
            with pytest.raises(ValueError, match=message):
                store.search_run_logs(primary_result["id"], **kwargs)
    finally:
        store.close()


def test_cancel_run_handles_queued_running_and_terminal_states(monkeypatch, tmp_path):
    child_script = tmp_path / "child.py"
    child_script.write_text(
        """import signal
import sys
import time
from pathlib import Path

marker = Path(sys.argv[1])

def stop(_signal, _frame):
    marker.write_text("terminated")
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop)
Path(sys.argv[2]).write_text(str(__import__("os").getpid()))
while True:
    time.sleep(0.1)
""",
        encoding="utf-8",
    )
    parent_script = tmp_path / "parent.py"
    parent_script.write_text(
        """import subprocess
import sys
import time
from pathlib import Path

subprocess.Popen([sys.executable, "child.py", "child-terminated.txt", "child.pid"])
Path("parent-ready.txt").write_text("ready")
while True:
    time.sleep(0.1)
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb", run_concurrency=1)
    try:
        command = store.register_command(
            name="cancellable",
            command=f"{sys.executable} {parent_script.name}",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="approved cancellation process-group test",
        )
        running = store.submit_command_profiled(command["id"], idempotency_key="running")
        for _ in range(200):
            running = store.run_result(running["id"])
            if running["status"] == "running" and (tmp_path / "child.pid").exists():
                break
            time.sleep(0.01)
        assert running["status"] == "running"

        queued = store.submit_command_profiled(command["id"], idempotency_key="queued")
        queued_before_cancel = store._run_job(queued["id"])
        assert queued_before_cancel is not None
        queued_cancelled = store.cancel_run(queued["id"])
        assert queued_cancelled["status"] == "cancelled"
        assert queued_cancelled["terminal"] is True
        assert queued_cancelled["cancellation_requested"] is True
        assert queued_cancelled["cancellation_requested_at"]
        assert store.cancel_run(queued["id"])["status"] == "cancelled"

        cancellation = store.cancel_run(running["id"])
        assert cancellation["status"] in {"running", "cancelled"}
        assert cancellation["cancellation_requested"] is True
        assert cancellation["cancellation_requested_at"]
        cancelled = store.wait_for_run(running["id"])
        assert cancelled["status"] == "cancelled"
        assert cancelled["parsed_summary"]["execution_error"] == "Run cancelled by request."
        assert store.cancel_run(running["id"])["status"] == "cancelled"
        for _ in range(100):
            if queued["id"] not in store._cancel_events:
                break
            time.sleep(0.01)
        with monkeypatch.context() as patch:
            patch.setattr(store, "_run_job", lambda _run_id: queued_before_cancel)
            store._execute_run_job(queued["id"])
        for _ in range(100):
            if (tmp_path / "child-terminated.txt").exists():
                break
            time.sleep(0.01)
        assert (tmp_path / "child-terminated.txt").read_text(encoding="utf-8") == "terminated"

        passed = store.run_command_profiled(
            store.register_command(
                name="short",
                command="echo passed",
                cwd=tmp_path.as_posix(),
                human_approved=True,
                approved_by="tester",
                approval_note="approved terminal cancellation test",
            )["id"]
        )
        with pytest.raises(ValueError, match="already terminal"):
            store.cancel_run(passed["id"])
        with pytest.raises(KeyError, match="run not found"):
            store.cancel_run("missing")
    finally:
        store.close()


def test_process_wait_escalates_cancellation(monkeypatch, tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:

        class FakeProcess:
            pid = 123

            def __init__(self):
                self.results = iter([None, None, 0])

            def poll(self):
                return next(self.results)

        moments = iter([0.0, 0.0, 3.0])
        signals = []
        event = threading.Event()
        event.set()
        monkeypatch.setattr(storage_module.time, "monotonic", lambda: next(moments))
        monkeypatch.setattr(storage_module.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(store, "_signal_process_group", lambda _process, selected: signals.append(selected))

        return_code, timed_out = store._wait_for_process(
            FakeProcess(),
            cancel_event=event,
            timeout_seconds=None,
        )

        assert return_code == 0
        assert timed_out is False
        assert signals == [signal.SIGTERM, signal.SIGKILL, signal.SIGKILL]
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
    assert format_age(0) == "0 seconds ago"
    assert format_age(603) == "10 minutes 3 seconds ago"
    assert format_age(90061) == "1 day 1 hour 1 minute 1 second ago"
    now = datetime(2026, 7, 16, 15, 0, tzinfo=UTC)
    assert relative_age(now - timedelta(seconds=603), now=now) == (603, "10 minutes 3 seconds ago")
    assert relative_age(datetime(2026, 7, 16, 14, 59, 57), now=now) == (3, "3 seconds ago")
    assert relative_age(now + timedelta(seconds=3), now=now) == (0, "0 seconds ago")
    assert relative_age("invalid", now=now) is None
    assert relative_age(None, now=now) is None
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
    compact = compact_run_result(
        {
            "id": "run",
            "command_id": "command",
            "command_name": "unit",
            "status": "failed",
            "terminal": True,
            "duration_ms": 12,
            "exit_code": 1,
            "repo_path": tmp_path.as_posix(),
            "branch": "main",
            "commit_sha": "abc",
            "coverage_ingest": summarize_coverage_ingest([]),
            "poll_after_ms": None,
            "queue_position": None,
            "age_seconds": 3,
            "age": "3 seconds ago",
            "eta_seconds": 0,
            "eta": "0 seconds",
            "estimated_completion_at": "2026-07-16T15:00:00Z",
            "cancellation_requested": False,
            "parsed_summary": summary,
        }
    )
    assert compact["counters"] == {"passed": 2, "skipped": 3, "failed": 1}
    assert compact["command_id"] == "command"
    assert compact["diagnostics_available"] is True
    assert compact["checkout_path"] == tmp_path.as_posix()
    assert "repo_path" not in compact
    assert "estimated_completion_at" not in compact
    assert "parsed_summary" not in compact

    search = search_log_file(
        stdout,
        stream="stdout",
        query="PASSED",
        case_sensitive=False,
        context_lines=1,
        max_matches=2,
        max_words=20,
    )
    assert search["match_count"] == 1
    assert search["returned_match_count"] == 1
    assert search["returned_line_count"] == 3
    assert search["returned_word_count"] == 5
    assert search["contexts"][0]["lines"][1]["match"] is True
    multi = search_log_file(
        stdout,
        stream="stdout",
        query=["PASSED", "skipped"],
        case_sensitive=False,
        context_lines=0,
        max_matches=5,
        max_words=20,
    )
    assert multi["match_count"] == 2
    assert multi["returned_match_count"] == 2
    overlapping = search_log_file(
        stderr,
        stream="stderr",
        query="a",
        case_sensitive=False,
        context_lines=1,
        max_matches=2,
        max_words=20,
    )
    assert overlapping["match_count"] == 2
    assert len(overlapping["contexts"]) == 1
    assert (
        search_log_file(
            tmp_path / "missing.log",
            stream="stderr",
            query="x",
            case_sensitive=True,
            context_lines=0,
            max_matches=1,
            max_words=20,
        )["contexts"]
        == []
    )
    assert truncate_to_word_budget("one two three", max_words=2) == ("one two", 2, True)
    assert truncate_to_word_budget("one two", max_words=3) == ("one two", 2, False)
    assert truncate_to_word_budget("one", max_words=0) == ("", 0, True)
    assert bounded_log_text("short", query="short", case_sensitive=True, matched=True) == "short"
    long_prefix = "x" * 600
    assert bounded_log_text(long_prefix, query="z", case_sensitive=False, matched=False) == "x" * 500
    centered = bounded_log_text(
        long_prefix + " NEEDLE " + "y" * 300,
        query="needle",
        case_sensitive=False,
        matched=True,
    )
    assert centered.startswith("…")
    assert "NEEDLE" in centered
    assert centered.endswith("…")
    list_centered = bounded_log_text(
        long_prefix + " NEEDLE " + "y" * 300,
        query=["missing", "needle"],
        case_sensitive=False,
        matched=True,
    )
    assert "NEEDLE" in list_centered
    with pytest.raises(ValueError, match="empty"):
        normalize_log_queries([])
    with pytest.raises(ValueError, match="at most 20"):
        normalize_log_queries(["x"] * 21)
    with pytest.raises(ValueError, match="500"):
        normalize_log_queries(["x" * 501])
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
    specs = normalize_artifact_specs(
        {"": "ignored", "json": {"path": "a.json", "format": "custom", "suite": "unit"}, "txt": "a.txt"}
    )
    assert [spec["kind"] for spec in specs] == ["json", "txt"]
    assert specs[0]["suite"] == "unit"
    assert specs[1]["suite"] is None
    with pytest.raises(ValueError, match="must be"):
        normalize_artifact_specs({"bad": 1})
    with pytest.raises(ValueError, match="missing path"):
        normalize_artifact_specs({"bad": {"path": ""}})
    with pytest.raises(ValueError, match="blank suite"):
        normalize_artifact_specs({"bad": {"path": "coverage.json", "suite": " "}})

    class UnreadablePath:
        def exists(self):
            raise OSError("unreadable")

    assert CoverageStore._artifact_file_state(UnreadablePath()) is None
    assert (
        summarize_coverage_ingest(
            [
                {"coverage_format": "lcov", "ingest_status": "ingested", "snapshot_id": "snapshot"},
                {"coverage_format": "lcov", "ingest_status": "failed", "snapshot_id": None},
            ]
        )["status"]
        == "partial"
    )
    assert (
        summarize_coverage_ingest([{"coverage_format": "lcov", "ingest_status": "skipped_stale", "snapshot_id": None}])[
            "status"
        ]
        == "skipped_stale"
    )
    assert (
        summarize_coverage_ingest([{"coverage_format": "lcov", "ingest_status": None, "snapshot_id": None}])["status"]
        == "pending"
    )
    assert summarize_coverage_ingest([{"coverage_format": "lcov", "snapshot_id": None}])["status"] == "not_recorded"


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
    conn.execute(
        """
        CREATE TABLE registered_commands (
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
    conn.execute(
        """
        CREATE TABLE runs (
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
    recorded_at = datetime.now(UTC)
    conn.execute(
        """
        INSERT INTO registered_commands VALUES (
            'legacy-command', ?, 'legacy', 'echo ok', ?, ?, ?, 'main', 'abc',
            '/bin/bash', 'tester', 'legacy approval', '[]', true
        )
        """,
        [recorded_at, tmp_path.as_posix(), tmp_path.as_posix(), tmp_path.as_posix()],
    )
    conn.execute(
        """
        INSERT INTO runs VALUES (
            'legacy-run', 'legacy-command', 'legacy', 'echo ok', ?, ?, ?, 'main', 'abc',
            ?, ?, 1234, 0, 'passed', 'stdout.log', 'stderr.log', '{}', '[]'
        )
        """,
        [tmp_path.as_posix(), tmp_path.as_posix(), tmp_path.as_posix(), recorded_at, recorded_at],
    )
    conn.execute(
        """
        CREATE TABLE run_jobs (
            id VARCHAR PRIMARY KEY,
            command_id VARCHAR NOT NULL,
            command_name VARCHAR NOT NULL,
            command VARCHAR NOT NULL,
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
            error VARCHAR NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE run_artifacts (
            run_id VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            exists BOOLEAN NOT NULL,
            size_bytes BIGINT
        )
        """
    )
    conn.close()
    store = CoverageStore(db)
    try:
        columns = {row[1] for row in store._conn.execute("PRAGMA table_info('lines')").fetchall()}
        assert "count_line" in columns
        snapshot_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('snapshots')").fetchall()}
        file_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('files')").fetchall()}
        assert {"total_regions", "covered_regions", "region_rate"} <= snapshot_columns
        assert {"total_regions", "covered_regions", "region_rate"} <= file_columns
        run_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('runs')").fetchall()}
        assert {"queued_at", "queue_duration_ms", "idempotency_key", "cancellation_requested_at"} <= run_columns
        job_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('run_jobs')").fetchall()}
        assert {"idempotency_key", "cancellation_requested_at"} <= job_columns
        artifact_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('run_artifacts')").fetchall()}
        assert {
            "coverage_format",
            "suite",
            "modified_by_run",
            "ingest_status",
            "snapshot_id",
            "ingest_error",
        } <= artifact_columns
        command_columns = {row[1] for row in store._conn.execute("PRAGMA table_info('registered_commands')").fetchall()}
        assert {
            "duration_estimate_ms",
            "duration_p90_ms",
            "duration_sample_count",
            "duration_stats_updated_at",
        } <= command_columns
        legacy = store.registered_command("legacy-command")
        assert legacy["duration_estimate_ms"] == 1234
        assert legacy["duration_p90_ms"] == 1234
        assert legacy["duration_sample_count"] == 1
        assert store._conn.execute("SELECT count(*) FROM run_jobs").fetchone() == (0,)
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

    assert store._conn.index_failures == 11


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
