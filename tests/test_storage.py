from __future__ import annotations

import sys
from pathlib import Path

import pytest

from coverage_mcp.parsers import parse_coverage_report
from coverage_mcp.storage import CoverageStore


def test_store_and_compare_snapshots(tmp_path):
    baseline_report = tmp_path / "base.lcov"
    current_report = tmp_path / "current.lcov"
    baseline_report.write_text(
        """TN:
SF:src/a.py
DA:1,1
DA:2,1
end_of_record
""",
        encoding="utf-8",
    )
    current_report.write_text(
        """TN:
SF:src/a.py
DA:1,1
DA:2,0
DA:3,1
end_of_record
""",
        encoding="utf-8",
    )

    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        baseline = parse_coverage_report(baseline_report, format="lcov")
        current = parse_coverage_report(current_report, format="lcov")
        baseline_id = store.store_report(
            baseline,
            repo_path=tmp_path.as_posix(),
            repo_key=tmp_path.as_posix(),
            branch="main",
            commit_sha="base",
            base_ref=None,
            suite="unit",
        )
        current_id = store.store_report(
            current,
            repo_path=tmp_path.as_posix(),
            repo_key=tmp_path.as_posix(),
            branch="feature",
            commit_sha="head",
            base_ref="main",
            suite="unit",
        )

        comparison = store.compare(snapshot_id=current_id, baseline_snapshot_id=baseline_id)

        assert comparison["overall"]["covered_lines_delta"] == 0
        assert comparison["overall"]["total_lines_delta"] == 1
        assert any(line["status"] == "regressed" and line["line_number"] == 2 for line in comparison["changed_lines"])
        assert any(line["status"] == "new" and line["line_number"] == 3 for line in comparison["changed_lines"])
    finally:
        store.close()


def test_find_baseline_snapshot_by_commit_then_branch(tmp_path):
    report_path = tmp_path / "base.lcov"
    report_path.write_text(
        """TN:
SF:src/a.py
DA:1,1
end_of_record
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        report = parse_coverage_report(report_path, format="lcov")
        snapshot_id = store.store_report(
            report,
            repo_path=tmp_path.as_posix(),
            repo_key="repo-key",
            branch="main",
            commit_sha="abc",
            base_ref=None,
            suite="unit",
        )

        assert store.find_baseline_snapshot(repo_key="repo-key", base_ref="main", base_sha="abc") == snapshot_id
        assert store.find_baseline_snapshot(repo_key="repo-key", base_ref="main", base_sha="missing") == snapshot_id
    finally:
        store.close()


def test_projects_and_insights_surface_investigation_items(tmp_path):
    baseline_report = tmp_path / "base.lcov"
    current_report = tmp_path / "current.lcov"
    baseline_report.write_text(
        """TN:
SF:src/a.py
DA:1,1
DA:2,1
end_of_record
SF:src/empty.py
DA:1,0
DA:2,0
DA:3,0
DA:4,0
DA:5,0
end_of_record
""",
        encoding="utf-8",
    )
    current_report.write_text(
        """TN:
SF:src/a.py
DA:1,1
DA:2,0
BRDA:2,0,0,0
BRDA:2,0,1,0
end_of_record
SF:src/empty.py
DA:1,0
DA:2,0
DA:3,0
DA:4,0
DA:5,0
end_of_record
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        baseline_id = store.store_report(
            parse_coverage_report(baseline_report, format="lcov"),
            repo_path=tmp_path.as_posix(),
            repo_key="repo-key",
            branch="main",
            commit_sha="base",
            base_ref=None,
            suite="unit",
        )
        current_id = store.store_report(
            parse_coverage_report(current_report, format="lcov"),
            repo_path=tmp_path.as_posix(),
            repo_key="repo-key",
            branch="feature",
            commit_sha="head",
            base_ref="main",
            suite="unit",
        )

        projects = store.projects()
        insights = store.insights(snapshot_id=current_id, baseline_snapshot_id=baseline_id)

        assert projects[0]["repo_key"] == "repo-key"
        assert projects[0]["latest_snapshot_id"] == current_id
        assert projects[0]["snapshot_count"] == 2
        assert projects[0]["latest_snapshot_age_seconds"] >= 0
        assert projects[0]["latest_snapshot_age"].endswith(" ago")
        assert insights["snapshot"]["age_seconds"] >= 0
        assert insights["snapshot"]["age"].endswith(" ago")
        categories = {item["category"] for item in insights["items"]}
        assert "zero-coverage-file" in categories
        assert "low-branch-coverage" in categories
        assert "line-regression" in categories
        assert insights["summary"]["high_count"] >= 1
    finally:
        store.close()


def test_registered_command_requires_human_approval(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        with pytest.raises(ValueError, match="human_approved"):
            store.register_command(
                name="unit",
                command="echo ok",
                cwd=tmp_path.as_posix(),
                human_approved=False,
                approved_by="human",
                approval_note="approve test command",
            )
    finally:
        store.close()


def test_run_command_profiled_records_bounded_summary_and_artifacts(tmp_path):
    script = tmp_path / "run_suite.py"
    script.write_text(
        """from pathlib import Path
import sys
Path("coverage.lcov").write_text("TN:\\nSF:src/a.py\\nDA:1,1\\nend_of_record\\n")
for index in range(30):
    print(f"stdout line {index}")
print("FAILED one synthetic test", file=sys.stderr)
sys.exit(2)
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="synthetic",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            artifact_paths={"lcov": "coverage.lcov"},
            human_approved=True,
            approved_by="tester",
            approval_note="approved synthetic command for bounded-output test",
        )

        run = store.run_command_profiled(command["id"], max_summary_lines=3)
        latest = store.latest_artifact(command_ref="synthetic", kind="lcov")

        assert run["command_id"] == command["id"]
        assert run["status"] == "failed"
        assert run["exit_code"] == 2
        assert run["artifact_paths"][0]["kind"] == "lcov"
        assert run["artifact_paths"][0]["exists"] is True
        assert run["age_seconds"] >= 0
        assert run["age"].endswith(" ago")
        assert latest is not None
        assert latest["run_id"] == run["id"]
        assert latest["run_age_seconds"] >= 0
        assert latest["run_age"].endswith(" ago")
        assert run["parsed_summary"]["stdout_line_count"] == 30
        assert run["parsed_summary"]["stderr_line_count"] == 1
        assert len(run["parsed_summary"]["excerpts"]) <= 3
        assert any("FAILED" in item["text"] for item in run["parsed_summary"]["excerpts"])
        search = store.search_run_logs(run["id"], "stdout line 15", context_lines=2, max_words=20)
        assert search["match_count"] == 1
        assert search["returned_line_count"] == 5
        assert search["returned_word_count"] == 15
        assert [line["line_number"] for line in search["contexts"][0]["lines"]] == [14, 15, 16, 17, 18]
        multi_search = store.search_run_logs(
            run["id"],
            ["stdout line 15", "FAILED"],
            context_lines=0,
            max_words=50,
        )
        assert multi_search["query"] == ["stdout line 15", "FAILED"]
        assert multi_search["queries"] == ["stdout line 15", "FAILED"]
        assert multi_search["match_count"] == 2
        assert multi_search["returned_line_count"] == 2
        stderr_search = store.search_run_logs(run["id"], "failed", stream="stderr", context_lines=0)
        assert stderr_search["returned_match_count"] == 1
        Path(run["stdout_path"]).unlink()
        Path(run["stderr_path"]).unlink()
        bounded = store.run_result(run["id"], max_summary_lines=1)
        assert bounded["parsed_summary"]["stdout_line_count"] == 30
        assert len(bounded["parsed_summary"]["excerpts"]) == 1
    finally:
        store.close()


def test_managed_run_auto_ingests_fresh_coverage_and_links_snapshot(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src/a.py").write_text("covered\nmissed\n", encoding="utf-8")
    script = tmp_path / "coverage_suite.py"
    script.write_text(
        """from pathlib import Path
import sys
Path("coverage.lcov").write_text("TN:\\nSF:src/a.py\\nDA:1,1\\nDA:2,0\\nend_of_record\\n")
print("1 failed")
sys.exit(2)
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="unit",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            artifact_paths={
                "coverage": {
                    "path": "coverage.lcov",
                    "coverage_format": "lcov",
                    "suite": "unit-coverage",
                }
            },
            human_approved=True,
            approved_by="tester",
            approval_note="approved automatic coverage ingestion test",
        )
        assert store._job_coverage_ingest({"command_id": command["id"]}, terminal=False)["status"] == "pending"
        terminal_ingest = store._job_coverage_ingest({"command_id": command["id"]}, terminal=True)
        assert terminal_ingest["status"] == "skipped_run_status"

        run = store.run_command_profiled(command["id"], idempotency_key="auto-ingest")
        artifact = run["artifact_paths"][0]
        snapshot_id = artifact["snapshot_id"]

        assert run["status"] == "failed"
        assert artifact["modified_by_run"] is True
        assert artifact["ingest_status"] == "ingested"
        assert artifact["ingest_error"] is None
        assert artifact["suite"] == "unit-coverage"
        assert run["coverage_ingest"] == {
            "status": "ingested",
            "configured_artifacts": 1,
            "ingested_artifacts": 1,
            "failed_artifacts": 0,
            "skipped_artifacts": 0,
            "snapshot_ids": [snapshot_id],
        }
        snapshot = store.snapshot(snapshot_id)
        assert snapshot["suite"] == "unit-coverage"
        assert snapshot["covered_lines"] == 1
        assert snapshot["total_lines"] == 2
        latest = store.latest_artifact(command_ref=command["id"], kind="coverage")
        assert latest is not None
        assert latest["snapshot_id"] == snapshot_id
        assert latest["ingest_status"] == "ingested"

        repeated = store.submit_command_profiled(command["id"], idempotency_key="auto-ingest")
        assert repeated["id"] == run["id"]
        assert repeated["coverage_ingest"]["snapshot_ids"] == [snapshot_id]
        assert len(store.list_snapshots(limit=10)) == 1
    finally:
        store.close()


def test_topology_is_computed_for_commands_runs_projects_and_snapshots(tmp_path):
    report_path = tmp_path / "coverage.lcov"
    report_path.write_text(
        """TN:
SF:src/a.py
DA:1,1
end_of_record
""",
        encoding="utf-8",
    )
    script = tmp_path / "run.py"
    script.write_text(
        """from pathlib import Path
Path("artifact.txt").write_text("ok")
print("1 passed")
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="topology-suite",
            command=f"{sys.executable} {script.name}",
            cwd=tmp_path.as_posix(),
            artifact_paths={"text": "artifact.txt"},
            human_approved=True,
            approved_by="tester",
            approval_note="approved topology command",
        )
        projects_before_snapshot = store.projects()
        run = store.run_command_profiled("topology-suite", max_summary_lines=5)
        snapshot = store.ingest_report(
            report_path.as_posix(),
            format="lcov",
            repo_path=tmp_path.as_posix(),
            branch="main",
            commit_sha="abc",
            suite="unit",
        )
        artifact = store.latest_artifact(command_ref="topology-suite", kind="text")

        assert projects_before_snapshot[0]["repo_key"] == tmp_path.as_posix()
        assert projects_before_snapshot[0]["command_count"] == 1
        assert projects_before_snapshot[0]["topology"]["kind"] == "project"
        assert command["topology"]["kind"] == "registered_command"
        assert command["topology"]["project"]["repo_key"] == tmp_path.as_posix()
        assert run["topology"]["kind"] == "run"
        assert run["topology"]["command"]["id"] == command["id"]
        assert run["topology"]["artifacts"][0]["kind"] == "text"
        assert snapshot["topology"]["kind"] == "coverage_snapshot"
        assert artifact is not None
        assert artifact["topology"]["kind"] == "run_artifact"
        assert store.object_topology("command", "topology-suite")["topology"]["command"]["id"] == command["id"]
        assert store.object_topology("run", run["id"])["topology"]["run"]["status"] == "passed"
        assert store.object_topology("snapshot", snapshot["id"])["topology"]["snapshot"]["suite"] == "unit"
    finally:
        store.close()
