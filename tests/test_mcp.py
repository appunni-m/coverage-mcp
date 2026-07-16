from __future__ import annotations

import asyncio
import sys

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from coverage_mcp.app import create_mcp
from coverage_mcp.storage import CoverageStore


def run(coro):
    return asyncio.run(coro)


def structured(payload):
    value = payload[1]
    return value["result"] if isinstance(value, dict) and set(value) == {"result"} else value


def test_mcp_tools_ingest_summarize_and_drill_down(tmp_path):
    report = tmp_path / "lcov.info"
    report.write_text(
        """TN:
SF:src/a.py
DA:1,1
DA:2,0
end_of_record
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = create_mcp(store)

        async def scenario():
            tools = await mcp.list_tools()
            assert {
                "ingest_coverage",
                "project_summaries",
                "register_test_command",
                "list_registered_commands",
                "run_command_profiled",
                "run_result",
                "object_topology",
                "coverage_summary",
                "coverage_files",
                "coverage_file",
                "coverage_insights",
                "changed_lines",
                "worktree_progress",
            }.issubset({tool.name for tool in tools})

            snapshot = structured(
                await mcp.call_tool(
                    "ingest_coverage",
                    {
                        "report_path": report.as_posix(),
                        "format": "lcov",
                        "repo_path": tmp_path.as_posix(),
                        "branch": "main",
                        "commit_sha": "abc",
                        "suite": "unit",
                    },
                )
            )
            assert snapshot["total_lines"] == 2

            summary = structured(await mcp.call_tool("coverage_summary", {"snapshot_id": snapshot["id"]}))
            assert summary["line_rate"] == 0.5

            files = structured(await mcp.call_tool("coverage_files", {"snapshot_id": snapshot["id"]}))
            assert files[0]["file_path"] == "src/a.py"

            file_payload = structured(
                await mcp.call_tool(
                    "coverage_file",
                    {"snapshot_id": snapshot["id"], "file_path": "src/a.py", "include_lines": True},
                )
            )
            assert [line["covered"] for line in file_payload["lines"]] == [True, False]

            projects = structured(await mcp.call_tool("project_summaries", {"limit": 10}))
            assert projects[0]["latest_snapshot_id"] == snapshot["id"]
            assert projects[0]["topology"]["kind"] == "project"

            insights = structured(await mcp.call_tool("coverage_insights", {"snapshot_id": snapshot["id"]}))
            assert "summary" in insights
            assert "items" in insights

        run(scenario())
    finally:
        store.close()


def test_mcp_registered_command_run_returns_bounded_profile(tmp_path):
    script = tmp_path / "run_command.py"
    script.write_text(
        """from pathlib import Path
import sys
Path("artifact.txt").write_text("ok")
for index in range(20):
    print(f"line {index}")
print("ERROR synthetic failure", file=sys.stderr)
sys.exit(1)
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = create_mcp(store)

        async def scenario():
            command = structured(
                await mcp.call_tool(
                    "register_test_command",
                    {
                        "name": "failing-suite",
                        "command": f"{sys.executable} {script.name}",
                        "cwd": tmp_path.as_posix(),
                        "artifact_paths": {"text": "artifact.txt"},
                        "human_approved": True,
                        "approved_by": "tester",
                        "approval_note": "approved synthetic MCP command",
                    },
                )
            )
            run = structured(
                await mcp.call_tool(
                    "run_command_profiled",
                    {"command_ref": command["id"], "max_summary_lines": 2},
                )
            )
            result = structured(await mcp.call_tool("run_result", {"run_id": run["id"], "max_summary_lines": 1}))
            commands = structured(await mcp.call_tool("list_registered_commands", {"limit": 5}))
            topology = structured(
                await mcp.call_tool(
                    "object_topology",
                    {"object_kind": "run", "object_ref": run["id"]},
                )
            )
            artifact = structured(
                await mcp.call_tool(
                    "latest_artifact",
                    {"command_ref": "failing-suite", "kind": "text"},
                )
            )

            assert run["status"] == "failed"
            assert result["id"] == run["id"]
            assert commands[0]["id"] == command["id"]
            assert run["topology"]["command"]["id"] == command["id"]
            assert topology["topology"]["kind"] == "run"
            assert run["parsed_summary"]["stdout_line_count"] == 20
            assert len(run["parsed_summary"]["excerpts"]) <= 2
            assert artifact["exists"] is True

        run(scenario())
    finally:
        store.close()


def test_mcp_remains_responsive_while_registered_command_runs(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        command = store.register_command(
            name="slow-suite",
            command=f"{sys.executable} -c 'import time; time.sleep(0.5)'",
            cwd=tmp_path.as_posix(),
            human_approved=True,
            approved_by="tester",
            approval_note="verify concurrent MCP queries",
        )
        mcp = create_mcp(store)

        async def scenario():
            started = asyncio.get_running_loop().time()
            run_task = asyncio.create_task(
                mcp.call_tool(
                    "run_command_profiled",
                    {"command_ref": command["id"]},
                )
            )
            queued_run_task = asyncio.create_task(
                mcp.call_tool(
                    "run_command_profiled",
                    {"command_ref": command["id"]},
                )
            )
            await asyncio.sleep(0.05)

            assert asyncio.get_running_loop().time() - started < 0.3
            commands = structured(
                await asyncio.wait_for(
                    mcp.call_tool("list_registered_commands", {"limit": 1}),
                    timeout=0.3,
                )
            )
            assert commands[0]["id"] == command["id"]
            assert not run_task.done()
            assert not queued_run_task.done()

            run_result = structured(await run_task)
            assert run_result["status"] == "passed"
            assert not queued_run_task.done()
            queued_run_result = structured(await queued_run_task)
            assert queued_run_result["status"] == "passed"

        run(scenario())
    finally:
        store.close()


def test_mcp_coverage_query_surface_and_resources(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.py").write_text("one\ntwo\nthree\n", encoding="utf-8")
    base = tmp_path / "base.lcov"
    current = tmp_path / "current.lcov"
    base.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,1\nend_of_record\n", encoding="utf-8")
    current.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:3,1\nend_of_record\n", encoding="utf-8")
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = create_mcp(store)

        async def scenario():
            base_snapshot = structured(
                await mcp.call_tool(
                    "ingest_coverage",
                    {
                        "report_path": base.as_posix(),
                        "format": "lcov",
                        "repo_path": tmp_path.as_posix(),
                        "branch": "main",
                        "commit_sha": "base",
                        "suite": "unit",
                    },
                )
            )
            current_snapshot = structured(
                await mcp.call_tool(
                    "ingest_coverage",
                    {
                        "report_path": current.as_posix(),
                        "format": "lcov",
                        "repo_path": tmp_path.as_posix(),
                        "branch": "feature",
                        "commit_sha": "head",
                        "suite": "unit",
                    },
                )
            )
            summary = structured(await mcp.call_tool("coverage_summary", {"snapshot_id": current_snapshot["id"]}))
            assert summary["id"] == current_snapshot["id"]
            latest_summary = structured(await mcp.call_tool("coverage_summary", {"repo_path": tmp_path.as_posix()}))
            assert latest_summary["id"] == current_snapshot["id"]
            file_without_lines = structured(
                await mcp.call_tool(
                    "coverage_file",
                    {"snapshot_id": current_snapshot["id"], "file_path": "src/a.py", "include_lines": False},
                )
            )
            assert "lines" not in file_without_lines
            comparison = structured(
                await mcp.call_tool(
                    "compare_to_baseline",
                    {"snapshot_id": current_snapshot["id"], "baseline_snapshot_id": base_snapshot["id"]},
                )
            )
            assert comparison["overall"]["total_lines_delta"] == 1
            changed = structured(
                await mcp.call_tool(
                    "changed_lines",
                    {
                        "snapshot_id": current_snapshot["id"],
                        "baseline_snapshot_id": base_snapshot["id"],
                        "only_regressions": True,
                    },
                )
            )
            assert changed[0]["status"] == "regressed"
            history = structured(
                await mcp.call_tool(
                    "line_history",
                    {"file_path": "src/a.py", "line_number": 1, "repo_path": tmp_path.as_posix()},
                )
            )
            assert len(history) == 2
            source = structured(
                await mcp.call_tool(
                    "source_context",
                    {"snapshot_id": current_snapshot["id"], "file_path": "src/a.py", "start": 2, "end": 2},
                )
            )
            assert source == [{"line_number": 2, "text": "two"}]
            worktree = structured(
                await mcp.call_tool(
                    "register_worktree",
                    {"path": tmp_path.as_posix(), "base_ref": "main"},
                )
            )
            worktree_comparison = structured(
                await mcp.call_tool(
                    "compare_to_baseline",
                    {"worktree_id": worktree["id"], "snapshot_id": current_snapshot["id"]},
                )
            )
            assert worktree_comparison["worktree"]["id"] == worktree["id"]
            progress = structured(
                await mcp.call_tool(
                    "worktree_progress",
                    {"worktree_id": worktree["id"], "suite": "unit"},
                )
            )
            assert progress["baseline"]["id"] == base_snapshot["id"]
            resources = await mcp.list_resources()
            assert "coverage://projects" in {str(resource.uri) for resource in resources}
            templates = await mcp.list_resource_templates()
            assert "coverage://snapshot/{snapshot_id}/files" in {str(template.uriTemplate) for template in templates}
            latest = list(await mcp.read_resource("coverage://snapshots/latest"))
            assert latest
            projects = list(await mcp.read_resource("coverage://projects"))
            assert projects
            snapshot_resource = list(await mcp.read_resource(f"coverage://snapshot/{current_snapshot['id']}/summary"))
            assert snapshot_resource
            insight_resource = list(await mcp.read_resource(f"coverage://snapshot/{current_snapshot['id']}/insights"))
            assert insight_resource
            files_resource = list(await mcp.read_resource(f"coverage://snapshot/{current_snapshot['id']}/files"))
            assert files_resource
            with pytest.raises(ToolError):
                await mcp.call_tool("latest_artifact", {"kind": "missing"})
            with pytest.raises(ToolError):
                await mcp.call_tool("coverage_summary", {"branch": "missing"})
            with pytest.raises(ToolError):
                await mcp.call_tool("compare_to_baseline", {})

        run(scenario())
    finally:
        store.close()
