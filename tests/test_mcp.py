from __future__ import annotations

import asyncio
import sys
import threading
import time
from pathlib import Path

import pytest
import uvicorn
from mcp import ClientSession
from mcp.server.fastmcp.exceptions import ToolError

try:
    from mcp.client.streamable_http import streamable_http_client
except ImportError:  # MCP 1.23 compatibility
    from mcp.client.streamable_http import streamablehttp_client as streamable_http_client

from coverage_mcp.app import create_app, create_mcp
from coverage_mcp.storage import CoverageStore

EXPECTED_MCP_INPUTS = {
    "project_summaries": {"limit"},
    "register_test_command": {
        "name",
        "command",
        "human_approved",
        "approved_by",
        "approval_note",
        "cwd",
        "shell",
        "artifact_paths",
    },
    "list_registered_commands": {"limit"},
    "run_command_profiled": {
        "command_ref",
        "timeout_seconds",
        "idempotency_key",
        "wait",
        "detailed",
    },
    "run_queue": {"limit"},
    "cancel_run": {"run_id", "detailed"},
    "run_result": {"run_id", "detailed"},
    "latest_run": {"command_ref", "detailed"},
    "search_run_logs": {
        "run_id",
        "query",
        "stream",
        "context_lines",
        "max_matches",
        "max_words",
        "case_sensitive",
    },
    "latest_artifact": {"kind", "command_ref"},
    "object_topology": {"object_kind", "object_ref"},
    "ingest_coverage": {"report_path", "format", "repo_path", "suite", "branch", "commit_sha", "base_ref"},
    "register_worktree": {"path", "base_ref", "name"},
    "worktree_progress": {"worktree_id", "suite", "file_path", "limit"},
    "coverage_summary": {"snapshot_id", "repo_path", "branch", "suite"},
    "coverage_files": {"snapshot_id", "limit"},
    "coverage_file": {"snapshot_id", "file_path", "include_lines"},
    "coverage_insights": {"snapshot_id", "baseline_snapshot_id", "limit"},
    "compare_to_baseline": {"snapshot_id", "baseline_snapshot_id", "worktree_id", "file_limit", "line_limit"},
    "changed_lines": {"snapshot_id", "baseline_snapshot_id", "file_path", "only_regressions", "limit"},
    "line_history": {"file_path", "line_number", "repo_path", "branch", "limit"},
    "source_context": {"snapshot_id", "file_path", "start", "end"},
}

EXPECTED_MCP_OUTPUTS = {
    "project_summaries": {"repo_key", "latest_snapshot_id", "latest_run_age", "topology"},
    "register_test_command": {"id", "command", "artifact_specs", "approved_by", "topology"},
    "list_registered_commands": {"id", "name", "duration_estimate_ms", "artifact_specs"},
    "run_command_profiled": {"id", "status", "terminal", "poll_after_ms", "coverage_ingest", "counters"},
    "run_queue": {"id", "status", "queue_position", "eta_seconds"},
    "cancel_run": {"id", "status", "cancellation_requested", "terminal"},
    "run_result": {"id", "status", "counters", "coverage_ingest", "age"},
    "latest_run": {"id", "status", "age", "age_seconds", "diagnostics_available"},
    "search_run_logs": {
        "run_id",
        "query",
        "match_count",
        "returned_line_count",
        "returned_word_count",
        "contexts",
    },
    "latest_artifact": {"run_id", "kind", "ingest_status", "snapshot_id", "run_age"},
    "object_topology": {"object_kind", "object_ref", "topology"},
    "ingest_coverage": {"id", "suite", "format", "line_rate", "age", "topology"},
    "register_worktree": {"id", "path", "base_ref", "baseline_snapshot_id", "topology"},
    "worktree_progress": {"worktree", "suite", "baseline", "current", "deltas", "points"},
    "coverage_summary": {"id", "suite", "total_lines", "line_rate", "warnings", "age"},
    "coverage_files": {"snapshot_id", "file_path", "total_lines", "line_rate", "raw_metrics"},
    "coverage_file": {"file", "lines"},
    "coverage_insights": {"snapshot", "baseline", "summary", "items"},
    "compare_to_baseline": {"baseline", "current", "overall", "files", "changed_lines", "worktree"},
    "changed_lines": {"file_path", "line_number", "status", "baseline_covered", "current_covered"},
    "line_history": {"snapshot_id", "created_at", "file_path", "line_number", "hits", "covered"},
    "source_context": {"line_number", "text"},
}

EXPECTED_MCP_RESOURCES = {"coverage://snapshots/latest", "coverage://projects"}
EXPECTED_MCP_RESOURCE_TEMPLATES = {
    "coverage://snapshot/{snapshot_id}/summary",
    "coverage://snapshot/{snapshot_id}/insights",
    "coverage://snapshot/{snapshot_id}/files",
}


def run(coro):
    return asyncio.run(coro)


def structured(payload):
    value = payload[1]
    return value["result"] if isinstance(value, dict) and set(value) == {"result"} else value


def resolved_schema(schema, root):
    if "$ref" in schema:
        return root["$defs"][schema["$ref"].rsplit("/", 1)[1]]
    return schema


def output_item_properties(schema):
    node = schema
    properties = node.get("properties", {})
    if set(properties) == {"result"}:
        node = resolved_schema(properties["result"], schema)
        if node.get("type") == "array":
            node = resolved_schema(node["items"], schema)
    variants = node.get("anyOf", [])
    if variants:
        merged = {}
        for variant in variants:
            merged.update(resolved_schema(variant, schema).get("properties", {}))
        return merged
    return node.get("properties", {})


def test_mcp_contract_has_exact_inventory_descriptions_and_bounds(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = create_mcp(store)

        async def scenario():
            tools = {tool.name: tool for tool in await mcp.list_tools()}
            assert set(tools) == set(EXPECTED_MCP_INPUTS)
            for name, expected_inputs in EXPECTED_MCP_INPUTS.items():
                tool = tools[name]
                assert tool.description
                properties = tool.inputSchema["properties"]
                assert set(properties) == expected_inputs
                assert all(properties[input_name].get("description") for input_name in expected_inputs)

                output_properties = output_item_properties(tool.outputSchema)
                assert EXPECTED_MCP_OUTPUTS[name] <= set(output_properties)
                assert output_properties, f"{name} has a generic output schema"
                for model_name, model_schema in {
                    "root": tool.outputSchema,
                    **tool.outputSchema.get("$defs", {}),
                }.items():
                    for field_name, field_schema in model_schema.get("properties", {}).items():
                        assert field_schema.get("description"), (
                            f"{name} output {model_name}.{field_name} has no description"
                        )

            run_schema = tools["run_command_profiled"].inputSchema["properties"]
            assert run_schema["detailed"]["default"] is False
            timeout_schema = run_schema["timeout_seconds"]["anyOf"][0]
            assert timeout_schema["minimum"] == 1
            assert timeout_schema["maximum"] == 86400
            idempotency_schema = run_schema["idempotency_key"]["anyOf"][0]
            assert idempotency_schema["minLength"] == 1
            assert idempotency_schema["maxLength"] == 200

            registration = tools["register_test_command"].inputSchema
            assert set(registration["required"]) == {
                "name",
                "command",
                "human_approved",
                "approved_by",
                "approval_note",
            }
            assert registration["properties"]["human_approved"]["const"] is True
            artifact = registration["$defs"]["ArtifactSpec"]
            assert set(artifact["required"]) == {"path"}
            assert all(field.get("description") for field in artifact["properties"].values())
            assert "automatic ingestion" in artifact["properties"]["coverage_format"]["description"]
            assert artifact["properties"]["suite"]["anyOf"][0]["minLength"] == 1
            artifact_formats = artifact["properties"]["coverage_format"]["anyOf"][0]["enum"]
            assert {"auto", "lcov", "coveragepy", "cobertura", "jacoco", "istanbul", "go", "llvm"} <= set(
                artifact_formats
            )

            coverage_formats = tools["ingest_coverage"].inputSchema["properties"]["format"]["enum"]
            assert {"auto", "lcov", "coveragepy", "cobertura", "jacoco", "istanbul", "go", "llvm"} <= set(
                coverage_formats
            )
            topology_kinds = tools["object_topology"].inputSchema["properties"]["object_kind"]["enum"]
            assert {"project", "command", "run", "snapshot", "worktree"} <= set(topology_kinds)

            run_output = tools["run_command_profiled"].outputSchema
            run_properties = output_item_properties(run_output)
            assert set(run_properties["status"]["enum"]) == {
                "queued",
                "running",
                "passed",
                "failed",
                "cancelled",
                "timeout",
                "interrupted",
                "internal_error",
            }
            ingest_output = resolved_schema(run_properties["coverage_ingest"], run_output)
            assert set(ingest_output["properties"]["status"]["enum"]) == {
                "not_configured",
                "pending",
                "ingested",
                "partial",
                "failed",
                "skipped_stale",
                "skipped_run_status",
                "not_recorded",
            }
            run_artifact = resolved_schema(run_properties["artifact_paths"]["items"], run_output)
            assert set(run_artifact["properties"]["ingest_status"]["anyOf"][0]["enum"]) == {
                "ingested",
                "failed",
                "missing",
                "skipped_stale",
                "skipped_run_status",
            }
            changed_output = output_item_properties(tools["changed_lines"].outputSchema)
            assert set(changed_output["status"]["enum"]) == {
                "new",
                "removed",
                "regressed",
                "improved",
                "changed",
            }

            resources = {str(resource.uri) for resource in await mcp.list_resources()}
            templates = {str(template.uriTemplate) for template in await mcp.list_resource_templates()}
            assert resources == EXPECTED_MCP_RESOURCES
            assert templates == EXPECTED_MCP_RESOURCE_TEMPLATES

            for tool_name, arguments in (
                ("run_command_profiled", {"command_ref": "unit", "timeout_seconds": 0}),
                ("run_command_profiled", {"command_ref": "unit", "idempotency_key": "x" * 201}),
                ("search_run_logs", {"run_id": "x", "query": "x", "max_words": 19}),
                ("register_test_command", {"name": "x", "command": "echo x", "human_approved": False}),
                ("ingest_coverage", {"report_path": "coverage.out", "format": "unknown"}),
                ("object_topology", {"object_kind": "unknown", "object_ref": "x"}),
            ):
                with pytest.raises(ToolError):
                    await mcp.call_tool(tool_name, arguments)

        run(scenario())
    finally:
        store.close()


def test_mcp_managed_command_auto_ingests_declared_coverage(tmp_path):
    script = tmp_path / "managed_coverage.py"
    script.write_text(
        """from pathlib import Path
Path("coverage.lcov").write_text("TN:\\nSF:src/a.py\\nDA:1,1\\nend_of_record\\n")
print("1 passed")
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
                        "name": "managed-unit",
                        "command": f"{sys.executable} {script.name}",
                        "cwd": tmp_path.as_posix(),
                        "artifact_paths": {
                            "coverage": {
                                "path": "coverage.lcov",
                                "coverage_format": "lcov",
                                "suite": "unit",
                            }
                        },
                        "human_approved": True,
                        "approved_by": "tester",
                        "approval_note": "approved MCP auto-ingestion test",
                    },
                )
            )
            result = structured(
                await mcp.call_tool(
                    "run_command_profiled",
                    {
                        "command_ref": command["id"],
                        "wait": True,
                        "idempotency_key": "managed-coverage",
                        "detailed": True,
                    },
                )
            )
            snapshot_id = result["coverage_ingest"]["snapshot_ids"][0]
            summary = structured(await mcp.call_tool("coverage_summary", {"snapshot_id": snapshot_id}))

            assert result["status"] == "passed"
            assert result["coverage_ingest"]["status"] == "ingested"
            assert result["artifact_paths"][0]["ingest_status"] == "ingested"
            assert summary["suite"] == "unit"
            assert summary["covered_lines"] == 1

        run(scenario())
    finally:
        store.close()


def test_streamable_http_protocol_with_official_client(tmp_path):
    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=0,
            log_level="critical",
        )
    )
    thread = threading.Thread(target=server.run, name="coverage-mcp-http-test", daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started
    port = server.servers[0].sockets[0].getsockname()[1]

    async def scenario():
        async with (
            streamable_http_client(f"http://127.0.0.1:{port}/mcp/") as (read_stream, write_stream, _),
            ClientSession(read_stream, write_stream) as session,
        ):
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "coverage-mcp"
            assert "system of record" in (initialized.instructions or "")
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == set(EXPECTED_MCP_INPUTS)
            for tool in tools.tools:
                assert tool.description
                assert set(tool.inputSchema["properties"]) == EXPECTED_MCP_INPUTS[tool.name]
                assert all(field.get("description") for field in tool.inputSchema["properties"].values())
                output_properties = output_item_properties(tool.outputSchema)
                assert EXPECTED_MCP_OUTPUTS[tool.name] <= set(output_properties)
                assert all(field.get("description") for field in output_properties.values())
            result = await session.call_tool("project_summaries", {"limit": 1})
            assert result.isError is False
            assert result.structuredContent == {"result": []}
            resources = await session.list_resources()
            assert {str(resource.uri) for resource in resources.resources} == EXPECTED_MCP_RESOURCES
            templates = await session.list_resource_templates()
            assert {
                str(template.uriTemplate) for template in templates.resourceTemplates
            } == EXPECTED_MCP_RESOURCE_TEMPLATES

    try:
        run(scenario())
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()


async def completed_run(mcp, run_id, *, detailed=False):
    for _ in range(200):
        result = structured(
            await mcp.call_tool(
                "run_result",
                {"run_id": run_id, "detailed": detailed},
            )
        )
        if result["terminal"]:
            return result
        await asyncio.sleep(0.02)
    raise AssertionError(f"run did not complete: {run_id}")


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
            assert {tool.name for tool in tools} == set(EXPECTED_MCP_INPUTS)

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
            assert snapshot["age_seconds"] >= 0
            assert snapshot["age"].endswith(" ago")

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
                    {
                        "command_ref": command["id"],
                        "idempotency_key": "failing-run",
                    },
                )
            )
            assert run["status"] in {"queued", "running"}
            assert run["terminal"] is False
            assert run["poll_after_ms"] == 1000
            assert run["eta_seconds"] is None
            run = await completed_run(mcp, run["id"])
            result = structured(await mcp.call_tool("run_result", {"run_id": run["id"]}))
            latest_run = structured(
                await mcp.call_tool(
                    "latest_run",
                    {"command_ref": command["id"]},
                )
            )
            detailed = structured(await mcp.call_tool("run_result", {"run_id": run["id"], "detailed": True}))
            logs = structured(
                await mcp.call_tool(
                    "search_run_logs",
                    {"run_id": run["id"], "query": "synthetic failure", "context_lines": 1},
                )
            )
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
            projects = structured(await mcp.call_tool("project_summaries", {"limit": 5}))

            assert run["status"] == "failed"
            assert run["eta_seconds"] == 0
            assert result["id"] == run["id"]
            assert latest_run["id"] == run["id"]
            assert latest_run["age_seconds"] >= 0
            assert latest_run["age"].endswith(" ago")
            assert commands[0]["id"] == command["id"]
            assert commands[0]["duration_sample_count"] == 1
            assert commands[0]["duration_estimate_ms"] >= 0
            assert detailed["topology"]["command"]["id"] == command["id"]
            assert topology["topology"]["kind"] == "run"
            assert detailed["parsed_summary"]["stdout_line_count"] == 20
            assert "excerpts" not in detailed["parsed_summary"]
            assert "parsed_summary" not in run
            assert logs["match_count"] == 1
            assert logs["contexts"][0]["stream"] == "stderr"
            assert any(line["match"] for line in logs["contexts"][0]["lines"])
            assert artifact["exists"] is True
            assert projects[0]["latest_run_age_seconds"] >= 0
            assert projects[0]["latest_run_age"].endswith(" ago")
            repeated = structured(
                await mcp.call_tool(
                    "run_command_profiled",
                    {"command_ref": command["id"], "idempotency_key": "failing-run"},
                )
            )
            assert repeated["id"] == run["id"]
            assert repeated["submission_reused"] is True
            with pytest.raises(ToolError, match="already terminal"):
                await mcp.call_tool("cancel_run", {"run_id": run["id"]})

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
            first = structured(await mcp.call_tool("run_command_profiled", {"command_ref": command["id"]}))
            second = structured(await mcp.call_tool("run_command_profiled", {"command_ref": command["id"]}))

            assert asyncio.get_running_loop().time() - started < 0.3
            assert first["terminal"] is False
            assert second["terminal"] is False
            for _ in range(100):
                queue = structured(await mcp.call_tool("run_queue", {"limit": 5}))
                if sum(item["status"] == "running" for item in queue) == 2:
                    break
                await asyncio.sleep(0.01)
            assert {item["id"] for item in queue} == {first["id"], second["id"]}
            assert sum(item["status"] == "running" for item in queue) == 2
            commands = structured(
                await asyncio.wait_for(
                    mcp.call_tool("list_registered_commands", {"limit": 1}),
                    timeout=0.3,
                )
            )
            assert commands[0]["id"] == command["id"]
            first_result, second_result = await asyncio.gather(
                completed_run(mcp, first["id"], detailed=True),
                completed_run(mcp, second["id"], detailed=True),
            )
            assert first_result["status"] == "passed"
            assert second_result["status"] == "passed"
            assert max(first_result["started_at"], second_result["started_at"]) < min(
                first_result["ended_at"], second_result["ended_at"]
            )
            cancellable = structured(await mcp.call_tool("run_command_profiled", {"command_ref": command["id"]}))
            cancellation = structured(await mcp.call_tool("cancel_run", {"run_id": cancellable["id"]}))
            assert cancellation["id"] == cancellable["id"]
            assert cancellation["cancellation_requested"] is True

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
            assert set(comparison["overall"]) == {
                "line_rate_delta",
                "covered_lines_delta",
                "total_lines_delta",
                "branch_rate_delta",
                "covered_branches_delta",
                "total_branches_delta",
                "function_rate_delta",
                "covered_functions_delta",
                "total_functions_delta",
                "region_rate_delta",
                "covered_regions_delta",
                "total_regions_delta",
            }
            assert "function_rate_delta" in comparison["files"][0]
            assert "region_rate_delta" in comparison["files"][0]
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
                    {
                        "worktree_id": worktree["id"],
                        "snapshot_id": current_snapshot["id"],
                        "file_limit": 1,
                        "line_limit": 1,
                    },
                )
            )
            assert worktree_comparison["worktree"]["id"] == worktree["id"]
            assert len(worktree_comparison["files"]) <= 1
            assert len(worktree_comparison["changed_lines"]) <= 1
            with pytest.raises(ToolError, match="cannot be used with worktree_id"):
                await mcp.call_tool(
                    "compare_to_baseline",
                    {
                        "worktree_id": worktree["id"],
                        "baseline_snapshot_id": base_snapshot["id"],
                    },
                )
            progress = structured(
                await mcp.call_tool(
                    "worktree_progress",
                    {"worktree_id": worktree["id"], "suite": "unit"},
                )
            )
            assert progress["baseline"]["id"] == base_snapshot["id"]
            resources = await mcp.list_resources()
            assert {str(resource.uri) for resource in resources} == EXPECTED_MCP_RESOURCES
            templates = await mcp.list_resource_templates()
            assert {str(template.uriTemplate) for template in templates} == EXPECTED_MCP_RESOURCE_TEMPLATES
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
                await mcp.call_tool("latest_run", {"command_ref": "missing"})
            with pytest.raises(ToolError):
                await mcp.call_tool("coverage_summary", {"branch": "missing"})
            with pytest.raises(ToolError):
                await mcp.call_tool("compare_to_baseline", {})

        run(scenario())
    finally:
        store.close()


def test_readme_documents_every_mcp_input_output_and_error():
    readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
    guide = readme.split("## MCP Usage Guide", 1)[1].split("## Worktree Baselines", 1)[0]

    for tool_name, inputs in EXPECTED_MCP_INPUTS.items():
        marker = f"### `{tool_name}`"
        assert guide.count(marker) == 1
        section = guide.split(marker, 1)[1].split("\n### ", 1)[0]
        assert "**Inputs:**" in section
        assert "**Returns:**" in section
        assert "**Errors:**" in section
        for input_name in inputs:
            assert f"`{input_name}`" in section, f"{tool_name} does not document {input_name}"

    for uri in EXPECTED_MCP_RESOURCES | EXPECTED_MCP_RESOURCE_TEMPLATES:
        assert f"`{uri}`" in guide
