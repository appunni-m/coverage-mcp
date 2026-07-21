from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import httpx
import pytest
import uvicorn
from mcp import ClientSession
from mcp.server.fastmcp.exceptions import ToolError

try:
    from mcp.client.streamable_http import streamable_http_client
except ImportError:  # MCP 1.23 compatibility
    from mcp.client.streamable_http import streamablehttp_client as streamable_http_client

from coverage_mcp.app import REPOSITORY_HEADER, create_app, create_mcp, daemon_mcp_is_healthy
from coverage_mcp.service import CoverageService, RequestContext
from coverage_mcp.storage import CoverageStore

EXPECTED_MCP_INPUTS = {
    "project_context": {"cursor", "max_words", "detailed"},
    "register_test_command": {
        "name",
        "command",
        "human_approved",
        "approved_by",
        "approval_note",
        "cwd",
        "shell",
        "artifact_paths",
        "max_words",
    },
    "run_test": {"command_ref", "timeout_seconds", "idempotency_key", "wait", "max_words"},
    "get_run_data": {"run_id", "max_words", "detailed"},
    "cancel_run": {"run_id", "max_words", "detailed"},
    "search_test_logs": {
        "run_id",
        "query",
        "stream",
        "context_lines",
        "max_matches",
        "max_words",
        "case_sensitive",
    },
    "ingest_coverage": {
        "report_path",
        "format",
        "suite",
        "branch",
        "commit_sha",
        "base_ref",
        "max_words",
    },
    "register_worktree": {"path", "base_ref", "name", "max_words"},
    "coverage_query": {
        "view",
        "snapshot_id",
        "baseline_snapshot_id",
        "suite",
        "branch",
        "file_path",
        "line_number",
        "line_ranges",
        "cursor",
        "max_words",
        "detailed",
    },
    "coverage_compare": {
        "view",
        "snapshot_id",
        "baseline_snapshot_id",
        "worktree_id",
        "suite",
        "file_path",
        "only_regressions",
        "cursor",
        "max_words",
        "detailed",
    },
    "source_context": {"snapshot_id", "file_path", "start", "end", "cursor", "max_words"},
}

EXPECTED_MCP_RESOURCES = {"coverage://context"}
EXPECTED_MCP_RESOURCE_TEMPLATES = {"coverage://snapshot/{snapshot_id}/summary"}


def run(coro):
    return asyncio.run(coro)


def start_http_server(app):
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="critical"))
    thread = threading.Thread(target=server.run, name="coverage-mcp-http-test", daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started
    port = server.servers[0].sockets[0].getsockname()[1]
    return server, thread, f"http://127.0.0.1:{port}"


def make_git_repo(path: Path) -> Path:
    path.mkdir()
    subprocess.run(["git", "init", "-b", "main", path.as_posix()], check=True, capture_output=True)
    subprocess.run(["git", "-C", path.as_posix(), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", path.as_posix(), "config", "user.name", "Test"], check=True)
    (path / "a.py").write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "-C", path.as_posix(), "add", "a.py"], check=True)
    subprocess.run(["git", "-C", path.as_posix(), "commit", "-m", "base"], check=True, capture_output=True)
    return path


def structured(payload):
    value = payload[1]
    return value["result"] if isinstance(value, dict) and set(value) == {"result"} else value


def data(payload):
    value = structured(payload)
    assert value["context"]["schema_revision"] == 7
    assert set(value) == {"context", "data", "page"}
    return value["data"]


def mcp_for(store: CoverageStore, path: Path):
    context = RequestContext(repo_key=path.resolve().as_posix(), checkout_path=path.resolve().as_posix())
    return create_mcp(store, CoverageService(store, lambda: context))


def test_mcp_contract_is_compact_described_and_word_budgeted(tmp_path):
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = mcp_for(store, tmp_path)

        async def scenario():
            tools = {tool.name: tool for tool in await mcp.list_tools()}
            assert "Start with project_context" in mcp.instructions
            assert "wait at least the returned poll_after_ms" in mcp.instructions
            assert "do not poll immediately" in mcp.instructions
            assert "Every response is {context,data,page}" in mcp.instructions
            assert set(tools) == set(EXPECTED_MCP_INPUTS)
            assert len(tools) == 11
            for name, expected_inputs in EXPECTED_MCP_INPUTS.items():
                tool = tools[name]
                assert tool.description
                assert tool.annotations is not None
                properties = tool.inputSchema["properties"]
                assert set(properties) == expected_inputs
                assert all(properties[field].get("description") for field in expected_inputs)
                if "detailed" in properties:
                    assert properties["detailed"]["default"] is False
                    assert "Keep false" in properties["detailed"]["description"]
                assert properties["max_words"]["minimum"] >= 20
                assert properties["max_words"]["maximum"] <= 5000
                output = tool.outputSchema
                assert {"context", "data", "page"} <= set(output["properties"])
                assert all(field.get("description") for field in output["properties"].values())

            read_only_tools = {
                "project_context",
                "get_run_data",
                "search_test_logs",
                "coverage_query",
                "coverage_compare",
                "source_context",
            }
            for name, tool in tools.items():
                assert tool.annotations is not None
                assert tool.annotations.readOnlyHint is (name in read_only_tools)
                if name in read_only_tools:
                    assert tool.annotations.destructiveHint is False
                    assert tool.annotations.idempotentHint is True
                    assert tool.annotations.openWorldHint is False
            assert tools["run_test"].annotations.destructiveHint is True
            assert tools["run_test"].annotations.openWorldHint is True
            assert "waiting the returned poll_after_ms" in tools["run_test"].description
            assert "This tool is read-only" in tools["get_run_data"].description
            assert "never starts, advances, reruns, or cancels" in tools["get_run_data"].description
            assert "do not immediately call again" in tools["get_run_data"].description
            assert "mutating counterpart" in tools["cancel_run"].description
            assert "one query string or a list of query strings" in tools["search_test_logs"].description
            assert "no matches is a successful empty result" in tools["search_test_logs"].description
            assert "summary, files, file gaps, insights, or line_history" in tools["coverage_query"].description
            assert "Direct mode uses snapshot_id plus baseline_snapshot_id" in tools["coverage_compare"].description
            assert "coverage_compare worktree mode" in tools["register_worktree"].description
            wait_description = tools["run_test"].inputSchema["properties"]["wait"]["description"]
            assert "get_run_data" in wait_description
            assert "run_result" not in wait_description

            assert set(tools["coverage_query"].inputSchema["properties"]["view"]["enum"]) == {
                "summary",
                "files",
                "file",
                "insights",
                "line_history",
            }
            assert set(tools["coverage_compare"].inputSchema["properties"]["view"]["enum"]) == {
                "overview",
                "files",
                "lines",
                "progress",
            }
            log_query = tools["search_test_logs"].inputSchema["properties"]["query"]
            assert {schema["type"] for schema in log_query["anyOf"]} == {"string", "array"}
            assert "any term is present" in log_query["description"]

            resources = {str(resource.uri) for resource in await mcp.list_resources()}
            templates = {str(template.uriTemplate) for template in await mcp.list_resource_templates()}
            assert resources == EXPECTED_MCP_RESOURCES
            assert templates == EXPECTED_MCP_RESOURCE_TEMPLATES

            for tool_name, arguments in (
                ("project_context", {"max_words": 49}),
                ("run_test", {"command_ref": "unit", "timeout_seconds": 0}),
                ("search_test_logs", {"run_id": "x", "query": "x", "max_words": 19}),
                ("register_test_command", {"name": "x", "command": "echo x", "human_approved": False}),
                ("ingest_coverage", {"report_path": "coverage.out", "format": "unknown"}),
                ("coverage_query", {"view": "unknown"}),
            ):
                with pytest.raises(ToolError):
                    await mcp.call_tool(tool_name, arguments)

        run(scenario())
    finally:
        store.close()


def test_mcp_coverage_query_compare_source_and_resources(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.py").write_text("one\ntwo\nthree\n", encoding="utf-8")
    base = tmp_path / "base.lcov"
    current = tmp_path / "current.lcov"
    base.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,1\nend_of_record\n", encoding="utf-8")
    current.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:3,1\nend_of_record\n", encoding="utf-8")
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = mcp_for(store, tmp_path)

        async def scenario():
            baseline = data(
                await mcp.call_tool(
                    "ingest_coverage",
                    {"report_path": base.as_posix(), "format": "lcov", "branch": "main", "suite": "unit"},
                )
            )
            current_snapshot = data(
                await mcp.call_tool(
                    "ingest_coverage",
                    {
                        "report_path": current.as_posix(),
                        "format": "lcov",
                        "branch": "feature",
                        "suite": "unit",
                    },
                )
            )
            assert current_snapshot["total_lines"] == 3
            assert "report_path" not in current_snapshot
            assert current_snapshot["warnings"] == []

            summary = data(
                await mcp.call_tool(
                    "coverage_query",
                    {"view": "summary", "snapshot_id": current_snapshot["id"]},
                )
            )
            assert summary["line_rate"] == pytest.approx(2 / 3)
            detailed_summary = data(
                await mcp.call_tool(
                    "coverage_query",
                    {"view": "summary", "snapshot_id": current_snapshot["id"], "detailed": True},
                )
            )
            assert detailed_summary["report_path"] == current.as_posix()
            files_result = structured(
                await mcp.call_tool(
                    "coverage_query",
                    {"view": "files", "snapshot_id": current_snapshot["id"], "max_words": 50},
                )
            )
            assert files_result["data"][0]["file_path"] == "src/a.py"
            assert "raw_metrics" not in files_result["data"][0]
            assert files_result["page"]["max_words"] == 50

            file_result = data(
                await mcp.call_tool(
                    "coverage_query",
                    {
                        "view": "file",
                        "snapshot_id": current_snapshot["id"],
                        "file_path": "src/a.py",
                        "line_ranges": [{"start": 1, "end": 3}],
                    },
                )
            )
            assert [line["covered"] for line in file_result["selected_lines"]] == [True, False, True]
            assert file_result["gaps"]["uncovered_line_count"] == 1

            insights = data(
                await mcp.call_tool(
                    "coverage_query",
                    {"view": "insights", "snapshot_id": current_snapshot["id"]},
                )
            )
            assert {"snapshot", "summary", "items"} <= set(insights)
            history = data(
                await mcp.call_tool(
                    "coverage_query",
                    {
                        "view": "line_history",
                        "suite": "unit",
                        "file_path": "src/a.py",
                        "line_number": 1,
                    },
                )
            )
            assert len(history) == 2

            overview = data(
                await mcp.call_tool(
                    "coverage_compare",
                    {
                        "view": "overview",
                        "snapshot_id": current_snapshot["id"],
                        "baseline_snapshot_id": baseline["id"],
                    },
                )
            )
            assert overview["overall"]["total_lines_delta"] == 1
            lines = data(
                await mcp.call_tool(
                    "coverage_compare",
                    {
                        "view": "lines",
                        "snapshot_id": current_snapshot["id"],
                        "baseline_snapshot_id": baseline["id"],
                        "only_regressions": True,
                    },
                )
            )
            assert lines["lines"][0]["status"] == "regressed"
            files = data(
                await mcp.call_tool(
                    "coverage_compare",
                    {
                        "view": "files",
                        "snapshot_id": current_snapshot["id"],
                        "baseline_snapshot_id": baseline["id"],
                    },
                )
            )
            assert files["files"][0]["file_path"] == "src/a.py"

            source_result = data(
                await mcp.call_tool(
                    "source_context",
                    {"snapshot_id": current_snapshot["id"], "file_path": "src/a.py", "start": 2, "end": 2},
                )
            )
            assert source_result["lines"] == [{"line_number": 2, "text": "two"}]
            with pytest.raises(ToolError, match="end must"):
                await mcp.call_tool(
                    "source_context",
                    {"snapshot_id": current_snapshot["id"], "file_path": "src/a.py", "start": 2, "end": 1},
                )
            with pytest.raises(ToolError, match="file not found"):
                await mcp.call_tool(
                    "source_context",
                    {"snapshot_id": current_snapshot["id"], "file_path": "src/missing.py", "start": 1, "end": 1},
                )

            context_resource = list(await mcp.read_resource("coverage://context"))
            summary_resource = list(await mcp.read_resource(f"coverage://snapshot/{current_snapshot['id']}/summary"))
            assert context_resource and summary_resource

        run(scenario())
    finally:
        store.close()


async def completed_run(mcp, run_id: str, *, detailed: bool = False):
    for _ in range(200):
        result = data(await mcp.call_tool("get_run_data", {"run_id": run_id, "detailed": detailed}))
        if result["terminal"]:
            return result
        await asyncio.sleep(0.02)
    raise AssertionError(f"run did not complete: {run_id}")


def test_mcp_managed_run_log_search_context_and_auto_ingestion(tmp_path):
    script = tmp_path / "managed.py"
    script.write_text(
        """from pathlib import Path
Path('coverage.lcov').write_text('TN:\\nSF:src/a.py\\nDA:1,1\\nend_of_record\\n')
print('1 passed')
print('diagnostic needle')
""",
        encoding="utf-8",
    )
    store = CoverageStore(tmp_path / "coverage.duckdb")
    try:
        mcp = mcp_for(store, tmp_path)

        async def scenario():
            command = data(
                await mcp.call_tool(
                    "register_test_command",
                    {
                        "name": "unit",
                        "command": f"{sys.executable} {script.name}",
                        "cwd": tmp_path.as_posix(),
                        "artifact_paths": {
                            "coverage": {"path": "coverage.lcov", "coverage_format": "lcov", "suite": "unit"}
                        },
                        "human_approved": True,
                        "approved_by": "tester",
                        "approval_note": "approved consolidated MCP runner test",
                    },
                )
            )
            submitted = data(
                await mcp.call_tool(
                    "run_test",
                    {"command_ref": command["id"], "idempotency_key": "managed", "wait": False},
                )
            )
            result = await completed_run(mcp, submitted["id"], detailed=True)
            assert result["status"] == "passed"
            assert result["coverage_ingest"]["status"] == "ingested"
            assert result["artifact_paths"][0]["snapshot_id"]

            logs = data(
                await mcp.call_tool(
                    "search_test_logs",
                    {"run_id": result["id"], "query": "needle", "max_words": 50},
                )
            )
            assert logs["match_count"] == 1
            assert "streams" not in logs
            multi_logs = data(
                await mcp.call_tool(
                    "search_test_logs",
                    {"run_id": result["id"], "query": ["needle", "passed"], "context_lines": 0, "max_words": 50},
                )
            )
            assert multi_logs["query"] == ["needle", "passed"]
            assert multi_logs["queries"] == ["needle", "passed"]
            assert multi_logs["match_count"] == 2
            repeated = data(
                await mcp.call_tool(
                    "run_test",
                    {"command_ref": command["id"], "idempotency_key": "managed"},
                )
            )
            assert repeated["id"] == result["id"]
            assert repeated["submission_reused"] is True
            project = structured(await mcp.call_tool("project_context", {"max_words": 100}))
            assert project["data"]["commands"][0]["id"] == command["id"]
            assert project["data"]["commands"][0]["command"] == f"{sys.executable} {script.name}"
            assert project["data"]["latest_run"]["id"] == result["id"]
            with pytest.raises(ToolError, match="already terminal"):
                await mcp.call_tool("cancel_run", {"run_id": result["id"]})

        run(scenario())
    finally:
        store.close()


def test_mcp_worktree_registration_and_progress_validation(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main", repo.as_posix()], check=True, capture_output=True)
    subprocess.run(["git", "-C", repo.as_posix(), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", repo.as_posix(), "config", "user.name", "Test"], check=True)
    (repo / "a.py").write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "-C", repo.as_posix(), "add", "a.py"], check=True)
    subprocess.run(["git", "-C", repo.as_posix(), "commit", "-m", "base"], check=True, capture_output=True)
    other = tmp_path / "other"
    other.mkdir()
    store = CoverageStore(repo / ".coverage-mcp" / "coverage.duckdb")
    context = RequestContext(repo_key=repo.as_posix(), checkout_path=repo.as_posix())
    try:
        mcp = create_mcp(store, CoverageService(store, lambda: context))

        async def scenario():
            registered = data(await mcp.call_tool("register_worktree", {"path": repo.as_posix(), "base_ref": "main"}))
            assert registered["path"] == repo.as_posix()
            with pytest.raises(ToolError, match="Git checkout"):
                await mcp.call_tool("register_worktree", {"path": other.as_posix(), "base_ref": "main"})
            with pytest.raises(ToolError, match="worktree_id and suite"):
                await mcp.call_tool("coverage_compare", {"view": "progress", "worktree_id": registered["id"]})

        run(scenario())
    finally:
        store.close()


def test_streamable_http_protocol_uses_consolidated_contract(tmp_path):
    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    server, thread, url = start_http_server(app)

    async def scenario():
        async with (
            streamable_http_client(f"{url}/mcp/") as (read_stream, write_stream, _),
            ClientSession(read_stream, write_stream) as session,
        ):
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "coverage-mcp"
            assert "schema 7" in (initialized.instructions or "")
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == set(EXPECTED_MCP_INPUTS)
            result = await session.call_tool("project_context", {"max_words": 100})
            assert result.isError is False
            assert result.structuredContent["context"]["schema_revision"] == 7
            resources = await session.list_resources()
            assert {str(resource.uri) for resource in resources.resources} == EXPECTED_MCP_RESOURCES

    try:
        run(scenario())
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()


def test_streamable_http_protocol_routes_repository_context(tmp_path):
    repo = make_git_repo(tmp_path / "repo")
    app = create_app(common_db_path=(tmp_path / "common.duckdb").as_posix())
    server, thread, url = start_http_server(app)

    async def scenario():
        async with (
            httpx.AsyncClient(headers={REPOSITORY_HEADER: repo.as_posix()}) as http_client,
            streamable_http_client(f"{url}/mcp/", http_client=http_client) as (read_stream, write_stream, _),
            ClientSession(read_stream, write_stream) as session,
        ):
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "coverage-mcp"
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == set(EXPECTED_MCP_INPUTS)
            result = await session.call_tool("project_context", {"max_words": 100})
            assert result.isError is False
            assert result.structuredContent["context"]["checkout_path"] == repo.as_posix()

    try:
        assert daemon_mcp_is_healthy(url, repo.as_posix()) is True
        run(scenario())
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()


def test_daemon_mcp_health_rejects_repository_selection_failure(monkeypatch, tmp_path):
    repo = make_git_repo(tmp_path / "repo")
    app = create_app(common_db_path=(tmp_path / "common.duckdb").as_posix())
    monkeypatch.setattr(
        app.state.coverage_store,
        "select",
        lambda _: (_ for _ in ()).throw(duckdb.IOException("database is locked")),
    )
    server, thread, url = start_http_server(app)

    try:
        assert daemon_mcp_is_healthy(url, repo.as_posix()) is False
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()


def test_readme_documents_consolidated_mcp_contract():
    readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
    guide = readme.split("## MCP Usage Guide", 1)[1].split("## Worktree Baselines", 1)[0]
    assert "The MCP server instructions plus `tools/list` are intended to be sufficient" in guide
    assert "Submit with `run_test(wait=false)`" in guide
    assert "Multiple query terms match a line when any term is present" in guide
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
