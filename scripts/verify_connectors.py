from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import anyio
import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from coverage_mcp.app import daemon_url


async def verify_connector(index: int, repository: Path, results: list[dict[str, Any]]) -> None:
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "coverage_mcp.app", "connect"],
        cwd=repository,
    )
    async with stdio_client(server) as (read_stream, write_stream), ClientSession(read_stream, write_stream) as session:
        initialized = await session.initialize()
        tools = await session.list_tools()
        context = await session.call_tool("project_context", {"max_words": 100})
        health = httpx.get(f"{daemon_url()}/health", timeout=2).json()
        results.append(
            {
                "index": index,
                "server": initialized.serverInfo.name,
                "tools": sorted(tool.name for tool in tools.tools),
                "schema_revision": context.structuredContent["context"]["schema_revision"],
                "daemon_pid": health["pid"],
            }
        )


async def main_async(connector_count: int) -> None:
    repository = Path.cwd().resolve()
    results: list[dict[str, Any]] = []
    async with anyio.create_task_group() as tasks:
        for index in range(connector_count):
            tasks.start_soon(verify_connector, index, repository, results)
    after = httpx.get(f"{daemon_url()}/health", timeout=2).json()
    expected_tools = {
        "coverage_compare",
        "coverage_query",
        "ingest_coverage",
        "project_context",
        "register_test_command",
        "register_worktree",
        "run_test",
        "search_test_logs",
        "source_context",
        "test_run",
    }
    assert len(results) == connector_count
    assert all(result["daemon_pid"] == after["pid"] for result in results)
    assert all(result["server"] == "coverage-mcp" for result in results)
    assert all(result["schema_revision"] == 7 for result in results)
    assert all(set(result["tools"]) == expected_tools for result in results)
    print(f"verified_connectors={connector_count}")
    print(f"shared_daemon_pid={after['pid']}")
    print("tool_count=10")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify concurrent stdio connectors reuse one Coverage MCP daemon.")
    parser.add_argument("--connectors", type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.connectors <= 50:
        parser.error("--connectors must be between 1 and 50")
    anyio.run(main_async, args.connectors)


if __name__ == "__main__":
    main()
