from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from coverage_mcp.git_utils import inspect_git
from coverage_mcp.storage import CoverageStore

DEFAULT_DB_NAME = ".coverage-mcp/coverage.duckdb"
DEFAULT_PORT = 59471


class IngestRequest(BaseModel):
    report_path: str
    format: str = "auto"
    repo_path: str | None = None
    branch: str | None = None
    commit_sha: str | None = None
    base_ref: str | None = None
    suite: str = "default"


class RegisterWorktreeRequest(BaseModel):
    path: str
    base_ref: str
    name: str | None = None


class CompareRequest(BaseModel):
    snapshot_id: str
    baseline_snapshot_id: str
    file_limit: int = Field(default=100, ge=1, le=1000)
    line_limit: int = Field(default=500, ge=1, le=5000)


class RegisterCommandRequest(BaseModel):
    name: str
    command: str
    cwd: str | None = None
    shell: str = "/bin/bash"
    artifact_paths: dict[str, Any] | None = None
    human_approved: bool = False
    approved_by: str
    approval_note: str
    enabled: bool = True


class RunCommandRequest(BaseModel):
    command_ref: str
    max_summary_lines: int = Field(default=80, ge=1, le=500)
    timeout_seconds: int | None = Field(default=None, ge=1, le=86400)


def create_app(db_path: str | None = None) -> FastAPI:
    store = CoverageStore(db_path or os.environ.get("COVERAGE_MCP_DB", default_db_path()))
    mcp = create_mcp(store)
    mcp_app = mcp.streamable_http_app()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with mcp.session_manager.run():
            try:
                yield
            finally:
                store.close()

    app = FastAPI(
        title="Coverage MCP",
        description="Local-first coverage time-series dashboard and MCP server.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.coverage_store = store

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> str:
        return DASHBOARD_HTML

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "db_path": store.db_path.as_posix()}

    @app.post("/api/ingest")
    def ingest(request: IngestRequest) -> dict[str, Any]:
        try:
            return store.ingest_report(
                request.report_path,
                format=request.format,
                repo_path=request.repo_path,
                branch=request.branch,
                commit_sha=request.commit_sha,
                base_ref=request.base_ref,
                suite=request.suite,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.post("/api/worktrees/register")
    def register_worktree(request: RegisterWorktreeRequest) -> dict[str, Any]:
        try:
            return store.register_worktree(request.path, base_ref=request.base_ref, name=request.name)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/worktrees")
    def worktrees(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
        return store.list_worktrees(limit=limit)

    @app.get("/api/worktrees/{worktree_id}/progress")
    def worktree_progress(
        worktree_id: str,
        suite: str | None = None,
        file_path: str | None = None,
        limit: int = Query(default=200, ge=1, le=2000),
    ) -> dict[str, Any]:
        try:
            return store.worktree_progress(
                worktree_id,
                suite=suite,
                file_path=file_path,
                limit=limit,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/projects")
    def projects(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
        return store.projects(limit=limit)

    @app.post("/api/commands/register")
    def register_command(request: RegisterCommandRequest) -> dict[str, Any]:
        try:
            return store.register_command(
                name=request.name,
                command=request.command,
                cwd=request.cwd,
                shell=request.shell,
                artifact_paths=request.artifact_paths,
                human_approved=request.human_approved,
                approved_by=request.approved_by,
                approval_note=request.approval_note,
                enabled=request.enabled,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/commands")
    def commands(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
        return store.list_registered_commands(limit=limit)

    @app.get("/api/commands/{command_ref}")
    def command(command_ref: str) -> dict[str, Any]:
        try:
            return store.registered_command(command_ref)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.post("/api/runs/profiled")
    def run_profiled(request: RunCommandRequest) -> dict[str, Any]:
        try:
            return store.run_command_profiled(
                request.command_ref,
                max_summary_lines=request.max_summary_lines,
                timeout_seconds=request.timeout_seconds,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/runs/latest")
    def latest_run(command_ref: str | None = None) -> dict[str, Any]:
        run = store.latest_run(command_ref=command_ref)
        if run is None:
            raise HTTPException(status_code=404, detail="no runs found")
        return run

    @app.get("/api/runs/{run_id}")
    def run(run_id: str, max_summary_lines: int = Query(default=80, ge=1, le=500)) -> dict[str, Any]:
        try:
            return store.run_result(run_id, max_summary_lines=max_summary_lines)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/artifacts/latest")
    def latest_artifact(kind: str, command_ref: str | None = None) -> dict[str, Any]:
        artifact = store.latest_artifact(command_ref=command_ref, kind=kind)
        if artifact is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        return artifact

    @app.get("/api/topology/{object_kind}/{object_ref:path}")
    def object_topology(object_kind: str, object_ref: str) -> dict[str, Any]:
        try:
            return store.object_topology(object_kind, object_ref)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots")
    def snapshots(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        return store.list_snapshots(repo_path=repo_path, branch=branch, suite=suite, limit=limit)

    @app.get("/api/snapshots/latest")
    def latest_snapshot(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
    ) -> dict[str, Any]:
        snapshot = store.latest_snapshot(repo_path=repo_path, branch=branch, suite=suite)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="no snapshots found")
        return snapshot

    @app.get("/api/snapshots/{snapshot_id}")
    def snapshot(snapshot_id: str) -> dict[str, Any]:
        try:
            return store.snapshot(snapshot_id)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/files")
    def files(
        snapshot_id: str,
        limit: int = Query(default=1000, ge=1, le=5000),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        try:
            return store.files(snapshot_id, limit=limit, offset=offset)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/insights")
    def insights(
        snapshot_id: str,
        baseline_snapshot_id: str | None = None,
        limit: int = Query(default=10, ge=1, le=50),
    ) -> dict[str, Any]:
        try:
            return store.insights(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                limit=limit,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/files/{file_path:path}")
    def file_coverage(snapshot_id: str, file_path: str) -> dict[str, Any]:
        try:
            return {
                "file": store.file_coverage(snapshot_id, file_path),
                "lines": store.lines(snapshot_id, file_path),
            }
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/trend")
    def trend(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        file_path: str | None = None,
        worktree_id: str | None = None,
        limit: int = Query(default=200, ge=1, le=2000),
    ) -> list[dict[str, Any]]:
        return store.trend(
            repo_path=repo_path,
            branch=branch,
            suite=suite,
            file_path=file_path,
            worktree_id=worktree_id,
            limit=limit,
        )

    @app.post("/api/compare")
    def compare_post(request: CompareRequest) -> dict[str, Any]:
        try:
            return store.compare(
                snapshot_id=request.snapshot_id,
                baseline_snapshot_id=request.baseline_snapshot_id,
                file_limit=request.file_limit,
                line_limit=request.line_limit,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/compare")
    def compare_get(
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_limit: int = Query(default=100, ge=1, le=1000),
        line_limit: int = Query(default=500, ge=1, le=5000),
    ) -> dict[str, Any]:
        try:
            return store.compare(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                file_limit=file_limit,
                line_limit=line_limit,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/worktrees/{worktree_id}/compare")
    def compare_worktree(worktree_id: str, snapshot_id: str | None = None) -> dict[str, Any]:
        try:
            return store.compare_worktree(worktree_id, snapshot_id=snapshot_id)
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/changed-lines")
    def changed_lines(
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_path: str | None = None,
        only_regressions: bool = False,
        limit: int = Query(default=500, ge=1, le=5000),
    ) -> list[dict[str, Any]]:
        try:
            return store.changed_lines(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                file_path=file_path,
                only_regressions=only_regressions,
                limit=limit,
            )
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/line-history")
    def line_history(
        file_path: str,
        line_number: int = Query(ge=1),
        repo_path: str | None = None,
        branch: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        return store.line_history(
            file_path=file_path,
            line_number=line_number,
            repo_path=repo_path,
            branch=branch,
            limit=limit,
        )

    @app.get("/api/source-lines")
    def source_lines(
        snapshot_id: str,
        file_path: str,
        start: int = Query(ge=1),
        end: int = Query(ge=1),
    ) -> list[dict[str, Any]]:
        try:
            return store.source_lines(snapshot_id=snapshot_id, file_path=file_path, start=start, end=end)
        except Exception as exc:
            raise _http_error(exc) from exc

    app.mount("/mcp", mcp_app)
    return app


def create_mcp(store: CoverageStore) -> FastMCP:
    mcp = FastMCP(
        "coverage-mcp",
        instructions=(
            "Use this server to ingest local coverage reports, inspect compact coverage summaries, "
            "compare snapshots to frozen baselines, and drill into exact changed lines."
        ),
        stateless_http=True,
        streamable_http_path="/",
    )

    @mcp.tool()
    def project_summaries(limit: int = 100) -> list[dict[str, Any]]:
        """List projects with latest coverage, snapshot count, and branch count."""
        return store.projects(limit=limit)

    @mcp.tool()
    def register_test_command(
        name: str,
        command: str,
        cwd: str | None = None,
        shell: str = "/bin/bash",
        artifact_paths: dict[str, Any] | None = None,
        human_approved: bool = False,
        approved_by: str = "",
        approval_note: str = "",
    ) -> dict[str, Any]:
        """Register an approved command that can be run later by id or name."""
        return store.register_command(
            name=name,
            command=command,
            cwd=cwd,
            shell=shell,
            artifact_paths=artifact_paths,
            human_approved=human_approved,
            approved_by=approved_by,
            approval_note=approval_note,
        )

    @mcp.tool()
    def list_registered_commands(limit: int = 100) -> list[dict[str, Any]]:
        """List approved registered commands, newest first."""
        return store.list_registered_commands(limit=limit)

    @mcp.tool()
    def run_command_profiled(
        command_ref: str,
        max_summary_lines: int = 80,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Run an approved registered command and return a bounded profiled summary."""
        return store.run_command_profiled(
            command_ref,
            max_summary_lines=max_summary_lines,
            timeout_seconds=timeout_seconds,
        )

    @mcp.tool()
    def run_result(run_id: str, max_summary_lines: int = 80) -> dict[str, Any]:
        """Return a bounded summary for a previously recorded run."""
        return store.run_result(run_id, max_summary_lines=max_summary_lines)

    @mcp.tool()
    def latest_artifact(kind: str, command_ref: str | None = None) -> dict[str, Any]:
        """Return the latest registered artifact for a command and artifact kind."""
        artifact = store.latest_artifact(command_ref=command_ref, kind=kind)
        if artifact is None:
            raise KeyError("artifact not found")
        return artifact

    @mcp.tool()
    def object_topology(object_kind: str, object_ref: str) -> dict[str, Any]:
        """Return computed topology for a project, command, run, snapshot, or worktree."""
        return store.object_topology(object_kind, object_ref)

    @mcp.tool()
    def ingest_coverage(
        report_path: str,
        format: str = "auto",
        repo_path: str | None = None,
        suite: str = "default",
        branch: str | None = None,
        commit_sha: str | None = None,
        base_ref: str | None = None,
    ) -> dict[str, Any]:
        """Ingest a local coverage report into an immutable snapshot."""
        return store.ingest_report(
            report_path,
            format=format,
            repo_path=repo_path,
            branch=branch,
            commit_sha=commit_sha,
            base_ref=base_ref,
            suite=suite,
        )

    @mcp.tool()
    def register_worktree(path: str, base_ref: str, name: str | None = None) -> dict[str, Any]:
        """Register a worktree and freeze its current baseline snapshot reference."""
        return store.register_worktree(path, base_ref=base_ref, name=name)

    @mcp.tool()
    def worktree_progress(
        worktree_id: str,
        suite: str | None = None,
        file_path: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Return one worktree's frozen baseline, independent trend, and metric deltas."""
        return store.worktree_progress(
            worktree_id,
            suite=suite,
            file_path=file_path,
            limit=limit,
        )

    @mcp.tool()
    def coverage_summary(
        snapshot_id: str | None = None,
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
    ) -> dict[str, Any]:
        """Return a compact overall coverage summary."""
        if snapshot_id:
            return store.snapshot(snapshot_id)
        snapshot = store.latest_snapshot(repo_path=repo_path, branch=branch, suite=suite)
        if snapshot is None:
            raise KeyError("no snapshots found")
        return snapshot

    @mcp.tool()
    def coverage_files(snapshot_id: str, limit: int = 100) -> list[dict[str, Any]]:
        """List files for a snapshot, ordered by lowest line coverage first."""
        return store.files(snapshot_id, limit=limit)

    @mcp.tool()
    def coverage_file(snapshot_id: str, file_path: str, include_lines: bool = True) -> dict[str, Any]:
        """Inspect one file's coverage, optionally including line records."""
        result: dict[str, Any] = {"file": store.file_coverage(snapshot_id, file_path)}
        if include_lines:
            result["lines"] = store.lines(snapshot_id, file_path)
        return result

    @mcp.tool()
    def coverage_insights(
        snapshot_id: str,
        baseline_snapshot_id: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Return prioritized coverage investigation items for a snapshot."""
        return store.insights(
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            limit=limit,
        )

    @mcp.tool()
    def compare_to_baseline(
        snapshot_id: str | None = None,
        baseline_snapshot_id: str | None = None,
        worktree_id: str | None = None,
        file_limit: int = 100,
        line_limit: int = 500,
    ) -> dict[str, Any]:
        """Compare a snapshot to a baseline snapshot or a registered worktree baseline."""
        if worktree_id:
            return store.compare_worktree(worktree_id, snapshot_id=snapshot_id)
        if not snapshot_id or not baseline_snapshot_id:
            raise ValueError("snapshot_id and baseline_snapshot_id are required without worktree_id")
        return store.compare(
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            file_limit=file_limit,
            line_limit=line_limit,
        )

    @mcp.tool()
    def changed_lines(
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_path: str | None = None,
        only_regressions: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return exact changed line records between two snapshots."""
        return store.changed_lines(
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            file_path=file_path,
            only_regressions=only_regressions,
            limit=limit,
        )

    @mcp.tool()
    def line_history(
        file_path: str,
        line_number: int,
        repo_path: str | None = None,
        branch: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return coverage history for one path-based file line."""
        return store.line_history(
            file_path=file_path,
            line_number=line_number,
            repo_path=repo_path,
            branch=branch,
            limit=limit,
        )

    @mcp.tool()
    def source_context(snapshot_id: str, file_path: str, start: int, end: int) -> list[dict[str, Any]]:
        """Read a bounded source line range for a covered file."""
        return store.source_lines(snapshot_id=snapshot_id, file_path=file_path, start=start, end=end)

    @mcp.resource("coverage://snapshots/latest", mime_type="application/json")
    def latest_snapshot_resource() -> dict[str, Any]:
        snapshot = store.latest_snapshot()
        return snapshot or {"error": "no snapshots found"}

    @mcp.resource("coverage://projects", mime_type="application/json")
    def projects_resource() -> list[dict[str, Any]]:
        return store.projects(limit=100)

    @mcp.resource("coverage://snapshot/{snapshot_id}/summary", mime_type="application/json")
    def snapshot_summary_resource(snapshot_id: str) -> dict[str, Any]:
        return store.snapshot(snapshot_id)

    @mcp.resource("coverage://snapshot/{snapshot_id}/insights", mime_type="application/json")
    def snapshot_insights_resource(snapshot_id: str) -> dict[str, Any]:
        return store.insights(snapshot_id=snapshot_id)

    @mcp.resource("coverage://snapshot/{snapshot_id}/files", mime_type="application/json")
    def snapshot_files_resource(snapshot_id: str) -> list[dict[str, Any]]:
        return store.files(snapshot_id, limit=500)

    return mcp


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (FileNotFoundError, ValueError)):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


def default_db_path(path: str | None = None) -> str:
    root = inspect_git(path).repo_key
    return (Path(root) / DEFAULT_DB_NAME).as_posix()


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Coverage MCP</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #687385;
      --border: #d8dde6;
      --accent: #0f766e;
      --accent-2: #2563eb;
      --danger: #b42318;
      --good: #067647;
      --warn: #b54708;
      --danger-soft: #fff1f0;
      --good-soft: #ecfdf3;
      --warn-soft: #fff8eb;
      --blue-soft: #eff6ff;
      --code-bg: #fbfcfe;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
      font-size: 14px;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 16px 24px;
      background: #ffffff;
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      z-index: 10;
    }
    h1 {
      font-size: 20px;
      line-height: 1.2;
      margin: 0;
      letter-spacing: 0;
    }
    main {
      max-width: 1600px;
      margin: 0 auto;
      padding: 20px 24px 32px;
    }
    button, input, select {
      font: inherit;
      border: 1px solid var(--border);
      background: #ffffff;
      color: var(--ink);
      border-radius: 6px;
      min-height: 36px;
    }
    button {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 0 12px;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
    }
    button:disabled {
      color: var(--muted);
      cursor: not-allowed;
    }
    input, select {
      padding: 0 10px;
      min-width: 160px;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
    }
    .grid {
      display: grid;
      gap: 16px;
    }
    .metrics {
      grid-template-columns: repeat(5, minmax(150px, 1fr));
    }
    .metric, .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
    }
    .metric {
      padding: 14px 16px;
      min-height: 92px;
    }
    .metric .label, .muted {
      color: var(--muted);
      font-size: 12px;
    }
    .metric .value {
      font-size: 28px;
      line-height: 1.15;
      margin-top: 8px;
      font-weight: 700;
    }
    .metric .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
    }
    .panel {
      overflow: hidden;
    }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }
    .panel-head h2 {
      margin: 0;
      font-size: 14px;
      letter-spacing: 0;
    }
    .panel-body {
      padding: 12px 14px;
    }
    .two-col {
      grid-template-columns: minmax(0, 1fr) 360px;
      align-items: start;
    }
    .overview-grid {
      grid-template-columns: minmax(0, 1fr) 420px;
      align-items: stretch;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 9px 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: middle;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }
    td.path {
      max-width: 520px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    tr.clickable {
      cursor: pointer;
    }
    tr.clickable:hover {
      background: #f2f5f8;
    }
    .bar {
      width: 120px;
      height: 8px;
      background: #edf1f5;
      border-radius: 999px;
      overflow: hidden;
    }
    .bar span {
      display: block;
      height: 100%;
      background: var(--accent);
    }
    .status-regressed { color: var(--danger); font-weight: 700; }
    .status-improved { color: var(--good); font-weight: 700; }
    .status-new, .status-removed, .status-changed { color: var(--warn); font-weight: 700; }
    .insight-list {
      display: grid;
      gap: 8px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .insight {
      display: grid;
      gap: 4px;
      padding: 10px 0;
      border-bottom: 1px solid var(--border);
    }
    .insight:last-child {
      border-bottom: 0;
    }
    .insight-top {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 48px;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }
    .badge-high { color: #ffffff; background: var(--danger); }
    .badge-medium { color: #1f2937; background: #f7c948; }
    .badge-info { color: #ffffff; background: var(--accent-2); }
    .insight-title {
      font-weight: 700;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .insight-detail {
      color: var(--muted);
      line-height: 1.35;
    }
    #trend {
      display: block;
      width: 100%;
      min-height: 240px;
    }
    .trend-meta {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: wrap;
      gap: 8px 14px;
      min-width: 0;
    }
    .trend-scope {
      min-width: 190px;
      min-height: 30px;
      height: 30px;
      padding: 0 8px;
      font-size: 12px;
    }
    .trend-legend {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px 12px;
    }
    .trend-key {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      color: #475467;
      font-size: 11px;
      white-space: nowrap;
    }
    .trend-swatch {
      width: 16px;
      height: 3px;
      border-radius: 2px;
      background: var(--series-color);
    }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .investigation-panel {
      min-height: 660px;
    }
    .investigation-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }
    .investigation-title {
      display: flex;
      align-items: baseline;
      gap: 10px;
      min-width: 0;
    }
    .investigation-title h2,
    .source-title h3,
    .pane-heading h3,
    .diagnosis-section h3 {
      margin: 0;
      letter-spacing: 0;
    }
    .investigation-title h2 {
      font-size: 15px;
    }
    .investigation-title code {
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .comparison-banner {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) repeat(4, auto);
      gap: 18px;
      align-items: center;
      padding: 10px 14px;
      color: #344054;
      background: #f8fafc;
      border-bottom: 1px solid var(--border);
    }
    .comparison-banner[hidden] {
      display: none;
    }
    .comparison-label {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .comparison-stat {
      display: flex;
      align-items: baseline;
      gap: 5px;
      white-space: nowrap;
    }
    .comparison-stat strong {
      font-size: 14px;
    }
    .investigation-grid {
      display: grid;
      grid-template-columns: 300px minmax(520px, 1fr) 290px;
      min-height: 610px;
    }
    .file-pane,
    .source-pane,
    .diagnosis-pane {
      min-width: 0;
    }
    .file-pane {
      border-right: 1px solid var(--border);
      background: #ffffff;
    }
    .pane-heading,
    .source-titlebar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 52px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
    }
    .pane-heading h3,
    .diagnosis-section h3 {
      font-size: 12px;
      color: #344054;
      text-transform: uppercase;
    }
    .file-controls {
      display: grid;
      gap: 8px;
      padding: 10px;
      border-bottom: 1px solid var(--border);
      background: #fbfcfe;
    }
    .file-controls input,
    .file-controls select {
      width: 100%;
      min-width: 0;
      min-height: 32px;
    }
    .file-list {
      max-height: 540px;
      overflow: auto;
    }
    .file-item {
      display: grid;
      gap: 7px;
      width: 100%;
      min-height: 76px;
      padding: 11px 12px;
      border: 0;
      border-bottom: 1px solid #e8edf3;
      border-radius: 0;
      text-align: left;
      background: #ffffff;
    }
    .file-item:hover {
      background: #f8fafc;
    }
    .file-item.selected {
      background: var(--blue-soft);
      box-shadow: inset 3px 0 0 var(--accent-2);
    }
    .file-row,
    .file-meta,
    .source-titlebar,
    .line-toolbar,
    .line-summary,
    .source-actions {
      display: flex;
      align-items: center;
    }
    .file-row {
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
    }
    .file-name {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #253044;
      font-weight: 650;
    }
    .file-rate {
      font-variant-numeric: tabular-nums;
      font-weight: 750;
    }
    .file-meta {
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 11px;
    }
    .file-delta.negative { color: var(--danger); }
    .file-delta.positive { color: var(--good); }
    .file-bar {
      height: 4px;
      overflow: hidden;
      background: #e7ebf0;
      border-radius: 2px;
    }
    .file-bar span {
      display: block;
      height: 100%;
      background: var(--good);
    }
    .file-item.attention .file-bar span {
      background: var(--warn);
    }
    .file-item.critical .file-bar span {
      background: var(--danger);
    }
    .source-pane {
      display: grid;
      grid-template-rows: auto auto minmax(0, 1fr);
      background: var(--code-bg);
    }
    .source-title {
      min-width: 0;
    }
    .source-title h3 {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 14px;
    }
    .source-title code {
      display: block;
      margin-top: 3px;
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .health-badge {
      flex: 0 0 auto;
      padding: 5px 8px;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 12px;
      font-weight: 750;
      background: #ffffff;
    }
    .health-badge.good { color: var(--good); border-color: #a6e3c5; background: var(--good-soft); }
    .health-badge.warn { color: var(--warn); border-color: #fedf89; background: var(--warn-soft); }
    .health-badge.danger { color: var(--danger); border-color: #fda29b; background: var(--danger-soft); }
    .line-toolbar {
      justify-content: space-between;
      gap: 10px;
      min-height: 50px;
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      background: #ffffff;
    }
    .line-summary {
      flex-wrap: wrap;
      gap: 6px 12px;
      min-width: 0;
    }
    .summary-item {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
    }
    .summary-mark {
      width: 7px;
      height: 7px;
      border-radius: 2px;
      background: #98a2b3;
    }
    .summary-mark.covered { background: var(--good); }
    .summary-mark.missed { background: var(--danger); }
    .summary-mark.branch { background: var(--warn); }
    .summary-mark.changed { background: var(--accent-2); }
    .source-actions {
      gap: 6px;
      flex: 0 0 auto;
    }
    .icon-button {
      width: 30px;
      min-width: 30px;
      min-height: 30px;
      justify-content: center;
      padding: 0;
      font-weight: 800;
    }
    .segmented {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      padding: 3px;
      border: 1px solid var(--border);
      border-radius: 7px;
      background: #f8fafc;
    }
    .segmented button {
      min-height: 26px;
      padding: 0 8px;
      border: 0;
      border-radius: 5px;
      background: transparent;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
    }
    .segmented button.active {
      color: var(--ink);
      background: #ffffff;
      box-shadow: 0 1px 2px rgba(15, 23, 42, 0.12);
    }
    .coverage-stage {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 16px;
      min-height: 0;
      max-height: 540px;
      overflow: hidden;
      background: var(--code-bg);
    }
    .coverage-viewer {
      min-width: 0;
      overflow: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 20px;
      scroll-behavior: smooth;
    }
    .source-line {
      display: grid;
      grid-template-columns: 50px 7px minmax(300px, 1fr) 58px;
      min-height: 24px;
      color: #273447;
      border-bottom: 1px solid #edf0f4;
      cursor: pointer;
    }
    .source-line:hover {
      background: #f0f4f8;
    }
    .source-line.selected {
      box-shadow: inset 0 0 0 1px #84adff;
      background: #eef4ff;
    }
    .source-line.missed {
      background: var(--danger-soft);
    }
    .source-line.branch-risk:not(.missed) {
      background: var(--warn-soft);
    }
    .source-line.regressed {
      box-shadow: inset 4px 0 0 var(--danger);
    }
    .source-line.improved {
      box-shadow: inset 4px 0 0 var(--good);
    }
    .source-line.new {
      box-shadow: inset 4px 0 0 var(--accent-2);
    }
    .source-line-number,
    .source-line-meta {
      display: flex;
      align-items: center;
      color: #7b8798;
      font-size: 11px;
      font-variant-numeric: tabular-nums;
      user-select: none;
    }
    .source-line-number {
      justify-content: flex-end;
      padding-right: 9px;
      border-right: 1px solid #e2e7ed;
    }
    .coverage-gutter {
      background: transparent;
    }
    .covered .coverage-gutter { background: var(--good); }
    .missed .coverage-gutter { background: var(--danger); }
    .branch-risk .coverage-gutter {
      background: var(--warn);
    }
    .line-code {
      display: block;
      min-width: 0;
      padding: 2px 10px;
      overflow: visible;
      white-space: pre;
    }
    .source-line-meta {
      justify-content: flex-end;
      padding: 0 7px;
      border-left: 1px solid #e2e7ed;
      white-space: nowrap;
    }
    .source-line.missed .source-line-meta {
      color: var(--danger);
      font-weight: 800;
    }
    .context-break {
      padding: 3px 12px 3px 67px;
      color: var(--muted);
      background: #f3f5f8;
      border-bottom: 1px solid #e2e7ed;
      font-family: ui-sans-serif, system-ui, sans-serif;
      font-size: 11px;
    }
    .coverage-map {
      position: relative;
      background: #eef1f5;
      border-left: 1px solid var(--border);
    }
    .map-mark {
      position: absolute;
      left: 2px;
      width: 11px;
      min-height: 3px;
      padding: 0;
      border: 0;
      border-radius: 1px;
      background: var(--danger);
      cursor: pointer;
    }
    .map-mark.branch { background: var(--warn); }
    .map-mark.regressed { background: #7a271a; }
    .diagnosis-pane {
      overflow: auto;
      max-height: 610px;
      border-left: 1px solid var(--border);
      background: #ffffff;
    }
    .diagnosis-section {
      padding: 14px;
      border-bottom: 1px solid var(--border);
    }
    .diagnosis-section h3 {
      margin-bottom: 10px;
    }
    .coverage-score {
      display: grid;
      grid-template-columns: 72px minmax(0, 1fr);
      gap: 12px;
      align-items: center;
    }
    .score-ring {
      display: grid;
      place-items: center;
      width: 72px;
      height: 72px;
      border-radius: 50%;
      background: conic-gradient(var(--good) calc(var(--coverage) * 1%), #e7ebf0 0);
    }
    .score-ring::before {
      content: "";
      grid-area: 1 / 1;
      width: 54px;
      height: 54px;
      border-radius: 50%;
      background: #ffffff;
    }
    .score-ring strong {
      grid-area: 1 / 1;
      z-index: 1;
      font-size: 15px;
    }
    .diagnosis-copy {
      color: #344054;
      line-height: 1.45;
    }
    .diagnosis-copy strong {
      display: block;
      margin-bottom: 3px;
      color: var(--ink);
    }
    .gap-list {
      display: grid;
      gap: 6px;
    }
    .gap-button {
      display: grid;
      grid-template-columns: 8px minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      width: 100%;
      min-height: 34px;
      padding: 5px 7px;
      text-align: left;
      border-color: #e2e7ed;
    }
    .gap-button:hover {
      background: #f8fafc;
    }
    .gap-dot {
      width: 7px;
      height: 7px;
      border-radius: 2px;
      background: var(--danger);
    }
    .gap-button.branch .gap-dot { background: var(--warn); }
    .gap-button code {
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .line-facts {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 10px;
    }
    .line-fact {
      padding: 8px;
      background: #f8fafc;
      border: 1px solid #e2e7ed;
      border-radius: 6px;
    }
    .line-fact span {
      display: block;
      color: var(--muted);
      font-size: 10px;
      text-transform: uppercase;
    }
    .line-fact strong {
      display: block;
      margin-top: 3px;
      font-size: 13px;
    }
    .history-track {
      display: flex;
      align-items: center;
      gap: 4px;
      min-height: 18px;
      margin: 9px 0 5px;
      overflow: hidden;
    }
    .history-point {
      flex: 1;
      max-width: 12px;
      height: 8px;
      border-radius: 2px;
      background: var(--danger);
    }
    .history-point.covered { background: var(--good); }
    .history-caption {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 10px;
    }
    .line-empty-source {
      color: #98a2b3;
      font-style: italic;
    }
    .empty {
      padding: 28px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 900px) {
      header { align-items: flex-start; flex-direction: column; }
      main { padding: 16px; }
      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      .metrics .metric {
        min-height: 82px;
        padding: 11px 12px;
      }
      .metrics .metric:last-child {
        grid-column: 1 / -1;
      }
      .metric .value {
        font-size: 23px;
      }
      .two-col, .overview-grid { grid-template-columns: 1fr; }
      td.path { max-width: 220px; }
      .toolbar { width: 100%; }
      input, select { min-width: 0; width: 100%; }
      .investigation-head,
      .line-toolbar {
        align-items: stretch;
        flex-direction: column;
      }
      .overview-grid .panel-head {
        align-items: flex-start;
        flex-direction: column;
      }
      .trend-meta {
        justify-content: flex-start;
      }
      .comparison-banner {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .comparison-label {
        grid-column: 1 / -1;
      }
      .investigation-grid {
        grid-template-columns: 1fr;
      }
      .file-pane,
      .diagnosis-pane {
        border: 0;
        border-bottom: 1px solid var(--border);
      }
      .file-list {
        max-height: 260px;
      }
      .diagnosis-pane {
        max-height: none;
      }
      .source-actions {
        flex-wrap: wrap;
      }
      .segmented {
        flex: 1;
      }
      .segmented button {
        flex: 1;
      }
      .source-line {
        grid-template-columns: 42px 6px minmax(240px, 1fr) 50px;
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>Coverage MCP</h1>
    <div class="toolbar">
      <select id="projectSelect" title="Project"></select>
      <select id="snapshotSelect" title="Snapshot"></select>
      <input id="reportPath" placeholder="coverage report path">
      <select id="format">
        <option value="auto">auto</option>
        <option value="lcov">lcov</option>
        <option value="coveragepy">coverage.py JSON</option>
        <option value="cobertura">Cobertura XML</option>
        <option value="jacoco">JaCoCo XML</option>
        <option value="istanbul">Istanbul JSON</option>
        <option value="go">Go coverprofile</option>
        <option value="llvm">LLVM JSON</option>
      </select>
      <button class="primary" id="ingestBtn" title="Ingest report">Ingest</button>
      <button id="refreshBtn" title="Refresh">Refresh</button>
    </div>
  </header>
  <main class="grid">
    <section class="grid metrics">
      <div class="metric"><div class="label">Project</div><div id="projectName" class="value" style="font-size:16px">--</div><div id="projectSub" class="sub"></div></div>
      <div class="metric"><div class="label">Line Coverage</div><div id="lineRate" class="value">--</div><div id="lineSub" class="sub"></div></div>
      <div class="metric"><div class="label">Branch Coverage</div><div id="branchRate" class="value">--</div><div id="branchSub" class="sub"></div></div>
      <div class="metric"><div class="label">Files</div><div id="fileCount" class="value">--</div><div class="sub">tracked in selected snapshot</div></div>
      <div class="metric"><div class="label">Snapshot</div><div id="snapshotTime" class="value" style="font-size:16px">--</div><div id="snapshotSub" class="sub"></div></div>
    </section>
    <section class="grid overview-grid">
      <div class="panel">
        <div class="panel-head">
          <h2>Coverage Trend</h2>
          <div class="trend-meta">
            <select id="trendScope" class="trend-scope" title="Trend lineage"></select>
            <div id="trendLegend" class="trend-legend"></div>
            <span class="muted" id="trendLabel"></span>
          </div>
        </div>
        <div class="panel-body"><svg id="trend" viewBox="0 0 900 240" role="img" aria-label="Coverage trend"></svg></div>
      </div>
      <div class="panel">
        <div class="panel-head"><h2>What To Investigate</h2><span class="muted" id="insightCount"></span></div>
        <div class="panel-body"><ul id="insightsBody" class="insight-list"></ul></div>
      </div>
    </section>
    <section class="panel investigation-panel">
      <div class="investigation-head">
        <div class="investigation-title">
          <h2>Coverage Investigation</h2>
          <code id="selectedFile">Select a file</code>
        </div>
        <div class="toolbar">
          <select id="baselineSelect" title="Baseline snapshot"></select>
          <button id="compareBtn" title="Compare with selected baseline">Compare</button>
        </div>
      </div>
      <div id="comparisonBanner" class="comparison-banner" hidden></div>
      <div class="investigation-grid">
        <aside class="file-pane">
          <div class="pane-heading">
            <h3>Files</h3>
            <span id="fileListCount" class="muted"></span>
          </div>
          <div class="file-controls">
            <input id="fileSearch" type="search" placeholder="Filter paths" aria-label="Filter files">
            <select id="fileSort" aria-label="Sort files">
              <option value="attention">Needs attention</option>
              <option value="coverage">Lowest coverage</option>
              <option value="path">Path</option>
            </select>
          </div>
          <div id="fileList" class="file-list"></div>
        </aside>
        <section class="source-pane">
          <div class="source-titlebar">
            <div class="source-title">
              <h3 id="fileName">Choose a file</h3>
              <code id="filePath">Coverage details will appear here</code>
            </div>
            <span id="fileHealth" class="health-badge">--</span>
          </div>
          <div class="line-toolbar">
            <div id="fileLineSummary" class="line-summary"></div>
            <div class="source-actions">
              <button id="prevGap" class="icon-button" type="button" title="Previous uncovered region" disabled>&#8593;</button>
              <button id="nextGap" class="icon-button" type="button" title="Next uncovered region" disabled>&#8595;</button>
              <div id="lineFilter" class="segmented">
                <button type="button" class="active" data-filter="source">Source</button>
                <button type="button" data-filter="missed">Misses</button>
                <button type="button" data-filter="branches">Branches</button>
                <button type="button" data-filter="changed">Changed</button>
              </div>
            </div>
          </div>
          <div class="coverage-stage">
            <div id="coverageViewer" class="coverage-viewer"><div class="empty">Select a file to inspect its coverage.</div></div>
            <div id="coverageMap" class="coverage-map" aria-label="Coverage overview"></div>
          </div>
        </section>
        <aside id="diagnosisPane" class="diagnosis-pane">
          <div class="diagnosis-section">
            <h3>Diagnosis</h3>
            <div id="diagnosisContent" class="diagnosis-copy muted">Select a file.</div>
          </div>
          <div class="diagnosis-section">
            <h3>Uncovered Regions</h3>
            <div id="gapList" class="gap-list"><div class="muted">No file selected.</div></div>
          </div>
          <div class="diagnosis-section">
            <h3>Line History</h3>
            <div id="lineInspector" class="muted">Select a source line.</div>
          </div>
        </aside>
      </div>
    </section>
  </main>
  <script>
    const state = {
      projects: [],
      snapshots: [],
      worktrees: [],
      projectKey: null,
      selected: null,
      files: [],
      lineFilter: 'source',
      fileQuery: '',
      selectedFile: null,
      selectedPayload: null,
      sourceByLine: new Map(),
      trendScope: null,
      comparison: null,
      changedByFile: new Map(),
      fileComparison: new Map(),
      focusLines: [],
      currentLine: null
    };
    const pct = value => value === null || value === undefined ? '--' : `${(value * 100).toFixed(1)}%`;
    const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    const shortPath = value => {
      const parts = String(value || '').split('/').filter(Boolean);
      return parts.slice(-2).join('/') || value || 'unknown';
    };

    async function getJSON(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) throw new Error(await response.text());
      return response.json();
    }

    function currentProject() {
      return state.projects.find(project => project.repo_key === state.projectKey) || null;
    }

    function projectSnapshots() {
      return state.projectKey ? state.snapshots.filter(snapshot => snapshot.repo_key === state.projectKey) : state.snapshots;
    }

    function projectWorktrees() {
      return state.projectKey ? state.worktrees.filter(worktree => worktree.repo_key === state.projectKey) : state.worktrees;
    }

    function referenceBranches() {
      const snapshots = projectSnapshots();
      const available = new Set(snapshots.map(snapshot => snapshot.branch).filter(Boolean));
      const configured = projectWorktrees().map(worktree => worktree.base_ref).filter(branch => available.has(branch));
      const preferred = ['main', 'master'].filter(branch => available.has(branch));
      const references = [...new Set([...configured, ...preferred])];
      if (references.length) return references;
      return [state.selected?.base_ref, state.selected?.branch, snapshots[0]?.branch].filter(Boolean).slice(0, 1);
    }

    function renderTrendScopes() {
      const select = document.getElementById('trendScope');
      const references = referenceBranches();
      const worktrees = projectWorktrees();
      const options = [
        ...references.map(branch => ({
          value: `reference:${branch}`,
          label: `Reference: ${branch}`
        })),
        ...worktrees.map(worktree => ({
          value: `worktree:${worktree.id}`,
          label: `Worktree: ${worktree.name || worktree.branch || shortPath(worktree.path)}`
        }))
      ];
      if (!options.some(option => option.value === state.trendScope)) {
        state.trendScope = options[0]?.value || null;
      }
      select.innerHTML = options.map(option => `<option value="${esc(option.value)}">${esc(option.label)}</option>`).join('')
        || '<option>No lineage available</option>';
      if (state.trendScope) select.value = state.trendScope;
    }

    function optionLabel(snapshot) {
      const date = new Date(snapshot.created_at).toLocaleString();
      const branch = snapshot.branch || 'no branch';
      return `${date} | ${branch} / ${snapshot.suite} | ${pct(snapshot.line_rate)}`;
    }

    function projectOptionLabel(project) {
      return `${shortPath(project.repo_path)} | ${pct(project.line_rate)} | ${project.snapshot_count} snapshots`;
    }

    async function refresh() {
      state.projects = await getJSON('/api/projects?limit=200');
      state.snapshots = await getJSON('/api/snapshots?limit=200');
      state.worktrees = await getJSON('/api/worktrees?limit=200');
      if (!state.projectKey && state.projects.length) state.projectKey = state.projects[0].repo_key;
      renderSnapshotSelectors();
      state.selected = projectSnapshots()[0] || state.snapshots[0] || null;
      if (state.selected && !state.projectKey) state.projectKey = state.selected.repo_key;
      renderProjectSelector();
      if (state.selected) document.getElementById('snapshotSelect').value = state.selected.id;
      await renderSelected();
    }

    function renderProjectSelector() {
      const select = document.getElementById('projectSelect');
      select.innerHTML = state.projects.map(project => `<option value="${esc(project.repo_key)}">${esc(projectOptionLabel(project))}</option>`).join('') || '<option>No projects</option>';
      if (state.projectKey) select.value = state.projectKey;
    }

    function renderSnapshotSelectors() {
      const snapshots = projectSnapshots();
      const options = snapshots.map(snapshot => `<option value="${snapshot.id}">${esc(optionLabel(snapshot))}</option>`).join('');
      document.getElementById('snapshotSelect').innerHTML = options || '<option>No snapshots</option>';
      document.getElementById('baselineSelect').innerHTML = options || '<option>No snapshots</option>';
    }

    async function renderSelected() {
      const snapshot = state.selected;
      if (!snapshot) {
        document.getElementById('fileList').innerHTML = '<div class="empty">Ingest a coverage report to populate the dashboard.</div>';
        document.getElementById('insightsBody').innerHTML = '<li class="empty">No coverage snapshots yet.</li>';
        return;
      }
      const project = currentProject();
      document.getElementById('projectName').textContent = project ? shortPath(project.repo_path) : shortPath(snapshot.repo_path);
      document.getElementById('projectSub').textContent = project ? `${project.snapshot_count} snapshots across ${project.branch_count} branches` : snapshot.repo_path;
      document.getElementById('lineRate').textContent = pct(snapshot.line_rate);
      document.getElementById('lineSub').textContent = `${snapshot.covered_lines} / ${snapshot.total_lines} covered lines`;
      document.getElementById('branchRate').textContent = pct(snapshot.branch_rate);
      document.getElementById('branchSub').textContent = `${snapshot.covered_branches} / ${snapshot.total_branches} covered branches`;
      document.getElementById('snapshotTime').textContent = new Date(snapshot.created_at).toLocaleString();
      document.getElementById('snapshotSub').textContent = `${snapshot.branch || 'no branch'} ${snapshot.commit_sha ? snapshot.commit_sha.slice(0, 12) : ''}`;
      state.files = await getJSON(`/api/snapshots/${snapshot.id}/files?limit=1000`);
      document.getElementById('fileCount').textContent = state.files.length;
      if (!state.files.some(file => file.file_path === state.selectedFile)) state.selectedFile = null;
      renderTrendScopes();
      await loadComparison(false, true);
      renderFiles();
      await renderInsights(document.getElementById('baselineSelect').value);
      await loadScopedTrend();
      if (state.files.length) await loadFile(state.selectedFile || state.files[0].file_path);
    }

    async function renderInsights(baselineId) {
      const body = document.getElementById('insightsBody');
      if (!state.selected) return;
      const baseline = baselineId && baselineId !== state.selected.id ? `&baseline_snapshot_id=${encodeURIComponent(baselineId)}` : '';
      const payload = await getJSON(`/api/snapshots/${state.selected.id}/insights?limit=8${baseline}`);
      document.getElementById('insightCount').textContent = `${payload.summary.high_count} high, ${payload.summary.medium_count} medium`;
      if (!payload.items.length) {
        body.innerHTML = '<li class="empty">No obvious investigation items for this snapshot.</li>';
        return;
      }
      body.innerHTML = payload.items.slice(0, 12).map(item => `
        <li class="insight${item.file_path ? ' clickable' : ''}" ${item.file_path ? `data-file="${esc(item.file_path)}" data-line="${item.line_number || ''}"` : ''}>
          <div class="insight-top">
            <span class="badge badge-${esc(item.severity)}">${esc(item.severity)}</span>
            <span class="insight-title">${esc(item.title)}</span>
          </div>
          <div class="insight-detail">${esc(item.detail)}</div>
          ${item.file_path ? `<code>${esc(item.file_path)}${item.line_number ? ':' + item.line_number : ''}</code>` : ''}
        </li>
      `).join('');
      body.querySelectorAll('[data-file]').forEach(item => item.addEventListener('click', () => {
        loadFile(item.dataset.file, Number(item.dataset.line) || null);
      }));
    }

    function missingLines(file) {
      return Math.max(0, Number(file.total_lines || 0) - Number(file.covered_lines || 0));
    }

    function missingBranches(file) {
      return Math.max(0, Number(file.total_branches || 0) - Number(file.covered_branches || 0));
    }

    function coverageTone(rate) {
      if (rate === null || rate === undefined || rate < 0.6) return 'critical';
      if (rate < 0.85) return 'attention';
      return '';
    }

    function formatPointDelta(delta) {
      if (delta === null || delta === undefined) return '';
      const points = delta * 100;
      return `${points > 0 ? '+' : ''}${points.toFixed(1)} pp`;
    }

    function filePriority(file) {
      const comparison = state.fileComparison.get(file.file_path);
      const regressionPenalty = comparison && comparison.line_rate_delta < 0 ? Math.abs(comparison.line_rate_delta) * 100 : 0;
      return missingLines(file) * 2 + missingBranches(file) * 3 + regressionPenalty * 5;
    }

    function renderFiles() {
      const list = document.getElementById('fileList');
      const query = state.fileQuery.toLowerCase();
      const sort = document.getElementById('fileSort').value;
      const files = state.files
        .filter(file => file.file_path.toLowerCase().includes(query))
        .sort((left, right) => {
          if (sort === 'path') return left.file_path.localeCompare(right.file_path);
          if (sort === 'coverage') return (left.line_rate ?? 1) - (right.line_rate ?? 1) || left.file_path.localeCompare(right.file_path);
          return filePriority(right) - filePriority(left) || (left.line_rate ?? 1) - (right.line_rate ?? 1);
        });
      document.getElementById('fileListCount').textContent = `${files.length} / ${state.files.length}`;
      if (!files.length) {
        list.innerHTML = '<div class="empty">No matching files.</div>';
        return;
      }
      list.innerHTML = files.map(file => {
        const missed = missingLines(file);
        const branchGaps = missingBranches(file);
        const comparison = state.fileComparison.get(file.file_path);
        const delta = comparison ? comparison.line_rate_delta : null;
        const deltaClass = delta < 0 ? 'negative' : delta > 0 ? 'positive' : '';
        return `
        <button class="file-item ${coverageTone(file.line_rate)}${state.selectedFile === file.file_path ? ' selected' : ''}" data-file="${esc(file.file_path)}" type="button">
          <span class="file-row">
            <span class="file-name" title="${esc(file.file_path)}">${esc(file.file_path)}</span>
            <span class="file-rate">${pct(file.line_rate)}</span>
          </span>
          <span class="file-bar"><span style="width:${Math.max(0, Math.min(100, (file.line_rate || 0) * 100))}%"></span></span>
          <span class="file-meta">
            <span>${missed} missed${branchGaps ? `, ${branchGaps} branch gaps` : ''}</span>
            <span class="file-delta ${deltaClass}">${formatPointDelta(delta)}</span>
          </span>
        </button>
      `;
      }).join('');
      list.querySelectorAll('[data-file]').forEach(item => item.addEventListener('click', () => loadFile(item.dataset.file)));
    }

    function lineStatus(line) {
      if (!line || !line.count_line) return 'neutral';
      return line.covered ? 'covered' : 'missed';
    }

    function changedLinesForFile(filePath) {
      return state.changedByFile.get(filePath) || new Map();
    }

    function lineSummary(payload) {
      const lines = payload.lines || [];
      const covered = lines.filter(line => line.count_line && line.covered).length;
      const missed = lines.filter(line => line.count_line && !line.covered).length;
      const branchRisk = lines.filter(line => Number(line.total_branches || 0) > Number(line.covered_branches || 0)).length;
      const changed = changedLinesForFile(payload.file.file_path).size;
      document.getElementById('fileLineSummary').innerHTML = `
        <span class="summary-item"><span class="summary-mark covered"></span>${covered} covered</span>
        <span class="summary-item"><span class="summary-mark missed"></span>${missed} missed</span>
        <span class="summary-item"><span class="summary-mark branch"></span>${branchRisk} branch gaps</span>
        <span class="summary-item"><span class="summary-mark changed"></span>${changed} changed</span>
      `;
    }

    async function loadSourceLines(filePath, lines) {
      if (!lines.length) return new Map();
      const numbers = lines.map(line => line.line_number);
      const end = Math.min(2000, Math.max(...numbers));
      const requests = [];
      for (let start = 1; start <= end; start += 200) {
        requests.push(
          getJSON(`/api/source-lines?snapshot_id=${state.selected.id}&file_path=${encodeURIComponent(filePath)}&start=${start}&end=${Math.min(end, start + 199)}`)
            .catch(() => [])
        );
      }
      const chunks = await Promise.all(requests);
      return new Map(chunks.flat().map(line => [line.line_number, line.text]));
    }

    function focusLines(payload) {
      const changed = changedLinesForFile(payload.file.file_path);
      if (state.lineFilter === 'missed') {
        return payload.lines.filter(line => line.count_line && !line.covered).map(line => line.line_number);
      }
      if (state.lineFilter === 'branches') {
        return payload.lines
          .filter(line => Number(line.total_branches || 0) > Number(line.covered_branches || 0))
          .map(line => line.line_number);
      }
      if (state.lineFilter === 'changed') return [...changed.keys()];
      const attention = payload.lines
        .filter(line => (line.count_line && !line.covered) || Number(line.total_branches || 0) > Number(line.covered_branches || 0))
        .map(line => line.line_number);
      return [...new Set([...attention, ...changed.keys()])].sort((a, b) => a - b);
    }

    function contextualLineSet(focus, maxLine) {
      const visible = new Set();
      focus.forEach(line => {
        for (let current = Math.max(1, line - 2); current <= Math.min(maxLine, line + 2); current += 1) visible.add(current);
      });
      return visible;
    }

    function renderCoverageMap(payload, maxLine) {
      const map = document.getElementById('coverageMap');
      const changed = changedLinesForFile(payload.file.file_path);
      const marks = payload.lines.filter(line => {
        const missed = line.count_line && !line.covered;
        const branchRisk = Number(line.total_branches || 0) > Number(line.covered_branches || 0);
        return missed || branchRisk || changed.has(line.line_number);
      }).slice(0, 600);
      map.innerHTML = marks.map(line => {
        const change = changed.get(line.line_number);
        const missed = line.count_line && !line.covered;
        const className = change?.status === 'regressed' ? 'regressed' : missed ? '' : 'branch';
        const top = Math.max(0, Math.min(99.5, ((line.line_number - 1) / Math.max(1, maxLine)) * 100));
        return `<button class="map-mark ${className}" data-line="${line.line_number}" style="top:${top}%" title="Line ${line.line_number}"></button>`;
      }).join('');
      map.querySelectorAll('[data-line]').forEach(mark => mark.addEventListener('click', () => {
        showLine(Number(mark.dataset.line));
      }));
    }

    function renderCoverageViewer(payload, sourceByLine) {
      const viewer = document.getElementById('coverageViewer');
      const coverageByLine = new Map(payload.lines.map(line => [line.line_number, line]));
      const sourceNumbers = [...sourceByLine.keys()];
      const coverageNumbers = payload.lines.map(line => line.line_number);
      const maxLine = Math.min(2000, Math.max(0, ...sourceNumbers, ...coverageNumbers));
      const allNumbers = sourceNumbers.length
        ? Array.from({length: maxLine}, (_, index) => index + 1)
        : [...new Set(coverageNumbers)].filter(line => line <= maxLine).sort((a, b) => a - b);
      const focus = focusLines(payload);
      state.focusLines = focus;
      const contextual = state.lineFilter === 'source' ? null : contextualLineSet(focus, maxLine);
      const visible = contextual ? allNumbers.filter(line => contextual.has(line)) : allNumbers;
      if (!visible.length) {
        const label = state.lineFilter === 'changed' ? 'No line-level changes against this baseline.' : `No ${state.lineFilter} coverage gaps.`;
        viewer.innerHTML = `<div class="empty">${esc(label)}</div>`;
        renderCoverageMap(payload, maxLine);
        updateNavigation();
        return;
      }
      const changed = changedLinesForFile(payload.file.file_path);
      let previous = null;
      viewer.innerHTML = visible.map(lineNumber => {
        const line = coverageByLine.get(lineNumber);
        const status = lineStatus(line);
        const branchRisk = Number(line?.total_branches || 0) > Number(line?.covered_branches || 0);
        const change = changed.get(lineNumber);
        const source = sourceByLine.get(lineNumber);
        const sourceText = source === undefined ? '<span class="line-empty-source">source unavailable</span>' : esc(source);
        const breakRow = previous !== null && lineNumber - previous > 1
          ? `<div class="context-break">${lineNumber - previous - 1} lines hidden</div>`
          : '';
        previous = lineNumber;
        let meta = '';
        if (line?.count_line && !line.covered) meta = 'MISS';
        else if (branchRisk) meta = `B ${line.covered_branches}/${line.total_branches}`;
        else if (line?.count_line) meta = `${line.hits}x`;
        const title = [
          line?.count_line ? `${line.hits} hits` : 'not executable',
          branchRisk ? `${line.covered_branches}/${line.total_branches} branches` : '',
          change ? `${change.status} vs baseline` : ''
        ].filter(Boolean).join(', ');
        return `${breakRow}
          <div id="source-line-${lineNumber}" class="source-line ${status}${branchRisk ? ' branch-risk' : ''}${change ? ` ${change.status}` : ''}" data-line="${lineNumber}" title="${esc(title)}">
            <span class="source-line-number">${lineNumber}</span>
            <span class="coverage-gutter"></span>
            <code class="line-code">${sourceText}</code>
            <span class="source-line-meta">${esc(meta)}</span>
          </div>
        `;
      }).join('');
      viewer.querySelectorAll('[data-line]').forEach(row => row.addEventListener('click', () => {
        inspectLine(Number(row.dataset.line));
      }));
      renderCoverageMap(payload, maxLine);
      updateNavigation();
    }

    function clusterLines(numbers) {
      const sorted = [...new Set(numbers)].sort((a, b) => a - b);
      const clusters = [];
      for (const line of sorted) {
        const current = clusters[clusters.length - 1];
        if (current && line === current.end + 1) current.end = line;
        else clusters.push({start: line, end: line});
      }
      return clusters;
    }

    function renderDiagnosis(payload) {
      const file = payload.file;
      const missed = payload.lines.filter(line => line.count_line && !line.covered).map(line => line.line_number);
      const branchGaps = payload.lines
        .filter(line => Number(line.total_branches || 0) > Number(line.covered_branches || 0))
        .map(line => line.line_number);
      const changed = changedLinesForFile(file.file_path);
      const regressions = [...changed.values()].filter(line => line.status === 'regressed');
      const clusters = clusterLines(missed);
      const comparison = state.fileComparison.get(file.file_path);
      let title = 'Coverage is complete';
      let detail = 'No executable line or branch gaps are reported for this file.';
      if (regressions.length) {
        title = `${regressions.length} regression${regressions.length === 1 ? '' : 's'} to recover`;
        detail = `Start at line ${regressions[0].line_number}; it was covered in the baseline and is missed now.`;
      } else if (missed.length) {
        const first = clusters[0];
        title = `${missed.length} executable line${missed.length === 1 ? '' : 's'} missed`;
        detail = `Start with the uncovered region at ${first.start === first.end ? `line ${first.start}` : `lines ${first.start}-${first.end}`}.`;
      } else if (branchGaps.length) {
        title = `${branchGaps.length} partial branch line${branchGaps.length === 1 ? '' : 's'}`;
        detail = `Line ${branchGaps[0]} is executed, but not every condition outcome is tested.`;
      }
      const rate = Math.round((file.line_rate || 0) * 1000) / 10;
      const delta = comparison ? formatPointDelta(comparison.line_rate_delta) : '';
      document.getElementById('diagnosisContent').innerHTML = `
        <div class="coverage-score">
          <div class="score-ring" style="--coverage:${Math.max(0, Math.min(100, rate))}"><strong>${rate.toFixed(1)}%</strong></div>
          <div><strong>${esc(title)}</strong><span>${esc(detail)}</span>${delta ? `<div class="file-delta ${comparison.line_rate_delta < 0 ? 'negative' : 'positive'}" style="margin-top:6px">${esc(delta)} vs baseline</div>` : ''}</div>
        </div>
      `;
      const gapItems = [
        ...clusters.map(cluster => ({kind: 'missed', start: cluster.start, end: cluster.end})),
        ...branchGaps.filter(line => !missed.includes(line)).map(line => ({kind: 'branch', start: line, end: line}))
      ].slice(0, 10);
      document.getElementById('gapList').innerHTML = gapItems.length ? gapItems.map(item => {
        const label = item.start === item.end ? `Line ${item.start}` : `Lines ${item.start}-${item.end}`;
        const count = item.end - item.start + 1;
        return `
          <button class="gap-button ${item.kind === 'branch' ? 'branch' : ''}" type="button" data-line="${item.start}" data-filter="${item.kind === 'branch' ? 'branches' : 'missed'}">
            <span class="gap-dot"></span><code>${label}</code><span class="muted">${item.kind === 'branch' ? 'branch' : `${count} line${count === 1 ? '' : 's'}`}</span>
          </button>
        `;
      }).join('') : '<div class="muted">No uncovered regions in this file.</div>';
      document.querySelectorAll('#gapList [data-line]').forEach(button => button.addEventListener('click', () => {
        setLineFilter(button.dataset.filter);
        showLine(Number(button.dataset.line));
      }));
    }

    function updateNavigation() {
      const enabled = state.focusLines.length > 0;
      document.getElementById('prevGap').disabled = !enabled;
      document.getElementById('nextGap').disabled = !enabled;
    }

    function showLine(lineNumber) {
      const row = document.getElementById(`source-line-${lineNumber}`);
      if (!row && state.lineFilter !== 'source') {
        setLineFilter('source');
        requestAnimationFrame(() => showLine(lineNumber));
        return;
      }
      row?.scrollIntoView({block: 'center', behavior: 'smooth'});
      if (row) inspectLine(lineNumber);
    }

    function navigateGap(direction) {
      if (!state.focusLines.length) return;
      const current = state.currentLine || (direction > 0 ? 0 : Number.MAX_SAFE_INTEGER);
      const ordered = [...state.focusLines].sort((a, b) => a - b);
      let target = direction > 0
        ? ordered.find(line => line > current)
        : [...ordered].reverse().find(line => line < current);
      if (target === undefined) target = direction > 0 ? ordered[0] : ordered[ordered.length - 1];
      showLine(target);
    }

    async function inspectLine(lineNumber) {
      if (!state.selectedPayload || !state.selectedFile) return;
      state.currentLine = lineNumber;
      document.querySelectorAll('.source-line.selected').forEach(row => row.classList.remove('selected'));
      document.getElementById(`source-line-${lineNumber}`)?.classList.add('selected');
      const metric = state.selectedPayload.lines.find(line => line.line_number === lineNumber);
      const change = changedLinesForFile(state.selectedFile).get(lineNumber);
      const inspector = document.getElementById('lineInspector');
      inspector.innerHTML = '<div class="muted">Loading history...</div>';
      const history = await getJSON(
        `/api/line-history?file_path=${encodeURIComponent(state.selectedFile)}&line_number=${lineNumber}&repo_path=${encodeURIComponent(state.selected.repo_path)}&limit=100`
      ).catch(() => []);
      if (state.currentLine !== lineNumber) return;
      const status = !metric?.count_line ? 'Not executable' : metric.covered ? 'Covered' : 'Missed';
      const branches = metric?.total_branches ? `${metric.covered_branches}/${metric.total_branches}` : 'None';
      inspector.innerHTML = `
        <div class="line-facts">
          <div class="line-fact"><span>Line</span><strong>${lineNumber}</strong></div>
          <div class="line-fact"><span>Status</span><strong>${status}</strong></div>
          <div class="line-fact"><span>Hits</span><strong>${metric?.hits ?? 0}</strong></div>
          <div class="line-fact"><span>Branches</span><strong>${branches}</strong></div>
        </div>
        ${change ? `<div class="file-delta ${change.status === 'regressed' ? 'negative' : change.status === 'improved' ? 'positive' : ''}">${esc(change.status)} against baseline</div>` : ''}
        ${history.length ? `
          <div class="history-track" title="${history.length} snapshots">
            ${history.map(point => `<span class="history-point${point.covered ? ' covered' : ''}"></span>`).join('')}
          </div>
          <div class="history-caption">
            <span>${new Date(history[0].created_at).toLocaleDateString()}</span>
            <span>${history.length} snapshots</span>
            <span>${new Date(history[history.length - 1].created_at).toLocaleDateString()}</span>
          </div>
        ` : '<div class="muted" style="margin-top:8px">No earlier line history.</div>'}
      `;
    }

    function setLineFilter(filter) {
      state.lineFilter = filter;
      document.querySelectorAll('#lineFilter button').forEach(button => button.classList.toggle('active', button.dataset.filter === filter));
      if (state.selectedPayload) renderCoverageViewer(state.selectedPayload, state.sourceByLine);
    }

    async function loadFile(filePath, targetLine = null) {
      state.selectedFile = filePath;
      state.currentLine = null;
      document.getElementById('selectedFile').textContent = filePath;
      document.getElementById('fileName').textContent = filePath.split('/').pop();
      document.getElementById('filePath').textContent = filePath;
      renderFiles();
      const payload = await getJSON(`/api/snapshots/${state.selected.id}/files/${encodeURIComponent(filePath).replaceAll('%2F', '/')}`);
      if (state.selectedFile !== filePath) return;
      state.selectedPayload = payload;
      lineSummary(payload);
      const sourceByLine = await loadSourceLines(filePath, payload.lines);
      if (state.selectedFile !== filePath) return;
      state.sourceByLine = sourceByLine;
      renderCoverageViewer(payload, sourceByLine);
      renderDiagnosis(payload);
      const rate = payload.file.line_rate;
      const health = document.getElementById('fileHealth');
      health.textContent = pct(rate);
      health.className = `health-badge ${rate >= 0.85 ? 'good' : rate >= 0.6 ? 'warn' : 'danger'}`;
      if (targetLine) requestAnimationFrame(() => showLine(targetLine));
    }

    function trendDeltaSummary(deltas) {
      const metrics = [
        ['line_rate', 'Line'],
        ['branch_rate', 'Branch'],
        ['function_rate', 'Function'],
        ['region_rate', 'Region']
      ];
      return metrics
        .filter(([key]) => deltas[key] !== null && deltas[key] !== undefined)
        .map(([key, label]) => `${label} ${formatPointDelta(deltas[key])}`)
        .join(' · ');
    }

    async function loadScopedTrend() {
      if (!state.selected || !state.trendScope) {
        renderTrend([], 'No lineage selected');
        return;
      }
      const [kind, reference] = state.trendScope.split(':', 2);
      if (kind === 'worktree') {
        const params = new URLSearchParams({suite: state.selected.suite, limit: '200'});
        let progress;
        try {
          progress = await getJSON(`/api/worktrees/${encodeURIComponent(reference)}/progress?${params}`);
        } catch {
          renderTrend([], `No frozen ${state.selected.suite} baseline for this worktree`);
          return;
        }
        const worktree = progress.worktree;
        const deltas = Object.values(progress.deltas).filter(value => value !== null && value !== undefined);
        const verdict = deltas.some(value => value < 0)
          ? 'regressed'
          : deltas.some(value => value > 0) ? 'improved' : 'unchanged';
        const name = worktree.name || worktree.branch || shortPath(worktree.path);
        const summary = trendDeltaSummary(progress.deltas);
        renderTrend(
          progress.points,
          `${name} vs frozen ${worktree.base_ref} · ${verdict}${summary ? ` · ${summary}` : ''}`
        );
        return;
      }
      const params = new URLSearchParams({
        repo_path: state.selected.repo_path,
        branch: reference,
        suite: state.selected.suite,
        limit: '200'
      });
      const points = await getJSON(`/api/trend?${params}`);
      renderTrend(points, `Reference ${reference} · ${state.selected.suite}`);
    }

    function renderTrend(points, label) {
      const svg = document.getElementById('trend');
      const legend = document.getElementById('trendLegend');
      document.getElementById('trendLabel').textContent = label || 'overall';
      svg.innerHTML = '';
      const series = [
        {key: 'line_rate', label: 'Line', color: '#0f766e'},
        {key: 'branch_rate', label: 'Branch', color: '#d97706'},
        {key: 'function_rate', label: 'Function', color: '#2563eb'},
        {key: 'region_rate', label: 'Region', color: '#c0265e'}
      ].filter(item => points.some(point => point[item.key] !== null && point[item.key] !== undefined));
      legend.innerHTML = series.map(item => {
        const latest = [...points].reverse().find(point => point[item.key] !== null && point[item.key] !== undefined);
        return `
          <span class="trend-key" style="--series-color:${item.color}">
            <span class="trend-swatch"></span>${item.label} <strong>${pct(latest?.[item.key])}</strong>
          </span>
        `;
      }).join('');
      const width = 900, height = 240, padX = 36, padY = 24;
      const grid = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      for (let i = 0; i <= 4; i++) {
        const y = padY + (height - padY * 2) * i / 4;
        grid.innerHTML += `<line x1="${padX}" y1="${y}" x2="${width - padX}" y2="${y}" stroke="#d8dde6" stroke-width="1"/><text x="2" y="${y + 4}" fill="#687385" font-size="11">${100 - i * 25}%</text>`;
      }
      svg.appendChild(grid);
      if (!points.length || !series.length) return;
      const xFor = index => padX + (width - padX * 2) * (points.length === 1 ? 0.5 : index / (points.length - 1));
      const yFor = value => height - padY - (value * (height - padY * 2));
      const baselineIndex = points.findIndex(point => point.point_kind === 'baseline');
      if (baselineIndex >= 0) {
        const x = xFor(baselineIndex);
        svg.innerHTML += `
          <line x1="${x}" y1="${padY}" x2="${x}" y2="${height - padY}" stroke="#98a2b3" stroke-width="1" stroke-dasharray="4 4"/>
          <text x="${x + 5}" y="${padY + 11}" fill="#667085" font-size="10">frozen baseline</text>
        `;
      }
      for (const item of series) {
        let segment = [];
        const segments = [];
        const circles = [];
        points.forEach((point, index) => {
          const value = point[item.key];
          if (value === null || value === undefined) {
            if (segment.length) segments.push(segment);
            segment = [];
            return;
          }
          const x = xFor(index);
          const y = yFor(value);
          segment.push(`${x},${y}`);
          const isBaseline = point.point_kind === 'baseline';
          circles.push(
            `<circle cx="${x}" cy="${y}" r="${isBaseline ? 4.5 : 3.25}" fill="${item.color}" stroke="${isBaseline ? '#344054' : '#ffffff'}" stroke-width="${isBaseline ? 2 : 1.5}"><title>${isBaseline ? 'Frozen baseline | ' : ''}${item.label}: ${pct(value)} | ${new Date(point.created_at).toLocaleString()}</title></circle>`
          );
        });
        if (segment.length) segments.push(segment);
        for (const coordinates of segments) {
          if (coordinates.length > 1) {
            svg.innerHTML += `<polyline fill="none" stroke="${item.color}" stroke-width="2.75" stroke-linecap="round" stroke-linejoin="round" points="${coordinates.join(' ')}"/>`;
          }
        }
        svg.innerHTML += circles.join('');
      }
    }

    async function ingest() {
      const reportPath = document.getElementById('reportPath').value.trim();
      if (!reportPath) return;
      await getJSON('/api/ingest', {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({report_path: reportPath, format: document.getElementById('format').value})
      });
      await refresh();
    }

    function preferredBaselineId() {
      if (!state.selected) return '';
      const candidates = projectSnapshots().filter(snapshot => snapshot.id !== state.selected.id);
      const worktree = state.worktrees.find(item =>
        item.repo_path === state.selected.repo_path &&
        item.baseline_snapshot_id &&
        (!item.branch || item.branch === state.selected.branch)
      );
      if (worktree && candidates.some(snapshot => snapshot.id === worktree.baseline_snapshot_id)) {
        return worktree.baseline_snapshot_id;
      }
      const ranked = candidates.map((snapshot, index) => {
        let score = -index;
        if (snapshot.suite === state.selected.suite) score += 100;
        if (snapshot.branch === state.selected.branch) score += 200;
        if (state.selected.base_ref && snapshot.branch === state.selected.base_ref) score += 400;
        return {snapshot, score};
      }).sort((left, right) => right.score - left.score);
      return ranked[0]?.snapshot.id || '';
    }

    async function loadComparison(refreshInsightList = true, chooseDefault = false) {
      const select = document.getElementById('baselineSelect');
      if (chooseDefault || !select.value || select.value === state.selected?.id) select.value = preferredBaselineId();
      const baselineId = select.value;
      const banner = document.getElementById('comparisonBanner');
      if (!state.selected || !baselineId || baselineId === state.selected.id) {
        state.comparison = null;
        state.changedByFile = new Map();
        state.fileComparison = new Map();
        banner.hidden = true;
        return;
      }
      const result = await getJSON(
        `/api/compare?snapshot_id=${state.selected.id}&baseline_snapshot_id=${baselineId}&file_limit=1000&line_limit=5000`
      );
      state.comparison = result;
      state.fileComparison = new Map(result.files.map(file => [file.file_path, file]));
      state.changedByFile = new Map();
      result.changed_lines.forEach(line => {
        if (!state.changedByFile.has(line.file_path)) state.changedByFile.set(line.file_path, new Map());
        state.changedByFile.get(line.file_path).set(line.line_number, line);
      });
      const statusCount = status => result.changed_lines.filter(line => line.status === status).length;
      const baseline = state.snapshots.find(snapshot => snapshot.id === baselineId);
      banner.hidden = false;
      banner.innerHTML = `
        <div class="comparison-label"><strong>Compared with</strong> ${esc(baseline ? optionLabel(baseline) : baselineId)}</div>
        <div class="comparison-stat"><strong class="${result.overall.line_rate_delta < 0 ? 'status-regressed' : result.overall.line_rate_delta > 0 ? 'status-improved' : ''}">${formatPointDelta(result.overall.line_rate_delta) || '0.0 pp'}</strong><span class="muted">overall</span></div>
        <div class="comparison-stat"><strong class="status-regressed">${statusCount('regressed')}</strong><span class="muted">regressed</span></div>
        <div class="comparison-stat"><strong class="status-improved">${statusCount('improved')}</strong><span class="muted">improved</span></div>
        <div class="comparison-stat"><strong>${result.changed_lines.length}</strong><span class="muted">line changes</span></div>
      `;
      renderFiles();
      if (state.selectedPayload && state.selectedPayload.file.file_path === state.selectedFile) {
        lineSummary(state.selectedPayload);
        renderCoverageViewer(state.selectedPayload, state.sourceByLine);
        renderDiagnosis(state.selectedPayload);
      }
      if (refreshInsightList) await renderInsights(baselineId);
    }

    async function compare() {
      await loadComparison(true);
    }

    document.getElementById('refreshBtn').addEventListener('click', refresh);
    document.getElementById('ingestBtn').addEventListener('click', ingest);
    document.getElementById('compareBtn').addEventListener('click', compare);
    document.getElementById('lineFilter').addEventListener('click', event => {
      if (!event.target.matches('button[data-filter]') || !state.selectedFile) return;
      setLineFilter(event.target.dataset.filter);
    });
    document.getElementById('prevGap').addEventListener('click', () => navigateGap(-1));
    document.getElementById('nextGap').addEventListener('click', () => navigateGap(1));
    document.getElementById('fileSearch').addEventListener('input', event => {
      state.fileQuery = event.target.value;
      renderFiles();
    });
    document.getElementById('fileSort').addEventListener('change', renderFiles);
    document.getElementById('trendScope').addEventListener('change', async event => {
      state.trendScope = event.target.value;
      await loadScopedTrend();
    });
    document.getElementById('projectSelect').addEventListener('change', async event => {
      state.projectKey = event.target.value;
      state.trendScope = null;
      renderSnapshotSelectors();
      state.selected = projectSnapshots()[0] || null;
      if (state.selected) document.getElementById('snapshotSelect').value = state.selected.id;
      await renderSelected();
    });
    document.getElementById('snapshotSelect').addEventListener('change', async event => {
      state.selected = state.snapshots.find(snapshot => snapshot.id === event.target.value) || null;
      if (state.selected) state.projectKey = state.selected.repo_key;
      renderProjectSelector();
      await renderSelected();
    });
    refresh().catch(error => {
      document.getElementById('fileList').innerHTML = `<div class="empty">${esc(error.message)}</div>`;
    });
  </script>
</body>
</html>
"""


def main() -> None:
    host = os.environ.get("COVERAGE_MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("COVERAGE_MCP_PORT", str(DEFAULT_PORT)))
    uvicorn.run(create_app(), host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
