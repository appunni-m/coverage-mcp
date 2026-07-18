from __future__ import annotations

import asyncio
import contextvars
import os
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from filelock import FileLock
from mcp.client.streamable_http import streamable_http_client
from mcp.server.fastmcp import FastMCP
from mcp.server.stdio import stdio_server
from pydantic import BaseModel

from coverage_mcp import __version__
from coverage_mcp.contracts import (
    ApiEnvelope,
    ApprovalNote,
    ApprovedBy,
    ArtifactPaths,
    BaseRef,
    Branch,
    CaseSensitiveLogSearch,
    CommandCwd,
    CommandReference,
    CommandText,
    CommitSha,
    CoverageComparisonView,
    CoverageFormat,
    CoverageLineRanges,
    CoverageQueryView,
    DetailedResponse,
    FilePath,
    HumanApproval,
    IdempotencyKey,
    LogContextLines,
    LogMatchLimit,
    LogQuery,
    LogStream,
    LogWordLimit,
    NonEmptyName,
    OnlyRegressions,
    OptionalBaseRef,
    OptionalFilePath,
    OptionalLabel,
    OptionalLineNumber,
    OptionalSnapshotId,
    OptionalSuite,
    OptionalWorktreeId,
    PageCursor,
    ReportPath,
    ResponseWordBudget,
    RunAction,
    RunId,
    ShellPath,
    SnapshotId,
    SourceBoundary,
    Suite,
    TimeoutSeconds,
    WaitForCompletion,
    WorktreePath,
)
from coverage_mcp.dashboard import DASHBOARD_HTML
from coverage_mcp.git_utils import inspect_git
from coverage_mcp.service import SCHEMA_REVISION, CoverageService, RequestContext, compact_command, compact_snapshot
from coverage_mcp.storage import (
    DEFAULT_RUN_CONCURRENCY,
    DEFAULT_RUN_RETENTION,
    CommonStore,
    CoverageStore,
)
from coverage_mcp.storage_helpers import compact_run_result

DEFAULT_DB_NAME = ".coverage-mcp/coverage.duckdb"
DEFAULT_PORT = 59471
REPOSITORY_HEADER = "x-coverage-mcp-repo"
DEFAULT_DAEMON_START_TIMEOUT_SECONDS = 10.0


def default_common_db_path() -> str:
    configured = os.environ.get("COVERAGE_MCP_COMMON_DB") or os.environ.get("COVERAGE_MCP_DB")
    if configured:
        return Path(configured).expanduser().as_posix()
    return (Path.home() / ".coverage-mcp" / "common.duckdb").as_posix()


def default_daemon_lock_path() -> str:
    return (Path(default_common_db_path()).parent / "daemon.lock").as_posix()


def daemon_url() -> str:
    host = os.environ.get("COVERAGE_MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("COVERAGE_MCP_PORT", str(DEFAULT_PORT)))
    return f"http://{host}:{port}"


class CoverageRepoStore:
    """Lazily opens one CoverageStore for each canonical Git repository."""

    def __init__(self, common_store: CommonStore) -> None:
        self.common_store = common_store
        self._stores: dict[str, CoverageStore] = {}
        self._lock = threading.RLock()

    def for_repository(self, path: str) -> CoverageStore:
        repo_key = inspect_git(path).repo_key
        with self._lock:
            store = self._stores.get(repo_key)
            if store is None:
                store = CoverageStore(
                    default_db_path(repo_key),
                    run_retention=int(os.environ.get("COVERAGE_MCP_RUN_RETENTION", DEFAULT_RUN_RETENTION)),
                    run_concurrency=int(os.environ.get("COVERAGE_MCP_RUN_CONCURRENCY", DEFAULT_RUN_CONCURRENCY)),
                )
                self._stores[repo_key] = store
                self.common_store.register_repository(repo_key)
            return store

    def close(self) -> None:
        with self._lock:
            stores = list(self._stores.values())
            self._stores.clear()
        for store in stores:
            store.close()


class RepositoryStoreRouter(CoverageStore):
    """Presents the selected repository store to REST and MCP handlers."""

    def __init__(self, stores: CoverageRepoStore) -> None:
        self.stores = stores
        self._selected: contextvars.ContextVar[CoverageStore | None] = contextvars.ContextVar(
            "coverage_mcp_repository_store", default=None
        )
        self._checkout: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "coverage_mcp_checkout_path", default=None
        )

    def select(self, path: str) -> tuple[contextvars.Token[CoverageStore | None], contextvars.Token[str | None]]:
        git = inspect_git(path)
        return self._selected.set(self.stores.for_repository(git.repo_key)), self._checkout.set(git.path)

    def reset(
        self,
        token: tuple[contextvars.Token[CoverageStore | None], contextvars.Token[str | None]],
    ) -> None:
        store_token, checkout_token = token
        self._selected.reset(store_token)
        self._checkout.reset(checkout_token)

    def request_context(self) -> RequestContext:
        store = self._selected.get()
        checkout = self._checkout.get()
        if store is None or checkout is None:
            raise RuntimeError("a repository must be selected before using coverage data")
        return RequestContext(repo_key=inspect_git(checkout).repo_key, checkout_path=checkout)

    def projects(self, limit: int = 100) -> list[dict[str, Any]]:
        store = self._selected.get()
        if store is not None:
            return store.projects(limit=limit)
        projects: list[dict[str, Any]] = []
        for registered in self.stores.common_store.repositories(limit=1000):
            try:
                repository_projects = self.stores.for_repository(str(registered["repo_key"])).projects(limit=1)
            except (FileNotFoundError, OSError, ValueError):
                repository_projects = []
            projects.append(repository_projects[0] if repository_projects else registered)
            if len(projects) >= limit:
                break
        return projects

    def close(self) -> None:
        self.stores.close()
        self.stores.common_store.close()

    def __getattr__(self, name: str) -> Any:
        store = self._selected.get()
        if store is None:
            raise RuntimeError("a repository must be selected before using coverage data")
        return getattr(store, name)


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
    timeout_seconds: TimeoutSeconds = None
    idempotency_key: IdempotencyKey = None
    wait: bool = False
    detailed: bool = False
    max_words: ResponseWordBudget = 600


def create_app(db_path: str | None = None, *, common_db_path: str | None = None) -> FastAPI:
    run_retention = int(os.environ.get("COVERAGE_MCP_RUN_RETENTION", DEFAULT_RUN_RETENTION))
    run_concurrency = int(os.environ.get("COVERAGE_MCP_RUN_CONCURRENCY", DEFAULT_RUN_CONCURRENCY))
    if db_path is None:
        store: CoverageStore = RepositoryStoreRouter(
            CoverageRepoStore(CommonStore(common_db_path or default_common_db_path()))
        )
    else:
        store = CoverageStore(db_path, run_retention=run_retention, run_concurrency=run_concurrency)
    if isinstance(store, RepositoryStoreRouter):
        context_provider = store.request_context
    else:
        standalone_git = inspect_git(Path(db_path or ".").parent.as_posix())

        def context_provider() -> RequestContext:
            return RequestContext(standalone_git.repo_key, standalone_git.path)

    service = CoverageService(store, context_provider)
    mcp = create_mcp(store, service)
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
        version=__version__,
        lifespan=lifespan,
    )
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"],
    )
    app.state.coverage_store = store
    app.state.coverage_service = service

    @app.middleware("http")
    async def security_headers(request: Any, call_next: Any) -> Any:
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; base-uri 'none'; connect-src 'self'; frame-ancestors 'none'; "
            "form-action 'self'; img-src 'self' data:; script-src 'unsafe-inline'; style-src 'unsafe-inline'"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    if isinstance(store, RepositoryStoreRouter):

        @app.middleware("http")
        async def select_repository(request: Any, call_next: Any) -> Any:
            if request.url.path in {"/", "/favicon.ico", "/health", "/api/projects"}:
                return await call_next(request)
            repo_path = request.headers.get(REPOSITORY_HEADER)
            if not repo_path:
                return JSONResponse(
                    status_code=400,
                    content={"detail": f"missing {REPOSITORY_HEADER} header"},
                )
            try:
                token = store.select(repo_path)
            except (FileNotFoundError, ValueError) as exc:
                return JSONResponse(status_code=400, content={"detail": str(exc)})
            try:
                return await call_next(request)
            finally:
                store.reset(token)

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> str:
        return DASHBOARD_HTML

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/health")
    def health() -> dict[str, Any]:
        if isinstance(store, RepositoryStoreRouter):
            return {
                "ok": True,
                "pid": os.getpid(),
                "version": __version__,
                "schema_revision": SCHEMA_REVISION,
                "common_db_path": store.stores.common_store.db_path.as_posix(),
                "repository_count": len(store.stores._stores),
                "run_retention": run_retention,
                "run_concurrency": run_concurrency,
            }
        return {
            "ok": True,
            "pid": os.getpid(),
            "version": __version__,
            "schema_revision": SCHEMA_REVISION,
            "db_path": store.db_path.as_posix(),
            "run_retention": store.run_retention,
            "run_concurrency": store.run_concurrency,
        }

    @app.post("/api/ingest")
    def ingest(
        request: IngestRequest,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            service.validate_repository_path(request.repo_path)
            response = service.ingest(
                request.report_path,
                format=request.format,
                suite=request.suite,
                branch=request.branch,
                commit_sha=request.commit_sha,
                base_ref=request.base_ref,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.post("/api/worktrees/register")
    def register_worktree(
        request: RegisterWorktreeRequest,
        max_words: int = Query(default=600, ge=50, le=5000),
    ) -> dict[str, Any]:
        try:
            response = service.worktree_registration(
                request.path,
                base_ref=request.base_ref,
                name=request.name,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/worktrees")
    def worktrees(
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = store.list_worktrees(limit=1000)
        if not detailed:
            values = [
                {
                    key: item.get(key)
                    for key in ("id", "name", "created_at", "path", "branch", "base_ref", "baseline_snapshot_id")
                }
                for item in values
            ]
        return service.collection(
            values,
            cursor=cursor,
            max_words=max_words,
            scope=f"worktrees:{detailed}",
        ).model_dump()

    @app.get("/api/worktrees/{worktree_id}/progress")
    def worktree_progress(
        worktree_id: str,
        suite: str | None = None,
        file_path: str | None = None,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.coverage_comparison(
                view="progress",
                snapshot_id=None,
                baseline_snapshot_id=None,
                worktree_id=worktree_id,
                suite=suite,
                file_path=file_path,
                only_regressions=False,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/projects")
    def projects(
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = store.projects(limit=1000)
        if not detailed:
            keys = (
                "repo_key",
                "repo_path",
                "snapshot_count",
                "branch_count",
                "command_count",
                "run_count",
                "latest_snapshot_id",
                "latest_snapshot_age",
                "latest_run_age",
                "latest_suite",
                "line_rate",
            )
            values = [{key: value.get(key) for key in keys} for value in values]
        try:
            selected, page = service.page(values, cursor=cursor, max_words=max_words, scope=f"projects:{detailed}")
            return ApiEnvelope.model_validate(
                {
                    "context": {
                        "repo_key": "*",
                        "checkout_path": "",
                        "suite": None,
                        "schema_revision": SCHEMA_REVISION,
                    },
                    "data": selected,
                    "page": page,
                }
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.post("/api/commands/register")
    def register_command(
        request: RegisterCommandRequest,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.command_registration(
                name=request.name,
                command=request.command,
                cwd=request.cwd,
                shell=request.shell,
                artifact_paths=request.artifact_paths,
                human_approved=request.human_approved,
                approved_by=request.approved_by,
                approval_note=request.approval_note,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/commands")
    def commands(
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = [compact_command(item, detailed=detailed) for item in store.list_registered_commands(limit=1000)]
        return service.collection(
            values,
            cursor=cursor,
            max_words=max_words,
            scope=f"commands:{detailed}",
        ).model_dump()

    @app.get("/api/commands/{command_ref}")
    def command(
        command_ref: str,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.envelope(compact_command(store.registered_command(command_ref), detailed=detailed))
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.post("/api/runs/profiled")
    def run_profiled(request: RunCommandRequest) -> dict[str, Any]:
        try:
            response = service.run_submission(
                request.command_ref,
                timeout_seconds=request.timeout_seconds,
                idempotency_key=request.idempotency_key,
                wait=request.wait,
                detailed=request.detailed,
            )
            return service.apply_budget(response, max_words=request.max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/runs/queue")
    def run_queue(
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = store.list_run_queue(limit=1000)
        if not detailed:
            values = [compact_run_result(run) for run in values]
        return service.collection(
            values, cursor=cursor, max_words=max_words, scope=f"run-queue:{detailed}"
        ).model_dump()

    @app.post("/api/runs/{run_id}/cancel")
    def cancel_run(
        run_id: str,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.run_state(run_id, action="cancel", detailed=detailed)
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/runs/latest")
    def latest_run(
        command_ref: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        latest = store.latest_run(command_ref=command_ref)
        if latest is None:
            raise HTTPException(status_code=404, detail="no runs found")
        response = service.run_state(str(latest["id"]), action="status", detailed=detailed)
        return service.apply_budget(response, max_words=max_words).model_dump()

    @app.get("/api/runs/{run_id}/logs/search")
    def search_run_logs(
        run_id: str,
        query: str = Query(min_length=1, max_length=500),
        stream: str = Query(default="both", pattern="^(both|stdout|stderr)$"),
        context_lines: int = Query(default=3, ge=0, le=10),
        max_matches: int = Query(default=5, ge=1, le=20),
        max_words: int = Query(default=400, ge=20, le=2000),
        case_sensitive: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.search_logs(
                run_id,
                query,
                stream=stream,
                context_lines=context_lines,
                max_matches=max_matches,
                max_words=max_words,
                case_sensitive=case_sensitive,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/runs/{run_id}")
    def run(
        run_id: str,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.run_state(run_id, action="status", detailed=detailed)
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/artifacts/latest")
    def latest_artifact(
        kind: str,
        command_ref: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        artifact = store.latest_artifact(command_ref=command_ref, kind=kind)
        if artifact is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        if not detailed:
            artifact = {
                key: artifact.get(key)
                for key in (
                    "run_id",
                    "command_name",
                    "kind",
                    "exists",
                    "coverage_format",
                    "ingest_status",
                    "snapshot_id",
                )
            }
        response = service.envelope(artifact, suite=artifact.get("suite"))
        return service.apply_budget(response, max_words=max_words).model_dump()

    @app.get("/api/topology/{object_kind}/{object_ref:path}")
    def object_topology(
        object_kind: str,
        object_ref: str,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            topology = store.object_topology(object_kind, object_ref)
            if not detailed:
                topology = {
                    "object_kind": topology.get("object_kind"),
                    "object_ref": topology.get("object_ref"),
                    "topology": {key: topology.get("topology", {}).get(key) for key in ("kind", "project")},
                }
            response = service.envelope(topology)
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots")
    def snapshots(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = [
            compact_snapshot(item, detailed=detailed)
            for item in store.list_snapshots(repo_path=repo_path, branch=branch, suite=suite, limit=1000)
        ]
        return service.collection(
            values,
            cursor=cursor,
            max_words=max_words,
            scope=f"snapshots:{repo_path}:{branch}:{suite}:{detailed}",
            suite=suite,
        ).model_dump()

    @app.get("/api/snapshots/latest")
    def latest_snapshot(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            service.validate_repository_path(repo_path)
            response = service.coverage_query(
                view="summary",
                snapshot_id=None,
                suite=suite,
                branch=branch,
                file_path=None,
                line_number=None,
                line_ranges=None,
                cursor=None,
                max_words=600,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}")
    def snapshot(
        snapshot_id: str,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.coverage_query(
                view="summary",
                snapshot_id=snapshot_id,
                suite=None,
                branch=None,
                file_path=None,
                line_number=None,
                line_ranges=None,
                cursor=None,
                max_words=600,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/files")
    def files(
        snapshot_id: str,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.coverage_query(
                view="files",
                snapshot_id=snapshot_id,
                suite=None,
                branch=None,
                file_path=None,
                line_number=None,
                line_ranges=None,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/insights")
    def insights(
        snapshot_id: str,
        baseline_snapshot_id: str | None = None,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.coverage_query(
                view="insights",
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                suite=None,
                branch=None,
                file_path=None,
                line_number=None,
                line_ranges=None,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/snapshots/{snapshot_id}/files/{file_path:path}")
    def file_coverage(
        snapshot_id: str,
        file_path: str,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.file_detail(
                snapshot_id=snapshot_id,
                file_path=file_path,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/trend")
    def trend(
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        file_path: str | None = None,
        worktree_id: str | None = None,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        values = store.trend(
            repo_path=repo_path,
            branch=branch,
            suite=suite,
            file_path=file_path,
            worktree_id=worktree_id,
            limit=2000,
        )
        if not detailed:
            values = [
                {key: item for key, item in value.items() if key not in {"minute_bucket", "file_path"}}
                for value in values
            ]
        return service.collection(
            values,
            cursor=cursor,
            max_words=max_words,
            scope=f"trend:{repo_path}:{branch}:{suite}:{file_path}:{worktree_id}:{detailed}",
            suite=suite,
        ).model_dump()

    @app.post("/api/compare")
    def compare_post(
        request: CompareRequest,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.coverage_comparison(
                view="overview",
                snapshot_id=request.snapshot_id,
                baseline_snapshot_id=request.baseline_snapshot_id,
                worktree_id=None,
                suite=None,
                file_path=None,
                only_regressions=False,
                cursor=None,
                max_words=max_words,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/compare")
    def compare_get(
        snapshot_id: str,
        baseline_snapshot_id: str,
        view: str = "overview",
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.coverage_comparison(
                view=view,
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                worktree_id=None,
                suite=None,
                file_path=None,
                only_regressions=False,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/worktrees/{worktree_id}/compare")
    def compare_worktree(
        worktree_id: str,
        snapshot_id: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            response = service.coverage_comparison(
                view="overview",
                snapshot_id=snapshot_id,
                baseline_snapshot_id=None,
                worktree_id=worktree_id,
                suite=None,
                file_path=None,
                only_regressions=False,
                cursor=None,
                max_words=max_words,
                detailed=detailed,
            )
            return service.apply_budget(response, max_words=max_words).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/changed-lines")
    def changed_lines(
        snapshot_id: str,
        baseline_snapshot_id: str,
        file_path: str | None = None,
        only_regressions: bool = False,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            return service.coverage_comparison(
                view="lines",
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                worktree_id=None,
                suite=None,
                file_path=file_path,
                only_regressions=only_regressions,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/line-history")
    def line_history(
        file_path: str,
        line_number: int = Query(ge=1),
        repo_path: str | None = None,
        branch: str | None = None,
        suite: str | None = None,
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
        detailed: bool = False,
    ) -> dict[str, Any]:
        try:
            service.validate_repository_path(repo_path)
            return service.coverage_query(
                view="line_history",
                snapshot_id=None,
                suite=suite,
                branch=branch,
                file_path=file_path,
                line_number=line_number,
                line_ranges=None,
                cursor=cursor,
                max_words=max_words,
                detailed=detailed,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    @app.get("/api/source-lines")
    def source_lines(
        snapshot_id: str,
        file_path: str,
        start: int = Query(ge=1),
        end: int = Query(ge=1),
        cursor: str | None = None,
        max_words: int = Query(default=600, ge=50, le=5000),
    ) -> dict[str, Any]:
        try:
            return service.source(
                snapshot_id=snapshot_id,
                file_path=file_path,
                start=start,
                end=end,
                cursor=cursor,
                max_words=max_words,
            ).model_dump()
        except Exception as exc:
            raise _http_error(exc) from exc

    app.mount("/mcp", mcp_app)
    return app


def create_mcp(store: CoverageStore, service: CoverageService | None = None) -> FastMCP:
    """Create the compact agent-facing MCP contract."""
    shared = service or CoverageService(store)
    mcp = FastMCP(
        "coverage-mcp",
        instructions=(
            f"Coverage MCP {__version__} schema 7 exposes a compact agent interface. Start with project_context, "
            "run only exact approved registrations, use coverage_query for snapshot reads, and use "
            "coverage_compare only for lineage-compatible snapshots. max_words is the primary response budget; "
            "continue collections with next_cursor. Always omit detailed or keep it false for normal work. Set it "
            "true only when a tool's description names a required audit/raw-provenance field; never use detailed "
            "to obtain logs."
        ),
        stateless_http=True,
        streamable_http_path="/",
    )

    @mcp.tool()
    async def project_context(
        cursor: PageCursor = None,
        max_words: ResponseWordBudget = 600,
        detailed: DetailedResponse = False,
    ) -> ApiEnvelope:
        """Return compact project, exact approved commands, newest run, and queue. Use detailed only for audit data."""
        return await asyncio.to_thread(
            shared.project_context,
            cursor=cursor,
            max_words=max_words,
            detailed=detailed,
        )

    @mcp.tool()
    async def register_test_command(
        name: NonEmptyName,
        command: CommandText,
        human_approved: HumanApproval,
        approved_by: ApprovedBy,
        approval_note: ApprovalNote,
        cwd: CommandCwd = None,
        shell: ShellPath = "/bin/bash",
        artifact_paths: ArtifactPaths = None,
        max_words: ResponseWordBudget = 600,
    ) -> ApiEnvelope:
        """Record one exact approved command; compact output already includes its complete execution definition."""
        response = await asyncio.to_thread(
            shared.command_registration,
            name=name,
            command=command,
            human_approved=human_approved,
            approved_by=approved_by,
            approval_note=approval_note,
            cwd=cwd,
            shell=shell,
            artifact_paths=artifact_paths,
            detailed=False,
        )
        return shared.apply_budget(response, max_words=max_words)

    @mcp.tool()
    async def run_test(
        command_ref: CommandReference,
        timeout_seconds: TimeoutSeconds = None,
        idempotency_key: IdempotencyKey = None,
        wait: WaitForCompletion = False,
        max_words: ResponseWordBudget = 600,
    ) -> ApiEnvelope:
        """Submit one approved test command in compact mode; poll test_run for state or exceptional audit detail."""
        response = await asyncio.to_thread(
            shared.run_submission,
            command_ref,
            timeout_seconds=timeout_seconds,
            idempotency_key=idempotency_key,
            wait=wait,
            detailed=False,
        )
        return shared.apply_budget(response, max_words=max_words)

    @mcp.tool()
    async def test_run(
        run_id: RunId,
        action: RunAction = "status",
        max_words: ResponseWordBudget = 600,
        detailed: DetailedResponse = False,
    ) -> ApiEnvelope:
        """Poll or cancel a run. Keep detailed false; use true only for artifact paths, exact timestamps, or audit."""
        response = await asyncio.to_thread(shared.run_state, run_id, action=action, detailed=detailed)
        return shared.apply_budget(response, max_words=max_words)

    @mcp.tool()
    async def search_test_logs(
        run_id: RunId,
        query: LogQuery,
        stream: LogStream = "both",
        context_lines: LogContextLines = 3,
        max_matches: LogMatchLimit = 5,
        max_words: LogWordLimit = 400,
        case_sensitive: CaseSensitiveLogSearch = False,
    ) -> ApiEnvelope:
        """Search retained output literally; returns only word-bounded merged context without redundant input echoes."""
        return await asyncio.to_thread(
            shared.search_logs,
            run_id,
            query,
            stream=stream,
            context_lines=context_lines,
            max_matches=max_matches,
            max_words=max_words,
            case_sensitive=case_sensitive,
        )

    @mcp.tool()
    async def ingest_coverage(
        report_path: ReportPath,
        format: CoverageFormat = "auto",
        suite: Suite = "default",
        branch: Branch = None,
        commit_sha: CommitSha = None,
        base_ref: OptionalBaseRef = None,
        max_words: ResponseWordBudget = 600,
    ) -> ApiEnvelope:
        """Ingest one external report compactly; parser warnings are included by default."""
        response = await asyncio.to_thread(
            shared.ingest,
            report_path,
            format=format,
            suite=suite,
            branch=branch,
            commit_sha=commit_sha,
            base_ref=base_ref,
            detailed=False,
        )
        return shared.apply_budget(response, max_words=max_words)

    @mcp.tool()
    async def register_worktree(
        path: WorktreePath,
        base_ref: BaseRef,
        name: OptionalLabel = None,
        max_words: ResponseWordBudget = 600,
    ) -> ApiEnvelope:
        """Register one linked checkout and return all useful frozen-baseline identity without topology duplication."""
        response = await asyncio.to_thread(
            shared.worktree_registration,
            path,
            base_ref=base_ref,
            name=name,
        )
        return shared.apply_budget(response, max_words=max_words)

    @mcp.tool()
    async def coverage_query(
        view: CoverageQueryView,
        snapshot_id: OptionalSnapshotId = None,
        baseline_snapshot_id: OptionalSnapshotId = None,
        suite: OptionalSuite = None,
        branch: Branch = None,
        file_path: OptionalFilePath = None,
        line_number: OptionalLineNumber = None,
        line_ranges: CoverageLineRanges = None,
        cursor: PageCursor = None,
        max_words: ResponseWordBudget = 600,
        detailed: DetailedResponse = False,
    ) -> ApiEnvelope:
        """Read compact coverage. Use detailed only for parser metadata/report provenance or raw file metrics."""
        return await asyncio.to_thread(
            shared.coverage_query,
            view=view,
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            suite=suite,
            branch=branch,
            file_path=file_path,
            line_number=line_number,
            line_ranges=[{"start": item["start"], "end": item["end"]} for item in line_ranges or []],
            cursor=cursor,
            max_words=max_words,
            detailed=detailed,
        )

    @mcp.tool()
    async def coverage_compare(
        view: CoverageComparisonView = "overview",
        snapshot_id: OptionalSnapshotId = None,
        baseline_snapshot_id: OptionalSnapshotId = None,
        worktree_id: OptionalWorktreeId = None,
        suite: OptionalSuite = None,
        file_path: OptionalFilePath = None,
        only_regressions: OnlyRegressions = False,
        cursor: PageCursor = None,
        max_words: ResponseWordBudget = 600,
        detailed: DetailedResponse = False,
    ) -> ApiEnvelope:
        """Compare compact lineage. Use detailed only when raw snapshot provenance is explicitly required."""
        return await asyncio.to_thread(
            shared.coverage_comparison,
            view=view,
            snapshot_id=snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            worktree_id=worktree_id,
            suite=suite,
            file_path=file_path,
            only_regressions=only_regressions,
            cursor=cursor,
            max_words=max_words,
            detailed=detailed,
        )

    @mcp.tool()
    async def source_context(
        snapshot_id: SnapshotId,
        file_path: FilePath,
        start: SourceBoundary,
        end: SourceBoundary,
        cursor: PageCursor = None,
        max_words: ResponseWordBudget = 600,
    ) -> ApiEnvelope:
        """Read word-bounded source lines with snapshot commit identity; no expanded mode is needed."""
        return await asyncio.to_thread(
            shared.source,
            snapshot_id=snapshot_id,
            file_path=file_path,
            start=start,
            end=end,
            cursor=cursor,
            max_words=max_words,
        )

    @mcp.resource("coverage://context", mime_type="application/json")
    async def context_resource() -> dict[str, Any]:
        return (
            await asyncio.to_thread(shared.project_context, cursor=None, max_words=600, detailed=False)
        ).model_dump()

    @mcp.resource("coverage://snapshot/{snapshot_id}/summary", mime_type="application/json")
    async def compact_summary_resource(snapshot_id: str) -> dict[str, Any]:
        return (
            await asyncio.to_thread(
                shared.coverage_query,
                view="summary",
                snapshot_id=snapshot_id,
                suite=None,
                branch=None,
                file_path=None,
                line_number=None,
                line_ranges=None,
                cursor=None,
                max_words=600,
                detailed=False,
            )
        ).model_dump()

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


def daemon_is_healthy(url: str | None = None) -> bool:
    try:
        response = httpx.get(f"{url or daemon_url()}/health", timeout=0.25)
        payload = response.json()
        return (
            response.status_code == 200
            and payload.get("ok") is True
            and payload.get("version") == __version__
            and payload.get("schema_revision") == SCHEMA_REVISION
        )
    except (httpx.HTTPError, ValueError):
        return False


def daemon_is_reachable(url: str | None = None) -> bool:
    """Distinguish an occupied Coverage MCP port from an absent daemon."""
    try:
        response = httpx.get(f"{url or daemon_url()}/health", timeout=0.25)
        return response.status_code == 200 and response.json().get("ok") is True
    except (httpx.HTTPError, ValueError):
        return False


def start_daemon() -> None:
    log_path = Path(default_common_db_path()).parent / "daemon.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, "-m", "coverage_mcp.app", "serve"],
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
        close_fds=True,
    )
    log_file.close()


def ensure_daemon(
    *,
    timeout_seconds: float = DEFAULT_DAEMON_START_TIMEOUT_SECONDS,
    sleep_seconds: float = 0.05,
) -> str:
    url = daemon_url()
    if daemon_is_healthy(url):
        return url
    lock_path = Path(default_daemon_lock_path())
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(lock_path)
    with lock:
        if daemon_is_healthy(url):
            return url
        if daemon_is_reachable(url):
            raise RuntimeError(
                f"Coverage MCP daemon at {url} uses an incompatible version or schema; stop it before reconnecting"
            )
        start_daemon()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if daemon_is_healthy(url):
                return url
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Coverage MCP daemon did not become healthy at {url}")


async def forward_mcp_messages(source: Any, destination: Any) -> None:
    try:
        async with source:
            async for message in source:
                if isinstance(message, Exception):
                    raise message
                await destination.send(message)
    finally:
        await destination.aclose()


async def proxy_stdio_to_http(url: str, repo_path: str) -> None:
    async with (
        stdio_server() as (stdio_read, stdio_write),
        httpx.AsyncClient(headers={REPOSITORY_HEADER: repo_path}) as client,
        streamable_http_client(f"{url}/mcp/", http_client=client) as (http_read, http_write, _),
        anyio.create_task_group() as task_group,
    ):
        task_group.start_soon(forward_mcp_messages, stdio_read, http_write)
        task_group.start_soon(forward_mcp_messages, http_read, stdio_write)


def connect() -> None:
    url = ensure_daemon()
    repo_path = inspect_git(None).path
    anyio.run(proxy_stdio_to_http, url, repo_path)


def serve() -> None:
    host = os.environ.get("COVERAGE_MCP_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError("Coverage MCP only supports loopback HTTP binding")
    port = int(os.environ.get("COVERAGE_MCP_PORT", str(DEFAULT_PORT)))
    uvicorn.run(create_app(), host=host, port=port, reload=False)


def main(argv: list[str] | None = None) -> None:
    arguments = sys.argv[1:] if argv is None else argv
    if not arguments or arguments == ["serve"]:
        serve()
    elif arguments == ["connect"]:
        connect()
    elif arguments == ["--version"]:
        print(__version__)
    else:
        raise SystemExit("usage: coverage-mcp [serve|connect|--version]")


if __name__ == "__main__":
    main()
