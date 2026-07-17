from __future__ import annotations

import runpy
import sys
import time
from pathlib import Path

import anyio
import httpx
import pytest
from fastapi.testclient import TestClient

from coverage_mcp import app as app_module
from coverage_mcp.app import create_app


def test_readme_lists_every_rest_endpoint(tmp_path):
    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    try:
        readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
        documented = {
            line.removeprefix("- `").removesuffix("`")
            for line in readme.splitlines()
            if line.startswith(("- `GET /api/", "- `POST /api/"))
        }
        exposed = {
            f"{method} {route.path}"
            for route in app.routes
            if route.path.startswith("/api/")
            for method in route.methods or set()
            if method in {"GET", "POST"}
        }

        assert documented == exposed
    finally:
        app.state.coverage_store.close()


def test_rest_endpoints_cover_success_and_error_paths(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.py").write_text("one\ntwo\nthree\n", encoding="utf-8")
    base = tmp_path / "base.lcov"
    current = tmp_path / "current.lcov"
    base.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,1\nend_of_record\n", encoding="utf-8")
    current.write_text("TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:3,1\nend_of_record\n", encoding="utf-8")

    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    with TestClient(app) as client:
        health = client.get("/health").json()
        assert health["version"] == "0.4.0"
        assert health["run_retention"] == 100
        assert health["run_concurrency"] == 4
        assert client.get("/api/snapshots/latest").status_code == 404
        assert client.get("/api/artifacts/latest?kind=missing").status_code == 404
        assert client.get("/api/runs/latest").status_code == 404
        assert client.get("/api/commands/missing").status_code == 404
        assert client.get("/api/topology/nope/value").status_code == 400

        base_snapshot = client.post(
            "/api/ingest",
            json={
                "report_path": base.as_posix(),
                "format": "lcov",
                "repo_path": tmp_path.as_posix(),
                "branch": "main",
                "commit_sha": "base",
                "suite": "unit",
            },
        ).json()
        current_snapshot = client.post(
            "/api/ingest",
            json={
                "report_path": current.as_posix(),
                "format": "lcov",
                "repo_path": tmp_path.as_posix(),
                "branch": "feature",
                "commit_sha": "head",
                "base_ref": "main",
                "suite": "unit",
            },
        ).json()

        assert client.get("/api/snapshots/latest?branch=missing").status_code == 404
        latest_response = client.get(f"/api/snapshots/latest?repo_path={tmp_path.as_posix()}&branch=feature")
        assert latest_response.json()["id"] == current_snapshot["id"]
        assert client.get(f"/api/snapshots/{current_snapshot['id']}").status_code == 200
        assert client.get("/api/snapshots/missing").status_code == 404
        assert client.get(f"/api/snapshots/{current_snapshot['id']}/files").status_code == 200
        assert client.get(f"/api/snapshots/{current_snapshot['id']}/files/src/a.py").status_code == 200
        assert client.get(f"/api/snapshots/{current_snapshot['id']}/files/nope.py").status_code == 404
        assert (
            client.get(
                f"/api/snapshots/{current_snapshot['id']}/insights?baseline_snapshot_id={base_snapshot['id']}"
            ).status_code
            == 200
        )
        assert client.get(f"/api/trend?repo_path={tmp_path.as_posix()}&branch=feature&suite=unit").status_code == 200
        assert client.get(f"/api/trend?repo_path={tmp_path.as_posix()}&file_path=src/a.py").status_code == 200
        assert (
            client.post(
                "/api/compare",
                json={"snapshot_id": current_snapshot["id"], "baseline_snapshot_id": base_snapshot["id"]},
            ).status_code
            == 200
        )
        assert (
            client.get(
                f"/api/compare?snapshot_id={current_snapshot['id']}&baseline_snapshot_id={base_snapshot['id']}"
            ).status_code
            == 200
        )
        assert client.get("/api/compare?snapshot_id=missing&baseline_snapshot_id=missing").status_code == 404
        assert (
            client.get(
                f"/api/changed-lines?snapshot_id={current_snapshot['id']}&baseline_snapshot_id={base_snapshot['id']}&file_path=src/a.py&only_regressions=true"
            ).json()[0]["status"]
            == "regressed"
        )
        assert (
            client.get(
                f"/api/line-history?file_path=src/a.py&line_number=1&repo_path={tmp_path.as_posix()}&branch=feature"
            ).status_code
            == 200
        )
        assert (
            client.get(
                f"/api/source-lines?snapshot_id={current_snapshot['id']}&file_path=src/a.py&start=1&end=500"
            ).json()[-1]["line_number"]
            == 3
        )
        assert (
            client.get(
                f"/api/source-lines?snapshot_id={current_snapshot['id']}&file_path=../secret&start=1&end=1"
            ).status_code
            == 400
        )
        assert (
            client.get(
                f"/api/source-lines?snapshot_id={current_snapshot['id']}&file_path=missing.py&start=1&end=1"
            ).status_code
            == 400
        )
        assert client.get("/api/worktrees").json() == []
        worktree_response = client.post(
            "/api/worktrees/register",
            json={"path": tmp_path.as_posix(), "base_ref": "main"},
        )
        assert worktree_response.status_code == 200
        worktree = client.get("/api/worktrees").json()[0]
        progress_response = client.get(f"/api/worktrees/{worktree['id']}/progress?suite=unit")
        assert progress_response.status_code == 200
        assert progress_response.json()["baseline"]["id"] == base_snapshot["id"]
        comparison_response = client.get(
            f"/api/worktrees/{worktree['id']}/compare?snapshot_id={current_snapshot['id']}"
        )
        assert comparison_response.status_code == 200
        assert client.get("/api/worktrees/missing/compare").status_code == 404


def test_rest_run_errors_and_timeout(tmp_path):
    script = tmp_path / "sleep.py"
    script.write_text("import time\ntime.sleep(2)\n", encoding="utf-8")
    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    with TestClient(app) as client:
        assert client.post("/api/runs/profiled", json={"command_ref": "missing"}).status_code == 404
        command = client.post(
            "/api/commands/register",
            json={
                "name": "slow",
                "command": f"{sys.executable} {script.name}",
                "cwd": tmp_path.as_posix(),
                "human_approved": True,
                "approved_by": "tester",
                "approval_note": "approved slow command",
            },
        ).json()
        cancellable = client.post(
            "/api/runs/profiled",
            json={"command_ref": command["id"], "idempotency_key": "cancel-via-rest"},
        ).json()
        cancellation = client.post(f"/api/runs/{cancellable['id']}/cancel").json()
        assert cancellation["status"] in {"running", "cancelled"}
        for _ in range(100):
            cancellation = client.get(f"/api/runs/{cancellable['id']}").json()
            if cancellation["terminal"]:
                break
            time.sleep(0.02)
        assert cancellation["status"] == "cancelled"
        assert client.post(f"/api/runs/{cancellable['id']}/cancel").json()["status"] == "cancelled"
        run = client.post(
            "/api/runs/profiled",
            json={"command_ref": command["id"], "timeout_seconds": 1, "max_summary_lines": 5, "wait": True},
        ).json()
        assert run["status"] == "timeout"
        assert client.get(f"/api/runs/{run['id']}?max_summary_lines=1").json()["id"] == run["id"]
        assert client.get("/api/runs/missing").status_code == 404
        assert client.post("/api/runs/missing/cancel").status_code == 404


def test_rest_error_wrappers_and_main(monkeypatch, tmp_path):
    app = create_app((tmp_path / "coverage.duckdb").as_posix())
    store = app.state.coverage_store
    with TestClient(app) as client:

        def value_error(*args, **kwargs):
            raise ValueError("bad input")

        def runtime_error(*args, **kwargs):
            raise RuntimeError("boom")

        for method_name, request in [
            ("ingest_report", lambda: client.post("/api/ingest", json={"report_path": "missing"})),
            (
                "register_worktree",
                lambda: client.post("/api/worktrees/register", json={"path": "missing", "base_ref": "main"}),
            ),
            ("worktree_progress", lambda: client.get("/api/worktrees/w/progress")),
            ("files", lambda: client.get("/api/snapshots/s/files")),
            ("insights", lambda: client.get("/api/snapshots/s/insights")),
            ("compare", lambda: client.post("/api/compare", json={"snapshot_id": "s", "baseline_snapshot_id": "b"})),
            ("changed_lines", lambda: client.get("/api/changed-lines?snapshot_id=s&baseline_snapshot_id=b")),
            ("source_lines", lambda: client.get("/api/source-lines?snapshot_id=s&file_path=f&start=1&end=1")),
        ]:
            monkeypatch.setattr(store, method_name, value_error)
            assert request().status_code == 400

        monkeypatch.setattr(store, "registered_command", runtime_error)
        assert client.get("/api/commands/x").status_code == 500

    calls = {}
    monkeypatch.setenv("COVERAGE_MCP_HOST", "0.0.0.0")
    monkeypatch.setenv("COVERAGE_MCP_PORT", "8765")
    monkeypatch.setattr(app_module, "create_app", lambda: "created-app")

    def fake_run(app, *, host, port, reload):
        if hasattr(app, "state") and hasattr(app.state, "coverage_store"):
            app.state.coverage_store.close()
        calls.update({"app": app, "host": host, "port": port, "reload": reload})

    monkeypatch.setattr(app_module.uvicorn, "run", fake_run)
    app_module.main([])
    assert calls == {"app": "created-app", "host": "0.0.0.0", "port": 8765, "reload": False}

    calls.clear()
    monkeypatch.setenv("COVERAGE_MCP_DB", (tmp_path / "script.duckdb").as_posix())
    monkeypatch.setattr(sys, "argv", [app_module.__file__])
    runpy.run_path(Path(app_module.__file__).as_posix(), run_name="__main__")
    assert calls["host"] == "0.0.0.0"
    assert calls["port"] == 8765
    assert calls["reload"] is False


def test_daemon_health_and_startup(monkeypatch, tmp_path):
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"ok": True}

    monkeypatch.setattr(app_module.httpx, "get", lambda *args, **kwargs: Response())
    assert app_module.daemon_is_healthy("http://daemon") is True

    def unavailable(*args, **kwargs):
        raise httpx.ConnectError("no")

    monkeypatch.setattr(app_module.httpx, "get", unavailable)
    assert app_module.daemon_is_healthy("http://daemon") is False

    monkeypatch.setattr(app_module, "default_daemon_lock_path", lambda: (tmp_path / "daemon.lock").as_posix())
    statuses = iter([False, False, True])
    monkeypatch.setattr(app_module, "daemon_is_healthy", lambda _url: next(statuses))
    started = []
    monkeypatch.setattr(app_module, "start_daemon", lambda: started.append(True))
    monkeypatch.setattr(app_module.time, "sleep", lambda _: None)
    assert app_module.ensure_daemon(timeout_seconds=1) == app_module.daemon_url()
    assert started == [True]

    monkeypatch.setattr(app_module, "daemon_is_healthy", lambda _url: True)
    assert app_module.ensure_daemon() == app_module.daemon_url()

    statuses = iter([False, True])
    monkeypatch.setattr(app_module, "daemon_is_healthy", lambda _url: next(statuses))
    assert app_module.ensure_daemon() == app_module.daemon_url()

    monkeypatch.setattr(app_module, "daemon_is_healthy", lambda _url: False)
    monotonic_values = iter([0.0, 0.5, 2.0])
    monkeypatch.setattr(app_module.time, "monotonic", lambda: next(monotonic_values))
    with pytest.raises(RuntimeError, match="did not become healthy"):
        app_module.ensure_daemon(timeout_seconds=1)


def test_daemon_start_and_cli_commands(monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "default_common_db_path", lambda: (tmp_path / "common.duckdb").as_posix())
    launched = {}

    def popen(command, **kwargs):
        launched.update({"command": command, **kwargs})

    monkeypatch.setattr(app_module.subprocess, "Popen", popen)
    app_module.start_daemon()
    assert launched["command"][-1] == "serve"
    assert launched["start_new_session"] is True
    assert (tmp_path / "daemon.log").exists()

    calls = []
    monkeypatch.setattr(app_module, "serve", lambda: calls.append("serve"))
    monkeypatch.setattr(app_module, "connect", lambda: calls.append("connect"))
    app_module.main(["serve"])
    app_module.main(["connect"])
    with pytest.raises(SystemExit, match="usage"):
        app_module.main(["unknown"])
    assert calls == ["serve", "connect"]


def test_forward_mcp_messages_closes_destination():
    async def scenario():
        sender, source = anyio.create_memory_object_stream(1)
        destination, received = anyio.create_memory_object_stream(1)
        await sender.send("message")
        await sender.aclose()
        await app_module.forward_mcp_messages(source, destination)
        assert await received.receive() == "message"
        with pytest.raises(anyio.EndOfStream):
            await received.receive()

    anyio.run(scenario)


def test_forward_mcp_messages_propagates_errors_and_proxy_starts_two_directions(monkeypatch):
    async def error_scenario():
        sender, source = anyio.create_memory_object_stream(1)
        destination, _ = anyio.create_memory_object_stream(1)
        await sender.send(RuntimeError("bad message"))
        await sender.aclose()
        with pytest.raises(RuntimeError, match="bad message"):
            await app_module.forward_mcp_messages(source, destination)

    anyio.run(error_scenario)

    class Context:
        def __init__(self, value):
            self.value = value

        async def __aenter__(self):
            return self.value

        async def __aexit__(self, *_):
            return False

    calls = []

    async def forward(source, destination):
        calls.append((source, destination))

    monkeypatch.setattr(app_module, "stdio_server", lambda: Context(("stdio-read", "stdio-write")))
    monkeypatch.setattr(app_module.httpx, "AsyncClient", lambda **kwargs: Context("http-client"))
    monkeypatch.setattr(
        app_module,
        "streamable_http_client",
        lambda *args, **kwargs: Context(("http-read", "http-write", lambda: None)),
    )
    monkeypatch.setattr(app_module, "forward_mcp_messages", forward)
    anyio.run(app_module.proxy_stdio_to_http, "http://daemon", "/repo")
    assert calls == [("stdio-read", "http-write"), ("http-read", "stdio-write")]


def test_connect_uses_shared_daemon_and_repository(monkeypatch):
    calls = []
    monkeypatch.setattr(app_module, "ensure_daemon", lambda: "http://daemon")
    monkeypatch.setattr(app_module, "inspect_git", lambda _path: type("Git", (), {"repo_key": "/repo"})())
    monkeypatch.setattr(app_module.anyio, "run", lambda function, *args: calls.append((function, args)))
    app_module.connect()
    assert calls == [(app_module.proxy_stdio_to_http, ("http://daemon", "/repo"))]
