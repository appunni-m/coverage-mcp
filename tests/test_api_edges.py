from __future__ import annotations

import runpy
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from coverage_mcp import app as app_module
from coverage_mcp.app import create_app


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
        assert client.get("/health").json()["version"] == "0.2.0"
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
        run = client.post(
            "/api/runs/profiled",
            json={"command_ref": command["id"], "timeout_seconds": 1, "max_summary_lines": 5, "wait": True},
        ).json()
        assert run["status"] == "timeout"
        assert client.get(f"/api/runs/{run['id']}?max_summary_lines=1").json()["id"] == run["id"]
        assert client.get("/api/runs/missing").status_code == 404


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
    app_module.main()
    assert calls == {"app": "created-app", "host": "0.0.0.0", "port": 8765, "reload": False}

    calls.clear()
    monkeypatch.setenv("COVERAGE_MCP_DB", (tmp_path / "script.duckdb").as_posix())
    runpy.run_path(Path(app_module.__file__).as_posix(), run_name="__main__")
    assert calls["host"] == "0.0.0.0"
    assert calls["port"] == 8765
    assert calls["reload"] is False
