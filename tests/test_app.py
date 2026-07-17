from __future__ import annotations

import sys
import time

import pytest
from fastapi.testclient import TestClient

from coverage_mcp.app import (
    REPOSITORY_HEADER,
    CoverageRepoStore,
    RepositoryStoreRouter,
    create_app,
    default_common_db_path,
    default_daemon_lock_path,
    default_db_path,
)
from coverage_mcp.storage import CommonStore


def test_app_ingests_and_lists_snapshot(tmp_path):
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
    app = create_app((tmp_path / "coverage.duckdb").as_posix())

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert 'id="projectSelect"' in dashboard.text
        assert 'id="insightsBody"' in dashboard.text
        assert 'id="coverageViewer"' in dashboard.text
        assert 'id="lineFilter"' in dashboard.text
        assert 'id="fileList"' in dashboard.text
        assert 'id="coverageMap"' in dashboard.text
        assert 'id="diagnosisPane"' in dashboard.text
        assert 'id="trendLegend"' in dashboard.text
        assert 'id="trendScope"' in dashboard.text
        assert "region_rate" in dashboard.text
        assert "Selected File Lines" not in dashboard.text

        response = client.post(
            "/api/ingest",
            json={
                "report_path": report.as_posix(),
                "format": "lcov",
                "repo_path": tmp_path.as_posix(),
                "branch": "main",
                "commit_sha": "abc",
            },
        )
        assert response.status_code == 200
        snapshot = response.json()
        assert snapshot["total_lines"] == 2

        snapshots = client.get("/api/snapshots").json()
        assert len(snapshots) == 1
        projects = client.get("/api/projects").json()
        assert projects[0]["snapshot_count"] == 1
        assert projects[0]["latest_snapshot_id"] == snapshot["id"]
        files = client.get(f"/api/snapshots/{snapshot['id']}/files").json()
        assert files[0]["file_path"] == "src/a.py"
        insights = client.get(f"/api/snapshots/{snapshot['id']}/insights").json()
        assert "summary" in insights
        assert "items" in insights


def test_default_database_is_anchored_to_repository_root(tmp_path):
    assert default_db_path(tmp_path.as_posix()) == (tmp_path / ".coverage-mcp" / "coverage.duckdb").as_posix()


def test_common_store_registers_repositories(tmp_path):
    store = CommonStore(tmp_path / "common.duckdb")
    try:
        first = store.register_repository("/repo/a")
        second = store.register_repository("/repo/a")
        assert first == second
        assert first.items() <= store.repositories()[0].items()
    finally:
        store.close()


def test_global_app_lazily_routes_coverage_to_repository_store(tmp_path):
    report = tmp_path / "coverage.lcov"
    report.write_text("TN:\nSF:a.py\nDA:1,1\nend_of_record\n", encoding="utf-8")
    app = create_app(common_db_path=(tmp_path / "common.duckdb").as_posix())

    with TestClient(app) as client:
        health = client.get("/health").json()
        assert health["common_db_path"] == (tmp_path / "common.duckdb").as_posix()
        assert health["repository_count"] == 0
        assert client.post("/api/ingest", json={"report_path": report.as_posix()}).status_code == 400

        headers = {REPOSITORY_HEADER: tmp_path.as_posix()}
        ingested = client.post(
            "/api/ingest",
            headers=headers,
            json={"report_path": report.as_posix(), "repo_path": tmp_path.as_posix()},
        )
        assert ingested.status_code == 200
        assert client.get("/api/snapshots", headers=headers).json()[0]["id"] == ingested.json()["id"]
        assert client.get("/api/projects").json()[0]["repo_key"] == tmp_path.as_posix()
        assert client.get("/health").json()["repository_count"] == 1


def test_global_app_reports_invalid_repository_selection(monkeypatch, tmp_path):
    app = create_app(common_db_path=(tmp_path / "common.duckdb").as_posix())
    monkeypatch.setattr(app.state.coverage_store, "select", lambda _: (_ for _ in ()).throw(ValueError("bad repo")))
    with TestClient(app) as client:
        response = client.get("/api/snapshots", headers={REPOSITORY_HEADER: tmp_path.as_posix()})
    assert response.status_code == 400
    assert response.json()["detail"] == "bad repo"


def test_default_common_database_uses_user_coverage_directory(monkeypatch, tmp_path):
    monkeypatch.setattr("coverage_mcp.app.Path.home", lambda: tmp_path)
    assert default_common_db_path() == (tmp_path / ".coverage-mcp" / "common.duckdb").as_posix()
    assert default_daemon_lock_path() == (tmp_path / ".coverage-mcp" / "daemon.lock").as_posix()


def test_repository_store_router_requires_selection_and_reuses_store(tmp_path):
    common = CommonStore(tmp_path / "common.duckdb")
    router = RepositoryStoreRouter(CoverageRepoStore(common))
    try:
        with pytest.raises(RuntimeError, match="repository"):
            _ = router.db_path
        token = router.select(tmp_path.as_posix())
        try:
            first = router.stores.for_repository(tmp_path.as_posix())
            assert router.projects() == first.projects()
            assert router.stores.for_repository(tmp_path.as_posix()) is first
        finally:
            router.reset(token)
    finally:
        router.close()


def test_app_registers_and_runs_approved_command(tmp_path):
    script = tmp_path / "run.py"
    script.write_text(
        """from pathlib import Path
Path("result.json").write_text("{}")
print("1 passed")
""",
        encoding="utf-8",
    )
    app = create_app((tmp_path / "coverage.duckdb").as_posix())

    with TestClient(app) as client:
        rejected = client.post(
            "/api/commands/register",
            json={
                "name": "unit",
                "command": f"{sys.executable} {script.name}",
                "cwd": tmp_path.as_posix(),
                "human_approved": False,
                "approved_by": "tester",
                "approval_note": "not approved",
            },
        )
        assert rejected.status_code == 400

        registered = client.post(
            "/api/commands/register",
            json={
                "name": "unit",
                "command": f"{sys.executable} {script.name}",
                "cwd": tmp_path.as_posix(),
                "artifact_paths": {"json": "result.json"},
                "human_approved": True,
                "approved_by": "tester",
                "approval_note": "approved API command test",
            },
        )
        assert registered.status_code == 200
        command = registered.json()
        assert command["topology"]["kind"] == "registered_command"
        assert client.get("/api/commands").json()[0]["id"] == command["id"]

        response = client.post(
            "/api/runs/profiled",
            json={"command_ref": command["id"], "max_summary_lines": 5, "idempotency_key": "api-unit"},
        )
        assert response.status_code == 200
        run = response.json()
        assert run["status"] in {"queued", "running"}
        assert run["terminal"] is False
        assert client.get("/api/runs/queue").json()
        for _ in range(100):
            run = client.get(f"/api/runs/{run['id']}?max_summary_lines=5").json()
            if run["terminal"]:
                break
            time.sleep(0.02)
        assert run["status"] == "passed"
        assert run["topology"]["command"]["id"] == command["id"]
        assert run["parsed_summary"]["counters"]["passed"] == 1
        repeated = client.post(
            "/api/runs/profiled",
            json={"command_ref": command["id"], "idempotency_key": "api-unit"},
        ).json()
        assert repeated["id"] == run["id"]
        assert repeated["submission_reused"] is True
        assert client.post(f"/api/runs/{run['id']}/cancel").status_code == 400
        assert client.get("/api/runs/latest").json()["id"] == run["id"]
        assert client.get(f"/api/runs/latest?command_ref={command['id']}").json()["id"] == run["id"]

        topology = client.get(f"/api/topology/run/{run['id']}")
        assert topology.status_code == 200
        assert topology.json()["topology"]["kind"] == "run"

        artifact = client.get("/api/artifacts/latest?command_ref=unit&kind=json")
        assert artifact.status_code == 200
        assert artifact.json()["exists"] is True
