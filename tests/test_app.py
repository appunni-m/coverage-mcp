from __future__ import annotations

import sys

from fastapi.testclient import TestClient

from coverage_mcp.app import create_app


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
            json={"command_ref": command["id"], "max_summary_lines": 5},
        )
        assert response.status_code == 200
        run = response.json()
        assert run["status"] == "passed"
        assert run["topology"]["command"]["id"] == command["id"]
        assert run["parsed_summary"]["counters"]["passed"] == 1
        assert client.get("/api/runs/latest").json()["id"] == run["id"]
        assert client.get(f"/api/runs/latest?command_ref={command['id']}").json()["id"] == run["id"]

        topology = client.get(f"/api/topology/run/{run['id']}")
        assert topology.status_code == 200
        assert topology.json()["topology"]["kind"] == "run"

        artifact = client.get("/api/artifacts/latest?command_ref=unit&kind=json")
        assert artifact.status_code == 200
        assert artifact.json()["exists"] is True
