from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, cast

import pytest

from coverage_mcp.service import (
    CoverageService,
    RequestContext,
    compact_command,
    compact_file,
    compact_history_point,
    compact_snapshot,
    decode_cursor,
    encode_cursor,
    project_run,
    serialized_word_count,
)
from coverage_mcp.storage import CoverageStore


def service_for(store: Any, path: Path) -> CoverageService:
    context = RequestContext(path.resolve().as_posix(), path.resolve().as_posix())
    return CoverageService(cast(CoverageStore, store), lambda: context)


def test_cursor_word_budget_and_projection_edges(tmp_path):
    anchor = "a" * 64
    cursor = encode_cursor(anchor, scope="files")
    assert decode_cursor(cursor, scope="files") == (anchor, 1)
    with pytest.raises(ValueError, match="occurrence must be positive"):
        encode_cursor(anchor, scope="files", occurrence=0)
    assert serialized_word_count({"message": "one two", "count": 2}) == 5
    for invalid in ("not-base64", base64.urlsafe_b64encode(b"{}").decode()):
        with pytest.raises(ValueError, match="invalid pagination cursor"):
            decode_cursor(invalid, scope="files")
    negative = base64.urlsafe_b64encode(
        json.dumps({"after": "not-a-hash", "occurrence": 1, "scope": "wrong"}).encode()
    ).decode()
    with pytest.raises(ValueError, match="does not belong"):
        decode_cursor(negative, scope="files")
    with pytest.raises(ValueError, match="does not belong"):
        decode_cursor(cursor, scope="other")

    service = service_for(object(), tmp_path)
    response = service.envelope({"message": "small"})
    assert service.apply_budget(response, max_words=50) is response
    with pytest.raises(ValueError, match="between"):
        service.apply_budget(response, max_words=49)
    with pytest.raises(ValueError, match="increase max_words"):
        service.apply_budget(service.envelope({"message": " ".join(["word"] * 60)}), max_words=50)
    with pytest.raises(ValueError, match="between"):
        service.page([], cursor=None, max_words=49, scope="x")
    with pytest.raises(ValueError, match="no longer matches"):
        service.page([], cursor=encode_cursor(anchor, scope="x"), max_words=50, scope="x")

    values = [{"text": " ".join([str(index)] * 30)} for index in range(3)]
    first, page = service.page(values, cursor=None, max_words=50, scope="items")
    assert len(first) == 1
    assert page["truncated"] is True
    second, next_page = service.page(values, cursor=page["next_cursor"], max_words=5000, scope="items")
    assert len(second) == 2
    assert next_page["truncated"] is False
    duplicate = {"text": " ".join(["same"] * 30)}
    duplicates = [duplicate, duplicate, duplicate]
    first_duplicate, duplicate_page = service.page(duplicates, cursor=None, max_words=50, scope="duplicates")
    assert first_duplicate == [duplicate]
    second_duplicate, second_duplicate_page = service.page(
        duplicates,
        cursor=duplicate_page["next_cursor"],
        max_words=50,
        scope="duplicates",
    )
    assert second_duplicate == [duplicate]
    final_duplicate, final_duplicate_page = service.page(
        duplicates,
        cursor=second_duplicate_page["next_cursor"],
        max_words=50,
        scope="duplicates",
    )
    assert final_duplicate == [duplicate]
    assert final_duplicate_page["next_cursor"] is None
    with pytest.raises(ValueError, match="defensive 5000-record cap"):
        service.page([{}] * 5001, cursor=None, max_words=50, scope="too-many")
    with pytest.raises(ValueError, match="defensive 5000-record cap"):
        service.page([{}], cursor=None, max_words=50, scope="incomplete", total=2)
    exact, _ = service.page([{"text": " ".join(["word"] * 49)}], cursor=None, max_words=50, scope="exact")
    assert len(exact) == 1

    assert compact_file({"raw_metrics": {"x": 1}}, detailed=True)["raw_metrics"] == {"x": 1}
    assert "snapshot_id" not in compact_file({"snapshot_id": "snapshot", "file_path": "a.py"})
    snapshot = compact_snapshot({"suite": "unit", "repo_path": "/worktree", "base_ref": "main", "warnings": ["lossy"]})
    assert snapshot["suite"] == "unit"
    assert snapshot["measurement_checkout_path"] == "/worktree"
    assert snapshot["base_ref"] == "main"
    assert snapshot["warnings"] == ["lossy"]
    command = compact_command(
        {
            "artifact_specs": [{"kind": "coverage"}],
            "command": "pytest",
            "cwd": tmp_path.as_posix(),
            "shell": "/bin/bash",
            "approved_by": "human",
            "approval_note": "approved",
        },
        detailed=True,
    )
    assert command["command"] == "pytest"
    assert command["artifact_specs"] == [{"kind": "coverage"}]
    assert "artifact_kinds" not in command
    history = compact_history_point({"snapshot_id": "snapshot", "suite": "unit", "file_path": "a.py", "line_number": 1})
    assert history == {"snapshot_id": "snapshot"}
    assert compact_history_point({"suite": "unit"}, detailed=True) == {"suite": "unit"}
    detailed_run = project_run({"parsed_summary": {"excerpts": ["large"], "counters": {"passed": 1}}}, detailed=True)
    assert detailed_run["parsed_summary"] == {"counters": {"passed": 1}}
    assert project_run({"parsed_summary": None}, detailed=True)["parsed_summary"] is None


def test_service_validation_and_error_views(monkeypatch, tmp_path):
    class FakeStore:
        def latest_snapshot(self, **kwargs):
            return None

        def snapshot(self, snapshot_id):
            return {"id": snapshot_id, "suite": "unit"}

        def compare(self, **kwargs):
            return {
                "baseline": {"id": "base", "suite": "unit"},
                "current": {"id": "current", "suite": "unit"},
                "overall": {},
                "files": [],
                "changed_lines": [],
            }

    service = service_for(FakeStore(), tmp_path)
    service.validate_repository_path(None)
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(ValueError, match="selected repository"):
        service.validate_repository_path(other.as_posix())
    with pytest.raises(KeyError, match="no snapshots"):
        service.coverage_query(
            view="summary",
            snapshot_id=None,
            suite="unit",
            branch=None,
            file_path=None,
            line_number=None,
            line_ranges=None,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="snapshot_id and file_path"):
        service.coverage_query(
            view="file",
            snapshot_id="current",
            suite=None,
            branch=None,
            file_path=None,
            line_number=None,
            line_ranges=None,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="line_number, and suite"):
        service.coverage_query(
            view="line_history",
            snapshot_id=None,
            suite=None,
            branch=None,
            file_path="a.py",
            line_number=1,
            line_ranges=None,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="view must"):
        service.coverage_query(
            view="unknown",
            snapshot_id="current",
            suite=None,
            branch=None,
            file_path=None,
            line_number=None,
            line_ranges=None,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="required without"):
        service.coverage_comparison(
            view="overview",
            snapshot_id=None,
            baseline_snapshot_id=None,
            worktree_id=None,
            suite=None,
            file_path=None,
            only_regressions=False,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="requested suite"):
        service.coverage_comparison(
            view="overview",
            snapshot_id="current",
            baseline_snapshot_id="base",
            worktree_id=None,
            suite="integration",
            file_path=None,
            only_regressions=False,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="view must"):
        service.coverage_comparison(
            view="unknown",
            snapshot_id="current",
            baseline_snapshot_id="base",
            worktree_id=None,
            suite=None,
            file_path=None,
            only_regressions=False,
            cursor=None,
            max_words=50,
            detailed=False,
        )
    with pytest.raises(ValueError, match="action"):
        service.run_state("run", action="unknown", detailed=False)

    wrong = tmp_path / "wrong"
    wrong.mkdir()
    with pytest.raises(ValueError, match="command cwd"):
        service.command_registration(
            name="unit",
            command="pytest",
            human_approved=True,
            approved_by="human",
            approval_note="approved",
            cwd=wrong.as_posix(),
            shell="/bin/bash",
            artifact_paths=None,
            detailed=False,
        )


def test_detailed_project_worktree_and_source_projections(monkeypatch, tmp_path):
    class DetailStore:
        def projects(self, **kwargs):
            return [{"repo_key": tmp_path.as_posix(), "repo_path": tmp_path.as_posix(), "topology": {}}]

        def list_registered_commands(self, **kwargs):
            return []

        def latest_run(self):
            return None

        def list_run_queue(self, **kwargs):
            return []

        def register_worktree(self, path, **kwargs):
            return {
                "id": "worktree",
                "name": "detail",
                "created_at": "2026-01-01T00:00:00Z",
                "path": path,
                "branch": "main",
                "head_sha": "head",
                "base_ref": "main",
                "base_sha": "base",
                "baseline_snapshot_id": None,
                "topology": {},
            }

        def snapshot(self, snapshot_id):
            return {"id": snapshot_id, "suite": "unit", "commit_sha": "head", "repo_path": tmp_path.as_posix()}

        def file_coverage(self, snapshot_id, file_path):
            return {"snapshot_id": snapshot_id, "file_path": file_path}

        def source_lines(self, **kwargs):
            return [{"line_number": 1, "text": "one"}]

    store = DetailStore()
    service = service_for(store, tmp_path)
    project = service.project_context(cursor=None, max_words=100, detailed=True)
    assert project.data["project"]["repo_key"] == tmp_path.as_posix()

    from coverage_mcp.git_utils import GitInfo

    git = GitInfo(
        path=tmp_path.as_posix(),
        repo_path=tmp_path.as_posix(),
        repo_key=tmp_path.as_posix(),
        branch="main",
        commit_sha="head",
    )
    import coverage_mcp.service as service_module

    monkeypatch.setattr(service_module, "inspect_git", lambda _path: git)
    worktree = service.worktree_registration(tmp_path.as_posix(), base_ref="main", name="detail")
    assert worktree.data["name"] == "detail"
    source = service.source(
        snapshot_id="snapshot",
        file_path="a.py",
        start=1,
        end=1,
        cursor=None,
        max_words=100,
    )
    assert source.data["snapshot_commit_sha"] == "head"


def test_default_context_and_ingest_guards(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    service = CoverageService(cast(CoverageStore, object()))
    assert service.context().checkout_path == tmp_path.as_posix()

    class IngestStore:
        def ingest_report(self, *args, **kwargs):
            return {"repo_key": "other", "suite": kwargs["suite"]}

    guarded = service_for(IngestStore(), tmp_path)
    with pytest.raises(ValueError, match="suite must"):
        guarded.ingest(
            "coverage.lcov",
            format="lcov",
            suite=" ",
            branch=None,
            commit_sha=None,
            base_ref=None,
            detailed=False,
        )
    with pytest.raises(ValueError, match="does not belong"):
        guarded.ingest(
            "coverage.lcov",
            format="lcov",
            suite="unit",
            branch=None,
            commit_sha=None,
            base_ref=None,
            detailed=False,
        )
