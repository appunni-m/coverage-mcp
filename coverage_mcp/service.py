from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from coverage_mcp.contracts import ApiEnvelope
from coverage_mcp.git_utils import inspect_git
from coverage_mcp.storage import CoverageStore, compact_run_result

SCHEMA_REVISION = 7
DEFAULT_MAX_WORDS = 600
MAX_COLLECTION_RECORDS = 5000


@dataclass(frozen=True, slots=True)
class RequestContext:
    repo_key: str
    checkout_path: str
    suite: str | None = None


def serialized_word_count(value: Any) -> int:
    """Count JSON tokens separated by whitespace/punctuation for a stable response budget."""
    serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    return len(re.findall(r"[^\s\[\]{},:]+", serialized))


def _cursor_scope(scope: str) -> str:
    return hashlib.sha256(scope.encode()).hexdigest()[:16]


def _cursor_anchor(value: Any) -> str:
    serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def encode_cursor(anchor: str, *, scope: str) -> str:
    payload = json.dumps({"after": anchor, "scope": _cursor_scope(scope)}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(cursor: str | None, *, scope: str) -> str | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
        anchor = str(payload["after"])
        cursor_scope = str(payload["scope"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("invalid pagination cursor") from exc
    if not re.fullmatch(r"[0-9a-f]{64}", anchor) or cursor_scope != _cursor_scope(scope):
        raise ValueError("pagination cursor does not belong to this query")
    return anchor


def compact_snapshot(value: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    keys = (
        "id",
        "created_at",
        "age_seconds",
        "age",
        "branch",
        "commit_sha",
        "base_ref",
        "suite",
        "format",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
    )
    result = {key: value.get(key) for key in keys}
    result["measurement_checkout_path"] = value.get("repo_path")
    result["warnings"] = value.get("warnings") or []
    if detailed:
        result.update(
            {
                "repo_path": value.get("repo_path"),
                "report_path": value.get("report_path"),
                "metadata": value.get("metadata") or {},
            }
        )
    return result


def compact_file(value: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    keys = (
        "file_path",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
    )
    result = {key: value.get(key) for key in keys}
    if detailed:
        result["raw_metrics"] = value.get("raw_metrics") or {}
    return result


def compact_command(value: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    keys = (
        "id",
        "name",
        "command",
        "cwd",
        "shell",
        "artifact_specs",
        "enabled",
        "created_at",
        "duration_estimate_ms",
        "duration_p90_ms",
        "duration_sample_count",
    )
    result = {key: value.get(key) for key in keys}
    result["artifact_specs"] = value.get("artifact_specs") or []
    if detailed:
        result.update(
            {
                "approved_by": value.get("approved_by"),
                "approval_note": value.get("approval_note"),
                "registration_branch": value.get("branch"),
                "registration_commit_sha": value.get("commit_sha"),
            }
        )
    return result


def compact_history_point(value: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    """Remove query identity repeated on every line-history point."""
    if detailed:
        return value
    omitted = {"suite", "file_path", "line_number"}
    return {key: item for key, item in value.items() if key not in omitted}


def project_run(value: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    """Keep retained output discoverable through search without embedding excerpts."""
    if not detailed:
        return compact_run_result(value)
    result = dict(value)
    summary = result.get("parsed_summary")
    if isinstance(summary, dict):
        result["parsed_summary"] = {key: item for key, item in summary.items() if key != "excerpts"}
    return result


class CoverageService:
    """One orchestration and projection layer shared by every public transport."""

    def __init__(
        self,
        store: CoverageStore,
        context_provider: Callable[[], RequestContext] | None = None,
    ) -> None:
        self.store = store
        self._context_provider = context_provider

    def context(self, *, suite: str | None = None) -> RequestContext:
        if self._context_provider is not None:
            selected = self._context_provider()
        else:
            checkout = Path.cwd().resolve().as_posix()
            git = inspect_git(checkout)
            selected = RequestContext(repo_key=git.repo_key, checkout_path=git.path)
        return RequestContext(selected.repo_key, selected.checkout_path, suite if suite is not None else selected.suite)

    def envelope(
        self,
        data: Any,
        *,
        suite: str | None = None,
        page: dict[str, Any] | None = None,
    ) -> ApiEnvelope:
        selected = self.context(suite=suite)
        return ApiEnvelope.model_validate(
            {
                "context": {
                    "repo_key": selected.repo_key,
                    "checkout_path": selected.checkout_path,
                    "suite": selected.suite,
                    "schema_revision": SCHEMA_REVISION,
                },
                "data": data,
                "page": page,
            }
        )

    def apply_budget(self, response: ApiEnvelope, *, max_words: int) -> ApiEnvelope:
        """Validate a singular response against the caller's primary word budget."""
        if not 50 <= max_words <= 5000:
            raise ValueError("max_words must be between 50 and 5000")
        word_count = serialized_word_count(response.data)
        if word_count > max_words:
            raise ValueError(f"response requires {word_count} words; increase max_words or request detailed=false")
        return response

    def validate_repository_path(self, repo_path: str | None) -> None:
        """Reject legacy path selectors that escape the connector-selected repository."""
        if repo_path is not None and inspect_git(repo_path).repo_key != self.context().repo_key:
            raise ValueError("repo_path does not belong to the selected repository")

    def collection(
        self,
        values: Sequence[Any],
        *,
        cursor: str | None,
        max_words: int,
        scope: str,
        suite: str | None = None,
    ) -> ApiEnvelope:
        selected, page = self.page(values, cursor=cursor, max_words=max_words, scope=scope)
        return self.envelope(selected, suite=suite, page=page)

    def page(
        self,
        values: Sequence[Any],
        *,
        cursor: str | None,
        max_words: int,
        scope: str,
        total: int | None = None,
    ) -> tuple[list[Any], dict[str, Any]]:
        if not 50 <= max_words <= 5000:
            raise ValueError("max_words must be between 50 and 5000")
        bounded = values[:MAX_COLLECTION_RECORDS]
        anchor = decode_cursor(cursor, scope=scope)
        start = 0
        if anchor is not None:
            start = next(
                (index + 1 for index, value in enumerate(bounded) if _cursor_anchor(value) == anchor),
                -1,
            )
            if start < 0:
                raise ValueError("pagination cursor no longer matches the available results")
        selected: list[Any] = []
        word_count = 0
        for value in bounded[start:]:
            item_words = serialized_word_count(value)
            if selected and word_count + item_words > max_words:
                break
            selected.append(value)
            word_count += item_words
            if word_count >= max_words:
                break
        known_total = len(values) if total is None else total
        consumed = start + len(selected)
        truncated = consumed < min(known_total, MAX_COLLECTION_RECORDS)
        page = {
            "returned": len(selected),
            "total": known_total,
            "word_count": word_count,
            "max_words": max_words,
            "truncated": truncated,
            "next_cursor": encode_cursor(_cursor_anchor(selected[-1]), scope=scope) if truncated and selected else None,
        }
        return selected, page

    def command_registration(
        self,
        *,
        name: str,
        command: str,
        human_approved: bool,
        approved_by: str,
        approval_note: str,
        cwd: str | None,
        shell: str,
        artifact_paths: dict[str, Any] | None,
        detailed: bool,
    ) -> ApiEnvelope:
        selected = self.context()
        resolved_cwd = cwd or selected.checkout_path
        if inspect_git(resolved_cwd).repo_key != selected.repo_key:
            raise ValueError("command cwd does not belong to the selected repository")
        registered = self.store.register_command(
            name=name,
            command=command,
            cwd=resolved_cwd,
            shell=shell,
            artifact_paths=artifact_paths,
            human_approved=human_approved,
            approved_by=approved_by,
            approval_note=approval_note,
        )
        return self.envelope(compact_command(registered, detailed=detailed))

    def run_submission(
        self,
        command_ref: str,
        *,
        timeout_seconds: int | None,
        idempotency_key: str | None,
        wait: bool,
        detailed: bool,
    ) -> ApiEnvelope:
        runner = self.store.run_command_profiled if wait else self.store.submit_command_profiled
        result = runner(
            command_ref,
            max_summary_lines=20,
            timeout_seconds=timeout_seconds,
            idempotency_key=idempotency_key,
        )
        return self.envelope(project_run(result, detailed=detailed))

    def run_state(self, run_id: str, *, action: str, detailed: bool) -> ApiEnvelope:
        if action == "status":
            result = self.store.run_result(run_id, max_summary_lines=20)
        elif action == "cancel":
            result = self.store.cancel_run(run_id, max_summary_lines=20)
        else:
            raise ValueError("action must be status or cancel")
        return self.envelope(project_run(result, detailed=detailed))

    def project_context(
        self,
        *,
        cursor: str | None,
        max_words: int,
        detailed: bool,
    ) -> ApiEnvelope:
        selected = self.context()
        projects = self.store.projects(limit=1)
        project = (
            projects[0]
            if projects
            else {
                "repo_key": selected.repo_key,
                "repo_path": selected.checkout_path,
                "snapshot_count": 0,
                "command_count": 0,
                "run_count": 0,
                "latest_snapshot_id": None,
            }
        )
        commands = [
            compact_command(item, detailed=detailed) for item in self.store.list_registered_commands(limit=1000)
        ]
        scope = f"project-context:{selected.repo_key}:{detailed}"
        selected_commands, page = self.page(
            commands,
            cursor=cursor,
            max_words=max_words,
            scope=scope,
        )
        latest = self.store.latest_run()
        active = [compact_run_result(item) for item in self.store.list_run_queue(limit=1000)]
        compact_project_keys = (
            "snapshot_count",
            "branch_count",
            "command_count",
            "run_count",
            "latest_snapshot_id",
            "latest_snapshot_age",
            "latest_snapshot_age_seconds",
            "latest_run_age",
            "latest_run_age_seconds",
            "latest_branch",
            "latest_commit_sha",
            "latest_suite",
            "latest_format",
            "total_lines",
            "covered_lines",
            "line_rate",
            "total_branches",
            "covered_branches",
            "branch_rate",
            "total_functions",
            "covered_functions",
            "function_rate",
            "total_regions",
            "covered_regions",
            "region_rate",
            "warnings",
        )
        compact_project = {key: project.get(key) for key in compact_project_keys}
        if detailed:
            compact_project = {key: value for key, value in project.items() if key != "topology"}
        return self.envelope(
            {
                "project": compact_project,
                "commands": selected_commands,
                "latest_run": compact_run_result(latest) if latest is not None else None,
                "active_runs": active,
            },
            page=page,
        )

    def search_logs(
        self,
        run_id: str,
        query: str,
        *,
        stream: str,
        context_lines: int,
        max_matches: int,
        max_words: int,
        case_sensitive: bool,
    ) -> ApiEnvelope:
        result = self.store.search_run_logs(
            run_id,
            query,
            stream=stream,
            context_lines=context_lines,
            max_matches=max_matches,
            max_words=max_words,
            case_sensitive=case_sensitive,
        )
        result.pop("case_sensitive", None)
        result.pop("streams", None)
        return self.envelope(result)

    def ingest(
        self,
        report_path: str,
        *,
        format: str,
        suite: str,
        branch: str | None,
        commit_sha: str | None,
        base_ref: str | None,
        detailed: bool,
    ) -> ApiEnvelope:
        selected = self.context(suite=suite)
        suite = suite.strip()
        if not suite:
            raise ValueError("suite must not be blank")
        path = Path(report_path).expanduser()
        if not path.is_absolute():
            path = Path(selected.checkout_path) / path
        snapshot = self.store.ingest_report(
            path.resolve().as_posix(),
            format=format,
            repo_path=selected.checkout_path,
            branch=branch,
            commit_sha=commit_sha,
            base_ref=base_ref,
            suite=suite,
        )
        if snapshot["repo_key"] != selected.repo_key:
            raise ValueError("coverage report does not belong to the selected repository")
        return self.envelope(compact_snapshot(snapshot, detailed=detailed), suite=suite)

    def worktree_registration(
        self,
        path: str,
        *,
        base_ref: str,
        name: str | None,
    ) -> ApiEnvelope:
        selected = self.context()
        git = inspect_git(path)
        if git.commit_sha is None or git.repo_key != selected.repo_key:
            raise ValueError("worktree must be a Git checkout of the selected repository")
        result = self.store.register_worktree(git.path, base_ref=base_ref.strip(), name=name)
        compact = {
            "id": result["id"],
            "name": result["name"],
            "created_at": result["created_at"],
            "path": result["path"],
            "branch": result["branch"],
            "head_sha": result["head_sha"],
            "base_ref": result["base_ref"],
            "base_sha": result["base_sha"],
            "baseline_snapshot_id": result["baseline_snapshot_id"],
        }
        return self.envelope(compact)

    def coverage_query(
        self,
        *,
        view: str,
        snapshot_id: str | None,
        baseline_snapshot_id: str | None = None,
        suite: str | None,
        branch: str | None,
        file_path: str | None,
        line_number: int | None,
        line_ranges: Sequence[Mapping[str, int]] | None,
        cursor: str | None,
        max_words: int,
        detailed: bool,
    ) -> ApiEnvelope:
        selected = self.context(suite=suite)
        snapshot = None
        if snapshot_id is not None:
            snapshot = self.store.snapshot(snapshot_id)
        elif view != "line_history":
            snapshot = self.store.latest_snapshot(repo_path=selected.checkout_path, branch=branch, suite=suite)
            if snapshot is None:
                raise KeyError("no snapshots found")
            snapshot_id = str(snapshot["id"])
        selected_suite = suite or (str(snapshot["suite"]) if snapshot is not None else None)

        if view == "summary":
            assert snapshot is not None
            return self.envelope(compact_snapshot(snapshot, detailed=detailed), suite=selected_suite)
        if view == "files":
            assert snapshot_id is not None
            values = [compact_file(item, detailed=detailed) for item in self.store.files(snapshot_id, limit=5000)]
            scope = f"coverage-files:{snapshot_id}:{detailed}"
            selected_values, page = self.page(values, cursor=cursor, max_words=max_words, scope=scope)
            return self.envelope(selected_values, suite=selected_suite, page=page)
        if view == "file":
            if snapshot_id is None or not file_path:
                raise ValueError("snapshot_id and file_path are required for file view")
            file = self.store.file_coverage(snapshot_id, file_path)
            selection = self.store.lines_in_ranges(snapshot_id, file_path, line_ranges or [])
            gaps = self.store.file_gaps(snapshot_id, file_path, max_ranges=100)
            ranges, page = self.page(
                gaps["ranges"],
                cursor=cursor,
                max_words=max_words,
                scope=f"coverage-file:{snapshot_id}:{file_path}",
            )
            gaps["ranges"] = ranges
            gaps["returned_range_count"] = len(ranges)
            result = {
                "file": compact_file(file, detailed=detailed),
                "gaps": gaps,
                "selected_lines": selection.pop("lines"),
                "line_selection": selection,
            }
            return self.envelope(result, suite=selected_suite, page=page)
        if view == "insights":
            assert snapshot_id is not None
            result = self.store.insights(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                limit=50,
            )
            items, page = self.page(
                result["items"],
                cursor=cursor,
                max_words=max_words,
                scope=f"coverage-insights:{snapshot_id}:{baseline_snapshot_id}:{detailed}",
            )
            data = {
                "snapshot": compact_snapshot(result["snapshot"], detailed=detailed),
                "baseline": (
                    compact_snapshot(result["baseline"], detailed=detailed) if result.get("baseline") else None
                ),
                "summary": result["summary"],
                "items": items,
            }
            return self.envelope(data, suite=selected_suite, page=page)
        if view == "line_history":
            if not file_path or line_number is None or not suite:
                raise ValueError("file_path, line_number, and suite are required for line_history view")
            values = [
                compact_history_point(item, detailed=detailed)
                for item in self.store.line_history(
                    file_path=file_path,
                    line_number=line_number,
                    repo_path=selected.checkout_path,
                    branch=branch,
                    suite=suite,
                    limit=1000,
                )
            ]
            selected_values, page = self.page(
                values,
                cursor=cursor,
                max_words=max_words,
                scope=f"line-history:{selected.repo_key}:{suite}:{branch}:{file_path}:{line_number}:{detailed}",
            )
            return self.envelope(selected_values, suite=suite, page=page)
        raise ValueError("view must be summary, files, file, insights, or line_history")

    def coverage_comparison(
        self,
        *,
        view: str,
        snapshot_id: str | None,
        baseline_snapshot_id: str | None,
        worktree_id: str | None,
        suite: str | None,
        file_path: str | None,
        only_regressions: bool,
        cursor: str | None,
        max_words: int,
        detailed: bool,
    ) -> ApiEnvelope:
        if view == "progress":
            if not worktree_id or not suite:
                raise ValueError("worktree_id and suite are required for progress view")
            progress = self.store.worktree_progress(worktree_id, suite=suite, file_path=file_path, limit=2000)
            points, page = self.page(
                progress["points"],
                cursor=cursor,
                max_words=max_words,
                scope=f"worktree-progress:{worktree_id}:{suite}:{file_path}",
            )
            progress["points"] = points
            if not detailed:
                progress["worktree"] = {key: progress["worktree"].get(key) for key in ("id", "path", "branch")}
            return self.envelope(progress, suite=suite, page=page)

        if worktree_id:
            comparison = self.store.compare_worktree(
                worktree_id, snapshot_id=snapshot_id, file_limit=5000, line_limit=5000
            )
        else:
            if not snapshot_id or not baseline_snapshot_id:
                raise ValueError("snapshot_id and baseline_snapshot_id are required without worktree_id")
            comparison = self.store.compare(
                snapshot_id=snapshot_id,
                baseline_snapshot_id=baseline_snapshot_id,
                file_limit=5000,
                line_limit=5000,
            )
        current_suite = str(comparison["current"]["suite"])
        if suite is not None and suite != current_suite:
            raise ValueError("requested suite does not match the current snapshot")
        base = {
            "baseline": compact_snapshot(comparison["baseline"], detailed=detailed),
            "current": compact_snapshot(comparison["current"], detailed=detailed),
            "overall": comparison["overall"],
        }
        if view == "overview":
            base["file_change_count"] = len(comparison["files"])
            base["line_change_count"] = len(comparison["changed_lines"])
            return self.envelope(base, suite=current_suite)
        if view == "files":
            values = comparison["files"]
        elif view == "lines":
            values = [
                item
                for item in comparison["changed_lines"]
                if not only_regressions or item.get("status") == "regressed"
            ]
        else:
            raise ValueError("view must be overview, files, lines, or progress")
        values, page = self.page(
            values,
            cursor=cursor,
            max_words=max_words,
            scope=f"coverage-compare:{comparison['current']['id']}:{comparison['baseline']['id']}:{view}:{only_regressions}",
        )
        base[view] = values
        return self.envelope(base, suite=current_suite, page=page)

    def source(
        self,
        *,
        snapshot_id: str,
        file_path: str,
        start: int,
        end: int,
        cursor: str | None,
        max_words: int,
    ) -> ApiEnvelope:
        if end < start:
            raise ValueError("end must be greater than or equal to start")
        snapshot = self.store.snapshot(snapshot_id)
        self.store.file_coverage(snapshot_id, file_path)
        lines = self.store.source_lines(snapshot_id=snapshot_id, file_path=file_path, start=start, end=end)
        values, page = self.page(
            lines,
            cursor=cursor,
            max_words=max_words,
            scope=f"source:{snapshot_id}:{file_path}:{start}:{end}",
        )
        data: dict[str, Any] = {
            "snapshot_commit_sha": snapshot.get("commit_sha"),
            "lines": values,
        }
        return self.envelope(data, suite=str(snapshot["suite"]), page=page)

    def file_detail(
        self,
        *,
        snapshot_id: str,
        file_path: str,
        cursor: str | None,
        max_words: int,
        detailed: bool,
    ) -> ApiEnvelope:
        snapshot = self.store.snapshot(snapshot_id)
        file = self.store.file_coverage(snapshot_id, file_path)
        lines = self.store.lines(snapshot_id, file_path, limit=20000)
        if not detailed:
            lines = [
                {
                    key: line.get(key)
                    for key in (
                        "line_number",
                        "hits",
                        "covered",
                        "count_line",
                        "total_branches",
                        "covered_branches",
                        "total_functions",
                        "covered_functions",
                    )
                }
                for line in lines
            ]
        selected, page = self.page(
            lines,
            cursor=cursor,
            max_words=max_words,
            scope=f"dashboard-file:{snapshot_id}:{file_path}:{detailed}",
        )
        return self.envelope(
            {"file": compact_file(file, detailed=detailed), "lines": selected},
            suite=str(snapshot["suite"]),
            page=page,
        )
