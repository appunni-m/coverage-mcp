from __future__ import annotations

import re
from collections import deque
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from coverage_mcp.contracts import MAX_SUMMARY_LINES, MAX_TIMEOUT_SECONDS, MIN_SUMMARY_LINES, MIN_TIMEOUT_SECONDS

MAX_LOG_QUERY_TERMS = 20


def normalize_log_queries(query: str | Sequence[str]) -> list[str]:
    queries = [query] if isinstance(query, str) else list(query)
    if not queries:
        raise ValueError("query must not be empty")
    if len(queries) > MAX_LOG_QUERY_TERMS:
        raise ValueError(f"query accepts at most {MAX_LOG_QUERY_TERMS} terms")
    for term in queries:
        if not term.strip():
            raise ValueError("query terms must not be blank")
        if len(term) > 500:
            raise ValueError("query terms must be at most 500 characters")
    return queries


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return current - baseline


def validate_run_limits(*, max_summary_lines: int, timeout_seconds: int | None = None) -> None:
    if not MIN_SUMMARY_LINES <= max_summary_lines <= MAX_SUMMARY_LINES:
        raise ValueError(f"max_summary_lines must be between {MIN_SUMMARY_LINES} and {MAX_SUMMARY_LINES}")
    if timeout_seconds is not None and not MIN_TIMEOUT_SECONDS <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout_seconds must be between {MIN_TIMEOUT_SECONDS} and {MAX_TIMEOUT_SECONDS}")


def relative_age(
    value: datetime | str | None,
    *,
    now: datetime | None = None,
) -> tuple[int, str] | None:
    timestamp = parse_datetime(value)
    if timestamp is None:
        return None
    current = now or utcnow()
    if current.tzinfo is not None:
        current = current.astimezone(UTC).replace(tzinfo=None)
    seconds = max(0, int((current - timestamp).total_seconds()))
    return seconds, format_age(seconds)


def parse_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    timestamp = value
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(
                timestamp.removesuffix("Z") + ("+00:00" if timestamp.endswith("Z") else "")
            )
        except ValueError:
            return None
    if timestamp.tzinfo is not None:
        return timestamp.astimezone(UTC).replace(tzinfo=None)
    return timestamp


def format_age(seconds: int) -> str:
    remaining = max(0, seconds)
    parts: list[str] = []
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)):
        amount, remaining = divmod(remaining, size)
        if amount:
            suffix = "" if amount == 1 else "s"
            parts.append(f"{amount} {unit}{suffix}")
    if not parts:
        parts.append("0 seconds")
    return " ".join(parts) + " ago"


def format_duration(seconds: int) -> str:
    return format_age(seconds).removesuffix(" ago")


def milliseconds_to_seconds(milliseconds: int) -> int:
    return (max(milliseconds, 0) + 999) // 1000


def serialize_datetime(value: datetime) -> str:
    return value.isoformat() + "Z"


def infer_topology(value: dict[str, Any]) -> dict[str, Any] | None:
    if "repo_key" not in value and "repo_path" not in value:
        return None
    project = {
        "repo_key": value.get("repo_key"),
        "repo_path": value.get("repo_path"),
    }
    git = {
        "branch": value.get("branch") or value.get("latest_branch"),
        "commit_sha": value.get("commit_sha") or value.get("latest_commit_sha") or value.get("head_sha"),
    }

    if "stdout_path" in value and "stderr_path" in value and "command_id" in value:
        raw_artifacts = value.get("artifact_paths")
        artifacts: list[Any] = raw_artifacts if isinstance(raw_artifacts, list) else []
        return {
            "kind": "run",
            "project": project,
            "command": {
                "id": value.get("command_id"),
                "name": value.get("command_name"),
            },
            "run": {
                "id": value.get("id"),
                "status": value.get("status"),
                "cwd": value.get("cwd"),
                "started_at": value.get("started_at"),
                "ended_at": value.get("ended_at"),
                "exit_code": value.get("exit_code"),
            },
            "git": git,
            "artifacts": [
                {
                    "kind": artifact.get("kind"),
                    "path": artifact.get("path"),
                    "exists": artifact.get("exists"),
                    "coverage_format": artifact.get("coverage_format"),
                    "suite": artifact.get("suite"),
                    "ingest_status": artifact.get("ingest_status"),
                    "snapshot_id": artifact.get("snapshot_id"),
                }
                for artifact in artifacts
                if isinstance(artifact, dict)
            ],
        }

    if "approved_by" in value and "artifact_specs" in value and "command" in value:
        raw_specs = value.get("artifact_specs")
        specs: list[Any] = raw_specs if isinstance(raw_specs, list) else []
        return {
            "kind": "registered_command",
            "project": project,
            "command": {
                "id": value.get("id"),
                "name": value.get("name"),
                "cwd": value.get("cwd"),
                "enabled": value.get("enabled"),
            },
            "registration": {
                "approved_by": value.get("approved_by"),
                "created_at": value.get("created_at"),
                "branch": value.get("branch"),
                "commit_sha": value.get("commit_sha"),
            },
            "artifact_kinds": [spec.get("kind") for spec in specs if isinstance(spec, dict)],
        }

    if "latest_snapshot_id" in value and "snapshot_count" in value:
        return {
            "kind": "project",
            "project": {
                **project,
                "snapshot_count": value.get("snapshot_count"),
                "command_count": value.get("command_count"),
                "run_count": value.get("run_count"),
            },
            "latest_snapshot": {
                "id": value.get("latest_snapshot_id"),
                "branch": value.get("latest_branch"),
                "commit_sha": value.get("latest_commit_sha"),
                "suite": value.get("latest_suite"),
                "format": value.get("latest_format"),
            },
        }

    if "baseline_snapshot_id" in value and "base_ref" in value and "path" in value:
        return {
            "kind": "worktree",
            "project": project,
            "worktree": {
                "id": value.get("id"),
                "path": value.get("path"),
                "name": value.get("name"),
                "branch": value.get("branch"),
                "head_sha": value.get("head_sha"),
            },
            "baseline": {
                "base_ref": value.get("base_ref"),
                "base_sha": value.get("base_sha"),
                "snapshot_id": value.get("baseline_snapshot_id"),
            },
        }

    if "report_path" in value and "suite" in value and "format" in value and "total_lines" in value:
        return {
            "kind": "coverage_snapshot",
            "project": project,
            "snapshot": {
                "id": value.get("id"),
                "suite": value.get("suite"),
                "format": value.get("format"),
                "report_path": value.get("report_path"),
                "created_at": value.get("created_at"),
            },
            "git": git,
        }

    if "run_id" in value and "kind" in value and "path" in value and "command_id" in value:
        return {
            "kind": "run_artifact",
            "project": project,
            "command": {
                "id": value.get("command_id"),
                "name": value.get("command_name"),
            },
            "run": {
                "id": value.get("run_id"),
                "status": value.get("status"),
                "exit_code": value.get("exit_code"),
            },
            "artifact": {
                "kind": value.get("kind"),
                "path": value.get("path"),
                "exists": value.get("exists"),
                "size_bytes": value.get("size_bytes"),
            },
        }

    return None


def summarize_coverage_ingest(artifact_paths: list[dict[str, Any]]) -> dict[str, Any]:
    coverage_artifacts = [artifact for artifact in artifact_paths if artifact.get("coverage_format")]
    if not coverage_artifacts:
        return {
            "status": "not_configured",
            "configured_artifacts": 0,
            "ingested_artifacts": 0,
            "failed_artifacts": 0,
            "skipped_artifacts": 0,
            "snapshot_ids": [],
        }

    statuses = [
        "not_recorded" if "ingest_status" not in artifact else str(artifact.get("ingest_status") or "pending")
        for artifact in coverage_artifacts
    ]
    ingested = statuses.count("ingested")
    failed = sum(status in {"failed", "missing"} for status in statuses)
    skipped = sum(status.startswith("skipped_") for status in statuses)
    if ingested == len(statuses):
        status = "ingested"
    elif ingested:
        status = "partial"
    elif failed:
        status = "failed"
    elif statuses and all(item == "skipped_stale" for item in statuses):
        status = "skipped_stale"
    elif statuses and all(item == "skipped_run_status" for item in statuses):
        status = "skipped_run_status"
    elif statuses and all(item == "not_recorded" for item in statuses):
        status = "not_recorded"
    else:
        status = "pending"
    return {
        "status": status,
        "configured_artifacts": len(coverage_artifacts),
        "ingested_artifacts": ingested,
        "failed_artifacts": failed,
        "skipped_artifacts": skipped,
        "snapshot_ids": [artifact["snapshot_id"] for artifact in coverage_artifacts if artifact.get("snapshot_id")],
    }


def normalize_artifact_specs(artifact_paths: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for kind, value in artifact_paths.items():
        kind = str(kind).strip()
        if not kind:
            continue
        if isinstance(value, str):
            path = value
            required = False
            coverage_format = None
            suite = None
        elif isinstance(value, dict):
            path = str(value.get("path", "")).strip()
            required = bool(value.get("required", False))
            coverage_format = value.get("coverage_format") or value.get("format")
            raw_suite = value.get("suite")
            suite = str(raw_suite).strip() if raw_suite is not None else None
            if raw_suite is not None and not suite:
                raise ValueError(f"artifact spec for {kind} has a blank suite")
        else:
            raise ValueError(f"artifact spec for {kind} must be a path string or object")
        if not path:
            raise ValueError(f"artifact spec for {kind} is missing path")
        specs.append(
            {
                "kind": kind,
                "path": path,
                "required": required,
                "coverage_format": coverage_format,
                "suite": suite,
            }
        )
    return specs


def summarize_run_logs(
    *,
    stdout_path: Path,
    stderr_path: Path,
    exit_code: int | None,
    status: str,
    duration_ms: int,
    max_summary_lines: int,
) -> dict[str, Any]:
    stdout = profile_log(stdout_path, stream="stdout", max_lines=max_summary_lines)
    stderr = profile_log(stderr_path, stream="stderr", max_lines=max_summary_lines)
    excerpts = [*stderr["interesting"], *stdout["interesting"]]
    if len(excerpts) < max_summary_lines:
        tail_needed = max_summary_lines - len(excerpts)
        excerpts.extend([*stderr["tail"], *stdout["tail"]][:tail_needed])
    excerpts = excerpts[:max_summary_lines]
    counters = merge_counters(stdout["counters"], stderr["counters"])
    return {
        "status": status,
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "stdout_line_count": stdout["line_count"],
        "stderr_line_count": stderr["line_count"],
        "counters": counters,
        "excerpts": excerpts,
        "truncated": stdout["line_count"] + stderr["line_count"] > len(excerpts),
        "stdout_path": stdout_path.as_posix(),
        "stderr_path": stderr_path.as_posix(),
    }


def compact_run_result(result: dict[str, Any]) -> dict[str, Any]:
    """Project a verbose storage record into the default polling contract."""
    raw_summary = result.get("parsed_summary")
    summary: dict[str, Any] = raw_summary if isinstance(raw_summary, dict) else {}
    status = str(result["status"])
    if status == "queued":
        age_seconds = int(result.get("queued_age_seconds") or 0)
        age = str(result.get("queued_age") or format_age(age_seconds))
    elif status == "running":
        age_seconds = int(result.get("running_age_seconds") or 0)
        age = str(result.get("running_age") or format_age(age_seconds))
    else:
        age_seconds = int(result.get("age_seconds") or 0)
        age = str(result.get("age") or format_age(age_seconds))
    stdout_count = summary.get("stdout_line_count")
    stderr_count = summary.get("stderr_line_count")
    diagnostics_available = any(isinstance(count, int) and count > 0 for count in (stdout_count, stderr_count))
    return {
        "id": result["id"],
        "command_id": result.get("command_id"),
        "command_name": result["command_name"],
        "status": status,
        "terminal": bool(result["terminal"]),
        "duration_ms": int(result["duration_ms"]),
        "exit_code": result.get("exit_code"),
        "counters": dict(summary.get("counters") or {}),
        "checkout_path": result["repo_path"],
        "branch": result.get("branch"),
        "commit_sha": result.get("commit_sha"),
        "coverage_ingest": result["coverage_ingest"],
        "poll_after_ms": result.get("poll_after_ms"),
        "queue_position": result.get("queue_position"),
        "age_seconds": age_seconds,
        "age": age,
        "eta_seconds": result.get("eta_seconds"),
        "eta": result.get("eta"),
        "cancellation_requested": bool(result.get("cancellation_requested")),
        "submission_reused": result.get("submission_reused"),
        "error": result.get("error") or summary.get("execution_error"),
        "diagnostics_available": diagnostics_available,
    }


def search_log_file(
    path: Path,
    *,
    stream: str,
    query: str | Sequence[str],
    case_sensitive: bool,
    context_lines: int,
    max_matches: int,
    max_words: int,
) -> dict[str, Any]:
    """Find literal matches in two streaming passes and retain only bounded context."""
    queries = normalize_log_queries(query)
    needles = queries if case_sensitive else [term.casefold() for term in queries]
    anchors: list[int] = []
    match_count = 0
    line_count = 0
    if path.exists():
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw in enumerate(handle, start=1):
                line_count = line_number
                haystack = raw if case_sensitive else raw.casefold()
                if any(needle in haystack for needle in needles):
                    match_count += 1
                    if len(anchors) < max_matches:
                        anchors.append(line_number)

    ranges: list[list[int]] = []
    for anchor in anchors:
        start, end = max(1, anchor - context_lines), min(line_count, anchor + context_lines)
        if ranges and start <= ranges[-1][1] + 1:
            ranges[-1][1] = max(ranges[-1][1], end)
        else:
            ranges.append([start, end])

    contexts: list[dict[str, Any]] = []
    returned_match_count = 0
    returned_line_count = 0
    returned_word_count = 0
    word_truncated = False
    range_index = 0
    current: dict[str, Any] | None = None
    if ranges and max_words and path.exists():
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw in enumerate(handle, start=1):
                while range_index < len(ranges) and line_number > ranges[range_index][1]:
                    if current is not None:
                        contexts.append(current)
                        current = None
                    range_index += 1
                if range_index >= len(ranges) or returned_word_count >= max_words:
                    break
                start, end = ranges[range_index]
                if line_number < start:
                    continue
                full_text = raw.rstrip("\n")
                haystack = full_text if case_sensitive else full_text.casefold()
                matched = any(needle in haystack for needle in needles)
                text = bounded_log_text(full_text, query=queries, case_sensitive=case_sensitive, matched=matched)
                text, word_count, line_truncated = truncate_to_word_budget(
                    text,
                    max_words=max_words - returned_word_count,
                )
                if current is None:
                    current = {"stream": stream, "start_line": line_number, "end_line": line_number, "lines": []}
                current["end_line"] = line_number
                current["lines"].append({"line_number": line_number, "text": text[:500], "match": matched})
                returned_line_count += 1
                returned_word_count += word_count
                word_truncated = bool(word_truncated or line_truncated)
                if matched:
                    returned_match_count += 1
        if current is not None:
            contexts.append(current)

    expected_context_end = ranges[-1][1] if ranges else 0
    actual_context_end = contexts[-1]["end_line"] if contexts else 0
    return {
        "match_count": match_count,
        "returned_match_count": returned_match_count,
        "returned_line_count": returned_line_count,
        "returned_word_count": returned_word_count,
        "truncated": match_count > len(anchors) or actual_context_end < expected_context_end or word_truncated,
        "contexts": contexts,
    }


def truncate_to_word_budget(text: str, *, max_words: int) -> tuple[str, int, bool]:
    """Preserve original spacing while truncating text at a whitespace-delimited word boundary."""
    words = list(re.finditer(r"\S+", text))
    if len(words) <= max_words:
        return text, len(words), False
    if max_words <= 0:
        return "", 0, bool(words)
    return text[: words[max_words - 1].end()], max_words, True


def bounded_log_text(text: str, *, query: str | Sequence[str], case_sensitive: bool, matched: bool) -> str:
    """Keep long matching lines centered on the query instead of returning an irrelevant prefix."""
    if len(text) <= 500:
        return text
    if not matched:
        return text[:500]
    queries = normalize_log_queries(query)
    haystack = text if case_sensitive else text.casefold()
    needles = queries if case_sensitive else [term.casefold() for term in queries]
    match_at = min((index for needle in needles if (index := haystack.find(needle)) >= 0), default=0)
    start = max(match_at - 200, 0)
    end = min(start + 500, len(text))
    prefix = "…" if start else ""
    suffix = "…" if end < len(text) else ""
    return prefix + text[start:end] + suffix


def profile_log(path: Path, *, stream: str, max_lines: int) -> dict[str, Any]:
    interesting: list[dict[str, Any]] = []
    tail: deque[dict[str, Any]] = deque(maxlen=max_lines)
    counters: dict[str, int] = {}
    line_count = 0
    if not path.exists():
        return {"line_count": 0, "interesting": [], "tail": [], "counters": {}}
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, raw in enumerate(handle, start=1):
            line_count = line_number
            text = raw.rstrip("\n")
            update_log_counters(counters, text)
            item = {"stream": stream, "line_number": line_number, "text": text[:1000]}
            tail.append(item)
            if len(interesting) < max_lines and is_interesting_log_line(text):
                interesting.append(item)
    return {
        "line_count": line_count,
        "interesting": interesting,
        "tail": list(tail),
        "counters": counters,
    }


def update_log_counters(counters: dict[str, int], text: str) -> None:
    lowered = text.lower()
    for key in ["passed", "failed", "skipped", "error", "errors", "failure", "failures"]:
        match = re.search(rf"\b(\d+)\s+{key}\b", lowered)
        if match:
            counters[key] = counters.get(key, 0) + int(match.group(1))


def merge_counters(*items: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for counters in items:
        for key, value in counters.items():
            merged[key] = merged.get(key, 0) + value
    return merged


def is_interesting_log_line(text: str) -> bool:
    lowered = text.lower()
    needles = [
        "error",
        "failed",
        "failure",
        "panic",
        "traceback",
        "exception",
        "assert",
        "segmentation fault",
        "timeout",
        "fatal",
    ]
    return any(needle in lowered for needle in needles)


def percent(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value * 100:.1f}%"


def percent_delta(value: float | None) -> str:
    if value is None:
        return "unknown"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.1f} points"


def _insight_sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
    severity_rank = {"high": 0, "medium": 1, "info": 2}
    severity = item.get("severity")
    severity_key = severity if isinstance(severity, str) else ""
    return (
        severity_rank.get(severity_key, 3),
        str(item.get("category", "")),
        str(item.get("file_path", "")),
    )
