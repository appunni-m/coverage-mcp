from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def rate(covered: int, total: int) -> float | None:
    if total <= 0:
        return None
    return covered / total


def normalize_report_path(path: str, repo_path: str | None = None) -> str:
    raw = Path(path).expanduser()
    if repo_path and raw.is_absolute():
        with suppress(ValueError):
            raw = raw.resolve().relative_to(Path(repo_path).expanduser().resolve())
    return raw.as_posix()


@dataclass(slots=True)
class LineCoverage:
    file_path: str
    line_number: int
    hits: int = 0
    covered: bool = False
    count_line: bool = True
    total_branches: int = 0
    covered_branches: int = 0
    total_functions: int = 0
    covered_functions: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def merge(self, other: LineCoverage) -> None:
        if other.count_line:
            self.hits = max(self.hits, other.hits)
            self.covered = self.covered or other.covered
        self.count_line = self.count_line or other.count_line
        self.total_branches += other.total_branches
        self.covered_branches += other.covered_branches
        self.total_functions += other.total_functions
        self.covered_functions += other.covered_functions
        if other.details:
            self.details.update(other.details)


@dataclass(slots=True)
class FileCoverage:
    file_path: str
    total_lines: int = 0
    covered_lines: int = 0
    total_branches: int = 0
    covered_branches: int = 0
    total_functions: int = 0
    covered_functions: int = 0
    total_regions: int = 0
    covered_regions: int = 0
    raw_metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def line_rate(self) -> float | None:
        return rate(self.covered_lines, self.total_lines)

    @property
    def branch_rate(self) -> float | None:
        return rate(self.covered_branches, self.total_branches)

    @property
    def function_rate(self) -> float | None:
        return rate(self.covered_functions, self.total_functions)

    @property
    def region_rate(self) -> float | None:
        return rate(self.covered_regions, self.total_regions)


@dataclass(slots=True)
class CoverageReport:
    format: str
    report_path: str
    files: list[FileCoverage]
    lines: list[LineCoverage]
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_lines(self) -> int:
        return sum(file.total_lines for file in self.files)

    @property
    def covered_lines(self) -> int:
        return sum(file.covered_lines for file in self.files)

    @property
    def total_branches(self) -> int:
        return sum(file.total_branches for file in self.files)

    @property
    def covered_branches(self) -> int:
        return sum(file.covered_branches for file in self.files)

    @property
    def total_functions(self) -> int:
        return sum(file.total_functions for file in self.files)

    @property
    def covered_functions(self) -> int:
        return sum(file.covered_functions for file in self.files)

    @property
    def total_regions(self) -> int:
        return sum(file.total_regions for file in self.files)

    @property
    def covered_regions(self) -> int:
        return sum(file.covered_regions for file in self.files)

    @property
    def line_rate(self) -> float | None:
        return rate(self.covered_lines, self.total_lines)

    @property
    def branch_rate(self) -> float | None:
        return rate(self.covered_branches, self.total_branches)

    @property
    def function_rate(self) -> float | None:
        return rate(self.covered_functions, self.total_functions)

    @property
    def region_rate(self) -> float | None:
        return rate(self.covered_regions, self.total_regions)


class CoverageBuilder:
    def __init__(self, repo_path: str | None = None) -> None:
        self.repo_path = repo_path
        self._lines: dict[tuple[str, int], LineCoverage] = {}
        self._file_metrics: dict[str, dict[str, Any]] = {}
        self._normalized_paths: dict[str, str] = {}

    def _normalize_path(self, path: str) -> str:
        normalized = self._normalized_paths.get(path)
        if normalized is None:
            normalized = normalize_report_path(path, self.repo_path)
            self._normalized_paths[path] = normalized
        return normalized

    def add_line(
        self,
        file_path: str,
        line_number: int,
        hits: int = 0,
        *,
        covered: bool | None = None,
        count_line: bool = True,
        total_branches: int = 0,
        covered_branches: int = 0,
        total_functions: int = 0,
        covered_functions: int = 0,
        details: dict[str, Any] | None = None,
    ) -> None:
        if line_number <= 0:
            return
        normalized = self._normalize_path(file_path)
        line_hits = max(0, int(hits)) if count_line else 0
        is_covered = (line_hits > 0 if covered is None else covered) if count_line else False
        line = LineCoverage(
            file_path=normalized,
            line_number=line_number,
            hits=line_hits,
            covered=is_covered,
            count_line=count_line,
            total_branches=max(0, int(total_branches)),
            covered_branches=max(0, int(covered_branches)),
            total_functions=max(0, int(total_functions)),
            covered_functions=max(0, int(covered_functions)),
            details=details or {},
        )
        key = (normalized, line_number)
        existing = self._lines.get(key)
        if existing is None:
            self._lines[key] = line
        else:
            existing.merge(line)

    def add_file_metrics(self, file_path: str, **metrics: Any) -> None:
        normalized = self._normalize_path(file_path)
        self._file_metrics.setdefault(normalized, {}).update(metrics)

    def build(
        self,
        *,
        format: str,
        report_path: str,
        warnings: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> CoverageReport:
        lines = sorted(self._lines.values(), key=lambda item: (item.file_path, item.line_number))
        by_file: dict[str, list[LineCoverage]] = {}
        for line in lines:
            by_file.setdefault(line.file_path, []).append(line)

        files: list[FileCoverage] = []
        file_paths = sorted(set(by_file) | set(self._file_metrics))
        for file_path in file_paths:
            file_lines = by_file.get(file_path, [])
            total_lines = sum(1 for line in file_lines if line.count_line)
            covered_lines = sum(1 for line in file_lines if line.count_line and line.covered)
            total_branches = sum(line.total_branches for line in file_lines)
            covered_branches = sum(line.covered_branches for line in file_lines)
            total_functions = sum(line.total_functions for line in file_lines)
            covered_functions = sum(line.covered_functions for line in file_lines)
            raw_metrics = dict(self._file_metrics.get(file_path, {}))
            total_lines = _normalized_metric(raw_metrics, "total_lines", total_lines)
            covered_lines = _normalized_metric(raw_metrics, "covered_lines", covered_lines)
            total_branches = _normalized_metric(raw_metrics, "total_branches", total_branches)
            covered_branches = _normalized_metric(raw_metrics, "covered_branches", covered_branches)
            total_functions = _normalized_metric(raw_metrics, "total_functions", total_functions)
            covered_functions = _normalized_metric(raw_metrics, "covered_functions", covered_functions)
            total_regions = _normalized_metric(raw_metrics, "total_regions", 0)
            covered_regions = _normalized_metric(raw_metrics, "covered_regions", 0)
            files.append(
                FileCoverage(
                    file_path=file_path,
                    total_lines=total_lines,
                    covered_lines=covered_lines,
                    total_branches=total_branches,
                    covered_branches=covered_branches,
                    total_functions=total_functions,
                    covered_functions=covered_functions,
                    total_regions=total_regions,
                    covered_regions=covered_regions,
                    raw_metrics=raw_metrics,
                )
            )

        return CoverageReport(
            format=format,
            report_path=str(report_path),
            files=files,
            lines=lines,
            warnings=warnings or [],
            metadata=metadata or {},
        )


def _normalized_metric(metrics: dict[str, Any], key: str, fallback: int) -> int:
    value = metrics.pop(key, None)
    if value is None:
        return fallback
    return max(0, int(value))
