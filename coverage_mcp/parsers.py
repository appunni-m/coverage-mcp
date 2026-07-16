from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from coverage_mcp.models import CoverageBuilder, CoverageReport


class CoverageParseError(ValueError):
    pass


SUPPORTED_FORMATS = {
    "auto",
    "lcov",
    "coverage.py",
    "coveragepy",
    "cobertura",
    "jacoco",
    "istanbul",
    "nyc",
    "go",
    "go-cover",
    "go-coverprofile",
    "llvm",
    "llvm-json",
}


def parse_coverage_report(
    report_path: str | Path,
    *,
    format: str = "auto",
    repo_path: str | None = None,
) -> CoverageReport:
    path = Path(report_path).expanduser()
    if not path.exists():
        raise CoverageParseError(f"coverage report does not exist: {path}")
    selected = normalize_format(format)
    if selected == "auto":
        selected = detect_format(path)

    if selected == "lcov":
        return parse_lcov(path, repo_path=repo_path)
    if selected == "coveragepy":
        return parse_coveragepy_json(path, repo_path=repo_path)
    if selected == "cobertura":
        return parse_cobertura_xml(path, repo_path=repo_path)
    if selected == "jacoco":
        return parse_jacoco_xml(path, repo_path=repo_path)
    if selected == "istanbul":
        return parse_istanbul_json(path, repo_path=repo_path)
    if selected == "go":
        return parse_go_coverprofile(path, repo_path=repo_path)
    if selected == "llvm":
        return parse_llvm_json(path, repo_path=repo_path)
    raise CoverageParseError(f"unsupported coverage format: {format}")


def normalize_format(format: str) -> str:
    lowered = format.strip().lower()
    aliases = {
        "coverage.py": "coveragepy",
        "coverage-json": "coveragepy",
        "coveragepy-json": "coveragepy",
        "nyc": "istanbul",
        "go-cover": "go",
        "go-coverprofile": "go",
        "coverprofile": "go",
        "llvm-json": "llvm",
    }
    normalized = aliases.get(lowered, lowered)
    if normalized not in {normalize for normalize in SUPPORTED_FORMATS if normalize != "coverage.py"}:
        raise CoverageParseError(f"unsupported coverage format: {format}")
    return normalized


def detect_format(path: Path) -> str:
    suffix = path.suffix.lower()
    head = path.read_text(encoding="utf-8", errors="replace")[:4096].lstrip()
    if suffix in {".lcov", ".info"} or head.startswith("TN:") or "\nSF:" in head:
        return "lcov"
    if head.startswith("mode:"):
        return "go"
    if suffix == ".xml" or head.startswith("<"):
        root = ET.parse(path).getroot()
        tag = _strip_ns(root.tag)
        if tag == "report" and (root.findall(".//package/sourcefile") or root.findall(".//counter")):
            return "jacoco"
        if tag == "coverage":
            return "cobertura"
        raise CoverageParseError(f"could not detect XML coverage format for {path}")
    if suffix == ".json" or head.startswith("{"):
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            if "data" in data:
                return "llvm"
            files = data.get("files")
            if isinstance(files, dict) and any(
                isinstance(value, dict) and "executed_lines" in value for value in files.values()
            ):
                return "coveragepy"
            if _looks_like_istanbul(data):
                return "istanbul"
        raise CoverageParseError(f"could not detect JSON coverage format for {path}")
    raise CoverageParseError(f"could not detect coverage format for {path}")


def parse_lcov(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    builder = CoverageBuilder(repo_path)
    warnings: list[str] = []
    current_file: str | None = None
    function_lines: dict[str, int] = {}

    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("SF:"):
            current_file = line[3:]
            function_lines = {}
            continue
        if line == "end_of_record":
            current_file = None
            function_lines = {}
            continue
        if current_file is None:
            continue
        if line.startswith("DA:"):
            parts = line[3:].split(",")
            if len(parts) >= 2:
                builder.add_line(current_file, _safe_int(parts[0]), _safe_int(parts[1]))
            continue
        if line.startswith("FN:"):
            try:
                fn_line_no, name = line[3:].split(",", 1)
            except ValueError:
                continue
            function_lines[name] = _safe_int(fn_line_no)
            continue
        if line.startswith("FNDA:"):
            try:
                hits_raw, name = line[5:].split(",", 1)
            except ValueError:
                continue
            function_line_no = function_lines.get(name)
            if function_line_no:
                hits = _safe_int(hits_raw)
                builder.add_line(
                    current_file,
                    function_line_no,
                    0,
                    covered=False,
                    count_line=False,
                    total_functions=1,
                    covered_functions=1 if hits > 0 else 0,
                )
            continue
        if line.startswith("BRDA:"):
            parts = line[5:].split(",")
            if len(parts) >= 4:
                taken = parts[3]
                covered = 0 if taken == "-" else 1 if _safe_int(taken) > 0 else 0
                builder.add_line(
                    current_file,
                    _safe_int(parts[0]),
                    0,
                    covered=False,
                    count_line=False,
                    total_branches=1,
                    covered_branches=covered,
                )
            continue
    if not builder.build(format="lcov", report_path=str(path)).lines:
        warnings.append("LCOV report contained no DA/BRDA/FNDA records.")
    return builder.build(format="lcov", report_path=str(path), warnings=warnings)


def parse_coveragepy_json(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    data = json.loads(path.read_text(encoding="utf-8"))
    files = data.get("files")
    if not isinstance(files, dict):
        raise CoverageParseError("coverage.py JSON report must contain a 'files' object")
    builder = CoverageBuilder(repo_path)
    warnings = ["coverage.py JSON reports line coverage as covered/missing, not execution counts."]
    for file_path, payload in files.items():
        if not isinstance(payload, dict):
            continue
        executed = {int(line) for line in payload.get("executed_lines", [])}
        missing = {int(line) for line in payload.get("missing_lines", [])}
        for line_no in sorted(executed | missing):
            covered = line_no in executed
            builder.add_line(file_path, line_no, 1 if covered else 0, covered=covered)
        branch_counts: dict[int, list[int]] = {}
        for branch in payload.get("executed_branches", []) or []:
            if isinstance(branch, list) and branch:
                branch_counts.setdefault(int(branch[0]), [0, 0])[1] += 1
        for branch in payload.get("missing_branches", []) or []:
            if isinstance(branch, list) and branch:
                branch_counts.setdefault(int(branch[0]), [0, 0])[0] += 1
        for line_no, (missing_count, covered_count) in branch_counts.items():
            builder.add_line(
                file_path,
                line_no,
                0,
                covered=False,
                count_line=False,
                total_branches=missing_count + covered_count,
                covered_branches=covered_count,
            )
        if "summary" in payload:
            builder.add_file_metrics(file_path, coveragepy_summary=payload["summary"])
    return builder.build(
        format="coveragepy",
        report_path=str(path),
        warnings=warnings,
        metadata={"meta": data.get("meta", {})},
    )


def parse_cobertura_xml(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    root = ET.parse(path).getroot()
    builder = CoverageBuilder(repo_path)
    for class_node in root.findall(".//class"):
        file_path = class_node.get("filename") or class_node.get("name")
        if not file_path:
            continue
        for line_node in class_node.findall("./lines/line"):
            line_no = _safe_int(line_node.get("number", "0"))
            hits = _safe_int(line_node.get("hits", "0"))
            total_branches, covered_branches = _parse_cobertura_branch_counts(line_node)
            builder.add_line(
                file_path,
                line_no,
                hits,
                total_branches=total_branches,
                covered_branches=covered_branches,
            )
        builder.add_file_metrics(
            file_path,
            line_rate=_safe_float(class_node.get("line-rate")),
            branch_rate=_safe_float(class_node.get("branch-rate")),
        )
    return builder.build(format="cobertura", report_path=str(path))


def parse_jacoco_xml(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    root = ET.parse(path).getroot()
    builder = CoverageBuilder(repo_path)
    for package in root.findall(".//package"):
        package_name = (package.get("name") or "").strip("/")
        for source in package.findall("./sourcefile"):
            name = source.get("name")
            if not name:
                continue
            file_path = f"{package_name}/{name}" if package_name else name
            for line_node in source.findall("./line"):
                missed_instructions = _safe_int(line_node.get("mi", "0"))
                covered_instructions = _safe_int(line_node.get("ci", "0"))
                missed_branches = _safe_int(line_node.get("mb", "0"))
                covered_branches = _safe_int(line_node.get("cb", "0"))
                builder.add_line(
                    file_path,
                    _safe_int(line_node.get("nr", "0")),
                    covered_instructions,
                    covered=covered_instructions > 0,
                    total_branches=missed_branches + covered_branches,
                    covered_branches=covered_branches,
                    details={"missed_instructions": missed_instructions},
                )
    return builder.build(format="jacoco", report_path=str(path))


def parse_istanbul_json(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not _looks_like_istanbul(data):
        raise CoverageParseError("Istanbul JSON must contain coverage objects with statementMap/s/f/branchMap/b")
    builder = CoverageBuilder(repo_path)
    warnings = ["Istanbul statement coverage is normalized to the starting line of each statement."]
    for key, payload in data.items():
        if not isinstance(payload, dict) or "statementMap" not in payload:
            continue
        file_path = payload.get("path") or key
        statement_map = payload.get("statementMap") or {}
        statement_hits = payload.get("s") or {}
        for statement_id, loc in statement_map.items():
            line_no = _location_line(loc)
            if line_no is None:
                continue
            hits = _safe_int(str(statement_hits.get(statement_id, 0)))
            builder.add_line(file_path, line_no, hits)

        function_map = payload.get("fnMap") or {}
        function_hits = payload.get("f") or {}
        for function_id, function in function_map.items():
            line_no = _location_line(function.get("loc") or function.get("decl") or function)
            if line_no is None:
                continue
            hits = _safe_int(str(function_hits.get(function_id, 0)))
            builder.add_line(
                file_path,
                line_no,
                0,
                covered=False,
                count_line=False,
                total_functions=1,
                covered_functions=1 if hits > 0 else 0,
            )

        branch_map = payload.get("branchMap") or {}
        branch_hits = payload.get("b") or {}
        for branch_id, branch in branch_map.items():
            line_no = _location_line(branch.get("loc") or branch)
            if line_no is None:
                continue
            counts = branch_hits.get(branch_id, [])
            if not isinstance(counts, list):
                counts = []
            total = len(counts)
            covered = sum(1 for count in counts if _safe_int(str(count)) > 0)
            builder.add_line(
                file_path,
                line_no,
                0,
                covered=False,
                count_line=False,
                total_branches=total,
                covered_branches=covered,
            )
    return builder.build(format="istanbul", report_path=str(path), warnings=warnings)


def parse_go_coverprofile(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    builder = CoverageBuilder(repo_path)
    warnings = ["Go coverprofiles report statement blocks; this expands each block to all touched lines."]
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines or not lines[0].startswith("mode:"):
        raise CoverageParseError("Go coverprofile must start with 'mode:'")
    pattern = re.compile(r"^(?P<file>.+):(?P<start>\d+)\.\d+,(?P<end>\d+)\.\d+\s+\d+\s+(?P<count>\d+)$")
    for raw in lines[1:]:
        match = pattern.match(raw.strip())
        if not match:
            continue
        file_path = match.group("file")
        start = _safe_int(match.group("start"))
        end = max(start, _safe_int(match.group("end")))
        hits = _safe_int(match.group("count"))
        for line_no in range(start, end + 1):
            builder.add_line(file_path, line_no, hits, covered=hits > 0)
    return builder.build(format="go", report_path=str(path), warnings=warnings)


def parse_llvm_json(path: Path, *, repo_path: str | None = None) -> CoverageReport:
    data = json.loads(path.read_text(encoding="utf-8"))
    builder = CoverageBuilder(repo_path)
    warnings = ["LLVM JSON segments are normalized to segment start lines; region-level detail is not fully preserved."]
    for unit in data.get("data", []) if isinstance(data, dict) else []:
        for file_payload in unit.get("files", []) if isinstance(unit, dict) else []:
            file_path = file_payload.get("filename")
            if not file_path:
                continue
            for segment in file_payload.get("segments", []) or []:
                if not isinstance(segment, list) or len(segment) < 4:
                    continue
                line_no = _safe_int(str(segment[0]))
                hits = _safe_int(str(segment[2]))
                has_count = bool(segment[3])
                if has_count:
                    builder.add_line(file_path, line_no, hits, covered=hits > 0)
            for branch in file_payload.get("branches", []) or []:
                branch_line_no, true_count, false_count = _llvm_branch_counts(branch)
                if branch_line_no is not None:
                    builder.add_line(
                        file_path,
                        branch_line_no,
                        0,
                        covered=False,
                        count_line=False,
                        total_branches=2,
                        covered_branches=(1 if true_count > 0 else 0) + (1 if false_count > 0 else 0),
                    )
            if "summary" in file_payload:
                builder.add_file_metrics(file_path, llvm_summary=file_payload["summary"])
    return builder.build(format="llvm", report_path=str(path), warnings=warnings)


def _parse_cobertura_branch_counts(line_node: ET.Element) -> tuple[int, int]:
    if line_node.get("branch", "").lower() != "true":
        return 0, 0
    covered_raw = line_node.get("branches-covered")
    valid_raw = line_node.get("branches-valid")
    if covered_raw is not None and valid_raw is not None:
        return _safe_int(valid_raw), _safe_int(covered_raw)
    condition = line_node.get("condition-coverage", "")
    match = re.search(r"\((\d+)\s*/\s*(\d+)\)", condition)
    if match:
        covered, total = match.groups()
        return _safe_int(total), _safe_int(covered)
    return 0, 0


def _looks_like_istanbul(data: dict[str, Any]) -> bool:
    return any(
        isinstance(value, dict) and "statementMap" in value and "s" in value
        for value in data.values()
    )


def _location_line(loc: Any) -> int | None:
    if not isinstance(loc, dict):
        return None
    if "start" in loc and isinstance(loc["start"], dict):
        return _safe_int(str(loc["start"].get("line", 0))) or None
    return _safe_int(str(loc.get("line", 0))) or None


def _llvm_branch_counts(branch: Any) -> tuple[int | None, int, int]:
    if isinstance(branch, list) and len(branch) >= 6:
        return _safe_int(str(branch[0])), _safe_int(str(branch[4])), _safe_int(str(branch[5]))
    if isinstance(branch, dict):
        line_no = _safe_int(str(branch.get("line") or branch.get("line_start") or 0))
        true_count = _safe_int(str(branch.get("true_count") or branch.get("trueCount") or 0))
        false_count = _safe_int(str(branch.get("false_count") or branch.get("falseCount") or 0))
        return line_no or None, true_count, false_count
    return None, 0, 0


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _safe_int(value: str | int | float | None) -> int:
    try:
        return int(float(value)) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
