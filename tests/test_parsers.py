from __future__ import annotations

import json

import pytest

from coverage_mcp.parsers import CoverageParseError, parse_coverage_report


def line_map(parsed):
    return {(line.file_path, line.line_number): line for line in parsed.lines}


def test_lcov_parser_keeps_line_branch_and_function_metrics_separate(tmp_path):
    report = tmp_path / "lcov.info"
    report.write_text(
        """TN:
SF:src/a.py
FN:10,helper
FNDA:1,helper
DA:1,1
DA:2,0
BRDA:2,0,0,0
BRDA:2,0,1,3
end_of_record
SF:src/metrics_only.py
FN:50,only_function
FNDA:4,only_function
BRDA:60,0,0,1
end_of_record
""",
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="lcov")
    lines = line_map(parsed)

    assert parsed.total_lines == 2
    assert parsed.covered_lines == 1
    assert parsed.total_branches == 3
    assert parsed.covered_branches == 2
    assert parsed.total_functions == 2
    assert parsed.covered_functions == 2
    assert lines[("src/a.py", 1)].covered is True
    assert lines[("src/a.py", 2)].covered is False
    assert lines[("src/a.py", 2)].total_branches == 2
    assert lines[("src/metrics_only.py", 50)].count_line is False
    assert lines[("src/metrics_only.py", 60)].count_line is False


def test_coveragepy_parser_preserves_exact_lines_and_branch_arcs(tmp_path):
    report = tmp_path / "coverage.json"
    report.write_text(
        json.dumps(
            {
                "meta": {"format": 2, "version": "7.0"},
                "files": {
                    "pkg/a.py": {
                        "executed_lines": [1, 3],
                        "missing_lines": [2],
                        "executed_branches": [[1, 3], [3, 5]],
                        "missing_branches": [[1, 2]],
                        "summary": {"covered_lines": 2, "num_statements": 3},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="coverage.py")
    lines = line_map(parsed)

    assert parsed.format == "coveragepy"
    assert parsed.total_lines == 3
    assert parsed.covered_lines == 2
    assert parsed.total_branches == 3
    assert parsed.covered_branches == 2
    assert lines[("pkg/a.py", 1)].covered is True
    assert lines[("pkg/a.py", 1)].total_branches == 2
    assert lines[("pkg/a.py", 2)].covered is False
    assert parsed.files[0].raw_metrics["coveragepy_summary"]["num_statements"] == 3
    assert parsed.warnings


def test_cobertura_parser_supports_condition_and_explicit_branch_counts(tmp_path):
    report = tmp_path / "cobertura.xml"
    report.write_text(
        """<coverage>
  <packages><package><classes><class filename="src/a.py" line-rate="0.5" branch-rate="0.5">
    <lines>
      <line number="1" hits="1"/>
      <line number="2" hits="0" branch="true" condition-coverage="50% (1/2)"/>
      <line number="3" hits="2" branch="true" branches-covered="2" branches-valid="3"/>
    </lines>
  </class></classes></package></packages>
</coverage>""",
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="cobertura")
    lines = line_map(parsed)

    assert parsed.total_lines == 3
    assert parsed.covered_lines == 2
    assert parsed.total_branches == 5
    assert parsed.covered_branches == 3
    assert lines[("src/a.py", 2)].total_branches == 2
    assert lines[("src/a.py", 3)].covered_branches == 2
    assert parsed.files[0].raw_metrics["line_rate"] == 0.5


def test_jacoco_parser_uses_instruction_and_branch_counters(tmp_path):
    report = tmp_path / "jacoco.xml"
    report.write_text(
        """<report name="r">
  <package name="com/example"><sourcefile name="App.java">
    <line nr="10" mi="0" ci="4" mb="1" cb="1"/>
    <line nr="11" mi="3" ci="0" mb="2" cb="0"/>
  </sourcefile></package>
</report>""",
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="jacoco")
    lines = line_map(parsed)

    assert parsed.files[0].file_path == "com/example/App.java"
    assert parsed.total_lines == 2
    assert parsed.covered_lines == 1
    assert parsed.total_branches == 4
    assert parsed.covered_branches == 1
    assert lines[("com/example/App.java", 11)].details["missed_instructions"] == 3


def test_istanbul_parser_normalizes_statements_functions_and_branches(tmp_path):
    report = tmp_path / "coverage-final.json"
    report.write_text(
        json.dumps(
            {
                "/repo/src/a.js": {
                    "path": "/repo/src/a.js",
                    "statementMap": {
                        "0": {"start": {"line": 1}, "end": {"line": 1}},
                        "1": {"start": {"line": 2}, "end": {"line": 2}},
                    },
                    "s": {"0": 0, "1": 3},
                    "fnMap": {"0": {"loc": {"start": {"line": 1}}}},
                    "f": {"0": 1},
                    "branchMap": {"0": {"loc": {"start": {"line": 3}}}},
                    "b": {"0": [1, 0]},
                }
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="nyc", repo_path="/repo")
    lines = line_map(parsed)

    assert parsed.files[0].file_path == "src/a.js"
    assert parsed.total_lines == 2
    assert parsed.covered_lines == 1
    assert parsed.total_functions == 1
    assert parsed.covered_functions == 1
    assert parsed.total_branches == 2
    assert parsed.covered_branches == 1
    assert lines[("src/a.js", 1)].covered is False
    assert lines[("src/a.js", 1)].covered_functions == 1
    assert lines[("src/a.js", 3)].count_line is False
    assert parsed.warnings


def test_go_coverprofile_parser_expands_blocks_to_lines(tmp_path):
    report = tmp_path / "cover.out"
    report.write_text(
        """mode: count
pkg/a.go:3.1,5.2 2 4
pkg/a.go:5.1,6.8 1 0
""",
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="go-coverprofile")
    lines = line_map(parsed)

    assert parsed.total_lines == 4
    assert parsed.covered_lines == 3
    assert lines[("pkg/a.go", 5)].hits == 4
    assert lines[("pkg/a.go", 6)].covered is False
    assert parsed.warnings


def test_llvm_parser_normalizes_segments_and_branch_counts(tmp_path):
    report = tmp_path / "coverage.json"
    report.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "files": [
                            {
                                "filename": "/repo/src/a.cc",
                                "segments": [[1, 1, 3, True, True], [2, 1, 0, True, True], [3, 1, 9, False, True]],
                                "branches": [[2, 3, 2, 8, 0, 1]],
                                "summary": {
                                    "lines": {"count": 2, "covered": 1},
                                    "branches": {"count": 2, "covered": 1},
                                    "functions": {"count": 3, "covered": 2},
                                    "regions": {"count": 5, "covered": 4},
                                },
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="llvm-json", repo_path="/repo")
    lines = line_map(parsed)

    assert parsed.files[0].file_path == "src/a.cc"
    assert parsed.total_lines == 2
    assert parsed.covered_lines == 1
    assert parsed.total_branches == 2
    assert parsed.covered_branches == 1
    assert parsed.total_functions == 3
    assert parsed.covered_functions == 2
    assert parsed.function_rate == pytest.approx(2 / 3)
    assert parsed.total_regions == 5
    assert parsed.covered_regions == 4
    assert parsed.region_rate == pytest.approx(0.8)
    assert ("src/a.cc", 3) not in lines
    assert parsed.files[0].raw_metrics["llvm_summary"]["lines"]["count"] == 2


@pytest.mark.parametrize(
    ("filename", "content", "expected_format"),
    [
        ("lcov.info", "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n", "lcov"),
        (
            "coverage.json",
            json.dumps({"files": {"a.py": {"executed_lines": [1], "missing_lines": []}}}),
            "coveragepy",
        ),
        ("cobertura.xml", "<coverage><packages /></coverage>", "cobertura"),
        ("jacoco.xml", '<report><counter type="LINE" missed="0" covered="1" /></report>', "jacoco"),
        (
            "coverage-final.json",
            json.dumps({"a.js": {"statementMap": {"0": {"start": {"line": 1}}}, "s": {"0": 1}}}),
            "istanbul",
        ),
        ("cover.out", "mode: atomic\npkg/a.go:1.1,1.2 1 1\n", "go"),
        ("llvm.json", json.dumps({"data": [{"files": []}]}), "llvm"),
    ],
)
def test_auto_detection_for_supported_formats(tmp_path, filename, content, expected_format):
    report = tmp_path / filename
    report.write_text(content, encoding="utf-8")

    parsed = parse_coverage_report(report, format="auto")

    assert parsed.format == expected_format


def test_parse_errors_are_clear_for_missing_and_unknown_reports(tmp_path):
    with pytest.raises(CoverageParseError, match="does not exist"):
        parse_coverage_report(tmp_path / "missing.lcov")

    report = tmp_path / "unknown.txt"
    report.write_text("not a coverage report", encoding="utf-8")

    with pytest.raises(CoverageParseError, match="could not detect"):
        parse_coverage_report(report)
