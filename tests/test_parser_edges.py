from __future__ import annotations

import json

import pytest

from coverage_mcp.parsers import CoverageParseError, parse_coverage_report


def test_unsupported_format_and_unknown_xml_json_errors(tmp_path):
    report = tmp_path / "coverage.txt"
    report.write_text("{}", encoding="utf-8")
    with pytest.raises(CoverageParseError, match="unsupported"):
        parse_coverage_report(report, format="made-up")

    xml = tmp_path / "unknown.xml"
    xml.write_text("<root />", encoding="utf-8")
    with pytest.raises(CoverageParseError, match="could not detect XML"):
        parse_coverage_report(xml)

    unknown_json = tmp_path / "unknown.json"
    unknown_json.write_text(json.dumps({"files": {"a.py": {}}}), encoding="utf-8")
    with pytest.raises(CoverageParseError, match="could not detect JSON"):
        parse_coverage_report(unknown_json)


def test_lcov_empty_and_malformed_records(tmp_path):
    report = tmp_path / "empty.info"
    report.write_text(
        """TN:

SF:src/a.py
FN:not-valid
FNDA:not-valid
BRDA:bad
end_of_record
""",
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="lcov")

    assert parsed.total_lines == 0
    assert parsed.warnings


def test_coveragepy_rejects_invalid_shape_and_skips_bad_file_payload(tmp_path):
    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps({"meta": {}}), encoding="utf-8")
    with pytest.raises(CoverageParseError, match="files"):
        parse_coverage_report(invalid, format="coveragepy")

    report = tmp_path / "coverage.json"
    report.write_text(json.dumps({"files": {"a.py": "bad"}}), encoding="utf-8")
    parsed = parse_coverage_report(report, format="coveragepy")
    assert parsed.total_lines == 0


def test_cobertura_and_jacoco_skip_missing_file_names(tmp_path):
    cobertura = tmp_path / "cobertura.xml"
    cobertura.write_text(
        """<coverage><packages><package><classes><class>
<lines><line number="1" hits="1"/></lines>
</class></classes></package></packages></coverage>""",
        encoding="utf-8",
    )
    jacoco = tmp_path / "jacoco.xml"
    jacoco.write_text(
        """<report><package name=""><sourcefile>
<line nr="1" mi="0" ci="1" mb="0" cb="0"/>
</sourcefile></package></report>""",
        encoding="utf-8",
    )

    assert parse_coverage_report(cobertura, format="cobertura").files == []
    assert parse_coverage_report(jacoco, format="jacoco").files == []

    no_counts = tmp_path / "no-counts.xml"
    no_counts.write_text(
        """<coverage><packages><package><classes><class filename="a.py">
<lines><line number="1" hits="0" branch="true"/></lines>
</class></classes></package></packages></coverage>""",
        encoding="utf-8",
    )
    parsed = parse_coverage_report(no_counts, format="cobertura")
    assert parsed.total_branches == 0


def test_istanbul_edge_locations_and_invalid_shape(tmp_path):
    invalid = tmp_path / "bad-istanbul.json"
    invalid.write_text(json.dumps({"x": {"statementMap": {}}}), encoding="utf-8")
    with pytest.raises(CoverageParseError, match="Istanbul"):
        parse_coverage_report(invalid, format="istanbul")

    report = tmp_path / "istanbul.json"
    report.write_text(
        json.dumps(
            {
                "skip": "not-a-payload",
                "a.js": {
                    "statementMap": {"0": {}, "1": {"line": 4}, "2": "bad"},
                    "s": {"0": 1, "1": 2},
                    "fnMap": {"0": {"decl": {"line": 5}}, "1": {}},
                    "f": {"0": 0, "1": 1},
                    "branchMap": {"0": {"line": 6}, "1": {}},
                    "b": {"0": "bad", "1": [0]},
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_coverage_report(report, format="istanbul")

    assert parsed.total_lines == 1
    assert parsed.covered_lines == 1
    assert parsed.total_branches == 0


def test_parser_helper_fallbacks(tmp_path, monkeypatch):
    report = tmp_path / "report.json"
    report.write_text("{}", encoding="utf-8")
    monkeypatch.setattr("coverage_mcp.parsers.normalize_format", lambda value: "unknown-normalized")
    with pytest.raises(CoverageParseError, match="unsupported"):
        parse_coverage_report(report, format="auto")

    from coverage_mcp import parsers

    assert parsers._safe_int("not-int") == 0
    assert parsers._safe_float(None) is None
    assert parsers._safe_float("not-float") is None


def test_go_and_llvm_edge_cases(tmp_path):
    bad_go = tmp_path / "bad.out"
    bad_go.write_text("", encoding="utf-8")
    with pytest.raises(CoverageParseError, match="mode"):
        parse_coverage_report(bad_go, format="go")

    go = tmp_path / "cover.out"
    go.write_text("mode: set\nnot a cover row\npkg/a.go:4.1,3.2 1 1\n", encoding="utf-8")
    parsed_go = parse_coverage_report(go, format="go")
    assert parsed_go.total_lines == 1

    llvm = tmp_path / "llvm.json"
    llvm.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "files": [
                            {"segments": [[1, 1, 1, True]]},
                            {
                                "filename": "src/a.cc",
                                "segments": [["bad"], [1, 1, 1, False], [2, 1, 1, True]],
                                "branches": [{"line": 2, "true_count": 1, "false_count": 0}, "bad"],
                                "summary": {"lines": "bad"},
                            },
                        ]
                    },
                    "bad",
                ]
            }
        ),
        encoding="utf-8",
    )
    parsed_llvm = parse_coverage_report(llvm, format="llvm")
    assert parsed_llvm.total_lines == 1
    assert parsed_llvm.total_branches == 2
