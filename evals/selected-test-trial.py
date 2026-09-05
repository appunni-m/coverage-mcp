#!/usr/bin/env python3
"""Run two existing parser tests, then inspect and compare their coverage artifacts.

Requires the documented Rust build/coverage dependencies. This is a selected-test
trial, not a full-suite gate or a billed-token measurement. Output stays in target/.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ.get("COVERAGE_MCP_TRIAL_DIR", ROOT / "target/evals/selected-test-trial"))
OUT.mkdir(parents=True, exist_ok=True)
ENV = dict(os.environ, DUCKDB_DOWNLOAD_LIB="1")
BINARY = Path(os.environ.get("COVERAGE_MCP_BIN", ROOT / "target/debug/coverage-mcp"))
# A no-default-features development binary needs the downloaded DuckDB at runtime.
ENV.setdefault("DYLD_LIBRARY_PATH", str(ROOT / "target/debug/deps"))
ENV.setdefault("LD_LIBRARY_PATH", str(ROOT / "target/debug/deps"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes():
    paths = subprocess.check_output(
        ["rg", "--files", "src", "tests", "-g", "*.rs"], cwd=ROOT, text=True
    ).splitlines()
    return {path: sha(ROOT / path) for path in sorted(paths)}


def run(name, command):
    start = time.monotonic()
    result = subprocess.run(command, cwd=ROOT, env=ENV, capture_output=True)
    (OUT / f"{name}.stdout").write_bytes(result.stdout)
    (OUT / f"{name}.stderr").write_bytes(result.stderr)
    record = {"command": command, "exit": result.returncode,
              "seconds": round(time.monotonic() - start, 3), "stdout_bytes": len(result.stdout)}
    if result.returncode:
        raise RuntimeError(f"{name} failed; inspect {OUT / (name + '.stderr')}")
    return result.stdout, record


def measured(name, test):
    before = source_hashes()
    report = OUT / f"{name}.json"
    _, execution = run(name, [
        "cargo", "llvm-cov", "--lib", "--no-default-features", "--locked",
        "--ignore-filename-regex", r"/src/main\.rs$", "--json", "--output-path", str(report),
        "--", f"parser::tests::{test}", "--exact", "--test-threads=1",
    ])
    # Cargo reports the test executable used; hash that exact build after execution.
    import re
    log = (OUT / f"{name}.stderr").read_text()
    match = re.search(r"Running unittests src/lib.rs \(([^)]+)\)", log)
    if not match:
        raise RuntimeError("Could not identify the measured test executable")
    binary_hash = sha(ROOT / match.group(1))
    if source_hashes() != before:
        raise RuntimeError("Source changed while the test ran; no receipt produced")
    output = (OUT / f"{name}.stdout").read_text()
    if "1 passed; 0 failed" not in output:
        raise RuntimeError("The exact selected test did not run and pass")
    receipt = {"report_sha256": sha(report), "build_id": binary_hash,
               "source_hashes": before, "scope": "selected_tests", "test_status": "passed"}
    Path(str(report) + ".context.json").write_text(json.dumps(receipt) + "\n")
    execution["report_bytes"] = report.stat().st_size
    execution["build_id"] = binary_hash
    return report, execution


baseline, base_run = measured("before", "lcov_parser_preserves_lines_and_branch_counts")
gap_bytes, gap_run = run("gaps", [str(BINARY), "gaps", "--repo", str(ROOT),
    "--measurement", str(baseline.relative_to(ROOT)), "--query", "parse_lcov", "--limit", "3", "--json"])
# Investigate with normal source search, not MCP source-page calls.
_, search_run = run("source-search", ["rg", "-n", "fn parse_lcov|lcov_parser_preserves_exact_named", "src/parser.rs"])
current, current_run = measured("after", "lcov_parser_preserves_exact_named_line_branch_and_function_observations")
if base_run["build_id"] != current_run["build_id"]:
    raise RuntimeError("Selected runs used different builds; do not claim comparable gain")
change_bytes, compare_run = run("compare", [str(BINARY), "compare", "--repo", str(ROOT),
    "--measurement", str(current.relative_to(ROOT)), "--baseline", str(baseline.relative_to(ROOT)),
    "--query", "parse_lcov", "--limit", "3", "--json"])
gaps, change = json.loads(gap_bytes), json.loads(change_bytes)
assert change["data"]["scope"] == "selected_tests", change
assert change["data"]["changes"]["regressed"] is None, change
assert change["data"]["measurement"]["tests"] == "passed", change
result = {"kind": "existing selected tests, same source and build", "baseline": base_run,
          "gaps": gap_run, "source_search": search_run, "current": current_run, "compare": compare_run,
          "outcome": change["data"], "query_response_bytes": len(gap_bytes) + len(change_bytes),
          "query_calls": 2, "registration_calls": 0, "polling_calls": 0, "source_page_calls": 0,
          "limitation": "Two existing tests, not a new input campaign or a model/billing experiment."}
# Reuse the baseline and the selected batch; no additional test execution.
baseline_hash = sha(baseline)
increment_bytes, increment_run = run("incremental", [str(BINARY), "compare", "--repo", str(ROOT),
    "--measurement", str(current.relative_to(ROOT)), "--baseline", str(baseline.relative_to(ROOT)),
    "--scope", "incremental", "--query", "parse_lcov", "--limit", "3", "--json"])
increment = json.loads(increment_bytes)["data"]
assert increment["status"] == "improved", increment
assert increment["incremental"]["combined"]["total"] == increment["baseline"]["coverage"]["total"]
assert increment["incremental"]["combined"]["covered"] == (
    increment["incremental"]["accepted"]["covered"] + increment["incremental"]["newly_covered"])
repeat_bytes, repeat_run = run("incremental-repeat", [str(BINARY), "compare", "--repo", str(ROOT),
    "--measurement", str(current.relative_to(ROOT)), "--baseline", str(baseline.relative_to(ROOT)),
    "--scope", "incremental", "--previous", str(current.relative_to(ROOT)), "--json"])
repeated = json.loads(repeat_bytes)["data"]
assert repeated["status"] == "no_gain", repeated
assert repeated["incremental"]["newly_covered"] == 0
assert repeated["incremental"]["combined"] == increment["incremental"]["combined"]
assert sha(baseline) == baseline_hash, "Baseline was overwritten"
result.update({"incremental": increment_run, "incremental_outcome": increment,
               "repeat": repeat_run, "repeat_outcome": repeated,
               "baseline_unchanged": True, "test_executions": 2,
               "query_calls": 4,
               "query_response_bytes": len(gap_bytes)+len(change_bytes)+len(increment_bytes)+len(repeat_bytes)})
(OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({"result": str(OUT / "result.json"), "status": change["data"]["status"],
                  "changes": change["data"]["changes"], "query_response_bytes": result["query_response_bytes"]}, indent=2))
