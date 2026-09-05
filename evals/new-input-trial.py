#!/usr/bin/env python3
"""Measure a newly authored external input against a fixed instrumented CLI.

Run without other coverage jobs: the trial clears shared profraw data between
the baseline and new batch. It does not rerun the baseline after authoring the
new input, modify Rust source, or prove reuse across changed instrumentation.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "target/incremental-verification/new-input-trial"
ENV = dict(os.environ, RUSTC_WRAPPER="", DUCKDB_DOWNLOAD_LIB="1")
ENV.setdefault("DYLD_LIBRARY_PATH", str(ROOT / "target/debug/deps"))
ENV.setdefault("LD_LIBRARY_PATH", str(ROOT / "target/debug/deps"))
READER = Path(os.environ.get("COVERAGE_MCP_BIN", ROOT / "target/debug/coverage-mcp"))
INSTRUMENTED = ROOT / "target/llvm-cov-target/debug/coverage-mcp"
COMMAND = ["cargo", "llvm-cov", "run", "--bin", "coverage-mcp",
           "--no-default-features", "--locked", "--no-report", "--"]
OUT.mkdir(parents=True, exist_ok=True)
RECORDS = []


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
    record = {"name": name, "command": command, "exit": result.returncode,
              "seconds": round(time.monotonic() - start, 3),
              "stdout_bytes": len(result.stdout)}
    RECORDS.append(record)
    if result.returncode:
        raise RuntimeError(f"{name} failed; inspect {OUT / (name + '.stderr')}")
    return result.stdout


def measured(name, input_path, expected_covered, expected_missing):
    # The build exists before each measured execution. Rebuilding is permitted
    # only if it produces exactly the same executable and measured source.
    inputs_before = source_hashes()
    binary_before = sha(INSTRUMENTED)
    run(f"{name}-clean", ["cargo", "llvm-cov", "clean", "--profraw-only"])
    raw = run(name, COMMAND + ["gaps", "--repo", str(ROOT), "--measurement",
                              str(input_path.relative_to(ROOT)), "--metric", "lines", "--json"])
    value = json.loads(raw)["data"]
    assert value["status"] == "measured", value
    assert value["measurement"]["coverage"] == {
        "metric": "lines", "covered": expected_covered,
        "missing": len(expected_missing), "total": expected_covered + len(expected_missing)
    }, value
    missing = sorted(loc["span"][0] for group in value["groups"] for loc in group["locations"])
    assert missing == expected_missing, value
    assert source_hashes() == inputs_before, "Measured source changed during execution"
    assert sha(INSTRUMENTED) == binary_before, "Instrumented executable changed"
    report = OUT / f"{name}.json"
    # Export only the captured executable. cargo llvm-cov report also discovers
    # unrelated test binaries left in the shared target directory; their mapping
    # must not become part of this fixed-executable measurement inventory.
    profiles = sorted((ROOT / "target/llvm-cov-target").glob("*.profraw"))
    assert profiles, "The selected case produced no coverage profile"
    profile = OUT / f"{name}.profdata"
    run(f"{name}-merge", [str(LLVM_PROFDATA), "merge", "-sparse",
                         *map(str, profiles), "-o", str(profile)])
    export = run(f"{name}-report", [str(LLVM_COV), "export", "-format=text",
                                   "-instr-profile", str(profile), "-object", str(INSTRUMENTED),
                                   "-ignore-filename-regex", r"/rustc/|/\.cargo/|/\.rustup/"])
    report.write_bytes(export)
    document = json.loads(report.read_bytes())
    measured_paths = {str(Path(file["filename"]).resolve().relative_to(ROOT))
                      for data in document["data"] for file in data["files"]}
    assert measured_paths <= inputs_before.keys(), "Unexpected measured source"
    assert not any(path.startswith(("target/", "evals/")) for path in measured_paths)
    assert source_hashes() == inputs_before
    assert sha(INSTRUMENTED) == binary_before
    receipt = {"report_sha256": sha(report), "build_id": binary_before,
               "source_hashes": inputs_before, "scope": "selected_tests", "test_status": "passed"}
    Path(str(report) + ".context.json").write_text(json.dumps(receipt) + "\n")
    return report, {"input_sha256": sha(input_path), "build_id": binary_before,
                    "source_sha256": hashlib.sha256(json.dumps(inputs_before, sort_keys=True).encode()).hexdigest(),
                    "report_sha256": sha(report), "report_bytes": report.stat().st_size,
                    "assertions_passed": 3, "measured_files": len(measured_paths)}


# Compile once before capturing any measurement identity. This setup invocation
# is not a baseline case, and its profile is cleared before baseline execution.
run("prepare", COMMAND + ["--version"])
sysroot = Path(run("rust-sysroot", ["rustc", "--print", "sysroot"]).decode().strip())
host = re.search(r"^host: (.+)$", run("rust-version", ["rustc", "-vV"]).decode(), re.MULTILINE).group(1)
llvm_bin = sysroot / "lib/rustlib" / host / "bin"
LLVM_COV = Path(ENV.get("LLVM_COV", llvm_bin / "llvm-cov"))
LLVM_PROFDATA = Path(ENV.get("LLVM_PROFDATA", llvm_bin / "llvm-profdata"))
base_input = OUT / "baseline-input.lcov"
new_input = OUT / "new-input.lcov"
new_input.unlink(missing_ok=True)
base_input.write_text("TN:\nSF:src/parser.rs\nDA:1,1\nDA:2,0\nend_of_record\n")
baseline, base_evidence = measured("baseline", base_input, 1, [2])
baseline_hash = sha(baseline)
baseline_receipt_hash = sha(Path(str(baseline) + ".context.json"))
assert not new_input.exists(), "New input must not exist during baseline measurement"

# Author a genuinely new case only AFTER the baseline is saved. It exercises
# named function/branch records as well as line records in the real LCOV parser.
new_input.write_text(
    "TN:new_named_branch_case\nSF:src/parser.rs\nFN:1,parse\nFNDA:1,parse\n"
    "DA:1,1\nDA:2,1\nDA:3,0\nBRDA:2,0,0,1\nBRDA:2,0,1,-\nend_of_record\n"
)
batch, batch_evidence = measured("new-batch", new_input, 2, [3])
assert base_evidence["build_id"] == batch_evidence["build_id"]
assert base_evidence["source_sha256"] == batch_evidence["source_sha256"]
args = [str(READER), "compare", "--repo", str(ROOT), "--scope", "incremental",
        "--baseline", str(baseline.relative_to(ROOT)), "--measurement", str(batch.relative_to(ROOT)),
        "--query", "parse_lcov", "--json"]
first = json.loads(run("compare", args))["data"]
assert first["status"] == "improved", first
inc = first["incremental"]
assert inc["evidence"] == "verified" and inc["newly_covered"] > 0, first
assert inc["combined"]["total"] == first["baseline"]["coverage"]["total"]
assert inc["combined"]["covered"] == inc["accepted"]["covered"] + inc["newly_covered"]
assert first["regression_checked"] is False
repeat = json.loads(run("repeat", args + ["--previous", str(batch.relative_to(ROOT))]))["data"]
assert repeat["status"] == "no_gain", repeat
assert repeat["incremental"]["newly_covered"] == 0, repeat
assert repeat["incremental"]["combined"] == inc["combined"]
assert sha(baseline) == baseline_hash
assert sha(Path(str(baseline) + ".context.json")) == baseline_receipt_hash
result = {"kind": "New external LCOV input against one fixed instrumented coverage-mcp executable",
          "baseline": base_evidence, "new_batch": batch_evidence,
          "new_input_authored_after_baseline": True, "baseline_reexecutions": 0,
          "baseline_unchanged": True, "measured_case_executions": 2,
          "query_calls": 2, "commands": RECORDS, "incremental_outcome": first,
          "repeat_outcome": repeat,
          "limitation": "External-input assertions only; no changed Rust source/harness, full-suite rerun, or billed-token measurement."}
(OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({"result": str(OUT / "result.json"), "status": first["status"],
                  "newly_covered": inc["newly_covered"], "combined": inc["combined"],
                  "baseline_reexecutions": 0}))
