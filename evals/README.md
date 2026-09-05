# Evaluate outcomes and workflow cost

Run `make mcp-evals` for deterministic evaluations of the two-query contract.
The runner creates temporary report artifacts, asks the user questions in
`cases.json`, and verifies actual statuses, source locations, denominator,
change categories, explicit errors, read-only annotations, and response sizes.
It writes `target/evals/mcp-eval-report.json` and fails if any check fails.

Cases cover selected-test gains, full regressions, zero gain, failed tests,
unverified artifacts, unavailable branch detail, missing reports, removed
execution tools, and invalid arguments. Unit and public transport tests cover
receipt conflicts, compacted history, malformed native records, pagination,
and HTTP/stdio consistency.

For a workflow trial, preserve pinned report artifacts and source/build identity,
ask for a few gaps, investigate with `rg` and bounded reads, run an existing
repository command, and compare explicit before/after reports. Record failures
before measurement as failures, not coverage gains. Compare equivalent tasks:
a targeted investigation and a whole-project inventory scan are different
workloads.

Measure query count, serialized visible bytes, errors/retries, latency, and
whether the result enabled a correct next step. Bytes and call counts are
proxies, not billed tokens. A deterministic evaluation is not a blinded live
model A/B trial. Do not claim token savings without account-level measurements.

The reproducible local trial is `python3 evals/selected-test-trial.py`. It runs
two existing parser tests with ordinary Cargo coverage commands, records producer
receipts, uses `rg`, and checks both selected-test comparison and incremental union. Repeating the
same batch must add zero coverage, keep the denominator fixed, and leave the
baseline file unchanged. Only the two selected tests are executed. See
[the measured evaluation](overhaul-2026-09-05.md) for results and limits.

`python3 evals/new-input-trial.py` tests genuinely new external inputs. It saves
a baseline from one LCOV case, authors another case afterward, and runs only that
new case against the same instrumented Coverage MCP executable. It checks output
assertions, verified marginal gain, a fixed denominator, zero gain on repetition,
and unchanged baseline artifacts. Native LLVM export is restricted to the measured
executable; Python assertions and input files are outside the coverage inventory.
Evidence and command timings go to
`target/incremental-verification/new-input-trial/result.json`. Run this trial
without concurrent coverage jobs: it clears shared raw profiles between cases.
It proves fixed-build external-input reuse, not reuse after changing Rust source
or a measured test harness.
