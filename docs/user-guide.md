# Find a gap and learn whether a test helped

Start from a specific coverage report or saved snapshot. Use a literal path or
function substring to narrow the result:

```sh
coverage-mcp gaps --measurement target/coverage.json --query decode --limit 3
coverage-mcp gaps --measurement target/coverage.json --query src/reader.rs --line 42
```

Default metric is regions when the report supplies them, otherwise lines.
Choose `--metric branches` for native LLVM true/false outcomes or `--metric
lines` for line coverage. An unavailable exact metric produces
`detail_unavailable`; it is not a claim of zero gaps.

LLVM observations are grouped by function. Other formats use source lines with
an unknown function name. Groups are ordered by missing observations and then
source location. Each group shows up to three spans and the omitted location
count. `group_count` includes groups outside the displayed page. Use the same
immutable report/snapshot and filters with `--offset` to continue. Prefer a
specific query over reading every page into a conversation. Scripts can read
the original complete artifact for exhaustive bookkeeping.

Search the returned path and function with `rg` and inspect nearby tests. A text
reference is a navigation clue, not proof that a particular test covers a region.
Use the repository's existing command to produce the next report.

## Interpret a comparison

```sh
coverage-mcp compare --baseline target/before.json --measurement target/after.json
```

`newly_observed` counts identical observations with zero baseline hits and
positive current hits. `not_observed` counts previously hit observations now
zero or absent. `added` and `removed` are differences in the observation
inventory; they are not verified gains. `remaining` is missing coverage in the
current measurement, not an accumulated union of every selected test.

Default scope is `selected_tests`. Unrelated absences are never regressions.
`--scope full` permits a regression claim only when both producer receipts
declare full scope and agree on measured source/build identity. Without those
receipts, results remain a limited coordinate comparison. Known source/build
conflicts, incompatible formats, unavailable detail, or unmeasured composite
entries make the comparison incomparable.

| Status | Meaning |
| --- | --- |
| `improved` | Compatible measured source/build; previously missing observations were hit |
| `no_gain` | Compatible comparison found no newly hit observations |
| `regression` | Verified full-scope comparison lost previously hit observations |
| `limited` | Coordinate changes available, measured source/build unverified |
| `incomparable` | Exact change cannot be claimed; read the reason |
| `tests_failed` | Current receipt says tests failed, regardless of coverage changes |

A zero-gain test can still protect behavior. Do not remove it based only on
coverage. Source state is separate from comparison compatibility: two reports
can match each other while both describe an older checkout. `changed_or_missing`
requires matching source or remeasurement before applying locations to current
code. Historical snapshots expose their recorded revision when available.

## Run only the new tests

Keep the existing baseline report and its `.context.json` receipt, if present.
Use the repository runner's case selector
to run only the new tests and export their coverage to a separate report.
When the measured source and instrumented build remain compatible, Coverage MCP
can reuse the baseline without running its tests again:

```sh
coverage-mcp compare --scope incremental \
  --baseline target/base.json --measurement target/new-tests.json
```

For example, a baseline covers 800 of 1,000 regions. The new tests hit 50 regions,
20 already in the baseline. The result is **30 new regions**, **830 / 1,000
combined**, **+3 percentage points**, and **170 remaining gaps**. Baseline hits
absent from the new run stay covered; a selected run cannot establish regressions.

To measure another batch against the baseline plus an earlier accepted batch:

```sh
coverage-mcp compare --scope incremental --baseline target/base.json \
  --previous target/new-tests.json --measurement target/next-tests.json
```

Repeat `--previous` for up to 16 batches. Repeated hits count once. Keep reports
in separate files; these commands never overwrite a baseline or update history.
For a longer chain, merge compatible raw coverage with the provider's native
merge command and retain its producer evidence, or establish a new baseline.

The dashboard defaults to **Incremental · baseline + new tests**. It separates
the batch-only report from baseline + earlier batches, added coverage, and the
combined total. **Keep this batch for the next comparison** adds a verified,
improving report to the earlier-batch selector for the current page; the original
baseline stays unchanged. Full-suite regression remains **not checked**.
Unverified estimates are labeled; failed or incompatible evidence has no
combined total. Source filters affect displayed locations, not these totals.

Keep the measured production source, toolchain, features, target, and coverage
mapping fixed. New external test inputs can use the same instrumented build.
If adding tests changes measured source or instrumentation, establish a fresh
compatible baseline. Exclude test implementation files consistently when the
intended denominator is production code. Missing reports, changed inventory,
or summary/detail disagreement cannot be combined. Receipts below establish
identity and test status; without them, results are coordinate estimates.

## Optional producer receipts

A runner/CI producer may write `REPORT.context.json` automatically. This is
optional metadata, not a prerequisite for ordinary investigation:

```json
{
  "report_sha256": "SHA-256 of the exact report bytes",
  "source_hashes": {"src/reader.rs": "SHA-256 of measured source bytes"},
  "build_id": "identity of toolchain, target, features and coverage mapping",
  "scope": "selected_tests",
  "test_status": "passed"
}
```

The producer must capture measured inputs before execution, ensure they did not
change during the run, and bind the output artifact to them. Hashing today's
source after finding an old report cannot establish provenance. A build ID
must change when relevant build configuration changes. The reader checks the
report digest, compares source hashes to the checkout, and requires complete
matching per-file identities for verified comparisons. Receipts are trusted
producer assertions, not signed attestations. `test_status` accepts `passed`,
`failed`, or `unknown`; `scope` accepts `full` or `selected_tests`.

Ordinary saved 0.15 snapshots lack this complete identity and remain unverified.
Composite snapshots retain inventory identities and unknown/missing component
states. Missing evidence stays in the denominator and appears with a reason;
it does not become an ordinary uncovered branch.

## Upgrade from 0.15

Update the runtime and client guidance together, then start a new task/MCP
session. The two tools are `coverage_gaps` and `coverage_compare`; all old tool
names and resources were removed without aliases. Use the existing test runner
for execution, local source search for investigation, and explicit reports for
comparison. HTTP ingest remains an optional archive operation.

Existing ordinary/composite snapshots and compacted detail remain readable.
Queued/running legacy jobs become interrupted on storage open and are never
restarted. Do not delete databases to upgrade. See [runtime recovery](operations.md#recovery)
for daemon replacement and backup requirements.
