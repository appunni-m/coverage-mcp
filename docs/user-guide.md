# Use Coverage MCP for day-to-day coverage work

This guide explains the questions Coverage MCP can answer and how to interpret
the result. If you have not installed the testing plugin or created a first
snapshot, begin with [Getting started](getting-started.md).

## The five terms you need

You can use Coverage MCP without learning its storage or protocol design. These
five terms are enough for normal work:

1. **Approved command** — the exact test-and-coverage command you allowed
   Coverage MCP to run. Its working directory, shell, and expected reports are
   recorded with the approval.
2. **Run** — one durable execution of an approved command. A run has its own ID,
   state, output summary, and artifact results.
3. **Snapshot** — an immutable, normalized coverage measurement created from a
   valid report. A passing test run without a valid report is not a snapshot.
4. **Baseline** — the older compatible snapshot used for a comparison. Coverage
   MCP records why a baseline was selected and refuses incompatible evidence.
5. **Claim status** — whether the requested conclusion is `supported`,
   `limited`, `not_measured`, `stale`, or `invalid`. Read the accompanying
   reasons whenever the status is not `supported`.

The usual flow is:

```text
approve once → run when needed → ingest report → review a bounded answer
```

## Ask for the outcome, not a tool call

The marketplace's `coverage-review` workflow chooses the smallest Coverage MCP
projection for your request. State the result you want in plain language.

| What you want to know | Example request |
| --- | --- |
| Best next test work | “Show the three highest-priority uncovered regions and explain why they rank first.” |
| Changed-code coverage | “Which executable lines changed in this worktree, and which of them are still uncovered?” |
| Coverage movement | “Compare this measurement with its parent commit and explain gains, regressions, and any limited claims.” |
| Recent history | “Show the recent line, branch, function, and region trend without returning raw records.” |
| Source around one gap | “Show bounded source context around the top uncovered region.” |
| Exact verification data | “Return the audit representation for this one file and snapshot.” |
| Possible duplicate tests | “Find named tests with exactly equal coverage and give me review candidates; do not remove anything.” |

The agent normally starts from project context, reuses a fresh matching run when
possible, and keeps response limits small. Ask for a wider result only after the
first bounded answer shows that more evidence is needed.

## Understand the available metrics

A report may provide up to four different metric families:

- **Lines** — executable source lines that ran at least once.
- **Branches** — alternative control-flow outcomes that ran.
- **Functions** — instrumented functions that ran.
- **Regions** — finer-grained compiler or producer regions, which may separate
  several expressions on one line.

Do not compare unlike metric families. A line percentage is not a branch or
region percentage, and a report format may not provide every family. Coverage
MCP keeps unavailable data explicit rather than turning it into zero.

The repository and suite also matter. Two percentages are comparable only when
their reports, repository identity, and baseline rules are compatible.

## Choose the right comparison

### Review changed code

Use a change review when you want to know how the current work relates to an
older point. Common baselines are:

- the parent commit;
- a named Git ref;
- the previous compatible snapshot;
- a durable worktree base;
- an exact snapshot ID.

An exact snapshot is the least ambiguous choice when the same baseline will be
reused across many smaller cases. If no baseline is appropriate, request a
no-baseline review and avoid language such as “improved” or “regressed.”

### Run one smaller incremental case

An incremental run answers: “What extra coverage did this selected case add to
a fixed full-suite result?” It does not pretend that the smaller case is a
replacement for the full suite.

The workflow needs:

- an existing approved command that accepts the intended case arguments;
- an explicit immutable baseline snapshot;
- the case-specific arguments;
- a distinct idempotency key for a materially different case.

Coverage MCP forms a deduplicated union of the baseline and selected case. That
union is the primary measurement. Evidence present in the baseline but absent
from a selected subset is `not_observed`, not a regression. A genuine regression
claim is available only when the current measurement is marked as a complete
snapshot.

This distinction prevents a one-test run from appearing to erase everything
covered by the full suite.

### Compare existing snapshots without running tests

If both measurements already exist, ask for a stored incremental comparison.
Supply the exact current and baseline IDs. This operation reads stored detail;
it does not run tests, invoke another coverage runner, or reparse the original
reports.

## Use composite coverage only when you need one cross-language denominator

A composite snapshot combines production coverage from Rust or WGSL, Python,
and JavaScript into one canonical region inventory. It is useful for a product
whose runtime spans several languages or generated variants and whose team has
defined one authoritative production-source inventory.

Composite setup is advanced. Every required package or backend variant must
produce compatible evidence, and shared source is deduplicated only through an
explicit logical-source alias. Missing, stale, malformed, unsupported, or
source-mismatched evidence leaves the result incomplete; it never silently
shrinks the denominator.

A composite result includes:

- exact covered and total canonical regions;
- one combined percentage;
- Rust (including WGSL), Python, and JavaScript summaries;
- blocking reasons and remediation categories;
- the inventory and snapshot provenance used for the result.

See [Composite coverage in the MCP reference](mcp-reference.md#composite-coverage)
for descriptor and comparison details.

## Interpret compact results

Coverage MCP defaults to compact output so a large report does not consume the
agent's context. Paths are grouped and line ranges use short status symbols.
The response includes a legend; the common changed-code symbols are:

| Symbol | Meaning |
| --- | --- |
| `+` | Added executable line is covered |
| `!` | Added executable line is uncovered |
| `~` | Changed line has a branch gap |
| `.` | Added line is not executable |
| `?` | Coverage is unavailable or unmeasured |

Composite and region comparisons use a separate legend:

| Symbol | Meaning |
| --- | --- |
| `+` | Region is new or coverage improved |
| `!` | Previously measured region regressed |
| `-` | Region was removed |
| `~` | Region exists on both sides with changed coverage |

Ask for the readable `review` representation when grouped prose is more useful
than compressed ranges. Reserve `audit` for exact records needed for
verification or export.

## Distinguish test, report, and comparison failures

These outcomes mean different things and should not be collapsed into one
“coverage failed” message:

| Outcome | Meaning |
| --- | --- |
| Test command failed | The approved process returned a failure or was interrupted. |
| Artifact missing or stale | The command did not create or update the declared report. |
| Parser failure | A report existed but did not match the declared supported format. |
| Coverage regression | A compatible complete measurement lost previously covered evidence. |
| Missing baseline | The requested comparison has no valid older measurement. |
| Not measured | The available evidence cannot support a coverage claim. |

For a failed run, ask for a bounded literal search of its retained stdout or
stderr. Do not request the entire log unless the small search cannot explain
the failure.

## Import a report produced elsewhere

Use an import when a coverage report was created outside Coverage MCP's managed
runner—for example, by CI or a historical local command. Tell the agent the
repository-relative report path, format, suite, branch, commit, and comparison
base when known.

An import records external evidence. It does not claim that Coverage MCP ran
the tests, and it does not infer missing Git lineage from the file contents.
After import, use the returned snapshot ID in a normal review.

Coverage reports are limited to 64 MiB. Numeric fields must be valid for the
declared format; malformed values are rejected instead of being changed to
zero.

## Review duplicate-coverage candidates carefully

For LCOV reports with named `TN:` records, Coverage MCP can group tests whose
complete normalized covered line, branch, and function observation sets are
identical. The result is bounded and paginated, so large test inventories do
not enter the agent context.

Coverage equality is not test equivalence. The comparison does not inspect
assertions, inputs, side effects, dependencies, or test source. Treat every
group as a human review candidate; Coverage MCP never deletes tests.

Formats without named per-test observations return `status="unavailable"`.
That does not mean that no duplicates exist.

## Use the dashboard for exploration

The dashboard at <http://127.0.0.1:59471/> is useful when you want to browse
several snapshots or files interactively. It includes:

- project and snapshot selection;
- line, branch, function, and region trends;
- changed and incremental coverage;
- bounded file and source detail;
- managed-run state and retained artifacts;
- composite production coverage;
- storage-compaction status.

The dashboard follows the newest snapshot until you explicitly pin a current
selection. It refreshes only while the tab is in the foreground and never runs
tests on its own.

For exact tool inputs and return envelopes, continue to the
[MCP reference](mcp-reference.md). For local files, limits, and recovery, see
the [operations guide](operations.md).
