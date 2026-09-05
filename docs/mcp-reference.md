# MCP reference — schema 18 / 0.16

Both transports call `mcp::dispatch_json_rpc`. Stdio uses newline-delimited
JSON-RPC through `coverage-mcp connect --repo PATH`; HTTP accepts POST `/mcp`
and `/mcp/`. Initialization advertises protocol `2025-03-26`, server version,
workflow instructions, and the tools capability. `tools/list` includes a
contract object containing schema revision, tool count, and SHA-256 of the
serialized tool list. Both tools are read-only, idempotent, non-destructive,
and local-world queries. Neither runs tests or imports reports.

## `coverage_gaps`

| Input | Type/default | Meaning |
| --- | --- | --- |
| `measurement` | optional string | Report path inside selected repo, or immutable ordinary/composite snapshot ID |
| `suite` | optional string | Saved suite selector; mismatch with ordinary snapshot is rejected |
| `query` | optional string, 1–256 UTF-8 bytes | Case-insensitive literal path/raw-function substring |
| `line` | optional positive integer | Restrict to observations spanning this one-based line |
| `metric` | `auto` | `auto`, `regions`, `branches`, or `lines` |
| `limit` | integer 5, range 1–20 | Maximum displayed groups |
| `offset` | nonnegative integer 0 | Continue stable filtered group ordering |

Omitting measurement selects the latest ordinary snapshot in a unique saved
suite. Multiple suites or an unscoped large history require an explicit
selector. It never chooses a baseline or silently mixes suites. Prefer an
artifact path to history discovery. A filename without an extension is accepted
when it exists; use `./name` to distinguish a report from a saved ID.

The `data` object contains `status` (`measured`, `incomplete`, or
`detail_unavailable`), `measurement`, `ranking`, `groups`, `group_count`,
`next_offset` (integer or null), `warnings` (up to three, each 240 characters),
`warnings_omitted`, and `next_action`. Empty groups may mean the filter matched
no missing observations, not that the complete project has full coverage.
An incomplete composite retains missing/unmeasured inventory entries.

## `coverage_compare`

Required `measurement` and `baseline` strings select two explicit report paths
or saved IDs. Optional `query`, `metric`, and `limit` match `coverage_gaps`.
Optional `scope` is `selected_tests` (default), `full`, or `incremental`. There is no implicit
baseline and no run polling. Unknown inputs, including `suite`, `line`, and
`offset`, are rejected for comparison.

`data` contains `status` (`improved`, `no_gain`, `regression`, `limited`,
`incomparable`, `tests_failed`), `measurement`, `baseline`, effective `scope`,
`reason`, `changes`, `gained`, `gained_group_count`, `remaining`, and
`remaining_group_count`. `changes` is null when incomparable; otherwise it
contains integer `newly_observed`, `not_observed`, `added`, `removed`, and
`regressed` (integer for verified full scope, otherwise null). Group counts
include omitted groups; each array is bounded by limit and the response budget.
Filter more narrowly to inspect a particular change.

Selected-test absences never become regressions. Both receipts must declare
full scope and matching source/build identities before `full` is effective.
Without identities, coordinates can be compared with status `limited`.
Known conflicting identities, different formats, unavailable metric detail,
or unmeasured composite records produce `incomparable`. Failed current tests
have status precedence while the reason and nullable changes retain comparison
limitations. [The user guide](user-guide.md) defines the observation categories
and receipt producer requirements.

### Incremental scope

Set `scope: "incremental"` with a fixed `baseline` and the report from only the
new tests as `measurement`. Optional `previous` is an array of up to 16 earlier
accepted report paths or saved IDs; it is rejected in other scopes. Empty means
compare the batch directly to the original baseline. `auto` selects the
baseline's metric. This query reads artifacts; execute selected tests with the
repository runner and keep each report in a separate file.

The reader unions the baseline and previous batches, then counts this batch's
marginal contribution. Repeated locations count once. LLVM generic instances
at the same source location are coalesced, with branch arms kept separate.
Exact observation counts must match each report's totals. A batch may contain
only part of the baseline inventory; absent baseline entries remain unchanged.
Observations outside the baseline inventory, missing detail, known source/build
conflicts, or changed/missing checkout source prevent a union. Rebaseline after
production source or instrumentation changes. Missing artifacts are errors.

The result retains `measurement` (batch-only totals) and `baseline` (original
report totals), sets `scope: "incremental"` and `regression_checked: false`, and
adds `incremental`:

- `metric`, `evidence` (`verified` or `unverified`), `baseline_scope`
  (`full`, `selected_tests`, or `unknown`), and `previous_batches`.
- `accepted`: baseline plus previous batches; `combined`: accepted plus current
  batch. Both contain `covered`, `total`, and `missing` with the fixed baseline
  denominator.
- `batch_covered`, `newly_covered`, `already_covered`,
  `gain_percentage_points`, and `remaining_gap_reduction_percent` (0 if no gaps
  remained). These totals always cover the whole baseline inventory.

`query` and `limit` filter only the displayed gained/remaining groups in this
mode. `remaining` is the union's gaps. `changes.newly_observed` is the marginal
count; `added` and `removed` are 0; `not_observed` and `regressed` are null.
There is no full-suite regression claim or implicit baseline update.

Matching build IDs, source hashes matching the checkout, and passing receipts
for the baseline and every batch yield `improved` or `no_gain`. Missing identity
or unknown test status yields `limited` with an explicitly unverified estimate.
A failed baseline or batch yields `tests_failed`; incompatible evidence yields
`incomparable`. In both cases `incremental` and `changes` are null and all groups
are empty. An earlier-batch error identifies its report. No partial union is
returned. Ordinary archived snapshots lack receipts and remain unverified.

## Shared result shapes and bounds

Successful tool calls use `result.structuredContent`:

```json
{
  "context": {
    "repo_key": "repository identity",
    "checkout_path": "/selected/repository",
    "suite": null,
    "schema_revision": 18
  },
  "data": {},
  "page": null
}
```

`content` contains one short text pointer to structuredContent, never a second
copy of the evidence. `isError` is false. The envelope is at most 12 KiB;
groups are removed from the end to fit, with counts and continuation adjusted.
Unusually long fixed metadata that cannot fit produces an explicit error.
The small JSON-RPC wrapper/tool metadata is outside this evidence cap.

A measurement has `ref`, `format`, `coverage` (`metric`, `covered`, `total`,
`missing`), `source` (`unverified`, `matches_receipt`, `changed_or_missing`),
`recorded_revision` (string/null), `build_id` (string/null), and `tests`
(`passed`, `failed`, `unknown`). Coverage totals come from the report;
grouped observations do not redefine them.

A group has `path`, `function` (string/null, display capped at 160 characters),
`count`, `unmeasured_reason` (string/null), `locations`, and
`locations_omitted`. A location has `span` `[start_line,start_column,end_line,
end_column]` and `arm` (`true`, `false`, or null). LLVM coordinates retain
provider semantics. Line-only/composite locations use column 0 (unknown).
The display contains at most three locations per group. Grouping merges equal
native identities using the greatest observed hit count. Gaps have zero hits;
unmeasured composite rows remain separate evidence with a reason.

Reports are capped at 64 MiB; optional context receipts at 1 MiB. Paths must
resolve inside the repository. Measurement references accept 1–4096 bytes.
Parsing and receipt hashing use the same captured report bytes. Source/build receipts are
optional and have a maximum 256-byte build ID. No source files or logs are
returned by either tool.

## Errors and remaining protocol methods

Malformed arguments, missing paths/IDs, cross-repository references, ambiguous
history, invalid reports/receipts, filesystem failures, database failures, and
timeouts become tool results with `isError:true`, one text error, and no
structured evidence. Fix the stated cause rather than increasing display limits.
JSON-RPC dispatch/selection errors use code `-32000` with the error message;
HTTP boundary validation also has HTTP status semantics in [the HTTP reference](http-api.md).

`ping` returns `{}`. `resources/list` and `resources/templates/list` return empty
lists. `resources/read` always fails: no resources or resource templates are
advertised. Notifications return no JSON-RPC response (HTTP 202). Unknown
methods error. Every former 0.15 tool name errors; see [migration](user-guide.md#upgrade-from-015).
