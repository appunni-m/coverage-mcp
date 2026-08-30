# MCP reference

This document is for MCP client authors, advanced users, and maintainers who
need the exact Coverage MCP workflow and public tool surface. Most users should
start with the [user guide](user-guide.md) and let the
[`coverage-review` workflow](https://github.com/appunni-m/codegen-marketplace/blob/main/plugins/testing/skills/coverage-review/SKILL.md)
choose tools and limits.

The executable is authoritative. Read `initialize` and `tools/list` from the
installed version before generating or validating requests. This source
revision advertises schema revision 16 and eight tools.

## Connect an MCP client

Codex users should install `testing@codegen-marketplace`; its required MCP
server launches the compatible connector automatically. A generic MCP client
can launch the published binary directly:

```json
{
  "mcpServers": {
    "coverage-mcp": {
      "command": "coverage-mcp",
      "args": ["connect", "--repo", "/absolute/path/to/repository"]
    }
  }
}
```

`connect` and its `stdio` alias read one JSON-RPC object per line from standard
input and write one response per line to standard output. Diagnostics use
standard error. The connector starts or reuses the shared loopback daemon and
forwards the selected repository in `x-coverage-mcp-repo`.

Direct HTTP clients may send the same JSON-RPC requests to
`POST http://127.0.0.1:59471/mcp/`. Both transports use
`mcp::dispatch_json_rpc` and therefore share tool schemas, validation, results,
notifications, resources, and errors.

## Required workflow

The safe normal sequence is:

1. Call `project_context` with compact defaults.
2. Reuse a fresh matching snapshot or an already active matching run when one
   exists.
3. If no approved command matches, show the human the exact command, working
   directory, shell, artifacts, report formats, and suites. Call
   `register_test_command` only after explicit approval.
4. Call `run_test` with `wait=false`, `reuse_if_unchanged=true`, and a stable
   idempotency key.
5. Save `run_id`, wait at least the returned `poll_after_ms`, then call
   `run_review` with that exact ID.
6. After successful ingestion, call `coverage_review` with the smallest task
   that answers the question. An incremental managed run already includes its
   automatic review; do not start a second comparison for the same run.
7. Carry returned run, snapshot, baseline, file, and source identifiers into
   follow-up requests. Never infer them from labels or raw reports.

Use `coverage_import` only for a report created outside the managed runner.
Use `cancel_run` only when the user no longer wants the run. Use
`find_duplicate_coverage_tests` only to identify review candidates.

## Successful result envelope

Every successful tool returns this shape:

```json
{
  "context": {
    "repo_key": "…",
    "checkout_path": "…",
    "suite": "…",
    "schema_revision": 16
  },
  "data": {},
  "page": null
}
```

For JSON-RPC `tools/call`, this envelope is in `structuredContent`. The text
`content` contains only a short compatibility hint and does not repeat the
result. Clients should use `structuredContent`.

`page` contains bounded collection and cursor information where applicable.
Cursors are opaque and query-scoped. Reuse them only with the same operation
and selectors.

## Response budgets and representations

Agent-facing output is compact by default:

- `max_words` accepts 50–5000 and defaults to 600;
- tools that expose `max_bytes` accept 1000–2000000 and default to 12000;
- compact reviews default to three files, five regions, and ten affected test
  IDs;
- compact history defaults to two detailed points;
- compact insight returns at most three grouped ranges for each target.

`representation` has three modes:

| Mode | Use |
| --- | --- |
| `compact` | Default token-efficient paths, arrays, and ranges |
| `review` | Readable grouped records for a human-facing explanation |
| `audit` | Exact records for deliberate verification or export |

Set `detailed=true` only when approval fields, exact artifact paths, or full
chronology are required. It does not remove hard collection caps.

## Tool summary

| Tool | Side effect | Purpose |
| --- | --- | --- |
| `project_context` | Read-only | Discover repository identity, freshness, commands, active runs, and the latest run |
| `register_test_command` | Local write | Store one immutable human-approved command and its artifacts |
| `run_test` | Command execution | Queue or reuse one approved managed run |
| `run_review` | Read-only | Read explicit run state or bounded log matches |
| `cancel_run` | Command execution | Request cancellation of one durable run |
| `coverage_import` | Local write | Ingest an external or historical report |
| `coverage_review` | Read-only | Return one bounded task-oriented coverage analysis |
| `find_duplicate_coverage_tests` | Read-only | Page exact named-test coverage-equivalence groups |

Unknown tools and unknown input fields are errors.

## `project_context`

Call this first.

Inputs:

| Field | Required | Meaning |
| --- | --- | --- |
| `cursor` | No | Opaque continuation for approved commands |
| `max_words` | No | Complete `data` word budget |
| `detailed` | No | Include audit-level command and provenance fields |

The compact result includes project identity, freshness and metrics, up to
eight approved command summaries, at most ten active runs, and `latest_run`.
Use `data.latest_run.id` as an explicit input to `run_review`; there is no
implicit latest-run selection in that tool.

Project-context paging has extra accounting fields:

- `page.reserved_words` — words reserved for the fixed project/latest-run/
  active-run summary;
- `page.word_count` — words in the returned command page;
- `page.response_word_count` — their combined count;
- `page.next_cursor` — continuation for commands only;
- `data.active_runs_truncated` — whether the ten-run hard cap omitted entries.

`detailed=true` does not lift the command-page or active-run cap.

## `register_test_command`

Register an exact command only after the human has approved every execution and
artifact field.

Required inputs:

| Field | Meaning |
| --- | --- |
| `name` | Stable command name |
| `command` | Exact approved shell command |
| `human_approved` | Must be `true` after approval |
| `approved_by` | Human or accountable approver |
| `approval_note` | Specific approved purpose and scope |

Optional inputs:

| Field | Meaning |
| --- | --- |
| `cwd` | Checkout directory for the command |
| `shell` | Shell executable; defaults at the service boundary |
| `artifact_paths` | Artifact specifications described below |
| `max_words` | Response word budget |
| `detailed` | Return expanded command fields |

An ordinary coverage artifact describes a repository-relative `path`, whether
it is `required`, its `coverage_format`, its logical `suite`, and optional
`detail_retention` (`compactable` or `incremental_base`). Non-coverage artifacts
may also be retained as run evidence.

Use one registration for a command whose approved argument boundary can serve
several cases. A single `{{args}}` placeholder is replaced with server-quoted
case arguments; if no placeholder exists, quoted arguments are appended. Do
not register a new command for every filter.

The result returns the immutable registration ID/name. Pass either as
`run_test.command_ref`.

## `run_test`

Required input:

| Field | Meaning |
| --- | --- |
| `command_ref` | Registered command UUID or name |

Optional inputs:

| Field | Meaning |
| --- | --- |
| `arguments` | Up to 64 case strings, each at most 512 characters and 8192 bytes in total |
| `timeout_seconds` | Maximum command duration |
| `idempotency_key` | Stable caller key for safe resubmission |
| `wait` | Whether to wait for terminal state; prefer `false` |
| `reuse_if_unchanged` | Reuse the latest compatible terminal run; defaults to `true` |
| `execution` | `{mode, identity?, label?}` where mode is `full`, `incremental`, or `custom` |
| `baseline` | Required for incremental mode; explicit ordinary or composite ID |
| `max_words` | Response word budget |

An incremental baseline has exactly one of these shapes:

```json
{"kind":"explicit","snapshot_id":"<ordinary-base-id>"}
```

```json
{"kind":"explicit","composite_snapshot_id":"<composite-base-id>"}
```

The server fingerprints mode, arguments, and baseline. `execution.identity` is
an optional caller label, not a caller-generated hash. Use a new idempotency key
when the effective case changes.

The result contains durable run state, including `id`, `status`, `terminal`,
persisted execution context, artifact/coverage ingestion, ordinary
`snapshot_ids`, and an optional `composite_snapshot_id`. A terminal incremental
run also contains `incremental_review` with `status` equal to `measured`,
`not_measured`, or `pending` while the run is active.

If a run produces several ordinary coverage artifacts, their snapshot IDs are
one selected measurement set. The automatic review unions all of them with the
explicit baseline.

## `run_review`

Required input:

| Field | Meaning |
| --- | --- |
| `run_id` | Exact durable run UUID |

Optional inputs:

| Field | Meaning |
| --- | --- |
| `view` | `status` (default) or `logs` |
| `query` | One to twenty non-blank literal log search terms |
| `stream` | `stdout`, `stderr`, or `both` |
| `context_lines` | 0–20, default 3 |
| `max_matches` | 1–50, default 5 |
| `case_sensitive` | Preserve case when matching |
| `max_words` | Response word budget |
| `max_bytes` | Complete response byte budget |

`status` reads current durable state and terminal ingestion evidence without
starting or advancing work. For an incremental run it returns the automatic
review again. `logs` returns only bounded literal matches.

## `cancel_run`

Inputs are `run_id` (required), plus optional `max_words` and `detailed`.
Cancellation reaches the managed process group. Use it only when the user no
longer wants the run, not as a polling mechanism.

The result reports the run's durable cancellation state. A process that already
finished may remain terminal in its existing state.

## `coverage_import`

Required input:

| Field | Meaning |
| --- | --- |
| `report_path` | Repository-relative external coverage artifact |

Optional inputs:

| Field | Meaning |
| --- | --- |
| `format` | `auto`, `lcov`, `coverage.py`, `cobertura`, `jacoco`, `istanbul`, `go`, or `llvm` |
| `suite` | Logical suite name |
| `branch` | Measured branch |
| `commit_sha` | Measured commit |
| `base_ref` | Known comparison base ref |
| `execution` | `{mode, identity, label?}` for external case provenance |
| `detail_retention` | `compactable` (default) or `incremental_base` |
| `max_words` | Response word budget |
| `max_bytes` | Complete response byte budget |

The import validates a report up to 64 MiB and returns ingestion state plus its
snapshot ID. It does not claim that `run_test` created the file. Follow with
`coverage_review` and carry the returned ID explicitly.

## `coverage_review`

`task` is required. Other inputs are optional objects:

| Field | Shape and purpose |
| --- | --- |
| `task` | `change`, `incremental`, `history`, `insight`, `source`, `audit`, or `all` |
| `measurement` | Current `snapshot_id`, `composite_snapshot_id`, or `run_id`, with optional suite, branch, and file selector |
| `baseline` | Baseline kind and its required snapshot, composite, worktree, or ref selector |
| `source` | Up to ten `{file_path,start,end}` ranges, optional inclusion, and 0–20 context lines |
| `history` | 1–5 detailed snapshots and a 2–50 point summary window |
| `limits` | File, region, affected-test, source-line, word, and byte limits |
| `representation` | `compact` (default), `review`, or `audit` |

Every result includes `focus`, `representation`, `claim_status`, `reasons`, a
canonical `measurement`, and a `baseline` when one is valid. Task-specific
objects include `change`, `incremental`, `history`, `insight`, `source`, or
exact audit records. `next_action` identifies a bounded follow-up when one is
available.

### Task selection

| Task | Returns |
| --- | --- |
| `change` | Changed executable lines, grouped gains or gaps, and baseline evidence |
| `incremental` | Explicit baseline-union-current measurement plus a separate scope-aware snapshot diagnostic |
| `history` | Recent compatible points and an aggregate window |
| `insight` | Ranked uncovered targets without a raw line dump |
| `source` | Bounded checkout source for exact grouped ranges |
| `audit` | Exact records for deliberate verification |
| `all` | Bounded change, history, and insight sections |

### Baseline kinds

Normal change review accepts:

- `worktree_base` with a durable `worktree_id`;
- `parent_commit`;
- `ref` with a Git `ref`;
- `previous_snapshot`;
- `explicit` with a snapshot ID;
- `none` when no change claim is wanted.

Incremental review accepts only `explicit`, with exactly one ordinary or
composite ID matching the measurement type. It never guesses the previous
snapshot.

### Incremental coverage

Example stored ordinary comparison:

```json
{
  "task": "incremental",
  "measurement": {"snapshot_id": "<case-snapshot-id>"},
  "baseline": {"kind": "explicit", "snapshot_id": "<fixed-base-id>"},
  "representation": "compact",
  "limits": {
    "max_files": 3,
    "max_regions": 5,
    "max_test_ids": 10,
    "max_words": 600,
    "max_bytes": 12000
  }
}
```

The primary `data.measurement` is the deduplicated union of the baseline and all
selected ordinary snapshots. `data.incremental` contains:

- the selected snapshot IDs and first-artifact provenance;
- signed line, branch, function, and region deltas;
- newly covered, not-observed, regressed, hit-count-only, added, and removed
  categories;
- merge exactness and fallback provenance;
- a separate replacement-style `diff`;
- bounded opaque named-test attribution when available;
- `relational` or `compacted_payload` detail sources.

For `measurement_scope.kind="selected_subset"`, missing baseline identities are
`not_observed`; `regressed` is zero and the diagnostic claim is limited. Only
`complete_snapshot` supports a genuine regression claim. Consumers must use the
server-provided union counts and rates instead of rebuilding a percentage from
the selected run.

Ordinary incremental measurements require the same repository and normalized
coverage format. Suite, branch, commit, command, working directory, and case
label are provenance, not test identity.

### Composite coverage

Register one required inventory artifact and one required coverage descriptor
for every component/package variant. A coverage descriptor has this shape:

```json
{
  "path": "target/python.json",
  "required": true,
  "coverage_format": "coveragepy",
  "composite": {
    "component_id": "python",
    "package_variant": "cpython",
    "language": "python",
    "format": "coveragepy",
    "inventory_artifact": "target/inventory.json",
    "role": "coverage",
    "logical_source_aliases": [
      {"path": "pkg/runtime.py", "logical_source_id": "python:pkg/runtime.py"}
    ],
    "toolchain_versions": {"coverage.py": "<recorded-version>"}
  }
}
```

The `coverage-mcp-inventory-v1` inventory owns the denominator. Its production
entries declare logical source IDs, repository-relative paths, language,
package, required variants, expected formats, source hashes, source revision,
and canonical regions. A producer that emits its own canonical spans uses
`coverage-mcp-regions-v1`.

Logical-source aliases are the only deduplication mechanism. Matching filenames
or bytes do not merge evidence. Missing, stale, malformed, unsupported, or
source-mismatched evidence leaves the composite incomplete and never reduces
the denominator.

Example stored composite comparison:

```json
{
  "task": "incremental",
  "measurement": {"composite_snapshot_id": "<case-composite-id>"},
  "baseline": {
    "kind": "explicit",
    "composite_snapshot_id": "<fixed-composite-id>"
  },
  "representation": "compact",
  "limits": {
    "max_regions": 20,
    "max_test_ids": 10,
    "max_words": 600,
    "max_bytes": 12000
  }
}
```

The composite snapshots must match repository, mapping version, and inventory
hash. Ordinary and composite selectors cannot be mixed. The result exposes one
exact canonical-region score plus Rust/WGSL, Python, and JavaScript summaries,
provenance, blocking reasons, remediation categories, and attribution status.

### Compact range symbols

Changed-code compact ranges use:

| Symbol | Meaning |
| --- | --- |
| `+` | Covered added executable line |
| `!` | Uncovered added executable line |
| `~` | Changed line with a branch gap |
| `.` | Non-executable added line |
| `?` | Unavailable or unmeasured coverage |

Region ranges use:

| Symbol | Meaning |
| --- | --- |
| `+` | Improved or newly measured region |
| `!` | Regressed measured region |
| `-` | Removed region |
| `~` | Region present on both sides with changed coverage |

Each response carries the applicable legend. In compact file metrics, `p` is
the path and `l`, `b`, `f`, and `r` are line, branch, function, and region
arrays whose positions are defined once by `file_legend`.

## `find_duplicate_coverage_tests`

All inputs are optional:

| Field | Default | Meaning |
| --- | ---: | --- |
| `snapshot_id` | Latest matching snapshot | Exact immutable snapshot |
| `suite` | None | Suite used when selecting the latest snapshot |
| `cursor` | None | Opaque continuation |
| `max_groups` | `10` | 1–500 groups in this page |
| `max_tests_per_group` | `10` | 2–1000 returned names per group |
| `max_words` | `600` | Complete word budget |
| `max_bytes` | `12000` | Complete byte budget |

The tool compares each named test's complete normalized set of
`{kind,file_path,line_number,region_key}` observations. Hit counts are ignored.
The result includes full group/test counts, returned names, truncation state,
and `page.next_cursor`.

Current LCOV `TN:` records provide named observations. Other formats return
`data.status="unavailable"`, not an empty no-duplicates claim. Coverage equality
does not compare test logic and never authorizes deletion.

## Resources

The public resource inventory is:

- `coverage://context` — compact current project, policy, command, and run
  context;
- `coverage://snapshot/{snapshot_id}/summary` — one compact immutable ordinary
  snapshot;
- `coverage://composite/{composite_snapshot_id}/summary` — one bounded
  immutable composite summary with inventory provenance.

Resources do not replace tools for execution or task-oriented review.

## JSON-RPC methods and errors

The server handles `initialize`, `tools/list`, `tools/call`, `resources/list`,
`resources/templates/list`, and `resources/read`. Methods beginning with
`notifications/` receive no response.

Every present argument is type-checked. Omitted optional fields receive the
documented default; a present wrong type is never treated as omitted. Unknown
top-level fields and unknown fields inside structured selectors are validation
errors.

MCP failures use a JSON-RPC error object with code `-32000` and a stable
human-readable message. Important classes are:

| Class | Client action |
| --- | --- |
| Validation | Correct the type, range, selector, path, cursor, or budget; do not retry unchanged |
| Not found | Refresh context or use an existing explicit ID |
| Busy | Wait with backoff or stop the competing owner |
| Timeout | Reduce work or retry a safe read with backoff |
| Runtime/I/O | Inspect run state, daemon health, or bounded logs before retrying |
| Storage/pool | Preserve database evidence and follow operator recovery; never delete a WAL or lock blindly |

The stdio bridge replays a request once only after connection refusal proves
that the daemon did not receive it. After an ambiguous timeout or transport
interruption, it restores daemon health for later work but does not replay a
potentially mutating call. Reuse the same idempotency key when the caller
chooses to retry a run submission.

See the [operations guide](operations.md#troubleshooting-by-symptom) for startup
and lock diagnostics.
