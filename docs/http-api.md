# Loopback HTTP API reference

Coverage MCP exposes a local JSON API for the embedded dashboard, operator
scripts, and MCP clients that do not use the native stdio connector. This API
is intended for processes running as the same local user. It is not designed
for public network deployment.

The default base URL is `http://127.0.0.1:59471`. Start the daemon with:

```sh
coverage-mcp serve
```

Check it with:

```sh
curl --fail http://127.0.0.1:59471/health
```

The server rejects non-loopback bind hosts and untrusted `Host` headers. It
does not provide HTTP authentication or CORS access. Do not expose the port
through a proxy without a separate authentication, authorization, and network
isolation design.

## Response conventions

Most successful project routes return:

```json
{
  "context": {
    "repo_key": "…",
    "checkout_path": "…",
    "suite": null,
    "schema_revision": 16
  },
  "data": {},
  "page": null
}
```

Paginated collections put returned counts, truncation, word budgets, and an
opaque `next_cursor` in `page`. A cursor is valid only for the same route and
selectors that produced it.

Errors use:

```json
{
  "detail": "validation error: …",
  "error": "validation error: …"
}
```

| HTTP status | Meaning |
| ---: | --- |
| `400` | Invalid JSON, field, type, range, selector, repository, or cursor |
| `404` | Route or requested object not found |
| `503` | Daemon, database, or connection-pool resource is busy |
| `504` | A configured DuckDB operation deadline expired |
| `500` | Storage, I/O, parser, pool, or other runtime failure |

Wrong methods return `405` on the dedicated health, dashboard, handoff, and MCP
paths; unmatched REST method/path pairs return the normal route-not-found
error.

Every JSON request body is capped by `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` (1 MiB
by default). Unknown or malformed values fail rather than being silently
replaced with defaults.

No OpenAPI document is currently published. The Rust route dispatcher and its
tests are the executable source of truth; this page is the human-readable route
inventory for schema revision 16.

## Select a repository

Project-scoped routes need a Git repository. Select it with one of:

1. `x-coverage-mcp-repo: /absolute/path/to/repository`;
2. a documented `repo_path` query or JSON body field;
3. a stable project ID in `/api/projects/{project-id}` routes;
4. the daemon's default repository when one was selected internally.

The native `connect --repo <path>` bridge adds the repository header
automatically.

`GET /api/projects` is intentionally usable without a repository selector. It
lists registered projects. Other common-daemon routes return a validation error
when no repository can be selected.

Example:

```sh
curl --fail \
  --header 'x-coverage-mcp-repo: /absolute/path/to/repository' \
  'http://127.0.0.1:59471/api/snapshots?max_words=600'
```

## Service and browser routes

| Method and path | Result |
| --- | --- |
| `GET /health` | Version, schema revision, PID, instance ID, executable, common database, configured limits, and repository count |
| `GET /` | Embedded coverage dashboard |
| `GET /graph` | Embedded function-hierarchy and file-dependency graph |
| `POST /mcp/` | Stateless JSON-RPC MCP; `/mcp` is equivalent |

The private `POST /_coverage-mcp/handoff` endpoint is reserved for authenticated
runtime version handoff. It is not a user administration API and its capability
is never returned by `/health`.

For MCP JSON-RPC methods and tools, see the [MCP reference](mcp-reference.md).

## Projects and dashboard

| Method and path | Main inputs | Result |
| --- | --- | --- |
| `GET /api/projects` | `cursor`, `max_words`; optional repository selection | Registered project summaries |
| `POST /api/projects` | `repo_path` and optional compaction settings | Create/select a project and apply its initial policy |
| `GET /api/projects/{project-id}` | Project ID, or `project` plus repository selector | Project summary and policy |
| `PATCH /api/projects/{project-id}` | Any compaction policy fields | Updated project settings |
| `POST /api/projects/{project-id}/compact` | Empty JSON object is sufficient | Result of one immediate compaction pass |
| `GET /api/dashboard` | Current/baseline ordinary or composite IDs; `history_limit` 2–50 | One bounded dashboard projection |
| `GET /api/dashboard/events` | Repository selector | Server-sent dashboard revision events |

Project policy fields are:

```json
{
  "compaction_enabled": true,
  "compaction_after_days": 30,
  "compaction_interval_seconds": 3600,
  "compaction_batch_size": 100
}
```

Project-policy ranges are 1–36500 days, 1–86400 seconds, and 1–10000 snapshots.

The dashboard accepts repeated `snapshot_id` parameters for up to 64 ordinary
current snapshots, plus one `baseline_snapshot_id`. Composite selection uses
`composite_snapshot_id` and `baseline_composite_snapshot_id`. Ordinary and
composite selectors are independent dashboard panels.

The event stream sends:

- `ready` with the 10-second refresh interval;
- `refresh` only when the selected dashboard revision changes;
- heartbeat comments between changes;
- `dashboard-error` for a recoverable refresh failure.

The embedded page keeps the stream open only while its tab is foregrounded.

## Ingestion and snapshots

| Method and path | Main inputs | Result |
| --- | --- | --- |
| `POST /api/ingest` | `report_path`; optional format, suite, Git provenance, execution, retention | External report ingestion and snapshot ID |
| `GET /api/snapshots` | Branch, suite, cursor, and word limit | Snapshot page |
| `GET /api/snapshots/latest` | Optional branch and suite | Latest matching snapshot |
| `GET /api/snapshots/{snapshot-id}` | Snapshot ID | Immutable snapshot summary |
| `GET /api/snapshots/{snapshot-id}/files` | Optional `file_path`, cursor, budget, detail flag | File page or one file's detail |
| `GET /api/snapshots/{snapshot-id}/files/{path...}` | Path segments, cursor, budget, detail flag | One file's detail |
| `GET /api/snapshots/{snapshot-id}/functions` | Optional file path and cursor | Source-mapped LLVM function records |
| `GET /api/snapshots/{snapshot-id}/dependencies` | Cursor and budget | Best-effort Rust source import edges |
| `GET /api/snapshots/{snapshot-id}/insights` | Optional baseline snapshot | Ranked gaps for the snapshot |

Example external import:

```sh
curl --fail \
  --request POST \
  --header 'content-type: application/json' \
  --header 'x-coverage-mcp-repo: /absolute/path/to/repository' \
  --data '{
    "report_path": "target/coverage.json",
    "format": "llvm",
    "suite": "unit",
    "detail_retention": "compactable"
  }' \
  http://127.0.0.1:59471/api/ingest
```

`report_path` must resolve within the selected repository and is limited to 64
MiB. `detail_retention` is `compactable` or `incremental_base`.

Function records contain source range, execution, and line/branch/function/
region completeness when the LLVM producer supplies them. Dependency records
are source `use`/`mod` edges, not a runtime call graph.

## Composite snapshots

| Method and path | Main inputs | Result |
| --- | --- | --- |
| `GET /api/composite-snapshots/{id}` | Region/test/word/byte limits | Bounded audit summary and inventory provenance |
| `GET /api/composite-snapshots/{id}/regions` | `max_regions` 1–100 | Canonical production regions |
| `GET /api/composite-snapshots/{id}/insights` | Optional file selector and limits | Ranked composite gaps |

Composite snapshots are produced by managed commands whose artifacts include
an authoritative inventory and all required component descriptors. There is no
standalone REST route that creates a composite snapshot from undeclared files.

## Comparisons, history, and source

| Method and path | Required selectors | Result |
| --- | --- | --- |
| `GET /api/trend` | Repository; optional branch, suite, file, worktree, and limit | Coverage trend points |
| `GET /api/incremental` | One or more ordinary current `snapshot_id` plus `baseline_snapshot_id`, or a composite current/base pair | Baseline-union-current measurement and scope-aware diff |
| `GET /api/compare` | Ordinary current/base pair, or composite current/base pair | Readable snapshot comparison |
| `POST /api/compare` | Same selectors in JSON | Readable snapshot comparison |
| `GET /api/changed-lines` | `snapshot_id`, `baseline_snapshot_id`; optional file and `only_regressions` | Exact changed-line records |
| `GET /api/line-history` | `file_path`, integer `line_number`; optional branch and suite | History for one source line |
| `GET /api/source-lines` | Ordinary or composite measurement, file path, one-based `start` and `end` | Bounded source with coverage annotations |

Ordinary incremental comparison accepts at most 64 repeated current snapshot
IDs. Defaults are eight files, 24 regions, and ten affected test IDs; the
accepted route maxima are 100 files, 500 regions, and 100 test IDs.

Composite comparison requires matching repository, mapping version, and
inventory hash. It accepts only `composite_snapshot_id` with
`baseline_composite_snapshot_id`. Mixing ordinary and composite selectors is a
validation error.

`GET /api/incremental` always treats an ordinary selected measurement as a
subset. Missing baseline hits are `not_observed`, not regressions. The primary
measurement is the deduplicated baseline union; the separate diff retains the
replacement-style diagnostic.

Source requests require one-based inclusive `start` and `end`. Composite
source accepts 10–500 lines and an explicit byte budget. Ordinary source is
cursor- and word-budgeted.

## Named-test duplicate candidates

| Method and path | Inputs | Result |
| --- | --- | --- |
| `GET /api/duplicate-coverage-tests` | Optional snapshot/suite/cursor; group, test, word, and byte limits | Exact named-test coverage-equivalence groups |

Defaults are ten groups and ten names per group. The result exposes full counts
and truncation even when only part of a group is returned. Named observations
currently come from LCOV `TN:` records. Coverage equality is not test-logic
equality and does not authorize deletion.

## Worktrees

| Method and path | Main inputs | Result |
| --- | --- | --- |
| `GET /api/worktrees` | Repository selector | Registered worktrees |
| `POST /api/worktrees/register` | `path`, `base_ref`, optional `name` | Durable worktree baseline registration |
| `GET /api/worktrees/{id}/progress` | Required suite; optional file | Worktree coverage progress |
| `GET /api/worktrees/{id}/compare` | Optional current snapshot | Worktree/base comparison |

The server verifies Git lineage. A label, branch name, or working directory is
not a substitute for the registered worktree ID and baseline.

## Approved commands and runs

| Method and path | Main inputs | Result |
| --- | --- | --- |
| `GET /api/commands` | Repository selector | Registered command list |
| `POST /api/commands/register` | Name, command, human approval record; optional cwd, shell, artifacts | Immutable command registration |
| `GET /api/commands/{id-or-name}` | Command reference | Full registration |
| `POST /api/runs/profiled` | `command_ref`; optional timeout, idempotency key, wait, detail | Submit one approved command |
| `GET /api/runs/queue` | Repository selector | Bounded active queue |
| `GET /api/runs/latest` | Optional `command_ref` | Latest matching run |
| `GET /api/runs/{run-id}` | Optional detail | Durable run state and ingestion evidence |
| `POST /api/runs/{run-id}/cancel` | Run ID | Cancellation result |
| `GET /api/runs/{run-id}/logs/search` | One or more `query` values; stream, context, count, case, budgets | Bounded literal log matches |

The REST `profiled` route is the basic managed-run endpoint. MCP `run_test`
adds the current schema's case arguments, execution context, reuse policy, and
automatic incremental-review interface; prefer MCP for agent-run workflows.

Command registration must set `human_approved=true` and include accountable
approval fields. A command executes with the daemon user's permissions.

## Artifacts and topology

| Method and path | Inputs | Result |
| --- | --- | --- |
| `GET /api/artifacts/latest` | Required artifact `kind`; optional command reference | Latest matching retained artifact |
| `GET /api/topology/run/{run-id}` | Run ID | Run/artifact topology |
| `GET /api/topology/snapshot/{snapshot-id}` | Snapshot ID | Snapshot/artifact topology |

Other topology kinds are validation errors.

## Pagination and retries

Collection routes use `cursor`, `max_words`, and route-specific count limits.
Follow `page.next_cursor` only when the first bounded page does not answer the
question. Do not increase every limit by default.

Correct `400` and `404` responses before retrying. A safe read may retry `503`
or `504` with backoff. For command submission, use the MCP workflow and a stable
idempotency key so an ambiguous client retry cannot create a different case.

For daemon ownership, database locks, configuration, and recovery, see the
[operations guide](operations.md).
