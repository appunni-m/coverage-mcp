# Coverage MCP

[![CI](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml)
[![Release workflow](https://github.com/appunni-m/coverage-mcp/actions/workflows/release.yml/badge.svg)](https://github.com/appunni-m/coverage-mcp/actions/workflows/release.yml)
[![Coverage policy](https://img.shields.io/badge/coverage-policy%20enforced-brightgreen.svg)](CONTRIBUTING.md#required-local-gate)
[![MSRV: 1.85+](https://img.shields.io/badge/MSRV-1.85%2B-orange.svg)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Open issues](https://img.shields.io/github/issues/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/issues)
[![Open pull requests](https://img.shields.io/github/issues-pr/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/pulls)
[![Contributors](https://img.shields.io/github/contributors/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/graphs/contributors)
[![Last commit](https://img.shields.io/github/last-commit/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/commits/main)

Local-first coverage history, test execution, and an MCP server in one Rust
binary. Coverage MCP keeps immutable coverage snapshots in DuckDB, exposes a
dashboard and REST API, and provides the same schema-7 projections over
loopback HTTP and native MCP stdio.

The project is designed for one user-level daemon shared by agents and Git
worktrees. It does not bind to a public interface and it does not require a
frontend build or a separate language runtime.

## Status

The Rust implementation is the only runtime and the checked-in Rust test suite
is the source of truth. The public contract is schema revision 7. The local
gate proves 100% function and line coverage for the measured Rust
library/runtime targets; `src/main.rs` is exercised by child-process smoke
tests and excluded from aggregate LLVM counters. LLVM region coverage remains
a separate diagnostic and is reported by the coverage command.

## Install and first success

Requirements:

- Rustup; the checkout pins Rust 1.85.1 (the declared 1.85 MSRV) with Cargo,
  rustfmt, Clippy, and LLVM tools;
- Git for repository identity and worktree lineage;
- a platform supported by bundled DuckDB.

Install from a checkout:

```sh
cargo install --path . --locked
coverage-mcp --version
```

Normal MCP clients should launch `connect`; they do not need a separately
started daemon. For direct HTTP or dashboard development, run the daemon
without installing it:

```sh
cargo run --locked -- serve
```

The daemon listens on `127.0.0.1:59471` by default. Verify it:

```sh
curl --fail http://127.0.0.1:59471/health
open http://127.0.0.1:59471/       # macOS; use a browser on other systems
```

The dashboard is embedded in the binary. It can inspect projects, snapshots,
file gaps, line history, source context, comparisons, test runs, retained
artifacts, and the project compaction policy.

## MCP transports

### Native stdio

Use `connect` when an MCP client expects a child process. Messages are
newline-delimited JSON-RPC on stdin and stdout; diagnostics never go to
stdout. The child is a lightweight bridge: it starts or reuses the locked
loopback daemon, selects its repository with `x-coverage-mcp-repo`, and keeps
DuckDB ownership in that one daemon even when several agents connect at once.
The daemon remains available when an individual stdio bridge exits, so later
sessions reuse the same owner and port.

An established stdio bridge also survives a daemon crash. If its next TCP
connection is refused, the bridge re-runs the same verified startup path,
reuses the unlocked stale lease file, starts one replacement daemon, and
replays that JSON-RPC request once because no server could have received it. If
a timeout or another interruption occurs once delivery may have begun, it
restores daemon health for following requests but does not replay a potentially
mutating call. Use the call's stable idempotency key when retrying that
ambiguous request.

When a newer connector finds an older Coverage MCP daemon on that port, it
recovers automatically. It first verifies the healthy loopback response
against the actively held `daemon.lock`, common database, process, executable,
and instance identity. New daemons then accept a capability-authenticated
graceful handoff; the first upgrade from a pre-handoff release uses the same
verified lease metadata to request process termination. The connector waits
for both the listener and lease to be released before starting its exact
binary. It never replaces a newer daemon, an equal-version incompatibility, a
different common database, an unlocked metadata file, or an unknown process
occupying the port.

If a daemon exits without completing its managed-run shutdown, reopening a
project store reconciles the durable queue before accepting work. Runs that
were already marked `running` become terminal `interrupted` results because
replaying an arbitrary approved command could duplicate side effects. Runs
that were still `queued` are restarted automatically through the normal
concurrency gate. Stale active state therefore clears without a database edit
or a manual connector restart.

For checkout-local development, run the binary through Cargo. This
incrementally compiles the current source and does not require a separate
install or release build:

```sh
cargo run --locked -- connect --repo /absolute/path/to/repository
```

The first Cargo invocation may compile bundled DuckDB and take longer than an
MCP client's startup timeout. Warm the target before connecting if needed:

```sh
cargo run --locked -- --version
```

The installed-binary form is also supported:

```sh
coverage-mcp connect --repo /absolute/path/to/repository
```

Coverage MCP is a native Rust executable, not a Python package. Do not launch
it with `uvx`, `uv run`, or `python`; a Git checkout of this repository has no
`pyproject.toml` or `setup.py`, so those launchers exit before the MCP
`initialize` response. Install the exact published crate when the MCP host is
not running from a checkout:

```sh
cargo install coverage-mcp --version '=0.9.2' --locked
```

### Marketplace bootstrap contract

The matching `testing@codegen-marketplace` Codex plugin declares a required
stdio server in `.mcp.json`. Its small POSIX bootstrap checks `PATH`, then a
versioned cache, then downloads the exact GitHub Release archive for macOS or
Linux on ARM64 or x86-64. It verifies the archive against `SHA256SUMS`, verifies
the extracted binary's version, installs it atomically under
`~/.coverage-mcp/runtime/<version>`, and immediately replaces itself with
`coverage-mcp connect`. Cargo is a fallback for unsupported hosts or a release
download failure, not the normal first-install path.

The bootstrap does not start, inspect, stop, or route around the daemon and it
has no custom lifecycle lock. All runtime orchestration is implemented by
`connect`: repository selection, fixed-port discovery, stale-lease recovery,
version handoff, daemon startup, and request forwarding. Only the daemon
process holds `daemon.lock`; HTTP clients and stdio bridges do not acquire it or
lock one another. Both transports can connect concurrently, subject to the
daemon's configured resource limits.

Supported prebuilt targets need POSIX `sh`, `curl`, `tar`, and either
`sha256sum` or `shasum`; they do not need Rust or Cargo. The fallback requires
an existing Rust toolchain and crates.io access. The bootstrap never executes
Python or Node, follows a moving Git branch, or writes diagnostics to MCP
stdout. A downstream plugin version must not be released until its exact crate
and all claimed release archives are published and a clean-cache bootstrap has
passed. Checkout development should continue to use the explicit Cargo
registration above.

The marketplace bootstrap is POSIX `sh` and targets macOS, Linux, and WSL.
Native Windows bootstrap is not currently claimed; install the pinned crate
manually and configure the MCP host with the absolute
`coverage-mcp.exe connect` command.

For a checkout-local MCP registration, point the client at Cargo explicitly:

```json
{
  "mcpServers": {
    "coverage-mcp": {
      "command": "cargo",
      "args": [
        "run", "--locked", "--manifest-path",
        "/absolute/path/to/coverage-mcp/Cargo.toml", "--", "connect",
        "--repo", "/absolute/path/to/repository"
      ]
    }
  }
}
```

The `stdio` subcommand is an alias. Every stdio connector starts or reuses the
shared daemon and forwards its repository selection over loopback HTTP. Only
the daemon opens `<repository>/.coverage-mcp/coverage.duckdb`; connectors have
no direct-database mode. A typical client entry is:

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

### Loopback HTTP

Normal stdio clients should use `connect`, which starts or reuses the daemon
automatically. When a client connects to HTTP directly instead of using the
stdio bridge, run `cargo run --locked -- serve` for a checkout or
`coverage-mcp serve` for an installed binary, then point the client at
`http://127.0.0.1:59471/mcp/`. The daemon maintains one common registry at
`~/.coverage-mcp/common.duckdb` by default and lazily opens each canonical Git
repository's `.coverage-mcp/coverage.duckdb`. Rust-era centralized project
databases under `~/.coverage-mcp/projects/` remain readable as a compatibility
fallback when no repository-local database exists. Set
`COVERAGE_MCP_COMMON_DB` to relocate the registry and daemon lock.

The HTTP transport and stdio transport call the same Rust dispatcher, tool
schemas, service projections, validation, and storage implementation.

To verify the connector before opening an MCP client, send one complete
newline-delimited `initialize` request and check that the first response has
`result.serverInfo.name` equal to `coverage-mcp`:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}' \
  | cargo run --locked --manifest-path /absolute/path/to/coverage-mcp/Cargo.toml \
      -- connect --repo /absolute/path/to/repository
```

If the client reports `connection closed: initialize response`, run this
probe directly and inspect the connector's stderr. That message means the
child process exited or emitted an invalid transport stream before the
handshake; it is not a coverage-query error. Check that the command is either
the native `coverage-mcp` executable or an explicit Cargo launcher with an
existing `Cargo.toml`, that `connect` is present, and that `--repo` points to a
Git checkout. An older verified daemon is replaced automatically. If startup
still reports an incompatible daemon, recovery deliberately refused an
unverified owner, a different common database, an equal or newer version, or
inconsistent health/lease identity; inspect `/health`, `daemon.lock`, and
`~/.coverage-mcp/daemon.log` without deleting them. A project database lock
means another daemon or external process already owns that repository store;
stop that competing owner instead of deleting the lock file.

Every present argument is type-checked. An omitted optional argument receives
the documented default; a present argument with the wrong JSON type is a
validation error and is never silently treated as omitted. The HTTP MCP route
also requires a JSON object with a string \`method\`; malformed JSON, malformed
headers, and missing required fields return an explicit error response.
HTTP JSON bodies are capped by `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` (1 MiB by
default). Coverage ingestion rejects reports larger than 64 MiB and rejects
malformed numeric fields instead of converting them to zero or silently
dropping them.

## MCP Usage Guide

Coverage MCP is a query interface, not a request for the full raw coverage
report. Each `tools/call` chooses one projection and returns only the fields
needed for that projection. It is expected—and usually more efficient—to make
several narrow calls for one task instead of asking one call to return every
file, line, branch, and parser detail.

Initialization instructions plus `tools/list` are intended to be sufficient
for an agent without reading this README. Start with `project_context`, use
only an exact approved command, submit asynchronously, wait for the returned
`poll_after_ms`, and then inspect the durable result. Coverage reads are
read-only and can be composed by carrying `snapshot_id`, `file_path`, and line
ranges from one response into the next request.

### Choose the smallest projection

| Question | First call | Minimum selection | Follow-up when needed |
| --- | --- | --- | --- |
| What should I attack next? | `coverage_query` with `view="targets"` | Optional `order_by`; default is `priority` | Call `source_context` only for the returned file/range. |
| What changed since the previous session? | `coverage_compare` with `view="regions"` | No ids required for automatic latest/previous selection; use `only_regressions=true` for red impact only | Call `source_context` for a regressed range. |
| Where are the red portions in one file? | `coverage_query` with `view="file"` | `snapshot_id`, `file_path` | Add `line_ranges` with one or more exact ranges to receive selected line records. |
| What does the code around a gap look like? | `source_context` | `snapshot_id`, `file_path`, contiguous `start`/`end` | Make another call for each disjoint range; each call is capped at 200 lines. |
| Which exact lines changed? | `coverage_compare` with `view="lines"` | `snapshot_id` and `baseline_snapshot_id`, or a `worktree_id` | Use only for an audit; `regions` is smaller for normal reasoning. |
| How did one line behave over time? | `coverage_query` with `view="line_history"` | `file_path`, `line_number`, and `suite` | Keep `detailed=false` unless raw history fields are required. |
| Do I need parser or provenance detail? | `coverage_query` with `view="summary"` or `view="files"` | The relevant snapshot selector | Set `detailed=true` only for that audit. |

The `targets` priority score is deterministic: `uncovered_lines × 100 +
uncovered_branches × 10 + uncovered_functions × 5`; ties are ordered by
`file_path`. This makes `order_by="priority"` a useful default while still
allowing `uncovered_lines`, `line_rate`, or `file_path` when the question calls
for a different ordering.

### Compose multiple narrow calls

MCP requests are stateless, so the client should retain ids and feed exact
results into the next call. Independent calls may be issued separately or in
parallel; dependent calls should wait for the earlier result. A typical
coverage investigation is:

1. Call `project_context` once and keep the selected repository context.
2. Call `coverage_query(view="targets", order_by="priority")` to get a short
   ranked list and its `snapshot.id`.
3. Call `coverage_compare(view="regions", only_regressions=true)` separately
   if the user also asked what got worse. This call can auto-select the latest
   and previous matching snapshots.
4. For the one or two regions worth inspecting, call `source_context` with the
   exact `file_path`, `start`, and `end` returned by `targets` or `regions`.
5. Use `coverage_query(view="file", line_ranges=[...])` or
   `coverage_compare(view="lines")` only if the user asks for exact line
   records or an audit trail.

For example, these are separate `tools/call` argument objects, not one large
request:

```json
{"view":"targets","order_by":"priority","max_words":400}
```

```json
{"view":"file","snapshot_id":"<snapshot-id-from-targets>","file_path":"src/parser.rs","line_ranges":[{"start":120,"end":127},{"start":201,"end":206}],"max_words":500}
```

```json
{"snapshot_id":"<snapshot-id-from-file>","file_path":"src/parser.rs","start":120,"end":127,"max_words":350}
```

The second call can select multiple disjoint ranges in one file request. The
third call is intentionally one contiguous source window; repeat it for the
next range rather than expanding it to the whole file. A response's
`data.targets[].regions[]` and `data.regions[]` are designed to be passed
directly into these follow-up calls.

### Tool reference

All tool failures are returned as an MCP tool error payload with a stable
human-readable message. Invalid required fields, unknown names, invalid
lineage, stale cursors, missing files, and unavailable snapshots are errors;
an empty log search is a successful empty result.

| Tool | Inputs | Returns and next step |
| --- | --- | --- |
| `project_context` | `cursor`, `max_words`, `detailed` | Project identity including the stable project `id`, compaction policy, approved commands, `latest_run`, active runs, and page metadata. Call first; use `data.latest_run.id` as the `run_id` for `get_run_data`. |
| `register_test_command` | `name`, `command`, `human_approved`, `approved_by`, `approval_note`, optional `cwd`, `shell`, `artifact_paths`, `max_words` | Immutable approval record. Human approval must be true; pass its id or name to `run_test`. |
| `run_test` | `command_ref`, optional `timeout_seconds`, `idempotency_key`, `wait`, `max_words` | Durable run id, queue/ETA, process counters, and coverage-ingest status. Prefer `wait=false`; failed setup and shutdown paths are terminalized rather than left running. |
| `get_run_data` | required `run_id`, `max_words`, `detailed` | Read-only durable state for exactly one run. It does not select the latest run implicitly; use `project_context.data.latest_run.id`. When `terminal=false`, wait at least `poll_after_ms` before calling again. |
| `cancel_run` | `run_id`, `max_words`, `detailed` | Cancellation request and terminal state. Use only when the user no longer wants the run. |
| `search_test_logs` | `run_id`, `query` string or array, optional `stream`, `context_lines`, `max_matches`, `max_words`, `case_sensitive` | Word-bounded stdout/stderr matches. Queries in an array use OR matching. Retained output is capped per stream; ask for matches or small context windows rather than full logs. |
| `ingest_coverage` | `report_path`, optional `format`, `suite`, `branch`, `commit_sha`, `base_ref`, `max_words` | Immutable snapshot summary, parser warnings, and provenance. Supported formats include LCOV, coverage JSON, Cobertura, JaCoCo, Istanbul, Go, and LLVM. Reports are size-bounded and malformed numeric fields are explicit validation errors. |
| `register_worktree` | `path`, `base_ref`, optional `name`, `max_words` | Worktree identity and frozen baseline snapshot for `coverage_compare`. |
| `coverage_query` | One `view` per call; optional snapshot/baseline selectors, `suite`, `branch`, `file_path`, `line_number`, `line_ranges`, `order_by`, `cursor`, `max_words`, `detailed` | `summary`, `files`, `targets`, `file`, `insights`, or `line_history` projection. `targets` returns ranked files with compact uncovered red regions; `order_by` is `priority` (default), `uncovered_lines`, `line_rate`, or `file_path`. Continue bounded collections with the cursor. Make another narrow call for source or history. |
| `coverage_compare` | One `view` per call; optional `snapshot_id`, `baseline_snapshot_id`, `worktree_id`, `suite`, `file_path`, `only_regressions`, `cursor`, `max_words`, `detailed` | `overview`, `files`, `lines`, `regions`, or `progress` comparison. `regions` groups improved/regressed/new/removed line ranges and, without ids, compares the latest snapshot with its previous matching snapshot. Select compatible lineage or a registered worktree; compose with `source_context` for code. |
| `source_context` | One contiguous `snapshot_id`, `file_path`, `start`, `end`, optional `cursor`, `max_words` | Numbered source lines for a bounded range already identified by coverage data, each marked `red`, `green`, `yellow`, or `gray`, plus grouped `red_regions`. Make separate calls for disjoint ranges. |

Every successful tool uses this envelope:

```json
{
  "context": {
    "repo_key": "…",
    "checkout_path": "…",
    "suite": "…",
    "schema_revision": 7
  },
  "data": {},
  "page": null
}
```

Coverage projection shapes are intentionally small:

| Projection | `data` shape and important fields |
| --- | --- |
| `coverage_query:summary` | One compact snapshot object: id, commit, suite, rates, and metric counts. |
| `coverage_query:files` | An array of compact file summaries; use only when you actually need the file list. |
| `coverage_query:targets` | `{snapshot, order_by, targets[]}`. Each target has `file_path`, uncovered counts, `priority`, and contiguous `regions`. |
| `coverage_query:file` | `{file, red_regions, gaps, selected_lines, line_selection}`. `selected_lines` is populated only for requested `line_ranges`; `red_regions` remains the compact GitHub-like gap map. |
| `coverage_query:insights` | `{snapshot, baseline, summary, items[]}` with prioritized findings. |
| `coverage_query:line_history` | An array of compact points for one `file_path` and `line_number`; `suite` is required. |
| `coverage_compare:overview` | `{baseline, current, overall, file_change_count, line_change_count}`. |
| `coverage_compare:files` | Baseline/current file metrics and deltas, ordered by change. |
| `coverage_compare:lines` | Exact changed line records; larger than regions and intended for audits. |
| `coverage_compare:regions` | `{baseline, current, overall, region_change_count, regions[]}` where each region has `file_path`, `status`, `start`, `end`, and `line_count`. |
| `coverage_compare:progress` | Worktree baseline plus paged progress points; requires `worktree_id` and `suite`. |
| `source_context` | `{snapshot_commit_sha, file_path, red_regions, lines[]}`. Each line has source text, line number, `status`, and `marker`. |

### Response budgets, pagination, and selection

- `max_words` is per call, not a budget shared across a sequence. It accepts
  `50`–`5000` and defaults to `600`. Use a smaller budget for a ranked first
  pass and a larger budget only for the exact follow-up you need.
- Collection pages report `returned`, `total`, `word_count`, `max_words`,
  `truncated`, and `next_cursor`. If `truncated` is true, repeat the identical
  view, filters, ordering, and budget with `cursor=page.next_cursor`.
- Cursors are opaque and query-scoped. Keep the view, selectors, filters,
  ordering, `detailed`, and `max_words` unchanged while continuing a page; if
  you change the query, start a new cursor instead of reusing the old one.
- `snapshot_id` is optional for normal snapshot reads and selects the latest
  snapshot for the selected checkout. `coverage_compare(view="regions")` can
  select the latest snapshot and its previous matching snapshot automatically.
- `line_ranges` accepts multiple inclusive `{start,end}` objects for one file.
  `source_context` accepts one contiguous range per call and caps the range at
  200 lines.
- `detailed=false` is the normal mode. It suppresses raw report paths,
  parser metadata, raw file metrics, and other audit-only fields; it never
  returns logs.
- Collections have a defensive 5,000-record cap. Refine the query with a
  snapshot, file, range, ordering, or regression filter instead of requesting
  an unbounded report.

### Errors and retry behavior

MCP returns a JSON-RPC error with a stable message; the same error classes are
used by the HTTP and stdio transports. Treat these classes differently:

| Class | Typical cause | Client action |
| --- | --- | --- |
| Validation (HTTP 400) | Missing/invalid view, range, ordering, budget, or cursor | Fix the request; do not retry unchanged. |
| Not found (HTTP 404) | No matching snapshot, previous snapshot, worktree, or source file | Narrow or correct the selector; an empty search is not an error. |
| Storage/runtime (HTTP 500) | Database, filesystem, parser, or process failure | Report the stable message and investigate the retained evidence. |
| Busy (HTTP 503) | Another daemon/store owns the resource or capacity is saturated | Retry the individual call with backoff. |
| Timeout (HTTP 504) | HTTP, pool checkout, or DuckDB deadline exceeded | Retry the individual read with backoff and a narrower query if possible. |

Coverage projections are read-only. `ingest_coverage`,
`register_test_command`, `run_test`, `cancel_run`, and `register_worktree` are
the state-changing or execution tools; use their explicit workflow and safety
annotations.

Resources:

- `coverage://context` — current project context, policy, commands, and active
  runs;
- `coverage://snapshot/{snapshot_id}/summary` — compact immutable snapshot
  summary.

The server advertises read-only safety annotations for query tools and
explicit mutation/execution annotations for registration, run, cancellation,
ingest, and worktree operations.

For normal agent work, use the compact projections in this order:

- `coverage_query(view="targets", order_by="priority")` answers what to attack
  next. Each item is one file with only its uncovered counts, a priority score,
  and contiguous `regions` such as `12-16`; it does not return every covered
  line or parser-specific detail.
- `coverage_compare(view="regions")` answers what changed since the previous
  session. It returns grouped ranges with `status` values `improved`,
  `regressed`, `new`, `removed`, or `changed`; pass `only_regressions=true` to
  narrow it to red impact. If both snapshot ids are omitted, the latest and
  previous matching snapshots are selected automatically.
- `source_context` is the follow-up when the actual file text is needed. Its
  bounded lines carry a coverage `status` and display `marker`, while
  `red_regions` identifies the missed executable portions without shipping a
  full raw coverage report.

Use `coverage_query(view="file", line_ranges=[...])` or
`coverage_compare(view="lines")` only when an exact per-line audit is needed;
the compact views intentionally avoid repeating covered-line JSON. Multiple
small calls are the intended way to answer multiple related questions while
keeping each response focused.

## Coverage storage and compaction

Snapshots and completed runs are immutable. The per-project background worker
compresses older file/line detail into a zstd payload while preserving the
same query results through transparent restoration. Compaction is enabled by
default for every newly created project, with these defaults:

| Setting | Default | Valid range |
| --- | ---: | ---: |
| `compaction_enabled` | `true` | `true` / `false` |
| `compaction_after_days` | `30` | 1–36500 days |
| `compaction_interval_seconds` | `3600` | 1–86400 seconds |
| `compaction_batch_size` | `100` | 1–10000 snapshots |

Configure defaults before the project is first opened with:

```sh
COVERAGE_MCP_COMPACTION_AFTER_DAYS=14 \
COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS=900 \
COVERAGE_MCP_COMPACTION_BATCH_SIZE=250 \
coverage-mcp serve
```

At project creation, `POST /api/projects` accepts `repo_path` and the same
`compaction_enabled`, `compaction_after_days`,
`compaction_interval_seconds`, and `compaction_batch_size` fields. Existing
projects can be edited with `PATCH /api/projects/{project}` or from the
dashboard. `POST /api/projects/{project}/compact` runs one immediate pass.
Project summaries expose `{project}` as a stable short SHA-256 identifier
derived from the canonical repository key. In common-daemon mode, these
project-specific routes can use that identifier without a repository header;
the header and `repo_path` query parameter remain supported for compatibility.
Project settings are applied per canonical repository, not per checkout.

The command-line one-shot pass is useful for maintenance jobs. It starts or
reuses the shared daemon and sends the maintenance request over loopback HTTP;
the CLI process never opens the project database:

```sh
coverage-mcp compact --repo /absolute/path/to/repository \
  --older-than-days 30
```

## REST surface

The loopback API uses the same response envelope and repository routing as
MCP. Important routes are:

- `GET /health` — version, schema revision, daemon path, PID, per-process
  instance ID, handoff support, registry, and worker configuration; the
  handoff capability itself is never returned;
- `GET /api/projects`, `POST /api/projects`, `GET/PATCH /api/projects/{id}` —
  project discovery and compaction policy;
- `POST /api/ingest` — report ingestion;
- `GET /api/snapshots`, `/api/snapshots/{id}`, and snapshot file/insight routes;
- `/api/compare`, `/api/changed-lines`, `/api/line-history`, and
  `/api/source-lines` — comparisons and bounded source views;
- `/api/commands`, `/api/runs`, `/api/artifacts`, and `/api/worktrees` —
  approved execution, retained evidence, and baselines;
- `POST /mcp/` — stateless JSON-RPC MCP over HTTP.

In common-daemon mode, select a repository with a project ID from
`GET /api/projects`, the `x-coverage-mcp-repo` header, or the documented
`repo_path` query/body field. The daemon rejects non-loopback bind hosts and
untrusted Host headers.

### Ownership, pooling, and deadlines

The daemon acquires an OS-backed exclusive lease at
`<common-db-parent>/daemon.lock` before binding its listener. A second daemon
using the same common database fails with a 503-style `resource busy` error.
The lock file records PID, executable, resource, instance identity, and a
per-process handoff capability; Unix permissions are restricted to `0600`, and
the capability is not exposed by `/health`. The operating system releases the
lease when the owner exits, so an unlocked leftover file is never treated as
proof of ownership. A newer connector may request shutdown only after the
health identity and actively held lease agree, then waits for the lease before
starting the replacement. Clients never take this lease: direct HTTP
connections and any number of stdio bridges can use the daemon concurrently,
subject to configured resource limits. Each project database has the same
protection at `<database>.lock`; this prevents daemons using different registry
locations, or another library process, from opening the same DuckDB file at the
same time. Stdio and compaction clients never open that file themselves.

Every project store uses a bounded DuckDB connection pool. Writes are
serialized through the store write gate, while read-only paths can use the
remaining pool capacity. Connection checkout has a deadline, and each DuckDB
operation has a watchdog that calls DuckDB's interrupt handle. HTTP requests
also have a deadline, MCP requests are capped by the configured concurrency
limit, keep-alive is disabled, and SIGINT/SIGTERM interrupts active queries
before stores and leases are closed. Managed commands capture stdout/stderr
through draining pipes with a per-stream byte cap, start in their own process
group, and terminate that group on timeout, cancellation, or shutdown. If
setup, polling, capture, or persistence fails, the durable job is marked
`failed` before the error is returned. Timeout and pool saturation errors are
reported explicitly; the server never deletes a WAL or lock file to recover.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `COVERAGE_MCP_HOST` | `127.0.0.1` | Loopback bind host; public binding is rejected. |
| `COVERAGE_MCP_PORT` | `59471` | HTTP port. |
| `COVERAGE_MCP_COMMON_DB` | `~/.coverage-mcp/common.duckdb` | Common registry database. |
| `COVERAGE_MCP_RUN_RETENTION` | `100` | Terminal runs retained per command. |
| `COVERAGE_MCP_RUN_CONCURRENCY` | `4` | Managed command workers. |
| `COVERAGE_MCP_HTTP_CONCURRENCY` | `16` | Concurrent HTTP MCP requests. |
| `COVERAGE_MCP_DB_POOL_SIZE` | `4` | Maximum DuckDB connections per project (1–16). |
| `COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS` | `5000` | Maximum pool checkout wait (50–120000 ms). |
| `COVERAGE_MCP_DB_QUERY_TIMEOUT_MS` | `30000` | Maximum one DuckDB operation (100–3600000 ms); must be shorter than the HTTP deadline. |
| `COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS` | `60` | Maximum HTTP request duration (1–3600 s). |
| `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` | `1048576` | Maximum JSON HTTP request body (1024–16777216 bytes). |
| `COVERAGE_MCP_RUN_LOG_MAX_BYTES` | `10485760` | Maximum retained stdout or stderr bytes per managed run (1024–1073741824 bytes); excess output is drained and reported as `truncated=true`. |
| `COVERAGE_MCP_COMPACTION_AFTER_DAYS` | `30` | Default age threshold for new projects. |
| `COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS` | `3600` | Default maintenance cadence for new projects. |
| `COVERAGE_MCP_COMPACTION_BATCH_SIZE` | `100` | Default maintenance batch for new projects. |

Environment values are validated at startup. Project patches are validated at
the storage boundary as well.

## Development

The repository uses strict, reproducible Cargo commands. The short commands
are available through `make`:

```sh
make fmt
make clippy
make test
make test-bundled  # optional full bundled-linkage test
make coverage
make migration-parity
make migration-benchmark
make migration-status
make mcp-evals  # opt-in; not part of CI
make docs
make lint
```

The full local gate is:

```sh
cargo fmt --all -- --check
DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
DUCKDB_DOWNLOAD_LIB=1 cargo test --workspace --all-targets --no-default-features --locked
DUCKDB_DOWNLOAD_LIB=1 cargo llvm-cov --lib --no-default-features --locked \
  --ignore-filename-regex '/src/main\.rs$' \
  --fail-under-lines 100 --fail-under-functions 100 \
  --fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1
DUCKDB_DOWNLOAD_LIB=1 RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-default-features --no-deps --locked
cargo build --release --locked
git diff --check
```

Fast verification asks `libduckdb-sys` to download the official DuckDB
release matching the Rust crate once, then links it dynamically while Cargo
runs the checks. This avoids rebuilding DuckDB's large C++ amalgamation in
every test profile. It requires network access on a cold cache. Normal
`cargo build`, `cargo install`, and release binaries keep the default
`bundled-duckdb` feature and remain self-contained; `make test-bundled`
provides an explicit full-suite linkage check.

The migration fixture manifest and input-only cases in
[`tests/fixtures`](tests/fixtures) record the public surface carried into
Rust. [`docs/rust-migration-parity.md`](docs/rust-migration-parity.md) records
the mapping and evidence state; it is not an alternate runtime. After the
lanes and coverage gate, `make migration-status` emits the fixed aggregate at
`target/migration/status-report.json` plus generated contract and status pages
under `docs/generated/`. Missing, dirty, or incompatible evidence is reported
as `not_proven`.

### MCP evaluation suite

The opt-in [`evals/README.md`](evals/README.md) describes the comprehensive
agent-facing evaluation suite. It covers independent usability, confusion,
token and compute efficiency, outcome-driven tool selection, compact coverage
workflows, protocol behavior, safety, validation, idempotent runs, pagination,
and retained evidence. Run it with `make mcp-evals`; it intentionally does not
run in CI or in the default workspace test commands.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the review workflow,
[`docs/architecture.md`](docs/architecture.md) for ownership boundaries, and
[`docs/releasing.md`](docs/releasing.md) for release verification.

## Security and support

Coverage MCP executes approved local commands with the current user's
permissions. Treat repositories, report files, retained logs, and command
definitions as untrusted local input. Keep the daemon on loopback, do not
expose its port through a proxy without an explicit security design, and do
not commit `.coverage-mcp/` databases.

Report vulnerabilities privately using [`SECURITY.md`](SECURITY.md). Use
[GitHub issues](https://github.com/appunni-m/coverage-mcp/issues) for
reproducible bugs and feature requests; include sanitized version, schema,
platform, health output, and reproduction details.

## License

Coverage MCP is released under the [MIT License](LICENSE).
