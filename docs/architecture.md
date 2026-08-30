# Architecture

Use this document when you are changing or reviewing Coverage MCP's process,
storage, concurrency, or trust boundaries. For installation and normal coverage
work, begin with [Getting started](getting-started.md) or the
[user guide](user-guide.md).

Coverage MCP's job is to preserve trustworthy local coverage evidence and
return bounded answers to agents, dashboards, and API clients. The Rust binary
implements that job with an HTTP daemon, native stdio transport, dashboard,
storage engine, parsers, and background maintenance worker.

## Runtime topology

```text
MCP client ── stdio bridge ──┐
MCP client ── stdio bridge ──┼── loopback HTTP daemon ── repository DuckDB stores
MCP client ── loopback HTTP ─┤              │
dashboard ── loopback HTTP ──┘              ├── zstd compacted detail payloads
                                            ├── managed command workers
                                            └── Git identity / worktree lineage
```

The HTTP daemon owns a common registry and lazily opens one project database
for each canonical Git repository. Linked worktrees resolve through Git's
common directory and share the repository store. A stdio process selects one
repository and forwards newline-delimited JSON-RPC to the daemon with a bounded
loopback HTTP request. The first connector starts the daemon on demand; racing
connectors using the same host, port, and common database discover the same
owner, which alone holds `daemon.lock`. Connectors and direct HTTP clients never
take this lease and may connect concurrently. The defaults are
`127.0.0.1:59471`, `~/.coverage-mcp/common.duckdb`, and
`~/.coverage-mcp/daemon.lock`. Before forwarding a request, a connector
requires the daemon health response to match its binary version, schema
revision, and common database. A newer connector replaces an older daemon only
after the health response and actively held daemon lease agree on the common
database, executable, process, resource, and instance. Unknown owners,
downgrades, and equal-version incompatibilities fail closed instead of
starting or terminating a second process. The daemon is independent of any
one stdio bridge and remains available after bridges disconnect. A bridge that
outlives a crashed daemon treats connection refusal as proof that its request
was not delivered, executes the same verified startup path, and replays that
request once. Other transport interruptions trigger daemon recovery for later
requests without replaying an operation whose commit status is ambiguous.

## Module ownership

| Module | Responsibility |
| --- | --- |
| `src/main.rs` | CLI, shared-daemon `serve`, `connect`/`stdio`, and daemon-routed `compact` lifecycle. |
| `src/http.rs` | Loopback HTTP server, repository routing, REST handlers, dashboard response, and health payload. |
| `src/mcp.rs` | Explicit schema-16 inventory, instructions, resource descriptions, tool dispatch, and shared JSON-RPC dispatch. |
| `src/service.rs` | Transport-neutral validation, response envelopes, word budgets, cursors, and compact projections. |
| `src/storage.rs` | DuckDB schema, immutable snapshots/composite snapshots, runs, worktrees, artifacts, project settings, and compaction transactions. |
| `src/composite.rs` | Versioned production inventory, canonical-region mapping, explicit alias deduplication, variant completeness, and exact component aggregation. |
| `src/lock.rs` | Process-lifetime daemon and per-database OS-backed exclusive leases. |
| `src/pool.rs` | Bounded DuckDB connection pooling, checkout deadlines, query interruption, and shutdown tracking. |
| `src/compaction.rs` | Policy and result types for background and manual detail compression. |
| `src/parser.rs` | LCOV, coverage JSON, Cobertura, JaCoCo, Istanbul, Go, and LLVM normalization. |
| `src/git.rs` | Canonical repository identity, worktree identity, merge-base, and ancestry checks. |
| `src/config.rs` | Environment defaults and validated runtime configuration. |
| `src/dashboard.html` | Embedded dependency-free dashboard document and client-side views. |

## Data and lifecycle invariants

- Coverage snapshots, registered command approvals, and completed runs are
  immutable records.
- Queue state is mutable only while a managed run is queued or active.
- Project settings are scoped to the canonical repository key, so linked
  worktrees use the same compaction policy and coverage history.
- Every public schema-16 response carries `repo_key`, `checkout_path`,
  `suite`, and `schema_revision` context.
- Collections use bounded fetches, word budgets, and opaque query-scoped
  cursors. A defensive record cap fails explicitly instead of silently losing
  data.
- Comparisons require compatible repository, suite, and lineage identity.
  Unknown parents are errors. Incremental comparisons are the explicit
  exception for suite identity: they require the same repository and format,
  but may compare different suites and case-specific execution arguments.
- Managed execution requires an immutable human-approved registration. Each
  stream is drained through a pipe and retained only up to the configured byte
  cap; summaries expose byte counts and `truncated`. Commands run in a
  dedicated process group so timeout, cancellation, and shutdown reach shell
  descendants. Any setup, polling, capture, or persistence failure
  terminalizes the durable job as `failed`.
- A composite snapshot is immutable and run-scoped. Its authoritative inventory
  owns the denominator; `logical_source_id` aliases are the only deduplication
  mechanism. LLVM/native/WASM, coverage.py, Istanbul, and
  `coverage-mcp-regions-v1` evidence map into one canonical region model. Every
  declared package/backend variant is required even when variants share a
  logical source. Missing, stale, malformed, unsupported, or source-mismatched
  evidence makes the composite incomplete and never lowers its denominator.
  Composite snapshots persist mapping version, inventory hash, checkout
  revision/cleanliness, command registration, toolchain versions, and artifact
  hashes, plus exact combined counts and Rust/WGSL, Python, and JavaScript
  component summaries.
- When a project store opens after an ungraceful daemon exit, persisted
  `running` jobs become terminal `interrupted` records and persisted `queued`
  jobs re-enter the normal worker/concurrency path. Running commands are not
  replayed because their external side effects may already have occurred.
- HTTP and stdio MCP calls use the same daemon-side
  `mcp::dispatch_json_rpc` function and therefore cannot diverge in tool
  behavior or error handling.
- Agent-facing projections are compact by default. `project_context` reserves
  its fixed project/run summary before paging commands and enforces the budget
  over the complete `data` object; `page.reserved_words` reports that fixed
  reservation, `page.word_count` reports only the command page, and
  `page.response_word_count` reports their combined accounting. Active runs are
  hard-capped at ten and command pages at eight compact summaries. Compact
  coverage reviews default to three files and five ranked targets;
  `data.active_runs_truncated` makes omitted active runs explicit and
  `page.next_cursor` continues commands only. Detailed mode does not lift
  either normal page cap. Compact history keeps two points by default and
  compact insight keeps three `[start,end,line_count]` ranges per target while
  exposing `region_count` and `regions_truncated`; audit is the explicit exact
  projection. An incremental terminal run automatically attaches a bounded
  `incremental_review`; when a run ingests multiple ordinary artifacts, the
  review unions their snapshot IDs. The standalone review tool remains
  independent of execution. `structuredContent` is the canonical JSON-RPC
  success payload; text content is only a compatibility hint.
- Named per-test coverage is normalized during LCOV parsing into `test_names`
  and `test_coverage_observations`. Duplicate candidates compare a full,
  ordered JSON representation of each test's `{kind,file_path,line_number,
  region_key}` set inside DuckDB; no hash-only grouping is used. The response
  exposes global counts and small cursor pages, while other report formats
  return `status=unavailable` because they do not provide named observations.
- Compaction includes both named-test tables in the zstd detail payload and
  deletes their relational rows in the same transaction as file and line
  detail. Restored compacted payloads use the same exact signature semantics.
- The embedded dashboard uses one bounded `GET /api/dashboard` projection for
  its metrics, timeline, run activity, incremental categories, file table, and
  compaction inventory. It accepts independent ordinary and composite current
  and baseline IDs, and presents one exact production-region percentage plus
  Rust/WGSL, Python, and JavaScript component summaries, blocking reasons, and
  remediation buckets. Ordinary measurements also present retention,
  relational-versus-compacted detail source, suite/format, persisted case
  arguments, and server-generated execution fingerprints so a full run and a
  case-specific incremental run are not visually conflated. It subscribes to
  `GET /api/dashboard/events`, which checks the server-side revision every ten
  seconds and emits a refresh event only when it changes. The browser keeps the
  stream and automatic refresh active only while the tab is foregrounded; it
  never polls individual REST projections or reruns tests.
- MCP `coverage_review(task="incremental")` is a standalone stored-detail
  projection. It requires an explicit current measurement and
  `baseline.kind="explicit"` with matching ordinary snapshot IDs or composite
  snapshot IDs. When a run produced several ordinary artifacts, all selected
  ordinary IDs are merged. The primary ordinary result is the deduplicated
  baseline-union-current projection; its nested `diff` is a separate
  replacement-style diagnostic. Selected-subset absence is `not_observed`, not
  regression; only `complete_snapshot` supports regression claims. Ordinary
  comparisons require the same repository and normalized format; composite
  comparisons additionally require the same mapping version and inventory hash.
  Both return grouped line categories, aggregate metric deltas, bounded opaque
  `test_id` set differences, and attribution status. Suite, branch, commit,
  command, cwd, and execution case are provenance, not test identity.
  Relational detail and the zstd compacted payload are equivalent sources for
  the projection; neither path invokes a test runner or reparses a report.
- `run_test` accepts optional case-specific string arguments without command
  re-registration. The durable `run_execution_context` stores those arguments
  and an explicit incremental baseline; reuse and idempotency compare the full
  context. A terminal incremental run invokes the same stored-detail projection
  used by `coverage_review`, unions all ordinary artifacts attached to the run,
  and attaches its bounded result.
- Composite region comparisons load all stored canonical rows for exact category
  counts before applying response limits. Public region/source projections stay
  bounded. Composite rows are independent of ordinary zstd detail compaction;
  child named-test observations use the existing relational-or-payload restore
  path, so a compacted fixed base remains comparable.

## Ownership, concurrency, and shutdown

The daemon takes `<common-db-parent>/daemon.lock` before binding its listener.
Each project store takes `<database>.lock` before opening DuckDB. These are
advisory OS file leases held by live file descriptors; a crashed process does
not leave an unrecoverable stale lock, and the metadata file is never treated
as proof of ownership by itself. A duplicate daemon or database owner fails
fast with holder metadata instead of deleting a lock or attempting concurrent
DuckDB access. These leases protect process and database ownership, not client
connections: HTTP and stdio requests can execute concurrently within the
configured MCP, pool, and deadline limits.

Daemon lease metadata includes a per-process instance ID and handoff
capability. The public health response includes the PID, instance ID, and
whether handoff is supported, but never the capability. On Unix the metadata
file is restricted to mode `0600`. During an upgrade, a newer connector reads
the capability only from the actively locked file, requests graceful shutdown
from that exact loopback instance, and waits for both its listener and lease to
disappear. A verified pre-handoff daemon without the endpoint receives a
platform-native termination request for the recorded PID. The connector never
downgrades a newer owner or takes over a different common database.

After an ungraceful daemon exit, the OS releases the lease even though the
metadata file remains. The next new or already-running stdio bridge acquires
that same file and starts the replacement; no operator deletes a lock, and no
client-to-client connection lock is introduced.

Each project store owns a bounded `r2d2` pool. The write gate preserves
DuckDB's single-writer semantics, while read-only operations use the pool with
a bounded checkout wait. Every checked-out connection is registered with a
query tracker. A watchdog interrupts the connection when the configured query
deadline expires; shutdown interrupts all tracked operations, waits for them
to release their leases, then closes the pool and database lock. HTTP requests
have an independent deadline and bounded JSON bodies, MCP requests use a
semaphore, and HTTP/1 keep-alive is disabled so idle client connections cannot
retain server tasks indefinitely. Coverage report parsing is capped at 64 MiB
and numeric fields are validated before normalization.

Database open and WAL replay errors fail closed. The runtime never removes a
WAL, lock, or database file automatically; operators should stop competing
owners and restore a verified database/WAL backup when replay cannot complete.

## Background compaction

Each opened project starts a maintenance worker. On the configured cadence it
reads the persisted policy, finds snapshots older than the project threshold,
serializes file/line detail, compresses it with zstd, and commits the compacted
payload in one DuckDB transaction. The original detail rows are removed only
after the compressed payload is durable. Query paths transparently restore the
payload when a detailed view is requested. Composite snapshots and canonical
composite regions are not part of the ordinary snapshot compaction transaction;
their immutable rows remain directly queryable. If an ordinary child snapshot
is compacted, named-test attribution restores its observations from the same
payload used by incremental review.

Settings are inserted with validated server defaults when a project is first
seen. REST `POST /api/projects` can provide project-creation overrides;
`PATCH /api/projects/{id}`, the dashboard, and the one-shot CLI pass handle
later changes. Manual passes update durable status and byte counters. The
dashboard's compaction inventory also counts relational snapshots, compressed
payloads, protected incremental bases, currently eligible snapshots, and
original versus compressed bytes, and marks payload inventory
inconsistencies explicitly. Project summaries
expose a stable short SHA-256 ID derived from the canonical repository key,
so common-daemon project routes can resolve the project without a repository
selection header.

## Storage layout

The shared daemon registry defaults to `~/.coverage-mcp/common.duckdb`, while
project stores live at `<repository>/.coverage-mcp/coverage.duckdb`. This
preserves coverage history and approvals across stdio client restarts without
allowing each client to own DuckDB. A centralized Rust-era
`projects/<stable-key>.duckdb` is reused only when it already exists and the
repository-local database does not. All connector and compaction requests go
through the shared daemon; only that daemon resolves and opens project stores.
All parent directories are created by the Rust storage layer.

The database contains project settings, repository registry rows, ordinary and
composite snapshots, canonical composite regions, files, lines, compacted
payloads, worktrees, registered commands, runs, jobs, run execution contexts,
artifacts, and run output metadata. Fresh schema
creation is performed at open and is intentionally owned by storage rather than
by transport code.

## Trust boundary

The daemon is a local developer tool. Loopback binding is an access boundary,
not authorization. Command registration and execution are explicit because a
registered command runs local code with the user's permissions. The server
rejects non-loopback bind hosts, validates Host headers, emits restrictive
browser headers, does not enable CORS, and serves no third-party assets.

Coverage reports, source files, logs, command strings, and database contents
must be treated as potentially hostile input. Request bodies, coverage reports,
and retained command output are explicitly size-bounded; malformed coverage
numbers are rejected. A deployment that exposes the
HTTP port beyond the local user boundary requires an external authentication,
authorization, and network-isolation design; that deployment is outside this
project's supported scope.
