# Architecture

Coverage MCP is a local-first Rust service for coverage history, approved test
execution, and agent-facing MCP access. The binary contains the HTTP daemon,
native stdio transport, dashboard, storage engine, parsers, and background
maintenance worker.

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
| `src/main.rs` | CLI, `serve`, `connect`/`stdio`, and one-shot `compact` lifecycle. |
| `src/http.rs` | Loopback HTTP server, repository routing, REST handlers, dashboard response, and health payload. |
| `src/mcp.rs` | Explicit schema-7 inventory, instructions, resource descriptions, tool dispatch, and shared JSON-RPC dispatch. |
| `src/service.rs` | Transport-neutral validation, response envelopes, word budgets, cursors, and compact projections. |
| `src/storage.rs` | DuckDB schema, immutable snapshots, runs, worktrees, artifacts, project settings, and compaction transactions. |
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
- Every public schema-7 response carries `repo_key`, `checkout_path`,
  `suite`, and `schema_revision` context.
- Collections use bounded fetches, word budgets, and opaque query-scoped
  cursors. A defensive record cap fails explicitly instead of silently losing
  data.
- Comparisons require compatible repository, suite, and lineage identity.
  Unknown parents are errors.
- Managed execution requires an immutable human-approved registration. Each
  stream is drained through a pipe and retained only up to the configured byte
  cap; summaries expose byte counts and `truncated`. Commands run in a
  dedicated process group so timeout, cancellation, and shutdown reach shell
  descendants. Any setup, polling, capture, or persistence failure
  terminalizes the durable job as `failed`.
- HTTP and stdio MCP calls use the same daemon-side
  `mcp::dispatch_json_rpc` function and therefore cannot diverge in tool
  behavior or error handling.

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
disappear. A verified legacy daemon without the endpoint receives a
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
payload when a detailed view is requested.

Settings are inserted with validated server defaults when a project is first
seen. REST `POST /api/projects` can provide project-creation overrides;
`PATCH /api/projects/{id}`, the dashboard, and the one-shot CLI pass handle
later changes. Manual passes update durable status and byte counters so the
dashboard and MCP context can show maintenance evidence. Project summaries
expose a stable short SHA-256 ID derived from the canonical repository key,
so common-daemon project routes can resolve the project without a repository
selection header.

## Storage layout

The shared daemon registry defaults to `~/.coverage-mcp/common.duckdb`, while
project stores live at `<repository>/.coverage-mcp/coverage.duckdb`. This
preserves coverage history and approvals across stdio client restarts without
allowing each client to own DuckDB. A centralized Rust-era
`projects/<stable-key>.duckdb` is reused only when it already exists and the
repository-local database does not. Explicit `connect --db <path>` and
`compact` remain standalone operations. All parent directories are created by
the Rust storage layer.

The database contains project settings, repository registry rows, snapshots,
files, lines, compacted payloads, worktrees, registered commands, runs, jobs,
artifacts, and run output metadata. Schema migration is performed at open and
is intentionally owned by storage rather than by transport code.

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
