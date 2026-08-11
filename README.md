# Coverage MCP

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

- Rust 1.85 or newer with Cargo;
- Git for repository identity and worktree lineage;
- a platform supported by bundled DuckDB.

Install from a checkout:

```sh
cargo install --path . --locked
coverage-mcp --version
```

For development, run the binary without installing it:

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
stdout.

```sh
coverage-mcp connect --repo /absolute/path/to/repository
```

The `stdio` subcommand is an alias. A project database is created at
`<repository>/.coverage-mcp/coverage.duckdb` unless `--db` or
`COVERAGE_MCP_DB` selects another path. A typical client entry is:

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

Run `coverage-mcp serve` once per user session and point an MCP client at
`http://127.0.0.1:59471/mcp/`. The daemon maintains one common registry at
`~/.coverage-mcp/common.duckdb` by default and lazily creates one project
database under `~/.coverage-mcp/projects/` for each canonical Git repository.
Set `COVERAGE_MCP_COMMON_DB` to relocate the registry and project directory.

The HTTP transport and stdio transport call the same Rust dispatcher, tool
schemas, service projections, validation, and storage implementation.

## MCP Usage Guide

Initialization instructions plus `tools/list` are intended to be sufficient
for an agent without reading this README. Start with `project_context`, use
only an exact approved command, submit asynchronously, wait for the returned
`poll_after_ms`, and then inspect the durable result. Every successful tool
response is `{context,data,page}`. `max_words` is the primary response budget;
collection results continue with `page.next_cursor`. `detailed` is false by
default and never returns logs.

All tool failures are returned as an MCP tool error payload with a stable
human-readable message. Invalid required fields, unknown names, invalid
lineage, stale cursors, missing files, and unavailable snapshots are errors;
an empty log search is a successful empty result.

| Tool | Inputs | Returns and next step |
| --- | --- | --- |
| `project_context` | `cursor`, `max_words`, `detailed` | Project identity, compaction policy, approved commands, latest run, active runs, and page metadata. Call first. |
| `register_test_command` | `name`, `command`, `human_approved`, `approved_by`, `approval_note`, optional `cwd`, `shell`, `artifact_paths`, `max_words` | Immutable approval record. Human approval must be true; pass its id or name to `run_test`. |
| `run_test` | `command_ref`, optional `timeout_seconds`, `idempotency_key`, `wait`, `max_words` | Durable run id, queue/ETA, process counters, and coverage-ingest status. Prefer `wait=false`. |
| `get_run_data` | `run_id`, `max_words`, `detailed` | Read-only durable run state. When `terminal=false`, wait at least `poll_after_ms` before calling again. |
| `cancel_run` | `run_id`, `max_words`, `detailed` | Cancellation request and terminal state. Use only when the user no longer wants the run. |
| `search_test_logs` | `run_id`, `query` string or array, optional `stream`, `context_lines`, `max_matches`, `max_words`, `case_sensitive` | Word-bounded stdout/stderr matches. Queries in an array use OR matching. |
| `ingest_coverage` | `report_path`, optional `format`, `suite`, `branch`, `commit_sha`, `base_ref`, `max_words` | Immutable snapshot summary, parser warnings, and provenance. Supported formats include LCOV, coverage JSON, Cobertura, JaCoCo, Istanbul, Go, and LLVM. |
| `register_worktree` | `path`, `base_ref`, optional `name`, `max_words` | Worktree identity and frozen baseline snapshot for `coverage_compare`. |
| `coverage_query` | `view`, optional snapshot/baseline selectors, `suite`, `branch`, `file_path`, `line_number`, `line_ranges`, `cursor`, `max_words`, `detailed` | `summary`, `files`, `file`, `insights`, or `line_history` projection. Continue bounded collections with the cursor. |
| `coverage_compare` | `view`, optional `snapshot_id`, `baseline_snapshot_id`, `worktree_id`, `suite`, `file_path`, `only_regressions`, `cursor`, `max_words`, `detailed` | `overview`, `files`, `lines`, or `progress` comparison. Select compatible lineage or a registered worktree. |
| `source_context` | `snapshot_id`, `file_path`, `start`, `end`, optional `cursor`, `max_words` | Numbered source lines for a bounded range already identified by coverage data. |

Resources:

- `coverage://context` — current project context, policy, commands, and active
  runs;
- `coverage://snapshot/{snapshot_id}/summary` — compact immutable snapshot
  summary.

The server advertises read-only safety annotations for query tools and
explicit mutation/execution annotations for registration, run, cancellation,
ingest, and worktree operations.

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
Project settings are applied per canonical repository, not per checkout.

The command-line one-shot pass is useful for maintenance jobs:

```sh
coverage-mcp compact --repo /absolute/path/to/repository \
  --older-than-days 30
```

## REST surface

The loopback API uses the same response envelope and repository routing as
MCP. Important routes are:

- `GET /health` — version, schema revision, daemon path, registry, and worker
  configuration;
- `GET /api/projects`, `POST /api/projects`, `GET/PATCH /api/projects/{id}` —
  project discovery and compaction policy;
- `POST /api/ingest` — report ingestion;
- `GET /api/snapshots`, `/api/snapshots/{id}`, and snapshot file/insight routes;
- `/api/compare`, `/api/changed-lines`, `/api/line-history`, and
  `/api/source-lines` — comparisons and bounded source views;
- `/api/commands`, `/api/runs`, `/api/artifacts`, and `/api/worktrees` —
  approved execution, retained evidence, and baselines;
- `POST /mcp/` — stateless JSON-RPC MCP over HTTP.

In common-daemon mode, select a repository with the
`x-coverage-mcp-repo` header or the documented `repo_path` query/body field.
The daemon rejects non-loopback bind hosts and untrusted Host headers.

### Ownership, pooling, and deadlines

The daemon acquires an OS-backed exclusive lease at
`<common-db-parent>/daemon.lock` before binding its listener. A second daemon
using the same common database fails with a 503-style `resource busy` error and
the lock file includes best-effort PID, executable, and resource metadata for
diagnostics. The operating system releases the lease when the owner exits, so
recovery does not depend on guessing whether a PID is stale. Each project
database has the same protection at `<database>.lock`; this prevents a daemon,
stdio process, and compaction process from opening the same DuckDB file at the
same time.

Every project store uses a bounded DuckDB connection pool. Writes are
serialized through the store write gate, while read-only paths can use the
remaining pool capacity. Connection checkout has a deadline, and each DuckDB
operation has a watchdog that calls DuckDB's interrupt handle. HTTP requests
also have a deadline, MCP requests are capped by the configured concurrency
limit, keep-alive is disabled, and SIGINT/SIGTERM interrupts active queries
before stores and leases are closed. Timeout and pool saturation errors are
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
make coverage
make docs
make lint
```

The full local gate is:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo llvm-cov --offline --lib --all-features --locked \
  --ignore-filename-regex '/src/main\.rs$' \
  --fail-under-lines 100 --fail-under-functions 100 \
  --fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

The migration fixture manifest and input-only cases in
[`tests/fixtures`](tests/fixtures) record the public surface carried into
Rust. [`docs/rust-migration-parity.md`](docs/rust-migration-parity.md) records
the mapping and evidence state; it is not an alternate runtime.

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
