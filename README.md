# Coverage MCP

[![CI](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml)
[![Release workflow](https://github.com/appunni-m/coverage-mcp/actions/workflows/release.yml/badge.svg)](https://github.com/appunni-m/coverage-mcp/actions/workflows/release.yml)
[![Coverage policy](https://img.shields.io/badge/coverage-policy%20enforced-brightgreen.svg)](https://github.com/appunni-m/coverage-mcp/blob/main/CONTRIBUTING.md#required-local-gate)
[![MSRV: 1.85+](https://img.shields.io/badge/MSRV-1.85%2B-orange.svg)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Open issues](https://img.shields.io/github/issues/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/issues)
[![Open pull requests](https://img.shields.io/github/issues-pr/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/pulls)
[![Contributors](https://img.shields.io/github/contributors/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/graphs/contributors)
[![Last commit](https://img.shields.io/github/last-commit/appunni-m/coverage-mcp.svg)](https://github.com/appunni-m/coverage-mcp/commits/main)

Local-first coverage history, test execution, and an MCP server in one Rust
binary. Coverage MCP keeps immutable coverage snapshots in DuckDB, exposes a
dashboard and REST API, and provides the consolidated schema-9 projections over
loopback HTTP and native MCP stdio.

The project is designed for one user-level daemon shared by agents and Git
worktrees. It does not bind to a public interface and it does not require a
frontend build or a separate language runtime.

## Status

The Rust implementation is the only runtime and the checked-in Rust test suite
is the source of truth. The public contract is schema revision 9 with seven
agent-facing tools. The local gate is configured to require 100% region, line,
and function coverage for the measured Rust library/runtime targets. `src/main.rs`
is exercised by child-process smoke tests and excluded from aggregate LLVM
counters.

## Documentation map

- **Use the server:** this README's MCP and REST sections, plus the
  self-describing `initialize` and `tools/list` responses.
- **Understand the design:** [`docs/architecture.md`](docs/architecture.md).
- **Contribute:** [the source-checkout guide](https://github.com/appunni-m/coverage-mcp/blob/main/CONTRIBUTING.md)
  and the repository's module-level Rust documentation.
- **Release or operate:** [the release guide](https://github.com/appunni-m/coverage-mcp/blob/main/docs/releasing.md),
  [`SECURITY.md`](SECURITY.md), and [`SUPPORT.md`](SUPPORT.md).

## Install and first success

Requirements:

- Rustup; the checkout pins Rust 1.85.1 (the declared 1.85 MSRV) with Cargo,
  rustfmt, Clippy, and LLVM tools;
- Git for repository identity and worktree lineage;
- a host supported by the bundled DuckDB build. The release workflow targets
  native archives for macOS and Linux on ARM64 and x86-64; other hosts need a
  working Rust/Cargo toolchain or an independently configured binary.

Install from a checkout:

```sh
cargo install --path . --locked
coverage-mcp --version
```

Normal MCP clients should launch `connect`; they do not need a separately
started daemon. For direct HTTP or dashboard development, run the daemon
without installing it:

```sh
cargo run --package coverage-mcp --locked -- serve
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
cargo run --package coverage-mcp --locked -- connect --repo /absolute/path/to/repository
```

The first Cargo invocation may compile bundled DuckDB and take longer than an
MCP client's startup timeout. Warm the target before connecting if needed:

```sh
cargo run --package coverage-mcp --locked -- --version
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
cargo install coverage-mcp --version '=0.10.0' --locked
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
stdio bridge, run `cargo run --package coverage-mcp --locked -- serve` for a checkout or
`coverage-mcp serve` for an installed binary, then point the client at
`http://127.0.0.1:59471/mcp/`. The daemon maintains one common registry at
`~/.coverage-mcp/common.duckdb` by default and lazily opens each canonical Git
repository's `.coverage-mcp/coverage.duckdb`, or the current centralized
project location under `~/.coverage-mcp/projects/` when that location is
selected. An incompatible database is not migrated or repaired; the server
creates a fresh schema when opened against a disposable store. Set
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
validation error and is never silently treated as omitted. Unknown public
arguments are validation errors, including unknown fields inside the structured
review selectors. The HTTP MCP route
also requires a JSON object with a string `method`; malformed JSON, malformed
headers, and missing required fields return an explicit error response.
HTTP JSON bodies are capped by `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` (1 MiB by
default). Coverage ingestion rejects reports larger than 64 MiB and rejects
malformed numeric fields instead of converting them to zero or silently
dropping them.

## MCP Usage Guide

The server's `initialize` instructions and `tools/list` contract are sufficient
for an agent to operate without this README. The public MCP surface is exactly
the seven tools listed below. This section documents task selection and wire
shapes; approval, polling, freshness, lineage, and reporting policy belong in
the marketplace's `plugins/testing/skills/coverage-review/SKILL.md` workflow in
the [companion marketplace repository](https://github.com/appunni-m/codegen-marketplace).

### Why use Coverage MCP instead of grepping a report?

Grepping LLVM JSON, LCOV, or another report is valid for a quick point-in-time
answer. Coverage MCP is valuable when the question is about change, history,
or trustworthiness:

| Raw report grep | Coverage MCP |
| --- | --- |
| One file at one moment | Immutable snapshots with repository, branch, commit, suite, and report provenance |
| Caller must infer which run produced it | Durable approved test runs, artifact fingerprints, freshness states, and explicit run ids |
| Repeated JSON keys and line records | Grouped ranges, compact symbols, response byte/word limits, and bounded source evidence |
| Manual diff and baseline selection | Changed executable lines, branch gaps, compatible baselines, parent/ref lineage, and reasons for limited claims |
| Custom history scripts | Latest two detailed points plus an aggregate window by default |
| No execution semantics | Human approval, polling, cancellation, idempotency, and unchanged-run reuse |

Use `coverage_import` when a report was produced outside the managed runner.
It records the report as external evidence; it does not pretend that the file
was produced by `run_test`.

### Contract-level sequence

The server advertises the required first call, safe execution sequence,
response budgets, and evidence rules in `initialize`. Use the task table below
to select the smallest projection, then carry the returned run, snapshot,
baseline, file, and source identifiers into the next request. The server never
requires a raw report dump to answer a supported review question.

### Coverage review tasks

| Question | Request |
| --- | --- |
| Did new code get covered? | `task="change"`; choose `baseline.kind="worktree_base"`, `"parent_commit"`, `"ref"`, `"previous_snapshot"`, `"explicit"`, or `"none"`. |
| What happened over time? | `task="history"`; default is two detailed snapshots and a ten-point summary. |
| What should be tested next? | `task="insight"`; returns ranked uncovered regions without a raw line dump. |
| What source surrounds selected gaps? | `task="source"` with up to ten grouped `{file_path,start,end}` ranges. |
| Which exact change records are needed? | `task="audit"` or `representation="audit"`; use deliberately because it is larger. |
| Need a bounded overview? | `task="all"`; combines change, history, and insight under one response budget. |

Example change request:

```json
{
  "task": "change",
  "measurement": {"run_id": "<terminal-run-id>"},
  "baseline": {"kind": "parent_commit"},
  "limits": {"max_files": 10, "max_regions": 20, "max_words": 600, "max_bytes": 12000},
  "representation": "review"
}
```

`measurement.snapshot_id` is explicit when already known. `measurement.run_id`
resolves the first ingested snapshot attached to that run. A missing or stale
measurement is reported as `not_measured` or `limited`; the server never turns
it into an `unchanged` claim.

`claim_status` is one of `supported`, `limited`, `not_measured`, `stale`, or
`invalid`. A status other than `supported` must be reported with the server's
`reasons` rather than summarized as a coverage result.

### Token-efficient representations

The default `review` representation is readable and groups related ranges.
For large diffs, request `compact`. It emits each file path once per file group
and uses field-specific range legends plus short ranges such as
`[120,127,"!"]`:

| Symbol | Meaning |
| --- | --- |
| `+` | added executable line covered |
| `!` | added executable line uncovered |
| `~` | changed line has a branch gap |
| `.` | added line is non-executable |
| `?` | coverage unavailable or unmeasured |

The `changed_code` legend applies to added executable-line ranges. The separate
`regions` projection uses the same compact shape but a different legend:

| Symbol | Region meaning |
| --- | --- |
| `+` | region coverage improved or region is newly measured |
| `!` | previously measured region regressed |
| `-` | region was removed from the comparison |
| `~` | region exists in both snapshots with changed coverage |

`audit` keeps exact records and is reserved for verification or export. Do not
ask for audit data merely to decide what to test next. History intentionally
returns two detailed points plus a compact aggregate for the next eight points
of the ten-point default window; increase limits only when the question needs
it. All responses are also bounded by `max_words` and, for review/run/import,
`max_bytes`.

In compact change reviews, file metrics use `p` for path and `l`/`b`/`f`/`r`
arrays for line/branch/function/region baseline, current, and delta values;
`file_legend` defines those array positions once.

### Public tool reference

| Tool | Purpose and important inputs |
| --- | --- |
| `project_context` | Read project identity, freshness, approved commands, active runs, and latest run. Paginate commands with `cursor`. |
| `register_test_command` | Store one exact human-approved command and its artifacts. `human_approved` must be true. |
| `run_test` | Submit an approved command. Prefer `wait=false`; use `idempotency_key` and the default `reuse_if_unchanged=true`. |
| `run_review` | Read one explicit `run_id`; `view="status"` returns durable state and terminal coverage, while `view="logs"` returns bounded literal matches. |
| `cancel_run` | Request cancellation for a run the user no longer wants. |
| `coverage_import` | Import a repository-relative external report with format, suite, branch, commit, and base provenance. Follow with `coverage_review`. |
| `coverage_review` | Bounded change/history/insight/source/audit/all analysis with structured measurement, baseline, source, history, limits, and representation selectors. |

Every successful tool uses this envelope:

```json
{
  "context": {"repo_key": "…", "checkout_path": "…", "suite": "…", "schema_revision": 9},
  "data": {},
  "page": null
}
```

### Compatibility and errors

Only the seven tools in the public reference above are part of the executable
MCP contract. REST resources and typed internal lineage operations are separate
from the MCP tool inventory.

Validation errors are not silently retried. Correct the request when a type,
range, lineage selector, budget, cursor, or path is invalid. Retry a read with
backoff only for busy, timeout, or transient runtime failures. Notifications
receive no response. HTTP and native stdio share this dispatcher and therefore
have the same contract.

Resources:

- `coverage://context` — current project context, policy, commands, and runs;
- `coverage://snapshot/{snapshot_id}/summary` — one compact immutable snapshot.


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

Development and release commands require a source checkout. Published Cargo
packages intentionally omit tests, fixtures, evaluator cases, maintainer
scripts, generated evidence, and internal release plans. See the
[source-checkout contribution guide](https://github.com/appunni-m/coverage-mcp/blob/main/CONTRIBUTING.md)
for the reproducible format, test, lint, coverage, migration, documentation,
and release commands, and the [release guide](https://github.com/appunni-m/coverage-mcp/blob/main/docs/releasing.md)
for artifact verification.

The published crate contains the runtime and its production-facing
documentation; it does not contain the repository's test corpus or opt-in
agent evaluator.

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
