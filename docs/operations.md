# Operate and troubleshoot Coverage MCP

This guide is for users who need to install the native binary manually,
connect a non-Codex MCP client, inspect local state, change limits, or recover
from a startup problem. Normal Codex users can begin with
[Getting started](getting-started.md); the marketplace plugin handles the
runtime automatically.

Coverage MCP is a local developer service, not a remotely deployable coverage
platform. It rejects non-loopback bind hosts and has no built-in user
authentication.

## Installation choices

### Codex: install from codegen-marketplace

This is the recommended route:

```sh
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

The plugin pins a compatible runtime and uses verified native release archives
on supported macOS, Linux, and WSL hosts. Do not clone the Coverage MCP source
repository for normal installation.

### Other MCP clients: install the published Cargo package

Install the latest published release from crates.io with Rust 1.85 or newer:

```sh
cargo install coverage-mcp --locked
coverage-mcp --version
```

Cargo builds the default `bundled-duckdb` feature, so the installed executable
does not require a separately managed DuckDB library.

## Command-line interface

The executable has three subcommands:

| Command | Use it for |
| --- | --- |
| `coverage-mcp connect --repo <path>` | Connect one MCP client's standard input/output to the shared daemon and select a Git checkout. This is the normal MCP command. |
| `coverage-mcp serve` | Start the shared loopback HTTP daemon explicitly for API or dashboard use. |
| `coverage-mcp compact --repo <path>` | Run one storage-compaction pass for a repository. |

Run `coverage-mcp <command> --help` for the installed version's exact options.
Invoking `coverage-mcp` without a subcommand starts `serve`.

### Generic MCP client configuration

Use the native connector when the client expects a child process:

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

The connector uses newline-delimited JSON-RPC on standard input and output.
Diagnostics go to standard error. `stdio` is an alias for `connect`.

The connector starts or reuses one user-level HTTP daemon. It does not open the
project database itself, and the daemon remains available after the connector
exits. Several connectors and direct HTTP clients may use that daemon at the
same time.

### Direct HTTP and dashboard use

Start the daemon explicitly when no MCP connector is present:

```sh
coverage-mcp serve
```

Check its health and open the dashboard:

```sh
curl --fail http://127.0.0.1:59471/health
```

- Dashboard: <http://127.0.0.1:59471/>
- Source graph: <http://127.0.0.1:59471/graph>
- MCP over HTTP: `POST http://127.0.0.1:59471/mcp/`

See the [HTTP API reference](http-api.md) for repository selection and REST
routes.

## Runtime and data locations

With default settings, Coverage MCP uses:

| Path | Purpose |
| --- | --- |
| `~/.coverage-mcp/common.duckdb` | User-level registry used by the shared daemon |
| `~/.coverage-mcp/daemon.lock` | OS-backed daemon ownership lease and metadata |
| `~/.coverage-mcp/daemon.log` | Output from a daemon started by `connect` |
| `<canonical-git-root>/.coverage-mcp/coverage.duckdb` | Repository coverage history, approvals, runs, and settings |
| `<canonical-git-root>/.coverage-mcp/coverage.duckdb.lock` | OS-backed project-database lease |
| `~/.coverage-mcp/runtime/<version>/` | Marketplace-managed runtime cache |

Linked Git worktrees resolve to the same canonical repository and share its
project database. A centralized database under
`~/.coverage-mcp/projects/<project-id>.duckdb` is reused only when it already
exists and the repository-local database does not.

Do not commit `.coverage-mcp/`. The databases and retained logs may reveal
local paths, commands, test names, and source-related evidence.

## Configuration

Command-line `serve` options override the corresponding environment variables.
All values are validated at startup; an invalid value fails explicitly.

| Variable | Default | Accepted value or effect |
| --- | ---: | --- |
| `COVERAGE_MCP_HOST` | `127.0.0.1` | Loopback host only; public binding is rejected |
| `COVERAGE_MCP_PORT` | `59471` | TCP port from 0 to 65535 |
| `COVERAGE_MCP_COMMON_DB` | `~/.coverage-mcp/common.duckdb` | Registry path; its parent also holds the daemon lock and log |
| `COVERAGE_MCP_RUN_RETENTION` | `100` | 1–10000 terminal runs retained per command |
| `COVERAGE_MCP_RUN_CONCURRENCY` | `4` | 1–32 managed command workers |
| `COVERAGE_MCP_HTTP_CONCURRENCY` | `16` | 1–128 concurrent HTTP MCP requests |
| `COVERAGE_MCP_DB_POOL_SIZE` | `4` | 1–16 DuckDB connections per open project |
| `COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS` | `5000` | 50–120000 ms to obtain a pooled connection |
| `COVERAGE_MCP_DB_QUERY_TIMEOUT_MS` | `30000` | 100–3600000 ms for one DuckDB operation; must be shorter than the HTTP deadline |
| `COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS` | `60` | 1–3600 seconds for one HTTP request |
| `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` | `1048576` | 1024–16777216 bytes for one JSON HTTP body |
| `COVERAGE_MCP_RUN_LOG_MAX_BYTES` | `10485760` | 1024–1073741824 retained bytes for each stdout/stderr stream |
| `COVERAGE_MCP_COMPACTION_AFTER_DAYS` | `30` | 1–36500 days for newly created project policies |
| `COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS` | `3600` | 60–86400 seconds for a newly created project policy |
| `COVERAGE_MCP_COMPACTION_BATCH_SIZE` | `100` | 1–10000 snapshots for a newly created project policy |

The marketplace bootstrap also accepts `COVERAGE_MCP_RUNTIME_DIR` to relocate
its versioned binary cache. That variable belongs to the plugin bootstrap, not
the server configuration.

## Request and execution limits

The main safety and resource limits are:

- HTTP JSON bodies: 1 MiB by default, configurable in the table above;
- imported coverage reports: 64 MiB;
- retained managed output: 10 MiB for each of stdout and stderr by default;
- MCP response word budget: 600 words by default, configurable per tool call;
- MCP and review byte budget: 12000 bytes by default where the tool exposes it;
- active managed commands: four by default, with an accepted range of 1–32.

Managed commands run in their own process group. Timeout, cancellation, or
daemon shutdown terminates the group so child processes do not continue
silently. Output beyond the configured cap is still drained but is reported as
truncated.

## Storage compaction

Coverage snapshots and completed runs are immutable. To keep older detailed
rows from growing indefinitely, each project can compress eligible snapshot
detail into a zstd payload. Queries restore that detail transparently when it
is needed.

New projects default to:

| Setting | Default | Project-policy range |
| --- | ---: | ---: |
| `compaction_enabled` | `true` | `true` or `false` |
| `compaction_after_days` | `30` | 1–36500 days |
| `compaction_interval_seconds` | `3600` | 1–86400 seconds |
| `compaction_batch_size` | `100` | 1–10000 snapshots |

Environment variables set defaults only when a project is first seen. Change
an existing project's policy from the dashboard or with
`PATCH /api/projects/{project-id}`.

Run one pass manually with:

```sh
coverage-mcp compact --repo /absolute/path/to/repository --older-than-days 30
```

A snapshot retained as an `incremental_base` is protected from automatic
compaction. A snapshot that was already compacted remains comparable because
its detail can be restored from the payload. Never delete a compacted payload
while its snapshot is still needed.

## Upgrade behavior

For Codex, update the marketplace and reinstall the plugin:

```sh
codex plugin marketplace upgrade codegen-marketplace
codex plugin add testing@codegen-marketplace
```

For a manual Cargo installation:

```sh
cargo install coverage-mcp --locked --force
```

Start a new MCP session after upgrading. A newer connector can replace a
verified older daemon while preserving the common and project databases. It
refuses to replace an unknown process, a daemon using another common database,
an equal-version incompatibility, or a newer daemon.

The runtime does not repair an incompatible project database in place. Review
release notes before upgrading across a documented storage-contract change.

## Backup and restore

Coverage MCP does not currently provide an online backup or restore command.
Do not treat a copy of a live DuckDB file without its current WAL state as a
verified backup.

Before making an offline copy:

1. Read `GET /health` and record the daemon PID and database paths.
2. End active managed runs.
3. Send the daemon a normal interrupt or termination signal and wait for it to
   exit cleanly.
4. Copy the common database and the complete `.coverage-mcp/` directory for
   each repository you intend to preserve.
5. Keep file ownership and permissions intact.

Restore only while no Coverage MCP daemon or other DuckDB owner has those files
open. A restore procedure has not been published as cross-platform release
evidence, so test recovery on disposable data before relying on it for an
important history.

## Uninstall and data removal

Remove the Codex plugin with:

```sh
codex plugin remove testing@codegen-marketplace
```

Remove a manually installed binary with:

```sh
cargo uninstall coverage-mcp
```

Neither command removes coverage history, approved commands, the marketplace
runtime cache, or daemon logs. Those remain in the paths listed under
[Runtime and data locations](#runtime-and-data-locations). Stop the daemon and
make any required backup before deleting them. Repository data and user-level
data are separate; deleting one does not remove the other.

## Troubleshooting by symptom

### The plugin is installed but the tools are missing

Run `codex plugin list` and confirm that `testing@codegen-marketplace` is
`installed, enabled`. Start a new Codex task after installation or upgrade;
existing tasks do not gain newly installed plugin capabilities retroactively.

### The client says `connection closed` during `initialize`

First verify that the configured executable exists and starts:

```sh
coverage-mcp --version
coverage-mcp connect --help
```

Then check that the client command is the native `coverage-mcp` executable,
that `connect` is present, and that `--repo` names a Git checkout. Coverage MCP
is not a Python or Node package; do not launch it through `uvx`, `pip`, `python`,
`node`, or `npx`.

For a direct protocol check, send one complete newline-delimited initialize
request:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}' \
  | coverage-mcp connect --repo /absolute/path/to/repository
```

The response should contain `result.serverInfo.name` equal to `coverage-mcp`.
Connector diagnostics appear on standard error.

### Coverage MCP selected the wrong repository

Inspect the `checkout_path` and `repo_key` returned by `project_context`. Start
the MCP connector from the intended checkout or pass an absolute `--repo` path.
Do not continue with snapshots from another repository.

### Startup reports an incompatible or unknown daemon

Inspect these files and endpoints without deleting anything:

```sh
curl --fail http://127.0.0.1:59471/health
```

- `~/.coverage-mcp/daemon.log`
- `~/.coverage-mcp/daemon.lock`
- the `common_db_path`, `daemon_path`, `pid`, `version`, and `schema_revision`
  fields from `/health`

The connector deliberately refuses an unverified listener or database owner.
Stop the competing owner normally. Do not delete a lock, WAL, or database as a
startup workaround.

### A project database reports `resource busy`

Another daemon or library process already owns the repository database. Use
the holder metadata in the error to identify that process, then stop the
competing owner. The lock file itself is only metadata; deleting it does not
release the operating-system lease and can destroy useful diagnostics.

### A run passed but no coverage snapshot appeared

Inspect the run's artifact outcomes. Common causes are:

- the declared report path was not created;
- an old report was left unchanged and was rejected as stale;
- the declared format did not match the file;
- the report exceeded 64 MiB;
- a numeric or structural field was malformed;
- the run was cancelled, timed out, or failed before ingestion.

A passing test state and a successful coverage-ingestion state are separate.

### A review says `not_measured`, `limited`, `stale`, or `invalid`

Read the returned `reasons` and `next_action`. Do not retry an invalid selector
unchanged. Correct type, range, path, cursor, lineage, or baseline errors first.
Use backoff only for an explicit busy, timeout, or transient runtime error.

### The dashboard is empty

Confirm `/health`, then select the intended project. A new project has no
coverage charts until a report has been ingested. The dashboard follows the
newest current snapshot unless you pinned another selection.

If the problem remains, follow [SUPPORT.md](../SUPPORT.md) and remove private
paths, source, logs, tokens, and database contents from any public report.
