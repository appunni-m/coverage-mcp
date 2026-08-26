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
dashboard and REST API, and provides the consolidated schema-15 projections over
loopback HTTP and native MCP stdio.

The project is designed for one user-level daemon shared by agents and Git
worktrees. It does not bind to a public interface and it does not require a
frontend build or a separate language runtime.

## Status

The Rust implementation is the only runtime and the checked-in Rust test suite
is the source of truth. The public contract is schema revision 15 with eight
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
artifacts, and the project compaction policy. Its main view is one bounded
dashboard projection, and the foreground page subscribes to a server-sent event
stream. It checks for new snapshots, runs, or compaction work at most once every
10 seconds; hidden tabs close the stream and pause automatic refreshes.

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
cargo install coverage-mcp --version '=0.15.0' --locked
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
the eight tools listed below. This section documents task selection and wire
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
| Custom history scripts | Latest two compatible points plus an aggregate over those points by default |
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

## Coverage review

### Parameterized runs and automatic incremental review

Register one human-approved command once, then reuse its ID or name for
case-specific runs. `run_test.arguments` supplies optional runtime strings;
the server shell-quotes them and replaces one approved `{{args}}` placeholder,
or appends them to the approved command. A different filter or test subset
does not require registering another command.

For example, after approving a command such as `cargo test -- {{args}}`, run a
smaller case against an explicit fixed base:

```json
{
  "command_ref": "coverage-tests",
  "arguments": ["--filter", "parser::handles_empty_input"],
  "execution": {"mode": "incremental", "label": "parser empty input"},
  "baseline": {"kind": "explicit", "snapshot_id": "<fixed-base-snapshot-id>"},
  "reuse_if_unchanged": true,
  "idempotency_key": "parser-empty-input-v1"
}
```

`execution.identity` is optional. The server persists a fingerprint of the
mode, arguments, and baseline, so unchanged-run reuse and idempotency cannot
cross case boundaries. Use a different idempotency key for each materially
different argument list. Obtain the fixed base ID from a completed full run's
`data.coverage_ingest.snapshot_ids[0]` or a compatible imported snapshot; the
server never infers it from the latest or previous snapshot.

An incremental `run_test` automatically attaches `incremental_review` after
terminal ingestion. If the run produces several ordinary artifacts, all
`data.coverage_ingest.snapshot_ids` are one selected measurement set. The
primary result is the deduplicated union of the fixed base and that set, so a
selected case cannot lower the displayed aggregate simply because it did not
hit every base region. `status="pending"` means the run is still active;
`status="not_measured"` and `reasons` mean no valid measurement is available.
`run_review(view="status")` returns the same durable review. This automatic
review is the normal path for fewer-test-case runs.

The incremental response deliberately has two blocks. `incremental.aggregate`
and its `metric_deltas` are the final base ∪ selected-run coverage. The nested
`incremental.diff` is a diagnostic current-versus-base replacement comparison;
for a selected subset, baseline hits missing from that run are
`not_observed`, not regressions. A complete snapshot can still report genuine
regressions.

Use standalone `coverage_review(task="incremental")` when the ordinary
snapshots, ordinary snapshot set, or composite snapshots already exist and only
a stored comparison is needed. It never reruns tests, invokes a parent runner,
or reparses a report, and it requires an explicit current measurement plus
`baseline.kind="explicit"` and the matching `snapshot_id` or
`composite_snapshot_id` selector.

### Composite production coverage

Use a composite snapshot when one managed run produces the production coverage
for Rust/WGSL, Python, and JavaScript. Register the command once with one
required inventory artifact and one required descriptor for every package or
backend variant. Later full or incremental cases reuse that registration and
only change `run_test.arguments`, `execution`, and the explicit baseline.

Each coverage descriptor has the following contract:

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
    "toolchain_versions": {"coverage.py": "7.x"}
  }
}
```

The inventory is authoritative for the denominator. Its
`coverage-mcp-inventory-v1` JSON contains `mapping_version`, the producing
`source_revision`, and `entries` with `logical_source_id`, repository-relative
`path`, `package`, `language`, `role` (`production` or `catalogue`), a SHA-256
`source_hash`, required `variants`, `expected_formats`, and canonical
`regions`. Only `production` entries contribute regions. Rust/native/WASM
LLVM, coverage.py executable lines, and Istanbul statement spans are mapped by
their declared format. WGSL or other span-rich producers should emit
`coverage-mcp-regions-v1`, whose artifact also declares `source_revision` and
per-region `source_hash` plus a stable discriminator.

Logical-source aliases are the only deduplication mechanism. Matching paths,
bytes, packages, or filenames never merge regions. Shared source regions may
therefore be counted once while every declared variant still has to be present,
fresh, well-formed, and source-compatible. Missing, stale, malformed, source-
mismatched, or unsupported evidence leaves the composite `incomplete` and
never reduces its denominator. The composite records covered, uncovered,
unmeasured, missing-artifact, stale-evidence, and source-mismatch states with
remediation buckets such as test gap, instrumentation gap, generated-variant
gap, and source mismatch.

After the run finishes, save `data.composite_snapshot_id`; ordinary child
reports remain in `data.coverage_ingest.snapshot_ids`. The composite projection
returns exact `covered_regions`, `total_regions`, `coverage_percent`, and only
three summaries: `rust` (including WGSL), `python`, and `javascript`.

```json
{
  "task": "incremental",
  "measurement": {"composite_snapshot_id": "<case-composite-id>"},
  "baseline": {
    "kind": "explicit",
    "composite_snapshot_id": "<fixed-composite-id>"
  },
  "representation": "compact",
  "limits": {"max_regions": 20, "max_test_ids": 10, "max_words": 600, "max_bytes": 12000}
}
```

Composite incremental review compares only stored canonical-region rows and
child named-test observations; it never reruns the fixed base, invokes a parent
runner, or reparses either report. It requires the same repository, mapping
version, and non-null inventory hash. `newly_covered` is the incremental gain;
`regressed`, `hit_count_only`, `added`, and `removed` are separate categories.
Source and audit requests accept the same composite measurement selector, and
the bounded response exposes provenance, exact component deltas, truncation,
and named-test attribution status. If a producer has no named observations,
attribution is explicitly `unavailable`; aggregate coverage remains valid.

Compaction does not rewrite composite rows. Ordinary child snapshots may be
compressed, but their file, line, and named-test projections are restored from
the zstd payload when composite attribution is requested. A fixed composite
base therefore remains immutable and comparable after compaction; retaining its
child reports as `detail_retention="incremental_base"` is still recommended for
operational cost and retention control.

| Question | Request |
| --- | --- |
| Did new code get covered? | `task="change"`; choose `baseline.kind="worktree_base"`, `"parent_commit"`, `"ref"`, `"previous_snapshot"`, `"explicit"`, or `"none"`. |
| What did this case add without rerunning the full base? | `task="incremental"` with an explicit ordinary snapshot, an ordinary snapshot set produced by a run, or a composite measurement and `baseline.kind="explicit"` plus the matching baseline selector. The server unions stored detail immediately and keeps the replacement diff separate. |
| What happened over time? | `task="history"`; default is the last two compatible points and an aggregate over those two points. |
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
  "limits": {"max_files": 3, "max_regions": 5, "max_words": 600, "max_bytes": 12000},
  "representation": "review"
}
```

Example incremental request:

```json
{
  "task": "incremental",
  "measurement": {"snapshot_id": "<case-snapshot-id>"},
  "baseline": {"kind": "explicit", "snapshot_id": "<fixed-base-snapshot-id>"},
  "limits": {"max_files": 3, "max_regions": 5, "max_test_ids": 10, "max_words": 600, "max_bytes": 12000},
  "representation": "compact"
}
```

Incremental review never selects an implicit previous snapshot and never runs
tests, invokes a parent runner, or reparses either report. It compares stored
rows from the fixed baseline with the union of one or more immutable ordinary
snapshots, allows the suites and case-specific execution arguments to differ,
and requires the same repository and normalized coverage format. The primary
result includes `aggregate` and `metric_deltas` for the deduplicated union;
`coverage_gain` reports new line, branch, function, and region identities.
The nested `diff` retains replacement-style grouped categories and
`detail_source` for each side (`relational` or `compacted_payload`). In a
`selected_subset` measurement, baseline identities absent from the selected
run are `not_observed` and `regressed` is zero; `complete_snapshot` is the
scope that supports real regression claims. Suite, branch, commit, command,
cwd, and execution case remain provenance only; they are never test identity.
A missing named-test projection sets attribution to `status="unavailable"` and
limits only attribution; aggregate coverage remains measured.

For a long-lived fixed base, import or ingest it with
`detail_retention="incremental_base"`. This prevents automatic compaction of
that snapshot. Retention is a safety/performance preference rather than a
correctness requirement: incremental comparison also restores files, lines,
and named-test observations from the compressed payload if compaction has
already occurred. Do not manually delete either snapshot or its compacted
payload while the base is in use.

`measurement.snapshot_id` is explicit when already known. `measurement.run_id`
may resolve to several ordinary snapshots when one run declares multiple
coverage artifacts; all are merged and listed in `current_snapshot_ids`. A
missing, malformed, or stale measurement is rejected or reported as
`not_measured`/`limited` as appropriate; the server never turns it into an
`unchanged` claim.

`claim_status` is one of `supported`, `limited`, `not_measured`, `stale`, or
`invalid`. A status other than `supported` must be reported with the server's
`reasons` rather than summarized as a coverage result.

### Token-efficient representations

The default `compact` representation emits each file path once per file group
and uses field-specific range legends plus short ranges such as
`[120,127,"!"]`:

Request `review` when readable grouped ranges are worth the additional context,
or `audit` when exact records are required.

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
ask for audit data merely to decide what to test next. History defaults to the
last two compatible points and an aggregate over those two points; older points
require an explicit `history.summary_window` increase. Compact insight items
return at most three `[start,end,line_count]` ranges per target, plus
`region_count` and `regions_truncated`; use audit only when exact region
records are required. All responses are also bounded by `max_words` and, for
review/run/import/duplicate-group, `max_bytes`.

In compact change reviews, file metrics use `p` for path and `l`/`b`/`f`/`r`
arrays for line/branch/function/region baseline, current, and delta values;
`file_legend` defines those array positions once.

### Public tool reference

| Tool | Purpose and important inputs |
| --- | --- |
| `project_context` | Read compact project identity, freshness, up to eight approved command summaries, capped active runs, and latest run. The `max_words` budget covers the complete data projection; paginate commands with `cursor` only when needed. Use `detailed=true` only for an audit. |
| `register_test_command` | Store one exact human-approved command and its artifacts. `human_approved` must be true; the default response is an id/name summary, and `detailed=true` returns command fields. |
| `run_test` | Submit one approved command. Prefer `wait=false`; pass optional case-specific `arguments` without registering another command, use `idempotency_key`, and keep the default `reuse_if_unchanged=true`. For incremental runs, pass `execution.mode="incremental"` plus `baseline.kind="explicit"` and exactly one ordinary `snapshot_id` or `composite_snapshot_id`; identity is optional because the server fingerprints mode, arguments, and baseline. After ingestion, the response includes automatic `incremental_review`; multiple ordinary artifact snapshots are unioned and listed in `current_snapshot_ids`, while composite runs also return `composite_snapshot_id`. |
| `run_review` | Read one explicit `run_id`; `view="status"` returns compact durable state, terminal ingestion evidence, and the automatic incremental review for incremental runs, while `view="logs"` returns bounded literal matches. Use standalone `coverage_review(task="incremental")` only for two already stored ordinary or composite measurements. |
| `cancel_run` | Request cancellation for a run the user no longer wants. |
| `coverage_import` | Import a repository-relative external report with format, suite, branch, commit, and base provenance. It accepts the same optional execution identity and `detail_retention="incremental_base"` for a fixed base. Follow with `coverage_review`. |
| `coverage_review` | Bounded change/incremental/history/insight/source/audit/all analysis. It accepts ordinary snapshot IDs or immutable composite IDs; compact defaults to three files, five regions, and ten affected test IDs; history returns two points, and insight returns three ranges per target plus truncation metadata. Use incremental for a baseline-union-current result plus its separate scope-aware snapshot diff; ordinary run selections may contain multiple artifact snapshots. Use audit for exact records. |
| `find_duplicate_coverage_tests` | Read-only, bounded candidate reduction for named tests with exactly equal covered line/branch/function observation sets. Defaults to the latest snapshot, ten groups, and ten names per group; follow `page.next_cursor` for more groups. |

### Exact duplicate coverage candidates

`find_duplicate_coverage_tests` is designed for large test inventories, including
100,000-test reports, without placing the full inventory in the agent context.
The server groups in DuckDB by the complete canonical observation set and emits
only a small page of names plus global counts. The default MCP request is:

```json
{
  "max_groups": 10,
  "max_tests_per_group": 10,
  "max_words": 600,
  "max_bytes": 12000
}
```

Each observation is compared by `kind`, repository-relative `file_path`,
one-based `line_number`, and the exact `region_key`. Current LCOV support
records covered `line`, `branch`, and `function` observations from non-blank
`TN:` records. Execution hit counts are ignored, so two tests with the same
covered set but different counts remain in one group. Tests with zero observed
coverage are retained as empty signatures and identified by
`coverage_observation_count: 0`.

The response has `data.status="measured"` when named observations are present,
`summary` counts, and `duplicate_test_groups` entries containing the returned
names, full `test_count`, and truncation state. Reports without named per-test
observations return `data.status="unavailable"`; they do not claim that no
duplicates exist. The REST equivalent is
`GET /api/duplicate-coverage-tests` with the same selectors and limits.

This is exact coverage equivalence, not logic equivalence. It does not inspect
test source, assertions, inputs, side effects, or dependencies, and it never
deletes tests. Treat every group as a review candidate before removing or
merging any test. Compaction preserves the named test and observation data, so
the result remains available after older snapshot detail is compressed.

`project_context` budget and truncation fields:

| Field | Meaning and impact |
| --- | --- |
| `data.active_runs_truncated` | `true` means the response shows only the first ten active runs. The list is not a proof that no other active work exists; `detailed=true` does not lift this cap. |
| `page.reserved_words` | Serialized-word count reserved for the fixed project/latest-run/active-run summary before commands are paged. |
| `page.word_count` | Serialized-word count for the returned command page only. It is not the complete response count. |
| `page.response_word_count` | `reserved_words + word_count`: the combined project-context budget count, which is bounded by `page.max_words`. |
| `page.next_cursor` | Opaque continuation for more commands only; it does not paginate active runs. |

Compact `coverage_review` fields are also intentionally bounded. A history
point keeps identity, lineage, and line/branch/function/region rates while
omitting repeated covered/total counters. An insight target's `regions` uses
`[start,end,line_count]`, `region_count` is the untruncated number of ranges,
and `regions_truncated=true` means only the first three ranges were returned.
The audit representation keeps the exact object records and does not add
these compact-only fields.

The compact defaults are intentional: raw command, path, artifact, and run
provenance fields require an explicit `detailed=true`; readable or exact
coverage detail requires a `representation` choice, and logs require
`run_review(view="logs")`. Terminal incremental run state includes the
automatic `incremental_review`; use standalone `coverage_review(task="incremental")`
when comparing stored ordinary snapshot sets or composite snapshots
independently of execution.

Every successful tool uses this envelope:

```json
{
  "context": {"repo_key": "…", "checkout_path": "…", "suite": "…", "schema_revision": 15},
  "data": {},
  "page": null
}
```

For JSON-RPC `tools/call`, `structuredContent` is canonical. The `content`
array contains only a short compatibility hint and does not repeat the JSON
envelope; clients should read `structuredContent` for the bounded result.

### Compatibility and errors

Only the eight tools in the public reference above are part of the executable
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
- `coverage://composite/{composite_snapshot_id}/summary` — one bounded
  immutable composite summary and its exact inventory/provenance audit fields.


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
- `POST /api/ingest` — report ingestion; accepts `execution={mode,identity,label}` and `detail_retention="incremental_base"` for case-specific reports and fixed bases;
- `GET /api/dashboard` — one bounded dashboard projection. It accepts
  ordinary `snapshot_id`/`baseline_snapshot_id` and independent composite
  `composite_snapshot_id`/`baseline_composite_snapshot_id` selectors plus
  `history_limit` (2–50). It returns ordinary current/baseline provenance,
  all four metric families, history, incremental categories, bounded
  file/region/test detail, run activity, compaction inventory, and a separate
  exact production-region projection with Rust, Python, and JavaScript
  component summaries;
- `GET /api/dashboard/events` — an SSE stream for the selected repository.
  It emits `ready` with a 10-second refresh interval, then `refresh` only when
  the dashboard revision changes, with heartbeat comments between changes and
  `dashboard-error` events for recoverable refresh failures. The embedded
  dashboard keeps this stream open only while its tab is foregrounded; the
  stream does not run tests or recalculate coverage;
- `GET /api/snapshots`, `/api/snapshots/{id}`, and snapshot file/insight routes;
- `GET /api/snapshots/{id}/functions` — source-mapped LLVM function records with
  line, region, branch, execution, and completeness metrics;
- `GET /api/snapshots/{id}/dependencies` — bounded best-effort Rust `use`/`mod`
  file edges for the source graph; these are source-import edges, not a runtime
  call graph;
- `GET /api/composite-snapshots/{id}` — bounded composite audit summary with
  exact inventory/provenance and component evidence;
- `GET /api/composite-snapshots/{id}/regions` and `/insights` — bounded
  canonical-region detail for composite dashboards and operators;
- `GET /api/incremental?snapshot_id=...&baseline_snapshot_id=...` — bounded
  explicit baseline-union-current coverage with a separate scope-aware
  snapshot diff, compaction detail-source metadata, metric/category deltas, and
  bounded named-test attribution (`max_files`, `max_regions`, and
  `max_test_ids`); the MCP run review accepts all ordinary snapshot IDs
  attached to a multi-artifact run;
- The incremental and compare routes also accept
  `composite_snapshot_id` plus `baseline_composite_snapshot_id`; source-lines
  accepts `composite_snapshot_id` and reads bounded checkout ranges with
  canonical-region annotations. Ordinary and composite selectors cannot be
  mixed.
- `/api/compare`, `/api/changed-lines`, `/api/line-history`, and
  `/api/source-lines` — comparisons and bounded source views;
- `GET /api/duplicate-coverage-tests` — exact, bounded named-test coverage
  groups with `snapshot_id`, `suite`, `max_groups`, `max_tests_per_group`,
  and `cursor` selectors;
- `/api/commands`, `/api/runs`, `/api/artifacts`, and `/api/worktrees` —
  approved execution, retained evidence, and baselines;
- `POST /mcp/` — stateless JSON-RPC MCP over HTTP.

The embedded dashboard is available at `/`; `/graph` opens the dedicated
source-graph page without the dashboard's timeline and maintenance panels.

In common-daemon mode, select a repository with a project ID from
`GET /api/projects`, the `x-coverage-mcp-repo` header, or the documented
`repo_path` query/body field. The daemon rejects non-loopback bind hosts and
untrusted Host headers.

The embedded dashboard has dedicated Incremental and Production region
coverage panels. Select the current report and an explicit fixed baseline, or
select the composite current/base IDs for the combined inventory view. The
incremental panel shows final deduplicated base ∪ selected-run coverage first,
then puts the raw snapshot diff in a separate diagnostic block. It shows
line/branch/function/region gains, not-observed evidence, regression counters
only for complete reports, file deltas, changed regions,
affected named test IDs, each snapshot's suite/format/retention/execution
identity, and whether detail came from relational rows or a compacted payload.
The production panel shows one exact canonical-region percentage, covered and
total regions, Rust (including WGSL), Python, and JavaScript component rates,
blocking reasons, remediation buckets, and the composite IDs used for the
comparison.
The progression chart and timeline keep lines, branches, functions, and
regions visible together; the file drawer adds bounded line detail only when
requested. The page receives SSE refresh events at a 10-second foreground-only
cadence and coalesces concurrent refreshes. It does not infer that a full run
and an incremental run used the same arguments; the run projection displays
persisted case arguments and the server-generated opaque execution fingerprint.

The Source coverage graph is a separate `/graph` page. Its explicit
**Function hierarchy** mode renders project/directory/file nodes and the LLVM
function records mapped to each file. Each function card shows `L
covered/total`, `R covered/total`, `B covered/total`, execution count, source
range, and weakest-metric completeness. Its **File dependencies** mode renders
source files and resolved import edges; these are source-import edges, not a
runtime call graph. Zoom now scales the selected graph canvas from 50% to
200% and never changes the graph's meaning. Non-LLVM reports remain useful in
the file mode, while the hierarchy explicitly reports files without mapped
function records instead of inventing them.

The Compaction health panel distinguishes relational snapshots, compressed
payloads, protected `incremental_base` snapshots, currently eligible work,
original/compressed bytes, saved bytes, and inventory consistency. A compacted
snapshot remains a valid incremental base because comparison restores its
stored files, lines, and named-test observations from the zstd payload.

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

The repository Makefile requires GNU Make 3.81 or newer and a POSIX shell. Use
`make ci` for the complete local gate, `make package` for the clean-tree Cargo
package check used by the release workflow, and `make release` for the bundled
release binary. These targets delegate build and test semantics to Cargo;
`make fmt-fix` is the source-mutating formatter and `make clean` removes only
Cargo build outputs.

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
