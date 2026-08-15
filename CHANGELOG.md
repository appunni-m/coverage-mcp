# Changelog

Notable user-visible changes are documented here. Coverage MCP follows Semantic Versioning after 1.0.0; before 1.0,
minor versions may contain breaking public-contract changes.

## 0.8.7 - 2026-08-15

### Fixed

- Made `coverage-mcp connect` automatically replace an older owned daemon on
  the fixed loopback port. New daemons use an authenticated graceful handoff;
  the first upgrade can also stop a verified legacy owner using its active
  daemon lease metadata. Recovery refuses unknown listeners, different common
  databases, equal-version incompatibilities, and downgrade attempts.
- Added per-process identity and handoff capability fields to `/health`, kept
  the handoff capability out of public responses and lock diagnostics, and
  restricted lock-file permissions on Unix. Recovery also handles an older
  daemon that becomes healthy during connector startup. HTTP and stdio clients
  remain concurrent and never acquire the daemon ownership lease.
- Pinned checkout builds and formatting to the same Rust 1.85.1 toolchain used
  by CI so local release gates enforce the crate's declared MSRV syntax.

## 0.8.6 - 2026-08-15

### Changed

- Reduced the locked dependency graph by removing unused DuckDB, CLI, URL, and
  error-derive features and dependencies. Quality, test, coverage, rustdoc,
  and package-verification jobs now download the exact matching DuckDB release
  instead of independently compiling its C++ amalgamation; default builds,
  installs, and release binaries remain self-contained with bundled DuckDB.
- Made the compaction worker actually wait on its existing wakeup signal, so
  store closure and settings updates no longer wait for a one-second polling
  sleep. This removes the repeated shutdown delay from database-heavy tests
  and from normal daemon shutdown.
- Documented the one-time crates.io bootstrap and made tagged releases
  idempotent when that exact version was already published manually. CI keeps
  long-lived registry tokens out of GitHub and uses trusted publishing for
  subsequent versions.

## 0.8.5 - 2026-08-15

### Fixed

- Allow up to 90 minutes and one Cargo compilation job for each isolated test
  and coverage release runner. Cold bundled-DuckDB debug and instrumented
  builds can exceed 45 minutes and starve the Actions agent when compilation is
  unconstrained even though warmed main-branch lanes complete in under ten
  minutes; the profiles remain isolated and retain all existing gates.

## 0.8.4 - 2026-08-15

### Fixed

- Fetch locked project dependencies before the intentionally offline coverage
  command in both CI and release workflows, so an isolated tag runner does not
  depend on the contents of a previously restored Cargo cache.
- Added the same public coverage totals, file, and exact-line diagnostics to
  release jobs that are already emitted by main-branch CI.

## 0.8.3 - 2026-08-15

### Fixed

- Split release quality, tests, coverage, package verification, binary builds,
  and evidence assembly across isolated hosted runners. This preserves every
  release gate without combining multiple bundled-DuckDB build profiles in one
  runner and starving the GitHub Actions agent.
- Publish only after the isolated clean package-verification job succeeds, and
  avoid recompiling bundled DuckDB after minting the short-lived crates.io
  trusted-publishing token.

## 0.8.2 - 2026-08-15

### Fixed

- Made the Unix completed-child fallback use the same non-generic wait-probe
  function body in production and injected tests, eliminating a Linux-only
  coverage mapping divergence without changing process-group termination.
- Added failure-only CI annotations and text evidence for the exact source
  files and lines behind a coverage-gate failure.
- Serialized the unchanged all-target CI test set in one Cargo process so a
  cold bundled-DuckDB build cannot starve the hosted runner.

## 0.8.1 - 2026-08-14

### Fixed

- Made the 100% line-coverage gate deterministic on the Rust 1.85.1 MSRV by
  removing failure-only assertion source regions and isolating the already
  exercised background-compaction maintenance path, including the
  platform-dependent Unix process-group fallback.
- Clarified that the daemon lease belongs only to the daemon process; HTTP and
  stdio clients remain independent concurrent connections.

## 0.8.0 - 2026-08-14

### Changed

- Completed the runtime migration to a single Rust binary for HTTP, native
  stdio MCP, REST, dashboard, storage, parsers, managed runs, and background
  compaction.
- Unified HTTP and stdio MCP JSON-RPC dispatch so inventory, safety behavior,
  resource reads, tool calls, notifications, and errors share one contract.
- Restored `connect` as a repository-selecting stdio bridge that starts or
  reuses one loopback daemon. Only the daemon process holds its ownership
  lease; concurrent Codex clients never lock one another. Explicit `--db`
  remains standalone.
- Restored repository-local project databases in shared-daemon mode, with a
  compatibility fallback for centralized Rust-era project stores.
- Synchronized startup, direct-HTTP, ownership, configuration, release, and
  generated migration documentation around the schema-7 shared-daemon flow.
- Documented the downstream Cargo bootstrap contract: exact published version,
  one short-lived installer lock, cached binary reuse, and a daemon-only
  process-ownership lease that never serializes HTTP or stdio clients.
- Added per-project background compaction of older coverage detail with
  project-creation defaults, dashboard/API edits, manual passes, and durable
  byte/status metrics.
- Added strict Cargo formatting, clippy, test, line-coverage, rustdoc, and
  supply-chain verification guidance.
- Added OS-backed single-instance and per-database leases, bounded DuckDB
  pooling, connection checkout deadlines, interruptible query deadlines,
  bounded HTTP/MCP concurrency, and graceful signal shutdown.

## 0.7.1 - 2026-07-21

### Changed

- Renamed the MCP run status fetch tool from `test_run` to `get_run_data`.
- Added a separate `cancel_run` tool so read-only status fetches and mutating cancellation are explicit.
- Clarified that agents must wait for the ETA-aware `poll_after_ms` before fetching non-terminal run data again.
- Removed the retired schema-revision 6 MCP implementation and migration reference.
- Replaced offset-bearing continuation tokens with record-anchored opaque cursors.
- Disambiguated duplicate cursor anchors and made defensive collection caps fail explicitly instead of losing records.
- Made public response models reject undeclared fields.
- Expanded packaging metadata and the supported runtime/toolchain policy.
- Separated the embedded dashboard document and storage projections from transport and persistence code.
- Restricted the daemon to loopback interfaces and added browser security headers and trusted-host validation.
- Hardened concurrent lazy startup against transient health-probe timeouts.
- Added explicit MCP safety annotations so Codex can invoke read-only tools without mutation approval.

### Added

- Contributor, governance, support, conduct, and security policies.
- Reproducible token-savings benchmark inputs, connector verification, release documentation, and Trusted Publishing.
- PEP 561 metadata for downstream type checkers of the legacy Python package.

## 0.7.0 - 2026-07-18

- Consolidated the agent interface into ten schema-revision 7 tools.
- Added word-budgeted responses, cursor pagination, compact-by-default projections, and strict lineage validation.
- Reworked coverage-file queries around grouped gaps and normalized multi-range source selection.
- Updated the dashboard to use the shared schema-revision 7 service projection.
