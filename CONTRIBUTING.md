# Contributing

Use a Git checkout for development. For installation and a first query, see the
[README](README.md). This project is maintained by Appunni M; proposals and
reviews happen through issues and pull requests. Discuss significant changes to
public contracts, storage, security, licensing, or governance before implementation.
Follow the [code of conduct](CODE_OF_CONDUCT.md).

## Build and verify

Install Rustup, Git, GNU Make 3.81 or newer, and a C++ build toolchain. The checked-in
`rust-toolchain.toml` selects Rust 1.85.1, rustfmt, Clippy, and LLVM tools. Install
the coverage tool using the same version as CI:

```sh
rustup toolchain install stable --profile minimal
cargo +stable install cargo-llvm-cov --locked --version 0.8.7
cargo build --locked
make ci
```

`make ci` checks formatting, Clippy, tests, library coverage, rustdoc, fixture
evidence, and the bundled release build. The coverage gate requires 100% lines,
functions, and regions for the library; `src/main.rs` is tested through subprocess
smoke tests and excluded from LLVM's aggregate. This is a verification policy,
not proof that every input or platform works.

For a focused check, use `make lint`, `make test`, `make coverage`, `make docs`,
`make release`, or `make check-diff`. `make help` lists the full interface.
`make test-ci-compile` and `make test-ci` separate compilation from serial test
phases; their default compilation limit is one Cargo job. Increase
`COVERAGE_MCP_TEST_BUILD_JOBS` only when memory permits.

Verification commands use `DUCKDB_DOWNLOAD_LIB=1` and `--no-default-features` to
load the matching prebuilt DuckDB library; a cold cache needs network access.
Default builds and installations bundle DuckDB. Run `make test-bundled` when
changing linkage. Do not substitute a faster verification build for the bundled
release check. CI definitions in `.github/workflows/` are the executable authority.

## Source map and contracts

```text
Existing test runner → coverage report → query engine → CLI / MCP / HTTP / dashboard
```

- `src/query.rs` reads complete artifacts or saved measurements, groups native
  observations, and bounds results. It never executes tests or returns source.
- `src/mcp.rs` owns tool schemas, initialization guidance, and the dispatcher
  shared by HTTP and stdio. Update the [MCP reference](docs/mcp-reference.md),
  lowest-layer test, and a public transport test when this contract changes.
- `src/http.rs` and `src/dashboard.html` expose the same query results plus
  optional archival administration. Update the [HTTP reference](docs/http-api.md)
  and exercise the dashboard when its behavior changes.
- `src/parser.rs`, `src/models.rs`, and `src/composite.rs` retain provider metrics,
  identities, and missing-evidence states. Display groups do not redefine totals.
- `src/storage.rs` owns DuckDB history and compaction. `src/service.rs` and storage
  also retain public Rust compatibility APIs for embedders, including execution
  and source projections; those APIs have no 0.16 MCP/REST routes. Reopening a
  store interrupts unfinished legacy jobs without running them.
- `src/main.rs`, `src/lock.rs`, and `src/pool.rs` own connector recovery, process
  leases, and database access. Test reopen, compaction, and ownership when changing
  storage or lifecycle behavior.

Document public Rust inputs, errors, side effects, and invariants at their
entry points. Run `cargo test --doc --no-default-features --locked` with
`DUCKDB_DOWNLOAD_LIB=1` when changing examples. Repository docs describe the
server contract; test execution uses the existing repository commands.

## Fixtures and evaluations

`tests/fixtures/manifest.yaml` is the input-only conformance specification.
`make migration-parity` records a manifest/revision-bound marker;
`make migration-benchmark` checks compaction correctness before timing fresh
stores. `make migration-status` combines those results with coverage into
`target/migration/status-report.json` and ignored `docs/generated/` pages.
Missing, stale, dirty, or independently unverified evidence stays `not_proven`.
Passing Rust conformance is not proof of parity with an unavailable source oracle.

`make mcp-evals` runs deterministic query outcomes. The `evals/` directory contains
its cases, the existing-test trial, and a dated measurement note. Reports and logs
belong under `target/`, not in commits. Name the measured dimension, target set,
and command when reporting coverage or performance.

## Pull requests and releases

Explain the user-visible change, contract/storage impact, and checks run. Update
[the changelog](CHANGELOG.md) for user-visible changes. Exclude credentials,
private source/reports, `.coverage-mcp/`, database/WAL files, and build output.
Security reports follow [SECURITY.md](SECURITY.md).

Maintainers publish using [the release guide](docs/releasing.md). Release tags
are immutable; documentation and the marketplace pin must match the released
runtime. Maintainer changes are announced publicly unless private safety details
prevent that. If maintenance ends, identify a successor or mark the project
unmaintained.
