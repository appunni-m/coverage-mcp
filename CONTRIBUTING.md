# Contributing

Coverage MCP is a Rust workspace with a public REST/MCP contract, persistent
DuckDB behavior, and an embedded dashboard. Keep those surfaces synchronized
when changing behavior.

## Prerequisites

- Rustup. The checked-in `rust-toolchain.toml` installs Rust 1.85.1, Cargo,
  rustfmt, Clippy, and LLVM tools so plain `cargo` commands match CI and enforce
  the declared Rust 1.85 MSRV;
- Git;
- `cargo-llvm-cov` for the required coverage gate. Install it with a current
  stable toolchain because recent releases require Rust newer than the
  project's 1.85 MSRV:

  ```sh
  rustup toolchain install stable --profile minimal
  cargo +stable install cargo-llvm-cov --locked --version 0.8.7
  ```
- optional `cargo-deny` and `cargo-audit` for supply-chain checks.

Build from a checkout:

```sh
cargo build --locked
```

## Change workflow

1. Read [`docs/architecture.md`](docs/architecture.md) and the relevant
   module documentation.
2. For MCP tool, resource, instruction, schema, or safety changes, update
   `src/mcp.rs`, the affected service/storage layer, tests, and the MCP Usage
   Guide in `README.md` together.
3. For REST changes, update route tests and the README REST surface.
4. For dashboard changes, keep the embedded document dependency-free and run
   its JavaScript syntax check.
5. For storage changes, add migration-safe tests and verify compaction,
   lineage, and database reopen behavior.
6. Keep public Rust APIs documented with errors, panic behavior, and safety
   invariants where relevant. Prefer `Result` for recoverable failures.
7. Update the changelog for user-visible changes.

## Required local gate

Run the same commands used by CI before opening a pull request. Do not override
the pinned checkout toolchain for these commands:

```sh
cargo fmt --all -- --check
DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
DUCKDB_DOWNLOAD_LIB=1 cargo test --workspace --all-targets --no-default-features --locked -- --test-threads=1
DUCKDB_DOWNLOAD_LIB=1 cargo llvm-cov --lib --no-default-features --locked \
  --ignore-filename-regex '/src/main\.rs$' \
  --fail-under-lines 100 --fail-under-functions 100 --fail-under-regions 100 \
  --fail-uncovered-lines 0 --fail-uncovered-functions 0 --fail-uncovered-regions 0 -- --test-threads=1
DUCKDB_DOWNLOAD_LIB=1 RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-default-features --no-deps --locked
cargo build --release --locked
git diff --check
```

Convenience aliases are available through `make lint`, `make test`,
`make coverage`, `make migration-parity`, `make migration-benchmark`,
`make migration-status`, and `make docs`. `make ci` runs the complete gate,
including the fixture-backed migration lanes and generated evidence status.
`make test-ci-compile` bounds Cargo compilation to one job, and `make test-ci`
runs the same library, binary, migration-status, migration parity, and smoke
targets as separate serial phases with per-target retained diagnostics; the
compaction benchmark is a separate phase from migration correctness. `make ci`
runs every phase. Override `COVERAGE_MCP_TEST_BUILD_JOBS` only when the host has
enough memory for concurrent Rust links.

The fast quality, test, coverage, and documentation commands download the
official DuckDB release matching `libduckdb-sys` once and use it only while
Cargo runs the command. This avoids repeatedly compiling DuckDB's large C++
amalgamation. A cold cache therefore needs network access. Default builds,
installs, and the required release build retain the `bundled-duckdb` feature
and produce a self-contained binary. Run `make test-bundled` when changing the
linkage feature itself.

GitHub Actions runs quality, tests, coverage, and the release build as
independent lanes. The test lane uses one Cargo process for every target and
runs each harness with one test thread. This keeps database and signal state
deterministic; the dedicated release lane separately compiles and verifies the
bundled native build. Migration evidence is assembled after the test and
coverage artifacts are uploaded.

The coverage threshold is a region, line, and function coverage gate. The report also displays
function and region data; do not describe a percentage without naming its
dimension and measured target set.

## Contract and safety review

MCP tools must document purpose, inputs, pagination/budget behavior, errors,
and the next workflow step. Safety annotations must describe actual side
effects. Never turn a mutating operation into a read-only annotation merely
to simplify client approval.

Command execution must retain the human approval record, exact command, cwd,
shell, and declared artifacts. Tests must not depend on private paths,
network services, or a pre-existing user database.

## Pull requests

Pull requests should state the user-visible result, storage/schema impact,
security impact, and exact verification commands. Keep commits focused and do
not include `.coverage-mcp/`, DuckDB WAL files, build output, credentials, or
private coverage reports.
