# Releasing

Releases are Rust binaries and crates.io packages built from an annotated Git
tag. The release workflow must reproduce the local gate before publishing an
artifact.

## Maintainer checklist

1. Confirm the working tree is clean, the default branch is protected, and all
   required checks pass.
2. Update `Cargo.toml` and `Cargo.lock` to the release version. Move the
   corresponding `CHANGELOG.md` entries into a dated release section.
3. Run `make ci`, including strict clippy, all-target tests, fixture-backed
   migration lanes, line/function coverage, generated migration status, and
   rustdoc warnings.
   Retain `target/migration/status-report.json` and the generated pages as
   release evidence; a dirty or missing lane is `not_proven`.
4. Build a release binary with `cargo build --release --locked` and inspect
   `coverage-mcp --version`.
5. Verify the binary in clean checkouts: start two default `connect` processes
   for different repositories, confirm they return stdio
   `initialize`/`tools/list` through one daemon PID on port `59471`, and confirm
   the daemon remains healthy after both bridges exit. Also verify HTTP
   `tools/list`, the dashboard, report ingest, a project policy edit, a manual
   compaction pass, and a database reopen.
6. Verify lifecycle hardening: start two daemons against one common database
   and confirm the second exits with `resource busy`; open one project database
   twice and confirm the second owner is rejected; exercise an incompatible
   connector/daemon health check, a bounded query, pool saturation, and SIGTERM
   shutdown; verify that the database and WAL are left untouched after each
   failure.
7. Run `cargo package --locked --allow-dirty --no-verify` only when a dirty
   package inspection is intentional; release packaging should use a clean
   tree.
8. Create an annotated immutable `v<version>` tag and publish through the
   configured crates.io/asset workflow. Never move a published tag.
9. After registry propagation, install the exact crate into an empty temporary
   root with `cargo install coverage-mcp --version =<version> --locked --bin
   coverage-mcp --root <temporary-root>`. Verify its version and both MCP
   transports; a local checkout or Git install is not registry evidence.
10. Update the downstream testing plugin's pinned version only after the
    published crate passes that clean install. Run two plugin launchers against
    one empty runtime cache, confirm one Cargo install under the versioned
    installer lock, then confirm both stdio bridges and a direct HTTP client
    connect concurrently while only the daemon process holds its ownership
    lock.
    Exercise each claimed launcher platform; record native Windows as
    unsupported until a Windows bootstrap and its clean-machine test exist.
11. Record the artifact checksums and release evidence. Do not publish a
    marketplace pin for an absent, yanked, or moving runtime source.

## Release evidence

A release note must identify the exact Rust version, target triple, schema
revision, test command, coverage dimension and denominator, storage migration
state, and any unsupported platform or feature. “100% coverage” is meaningful
only when it names the measured dimension and target set.

The runtime defaults and override ranges for pool size, connection checkout,
DuckDB query, and HTTP request deadlines are part of the release contract and
are reported by `/health`.

## Recovery

If an artifact is bad, revoke or yank it according to the registry policy and
publish a corrected version. Do not overwrite a database schema or retarget a
release tag. A user-visible storage migration requires an upgrade note and a
backup/restore procedure before release. Never delete a lock or WAL as a
startup workaround: stop the owner, preserve the evidence, and restore from a
verified backup if DuckDB cannot replay the WAL.
