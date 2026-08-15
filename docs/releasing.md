# Releasing

Releases are Rust binaries and crates.io packages built from an annotated Git
tag. The release workflow must reproduce the local gate before publishing an
artifact.

## Maintainer checklist

1. Confirm the working tree is clean, the default branch is protected, and all
   required checks pass. Confirm `rust-toolchain.toml`, the CI toolchain, and
   the release workflow all select Rust 1.85.1.
2. Update `Cargo.toml` and `Cargo.lock` to the release version. Move the
   corresponding `CHANGELOG.md` entries into a dated release section.
3. Run `make ci`, including strict clippy, all-target tests, fixture-backed
   migration lanes, line/function coverage, generated migration status, and
   rustdoc warnings. The verification lanes use the exact prebuilt DuckDB
   release; the all-target test lane exposes compilation and execution as
   separate steps, uses one Cargo compilation job, and retains line-table debug
   information. Runtime execution is split into library, binary,
   migration-status, migration parity, compaction benchmark, and daemon/stdio
   smoke steps with a retained log for each target. Branch and tag test jobs
   both allow a 60-minute cold-cache window; the release build in step 4 is the
   required bundled-linkage check.
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
   twice and confirm the second owner is rejected. Start the previous released
   daemon, connect with the candidate binary, and confirm the connector verifies
   ownership, stops the older daemon, starts the candidate on the same port,
   and preserves the database. Also verify authenticated handoff, refusal of an
   unknown port occupant, downgrade/equal-version refusal, a bounded query,
   pool saturation, and SIGTERM shutdown; verify that the database and WAL are
   left untouched after each failure.
7. Run `cargo package --locked --allow-dirty --no-verify` only when a dirty
   package inspection is intentional; release packaging should use a clean
   tree.
8. Create an annotated immutable `v<version>` tag and publish through the
   configured crates.io/asset workflow. For the first crates.io version, use
   the bootstrap procedure below before pushing the tag. The workflow
   reproduces quality, all-target tests, coverage, clean package verification,
   bundled release compilation, and migration-evidence validation on isolated
   hosted runners before it mints the trusted-publishing token. Feature
   selection accelerates only package verification; the packaged crate still
   defaults to bundled DuckDB. Never move a published tag.
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

## First crates.io release

Trusted Publishing cannot create a crate. Bootstrap the first version with a
maintainer API token, then use short-lived OIDC credentials for later tags:

1. Verify the maintainer email in the crates.io profile. Create a short-lived
   API token with permission to publish new crates and no crate-name
   restriction. Keep it out of the repository and CI secrets; install it with
   `cargo login` and protect `~/.cargo/credentials.toml` as mode `0600`.
2. Push the release commit and require its normal branch CI to pass. From a
   separate clean checkout of that exact commit, run the clean package gate and
   `cargo publish --locked`. Never use `--allow-dirty` for a release.
3. Wait until `cargo info coverage-mcp@<version> --registry crates-io` succeeds.
   Configure the crates.io Trusted Publisher for repository
   `appunni-m/coverage-mcp`, workflow `release.yml`, and environment
   `crates-io`.
4. Create and push the annotated tag. The workflow recognizes the exact
   manually published bootstrap version and skips a duplicate upload while
   still reproducing every build, test, coverage, package, and evidence lane.

Revoke the bootstrap token after the first release. Do not store a long-lived
`CARGO_REGISTRY_TOKEN` in GitHub; subsequent releases use the ephemeral output
from `rust-lang/crates-io-auth-action`.

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
