# Release preparation

This guide is for maintainers publishing the existing crates.io package and
native binaries. The executable release definition is
[`.github/workflows/release.yml`](https://github.com/appunni-m/coverage-mcp/blob/main/.github/workflows/release.yml).
Local builds and a development marketplace pin are not published-release evidence.

## Prepare a candidate

1. Update `Cargo.toml`, `Cargo.lock`, and the dated changelog. The tag must be
   `v<package-version>`. Use a clean checkout and the pinned Rust 1.85.1 toolchain.
2. Run `make ci` and `make package`. The latter verifies the clean source package
   using prebuilt DuckDB; the release build still bundles DuckDB. Retain the
   generated migration report and check for `not_proven` lanes.
3. Check `coverage-mcp --version`, `--help`, `/health`, and `tools/list` through
   HTTP and stdio. Test both file queries and saved history. The 0.16 contract is
   schema 18 with two read-only tools; it must match the marketplace digest.
4. Verify the daemon lifecycle smoke tests: shared ownership, crash recovery,
   upgrade from a verified older owner, unknown-owner/downgrade refusal, and
   database reopen. Both queued and running legacy jobs must become interrupted
   without execution. Preserve stores and WALs throughout upgrade testing.
5. Review the package and archive contents, user documentation, and release notes.
   Include storage changes, compatibility limits, and evidence scope. Never
   describe an unexecuted target as verified.

## Publish and verify

Create an annotated immutable tag only when the release is authorized. Pushing
it triggers quality, tests, coverage, package, evidence, and native-build jobs.
The workflow publishes through configured crates.io trusted publishing, then
creates a GitHub Release with checksums and build attestations. Repository
permissions and registry publisher configuration must already be in place.

The workflow builds and executes bundled binaries on matching hosts for:

- `aarch64-apple-darwin` and `x86_64-apple-darwin`;
- `aarch64-unknown-linux-gnu` and `x86_64-unknown-linux-gnu`.

Native Windows is not part of this archive/bootstrap matrix. Verify every
published `coverage-mcp-<version>-<target>.tar.gz` against `SHA256SUMS`, extract it,
and run its version/help commands on the matching host. Inspect the shipped
README, guides, and license. Separately install the exact published crate in an
empty temporary root with `cargo install coverage-mcp --version =<version>
--locked --bin coverage-mcp --root <temporary-root>`. A source-checkout install
does not establish registry availability.

Distribute the marketplace update only after those checks. Verify its exact
version, schema, tool count, and digest; exercise a clean bootstrap cache and
concurrent connectors. Unsupported archive targets may use the exact Cargo
fallback when a suitable toolchain exists. Record the tested platforms and
checksums; do not promise untested installations.

## Recover a partial release

If crates.io succeeds but GitHub Release creation fails, keep the tag and crate.
The [release asset recovery workflow](https://github.com/appunni-m/coverage-mcp/blob/main/.github/workflows/release-assets-recovery.yml)
uses a completed Release run for that exact tag/commit, validates the retained
bundle against `SHA256SUMS`, re-attests it, and creates the missing release.
It also accepts an explicit tag and optional source-run ID through manual
dispatch. An existing GitHub Release makes recovery a no-op.

For a bad artifact, follow registry withdrawal policy and publish a corrected
version; do not move a released tag. Storage changes need a backup/restore note.
Stop the owner before restoring a verified backup. Deleting a lease or WAL to
force startup is not a recovery procedure.
