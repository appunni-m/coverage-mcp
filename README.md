# Coverage MCP

Find missing coverage, inspect the returned locations with `rg`, run your
existing test command, and compare the reports. Coverage MCP reads LLVM JSON,
LCOV, coverage.py JSON, Istanbul JSON, Cobertura, JaCoCo, and Go coverprofiles.

**Development status:** this checkout is 0.16.0, schema 18. It replaces the old
API with two read-only tools: `coverage_gaps` and `coverage_compare`. The local
runtime and marketplace update have been tested; publishing them is a separate
release step. See [upgrading from 0.15](docs/user-guide.md#upgrade-from-015).

## Install

For Codex, install the `testing` plugin from
[codegen-marketplace](https://github.com/appunni-m/codegen-marketplace).
The plugin pins the runtime version and starts the shared daemon. The published
plugin may still provide the earlier contract until 0.16 is released.

For contributors testing this checkout, Rust 1.85.1 and a C++ build toolchain are
required for the bundled DuckDB build:

```sh
cargo install --path . --locked --bin coverage-mcp
coverage-mcp --version
```

## Try a report

After installation, run this in a project directory:

```sh
mkdir -p target
printf 'TN:\nSF:example.rs\nDA:1,1\nDA:2,0\nend_of_record\n' > target/example.lcov
coverage-mcp gaps --measurement target/example.lcov
```

The result identifies line 2 as missing. Source identity is unverified and test
status is unknown; the example does not require an `example.rs` source file.

For real measurements, preserve the baseline before running tests:

```sh
cp target/coverage.json target/baseline.json
# Run only the new cases with the repository's existing coverage command.
coverage-mcp gaps --measurement target/coverage.json --query parse_header
coverage-mcp compare --scope incremental --baseline target/baseline.json --measurement target/coverage.json
```

Incremental mode reports new coverage and the baseline-plus-batch total. Pass
`--previous REPORT` for earlier batches; see [incremental use](docs/user-guide.md#run-only-the-new-tests).

Report-file CLI queries need no server, database, or import. Each answer shows
a few source locations and omitted counts. Selected-test absences are not
regressions; source/build receipts are needed for verified comparison claims.
Use `--json` for structured output or `--help` for command options.

To use MCP and saved history, run `coverage-mcp connect --repo /absolute/project`.
It starts or reuses the loopback daemon; the dashboard is then available at
<http://127.0.0.1:59471/>.

## Guides

- [Queries, comparison meaning, receipts, and migration](docs/user-guide.md)
- [MCP inputs, results, and errors](docs/mcp-reference.md)
- [HTTP routes](docs/http-api.md)
- [Configuration, storage, and troubleshooting](docs/operations.md)
- [Contributing and release preparation](https://github.com/appunni-m/coverage-mcp/blob/main/CONTRIBUTING.md)

For bugs, see [support](SUPPORT.md). Report vulnerabilities through the
[security policy](SECURITY.md). Licensed under [MIT](LICENSE).
