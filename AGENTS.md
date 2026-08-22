# Coverage MCP Agent Notes

This repository owns the Rust Coverage MCP binary, REST API, MCP contract,
storage behavior, dashboard, migration fixtures, and primary README. Keep
these surfaces synchronized when changing behavior.

## MCP contract change checklist

For changes to tool names, inputs, output shapes, descriptions, instructions,
safety annotations, resources, pagination, or workflow:

1. Update `src/mcp.rs`. Initialization instructions must be sufficient for an
   agent without opening the README; tool descriptions must explain purpose,
   mode choices, budget/pagination behavior, errors, and the next step.
2. Update the service/storage implementation when validation or side effects
   change. Keep compact envelopes and hidden detailed fields consistent across
   HTTP and stdio.
3. Update `README.md` MCP Usage Guide with every tool, input, return shape,
   error class, resource, and workflow rule.
4. Add the lowest-layer behavior test and one public HTTP or stdio test when a
   public input/output changes.
5. Verify both transports. The shared dispatcher is
   `mcp::dispatch_json_rpc`; do not fork transport-specific semantics.

## Workflow ownership

Operational approval, polling, freshness, lineage, response-budget, and
reporting rules belong in the marketplace's `testing:coverage-review` skill
and the campaign skill that composes it. Keep this repository's copy limited to
the server contract and implementation invariants; do not duplicate workflow
instructions here. The server must still advertise a complete self-describing
workflow through `initialize` instructions and `tools/list` descriptions.

## Rust verification

The required local gate is:

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

After a release or installed-binary change, rebuild with
`cargo install --path . --locked`, stop the old shared daemon so the first new
`coverage-mcp connect` starts the rebuilt binary, verify `/health`, and make a
live `tools/list` call over both transports.

When external marketplace/plugin guidance changes, update the corresponding
documentation in the marketplace checkout only after the Rust contract and
local tests are green. Do not restore or add a second runtime to satisfy
connector documentation.
