# Rust migration and parity evidence

The migration is complete at the runtime boundary: the repository contains
one Rust implementation, Rust tests, and input-only migration fixtures. A
separate source runtime is not required, installed, imported, or executed.
This document preserves the behavior-family mapping so future
changes can be reviewed against the current schema-16 contract.

This is a maintainer document for a source checkout. Published Cargo packages
intentionally omit the test corpus, fixtures, and generated evidence described
here.

## Surface mapping

| Behavior family | Rust implementation | Rust evidence |
| --- | --- | --- |
| Report parsing and normalization | `src/parser.rs`, `src/models.rs` | `rust_migrates_all_parser_formats_and_aliases`, model merge/rate/path tests |
| Git identity and worktree lineage | `src/git.rs`, `src/storage.rs` | `rust_lineage_baseline_and_guards`, storage lineage tests |
| DuckDB snapshots, queries, runs, artifacts, and compaction | `src/storage.rs`, `src/compaction.rs` | `rust_storage_queries_compare_and_compacts_old_detail`, storage tests, smoke test |
| Composite inventory, canonical regions, variant enforcement, and exact aggregation | `src/composite.rs`, `src/storage.rs`, `src/service.rs` | composite storage/compaction regression and composite review tests |
| Response budgets, cursors, compact projections, and service validation | `src/service.rs` | `rust_service_pagination_projection_and_mcp_contract_match` |
| MCP inventory, schemas, resources, safety, and JSON-RPC dispatch | `src/mcp.rs` | MCP contract assertions, HTTP wire test, daemon-only CLI rejection test |
| Shared-daemon stdio lifecycle, crash autorecovery, authenticated version handoff, and repository routing | `src/main.rs`, `src/http.rs`, `src/lock.rs` | concurrent two-repository connector smoke test, same-bridge crash/stale-file recovery, daemon-owner assertion, handoff authorization/process-exit test, and older-version replacement probe |
| REST routing, health, dashboard, SSE refresh, common registry, and lifecycle | `src/http.rs`, `src/dashboard.html` | live REST/dashboard/SSE/health/MCP test and browser smoke verification |
| Background detail compaction | `src/compaction.rs`, `src/storage.rs` | policy default/edit/manual-pass, compressed payload, transparent restore tests |
| Named-test duplicate coverage | `src/parser.rs`, `src/models.rs`, `src/storage.rs`, `src/service.rs`, `src/mcp.rs` | exact LCOV observation parser test, DuckDB grouping/compaction test, service/MCP and live HTTP wire tests |

## Compatibility invariants

- The public MCP inventory contains exactly eight tools and schema revision 16.
  Unsupported names use the ordinary unknown-tool error path.
- Every successful public projection uses the `{context,data,page}` envelope.
- Word budgets, opaque query-scoped cursors, defensive collection caps, and
  compact-by-default detailed fields remain enforced at the service boundary.
- `project_context` reports the ten-entry active-run cap through
  `data.active_runs_truncated`; its page distinguishes fixed-summary
  `reserved_words`, command-page `word_count`, and combined
  `response_word_count` so callers can account for the complete `max_words`
  budget. Command pages contain at most eight compact summaries and continue
  with `page.next_cursor`; detailed mode expands fields but does not lift the
  active-run or command-page caps.
- Compact history defaults to two points and omits repeated total/covered
  counters. Compact insight targets return at most three
  `[start,end,line_count]` ranges plus `region_count` and
  `regions_truncated`; audit retains exact region records.
- Compact review limits default to three files and five ranked targets so a
  combined `task=all` call remains usable within the normal response budget;
  callers can raise either limit explicitly.
- MCP JSON-RPC clients read the bounded envelope from `structuredContent`; the
  text content is only a compatibility hint. Terminal incremental runs expose
  their bounded automatic review in the run result and `run_review` status;
  `coverage_review(task="incremental")` remains the standalone comparison for
  already stored ordinary snapshot sets or composite snapshots.
- A fresh schema is created when a store opens; incompatible database contents
  are disposable and are not repaired through compatibility migrations.
- Compacted snapshots remain readable through file, line, source, insight, and
  comparison queries. Incremental review compares immutable stored snapshots
  without rerunning tests or recalculating coverage; it reports the
  deduplicated baseline-union-current aggregate, a separate scope-aware diff,
  bounded regions, detail source, and named-test attribution when both reports
  provide named observations.
- `detail_retention=incremental_base` excludes a fixed comparison base from
  automatic compaction. If a snapshot is compacted anyway, incremental review
  restores files, lines, and named observations from the compressed payload.
- Composite snapshots use `coverage-mcp-inventory-v1` and
  `coverage-mcp-regions-v1` mappings. The inventory owns exact production
  regions; explicit logical-source aliases alone permit deduplication. Every
  declared variant remains required, and missing/stale/malformed/source-
  mismatched evidence makes the immutable composite incomplete. Composite
  incremental review requires matching repository, mapping version, and
  inventory hash and compares all stored canonical rows before response
  truncation.
- Managed runs and snapshots persist a `full`, `incremental`, or `custom`
  execution context, including optional case-specific arguments and an explicit
  incremental baseline. Reuse and idempotency require context equality; when a
  caller omits an identity, the server generates a stable fingerprint from the
  mode, arguments, and baseline. It does not guess which tests an argument list
  means.
- `find_duplicate_coverage_tests` compares the complete normalized
  `{kind,file_path,line_number,region_key}` set for each named test, ignores hit
  counts, returns small cursor pages plus global counts, and reports
  `status=unavailable` when the artifact has no named observations. It is a
  coverage-equivalence candidate signal, not logic equivalence or a deletion
  operation. Named-test rows are included in compaction payloads and restored
  with the same semantics.
- HTTP and native stdio call the same JSON-RPC dispatcher and therefore share
  notifications, inventory, resource, tool, and error semantics.
- Concurrent stdio connectors start or reuse one loopback daemon, route their
  repositories independently, and never become competing DuckDB owners.
- A newer connector automatically replaces only a verified older daemon;
  unknown listeners, different registries, equal-version incompatibilities,
  and downgrade attempts fail closed.

## Fixture and evidence contract

The machine-readable manifest in the source checkout at
`tests/fixtures/manifest.yaml` identifies the schema-16 contract, Rust target
profile, public surfaces, input-only
parity cases, coverage plan, and compaction benchmark workload. Inputs contain
no expected outputs; generated reports, logs, and coverage artifacts are
runtime evidence and are never checked in as claimed results.

The manifest is a migration specification, not a second runtime. The Rust
tests are the executable authority. Any percentage in a release or review
must name its dimension, target set, numerator, denominator, and command.

Run the fixture-backed migration lanes with:

```sh
make migration-parity
make migration-benchmark
```

The benchmark command performs the declared warmup and measurement iterations
against fresh DuckDB stores, gates correctness before timing, checks the
manifest's five-second median latency budget, and writes its machine-readable
result to `target/migration/benchmark-result.json`.

`make migration-parity` records a Rust-generated, manifest- and revision-bound
marker after the conformance test completes. `migration-status` ignores a
missing, malformed, or stale marker and records the lane as `not_proven`; a
bare timestamp or a stale worktree result can never be treated as current proof.

After the parity, benchmark, and coverage lanes have completed, render the
fixed aggregate and generated pages with:

```sh
make migration-status
```

The command writes `target/migration/status-report.json` and the manifest's
generated pages under `docs/generated/`. Those paths are build artifacts and
are intentionally ignored by Git. Missing artifacts, a dirty target, and the
unavailable independent source oracle are rendered as `not_proven`; the generator never
turns a passing Rust conformance test into cross-runtime parity evidence.

## Verification

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
```

The non-default verification mode downloads the exact matching official
DuckDB release instead of compiling its C++ amalgamation in every test
profile. The release build still enables bundled DuckDB by default and proves
the self-contained runtime shipped to users.

The coverage gate requires 100% measured region, function, and line coverage
for the Rust library/runtime target set. The schema-16 rewrite audit is measured
by the same command above. The CLI launcher is excluded from aggregate LLVM counters
because it is exercised in child processes; daemon-only CLI and concurrent
shared-daemon stdio smoke tests still verify that launcher directly. Branch
coverage is not a separate threshold in this gate; region coverage is enforced
and reported explicitly for the measured target set.

## Compaction policy carried forward

New projects default to enabled compaction after 30 days, on a 3600-second
cadence, with a 100-snapshot batch. Creation-time overrides are accepted by
the project API and environment defaults; later edits are available through
REST, dashboard, and the manual CLI pass.
