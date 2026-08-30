# Changelog

Notable user-visible changes are documented here. Coverage MCP follows
Semantic Versioning after 1.0.0; before 1.0, minor versions may contain
breaking public-contract changes.

## Unreleased

### Changed

- Reworked the documentation around user outcomes: the README now leads with
  coverage-review use cases, codegen-marketplace is the primary Codex
  installation path, and the published crates.io package is the manual
  alternative.
- Moved MCP schemas, HTTP routes, daemon configuration, storage behavior, and
  troubleshooting out of the README into focused getting-started, user,
  operations, MCP, and HTTP guides.

## 0.15.5 - 2026-08-29

### Fixed

- Large LLVM coverage reports no longer re-prepare every DuckDB detail insert,
  preventing valid imports and managed-run ingestion from hitting the database
  operation watchdog.

## 0.15.4 - 2026-08-29

### Fixed

- Live dashboard refreshes now follow the newest current snapshot unless the
  user explicitly pins one or more current artifacts.

## 0.15.3 - 2026-08-28

### Fixed

- Dashboard incremental unions now use the nearest earlier compatible full or
  legacy snapshot as their automatic base, skipping incremental subset runs.
  Projects without a complete base no longer report an incremental subset as a
  rolling comparison baseline.

## 0.15.2 - 2026-08-28

### Fixed

- Schema 16 makes incremental ordinary reviews use the same full-shaped
  `measurement` and `baseline` projections as full reviews. The canonical
  `measurement` is the server-computed baseline ∪ selected-run union; raw run
  provenance, signed deltas, gain/decrement categories, merge provenance, and
  the replacement diagnostic diff remain supplemental incremental data.
- The dashboard and source graph consume the canonical union projection for
  their default metrics and file cards instead of reconstructing coverage from
  separate aggregate numerator/denominator fields.

## 0.15.1 - 2026-08-27

### Fixed

- Preserve complete file-summary line counts when incremental reports contain
  sparse detailed line rows, so the primary baseline union cannot decrease.
  Such aggregates expose `merge.exact=false` with
  `conservative_max_fallback` provenance.

## 0.15.0 - 2026-08-26

### Added

- Schema 15 separates incremental coverage into two explicit views: the
  primary deduplicated union of baseline plus selected evidence, and a nested
  snapshot diff for diagnosis. A selected-case run therefore cannot be
  reported as a code regression merely because it did not exercise the full
  baseline.
- Incremental reviews now merge all ordinary coverage snapshots attached to a
  run. Lines use canonical file/line identities; LLVM functions, branches, and
  regions use preserved source-mapped identities when available, with a
  conservative fallback and explicit merge provenance for legacy reports.
- The dashboard's incremental panel shows final union coverage, line/branch/
  function/region gains, not-observed evidence, and the diagnostic diff in a
  separate block.

## 0.14.0 - 2026-08-26

### Added

- The dedicated `/graph` page separates **Function hierarchy** from **File
  dependencies**. The hierarchy maps source files to the LLVM function
  records projected for them; the dependency mode shows best-effort resolved
  source-import edges rather than implying a runtime call graph.
- LLVM-mapped function inspection now shows demangled source names, LLVM
  symbols, source ranges, execution counts, line/region/branch completeness,
  and explicit reasons for missing coverage. Files without LLVM records remain
  visible without invented function nodes.

### Changed

- The dashboard now links to the focused graph page instead of embedding a
  second graph panel. Graph zoom scales the SVG canvas from 50% to 200% and no
  longer changes the graph's meaning; source line detail remains available
  from the selected file or function.

## 0.13.0 - 2026-08-25

### Added

- Schema 14 adds immutable composite coverage snapshots for one managed run.
  A registered command can declare one authoritative inventory plus explicit
  component/package-variant evidence without being registered again for each
  coverage case.
- Composite aggregation maps LLVM, coverage.py, Istanbul, and
  `coverage-mcp-regions-v1` evidence to canonical regions, deduplicating only
  through explicit logical-source aliases. Missing, stale, malformed, or
  source-mismatched evidence leaves the composite incomplete and never lowers
  its denominator.
- `run_test` and `run_review` expose `composite_snapshot_id`; the
  `coverage_review` measurement and baseline selectors accept composite IDs,
  and `task="incremental"` automatically compares two compatible composite
  snapshots with exact region deltas and bounded test attribution.

### Changed

- Composite provenance records checkout revision/cleanliness, command,
  inventory hash, mapping version, toolchain versions, and artifact hashes.
- Composite review returns exact combined counts and only Rust (including
  WGSL), Python, and JavaScript component summaries. Ordinary snapshots,
  managed command registration, child ingestion, and compaction behavior remain
  compatible; compacted ordinary child detail is restored through the existing
  payload path when named-test attribution is requested.
- The GNU Make contributor interface now has a real workspace `build`, an
  explicit clean-tree `package` check, and a complete `ci` gate that includes
  the bundled release build; contributor and release documentation match those
  targets.

## 0.12.0 - 2026-08-25

### Added

- Schema 13 adds parameterized managed runs: one approved command can receive
  bounded case-specific arguments without re-registration. The server persists
  the arguments, explicit incremental baseline, and server-generated execution
  fingerprint so reuse and idempotency remain case-safe.
- Incremental managed runs automatically attach a bounded stored-snapshot
  review after terminal coverage ingestion. Standalone
  `coverage_review(task="incremental")` remains available for two existing
  snapshots and never reruns tests or reparses reports.

### Changed

- Incremental comparisons distinguish newly covered, regressed, hit-count-only,
  added, and removed observations, expose bounded opaque test attribution, and
  remain valid when detail has been compacted.
- The dashboard and REST projections expose incremental provenance, compaction
  detail sources, progression metrics, and server-sent refresh events. The
  dashboard checks for updates every ten seconds in the foreground and pauses
  its stream in hidden tabs.

## 0.11.0 - 2026-08-23

### Added

- Schema 10 adds `find_duplicate_coverage_tests`, a bounded, paginated read-only
  tool that groups named tests by exact covered line/branch/function coverage
  observations. It ignores hit counts, preserves named observations through
  compaction, and reports unavailable evidence when the format has no test
  names; coverage equality is not logic equality.

## 0.10.1 - 2026-08-23

### Changed

- Reduced default MCP context growth with bounded project command pages,
  active-run projections, two-point history, and compact insight ranges.
- Made complete response-budget accounting and compact truncation state
  explicit so agents can request more detail only when needed.
- Kept terminal run responses and the JSON-RPC text content minimal while
  preserving the canonical structured MCP result over HTTP and stdio.

## 0.10.0 - 2026-08-22

### Added

- A consolidated schema-9 MCP contract with seven bounded agent-facing tools.
- Compact change, region, history, source, and audit projections with explicit
  measurement lineage, freshness, provenance, and response budgets.
- A production Cargo package boundary that excludes tests, fixtures, evaluator
  data, and maintainer-only files.

### Changed

- The Rust server is the only runtime and owns identical MCP semantics over
  loopback HTTP and native stdio.
- Review requests require an explicit `task`; unknown fields are rejected at
  the wire boundary.
- Coverage history returns two detailed points plus an aggregate window by
  default, while exact records remain an explicit audit representation.
