# Changelog

Notable user-visible changes are documented here. Coverage MCP follows
Semantic Versioning after 1.0.0; before 1.0, minor versions may contain
breaking public-contract changes.

## 0.11.0 - 2026-08-23

### Added

- Schema 10 adds `find_duplicate_coverage_tests`, a bounded, paginated read-only
  tool that groups named tests by exact covered line/branch/function coverage
  observations. It ignores hit counts, preserves named observations through
  compaction, and reports unavailable evidence when the format has no test
  names; coverage equality is not logic equality.

## Unreleased

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
