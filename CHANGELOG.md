# Changelog

Notable user-visible changes are documented here. Coverage MCP follows
Semantic Versioning after 1.0.0; before 1.0, minor versions may contain
breaking public-contract changes.

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
