# Changelog

Notable user-visible changes are documented here. Coverage MCP follows Semantic Versioning after 1.0.0; before 1.0,
minor versions may contain breaking public-contract changes.

## Unreleased

### Changed

- Removed the retired schema-revision 6 MCP implementation and migration reference.
- Replaced offset-bearing continuation tokens with record-anchored opaque cursors.
- Disambiguated duplicate cursor anchors and made defensive collection caps fail explicitly instead of losing records.
- Made public response models reject undeclared fields.
- Expanded packaging metadata and the supported Python CI matrix.
- Separated the embedded dashboard document and storage projections from transport and persistence code.
- Restricted the daemon to loopback interfaces and added browser security headers and trusted-host validation.
- Hardened concurrent lazy startup against transient health-probe timeouts.

### Added

- Contributor, governance, support, conduct, and security policies.
- Reproducible token-savings benchmark inputs, connector verification, release documentation, and Trusted Publishing.
- PEP 561 metadata for downstream type checkers.

## 0.7.0 - 2026-07-18

- Consolidated the agent interface into ten schema-revision 7 tools.
- Added word-budgeted responses, cursor pagination, compact-by-default projections, and strict lineage validation.
- Reworked coverage-file queries around grouped gaps and normalized multi-range source selection.
- Updated the dashboard to use the shared schema-revision 7 service projection.
