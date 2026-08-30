# Support

Start with the symptom-based checks in the
[operations guide](docs/operations.md#troubleshooting-by-symptom). They cover
missing plugin tools, MCP initialization failures, wrong-repository selection,
daemon conflicts, database locks, missing snapshots, and limited claims.

Use [GitHub issues](https://github.com/appunni-m/coverage-mcp/issues) for
reproducible bugs and focused feature requests. Include:

- Coverage MCP version and schema revision from `/health` when available;
- codegen-marketplace testing-plugin version, or the manual Cargo installation
  command used;
- operating system and CPU architecture; include the Rust version only for a
  Cargo build or source checkout;
- sanitized `GET /health` output;
- the exact command or MCP method that failed;
- a minimal reproducible repository or fixture when possible.

Remove credentials, private source, logs, local paths, tokens, and database
contents that are not necessary to reproduce the issue. Search existing issues
first. Security vulnerabilities must be reported through
[`SECURITY.md`](SECURITY.md), not a public issue.

Coverage MCP is maintained on a best-effort basis; response times are not
promised and there is no commercial support SLA.
