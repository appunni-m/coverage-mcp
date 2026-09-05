# Runtime and storage

Report-file `gaps` and `compare` CLI calls do not start a server or open a
DuckDB database. `connect` provides MCP and saved-history access through one
shared loopback daemon. `serve` runs that daemon explicitly.

```sh
coverage-mcp serve --host 127.0.0.1 --port 59471
coverage-mcp connect --repo /absolute/repository
coverage-mcp compact --repo /absolute/repository --older-than-days 30
```

Each repository retains `.coverage-mcp/coverage.duckdb`. The shared registry
is `~/.coverage-mcp/common.duckdb`; its directory contains daemon lease/log
state. Back up database files only with the owning process stopped or through
a consistent database backup. Do not delete stores for an upgrade. Compaction
restores complete ordinary detail transparently and preserves composite data.

## Configuration

Command-line `serve` options override the corresponding environment variables.
All values are validated at startup; an invalid value fails explicitly.

| Variable | Default | Accepted value or effect |
| --- | ---: | --- |
| `COVERAGE_MCP_HOST` | `127.0.0.1` | Loopback host only; public binding is rejected |
| `COVERAGE_MCP_PORT` | `59471` | TCP port from 0 to 65535 |
| `COVERAGE_MCP_COMMON_DB` | `~/.coverage-mcp/common.duckdb` | Registry path; its parent also holds the daemon lock and log |
| `COVERAGE_MCP_RUN_RETENTION` | `100` | 1–10000 legacy execution records |
| `COVERAGE_MCP_RUN_CONCURRENCY` | `4` | 1–32 legacy adapter workers; no query execution routes |
| `COVERAGE_MCP_HTTP_CONCURRENCY` | `16` | 1–128 concurrent HTTP MCP requests |
| `COVERAGE_MCP_DB_POOL_SIZE` | `4` | 1–16 DuckDB connections per open project |
| `COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS` | `5000` | 50–120000 ms to obtain a pooled connection |
| `COVERAGE_MCP_DB_QUERY_TIMEOUT_MS` | `30000` | 100–3600000 ms for one DuckDB operation; must be shorter than the HTTP deadline |
| `COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS` | `60` | 1–3600 seconds for one HTTP request |
| `COVERAGE_MCP_HTTP_MAX_BODY_BYTES` | `1048576` | 1024–16777216 bytes for one JSON HTTP body |
| `COVERAGE_MCP_RUN_LOG_MAX_BYTES` | `10485760` | 1024–1073741824 bytes per legacy Rust execution-adapter stream |
| `COVERAGE_MCP_COMPACTION_AFTER_DAYS` | `30` | 1–36500 days for newly created project policies |
| `COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS` | `3600` | 60–86400 seconds for a newly created project policy |
| `COVERAGE_MCP_COMPACTION_BATCH_SIZE` | `100` | 1–10000 snapshots for a newly created project policy |

The marketplace bootstrap also accepts `COVERAGE_MCP_RUNTIME_DIR` to relocate
its versioned binary cache. That variable belongs to the plugin bootstrap, not
the server configuration.

## Limits and compaction

New project policies enable compaction after 30 days, check every 3600 seconds,
and process batches of 100. PATCH `/api/projects/{id}` changes
`compaction_enabled` (boolean), `compaction_after_days` (1–36500),
`compaction_interval_seconds` (1–86400), or `compaction_batch_size` (1–10000).
Environment configuration supplies defaults for newly created projects only.
Snapshots retained as incremental baselines keep their historical protection.

## Recovery

A connector recovers a crashed daemon using its process lease and verifies the
new runtime. It does not trust an unrelated process serving a similar health
response. After an installed-binary replacement, stop the old daemon cleanly,
reconnect, and check health plus tools/list through HTTP and stdio. A 0.16
server reports schema 18 and exactly two tools.

Unfinished legacy managed jobs become interrupted when storage opens. They are
not resumed. Run needed tests using their existing repository commands.

If a query is slow, record report size and query latency first. It currently
parses the entire artifact to compute exact results before bounding the answer.
Use completed artifacts from the producer; parsing and hashing share one
captured byte buffer. Missing/invalid artifacts and ambiguous saved suites are explicit errors;
a larger display limit cannot fix them.

## Troubleshooting by symptom

| Symptom | Check and next step |
| --- | --- |
| Old or missing plugin tools | Compare `/health` and `tools/list` with the plugin pin; update both runtime and guidance, then start a new task. |
| MCP initialization closes | Inspect connector stderr and daemon logs. Verify the configured binary exists and its version matches the pin. |
| Wrong project or ambiguous latest | Pass `--repo` / `x-coverage-mcp-repo` and an explicit report or snapshot; add a saved suite selector when needed. |
| Port occupied or database busy | Identify the owning process. Stop only the verified old daemon; preserve lock and WAL files. |
| Missing report or limited comparison | Check the artifact path and producer receipt. Re-run the existing measurement command when evidence is stale; larger response limits cannot repair it. |

CLI queries write results to stdout and errors to stderr. Successful queries
exit 0; execution errors exit 1 and CLI syntax errors exit 2. A JSON result
such as `limited`, `incomparable`, or `tests_failed` is still a successful query;
inspect its status rather than treating exit 0 as a passing test run.

For unresolved problems, follow [support](../SUPPORT.md). Removing a plugin does
not remove history or stop an already-running shared daemon. Stop the verified
owner and back up its files before any deliberate state removal.
