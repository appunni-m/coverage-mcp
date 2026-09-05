# HTTP API — 0.16

The server binds to loopback (default `127.0.0.1:59471`) and validates Host
headers. Requests have no cross-origin allowance. Select a repository with
`x-coverage-mcp-repo: /absolute/repository` or the `repo_path` query parameter.
A configured default repository is used otherwise. Except unscoped project
listing and runtime/MCP discovery, repository selection is required.

| Method and path | Input | Result |
| --- | --- | --- |
| GET `/` | none | Dashboard |
| GET `/favicon.ico` | none | 204 |
| GET `/health` | none | Runtime version, schema, instance, process, storage and timeout settings |
| POST `/mcp` or `/mcp/` | JSON-RPC | [Shared MCP contract](mcp-reference.md) |
| POST `/api/coverage/gaps` | JSON `coverage_gaps` arguments | Same structured evidence envelope as MCP |
| POST `/api/coverage/compare` | JSON `coverage_compare` arguments | Same structured evidence envelope as MCP, including incremental union with `scope=incremental` and optional `previous` |
| GET `/api/projects` | optional `cursor`, `max_words` after selection | Project collection; unscoped discovery returns registered projects without cursor paging |
| GET `/api/snapshots` | optional `branch`, `suite`, `cursor`, `max_words` | Ordinary history entries: `id`, `suite`, `created_at`, `format` |
| POST `/api/ingest` | `report_path`; optional `format=auto`, `suite=default`, `repo_path`, `branch`, `commit_sha`, `base_ref` | Archive an ordinary report and return a snapshot envelope |
| PATCH `/api/projects/{id}` | compaction settings | Updated project settings envelope |
| POST `/api/projects/{id}/compact` | empty object | Compaction result envelope |

Use the literal project ID `project` with a selected repository; an actual ID
from project listing resolves through the registry. Compaction settings are
`compaction_enabled`, `compaction_after_days`, `compaction_interval_seconds`,
and `compaction_batch_size`. See [operations](operations.md) for ranges.
Collection APIs use the existing `page` cursor envelope; their default display
budget is 600 words and is separate from the fixed query evidence byte cap.
Unscoped project discovery bypasses that paging budget and reports registry
entries; use project-scoped collection requests for budgeted pagination.

```sh
curl -s http://127.0.0.1:59471/api/coverage/gaps \
  -H 'Content-Type: application/json' \
  -H 'x-coverage-mcp-repo: /absolute/repository' \
  -d '{"measurement":"target/coverage.json","query":"decode","limit":3}'
```

Ingest is an optional archive operation, not part of the normal report-file
query path. It does not run a command or establish measured source provenance.
Existing composite snapshots are readable by ID; composing new multi-component
measurements is no longer an HTTP workflow.

The source graph, dashboard stream, source-line reads, per-file/region
collections, run/command/artifact/worktree/topology operations, old comparisons,
legacy incremental analysis, and duplicate-test endpoints were removed. They have no
compatibility aliases. Use local source search and the existing runner.

Boundary validation/parse errors return HTTP 400, unknown routes/entities 404 after valid repository selection,
timeouts 504, and storage/runtime/I/O errors 500, with `{"error":"message"}`.
MCP tool errors are protocol results with `isError:true` (HTTP 200), not empty
successes. GET on the MCP endpoint returns 405. Notifications return HTTP 202.

A capability-protected daemon handoff endpoint remains private to the connector.
It requires the current lease token and exists only for safe runtime upgrades;
it is not an agent tool or application route.
