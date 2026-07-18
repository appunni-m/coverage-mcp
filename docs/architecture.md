# Architecture

Coverage MCP is a local-first test and coverage service for multiple agents working in the same Git repository.

## Runtime ownership

One user-level HTTP daemon owns a common repository registry and lazily opens one coverage DuckDB for each canonical
Git repository. Linked worktrees resolve through Git's common directory and share the same repository store. Each MCP
client may launch a small stdio connector, but connectors proxy to the existing daemon and never create their own core
service or database.

```text
agent stdio connectors
        │
        ▼
shared loopback HTTP/MCP daemon
        │
        ├── CommonStore: repository registry
        ├── CoverageStore: repository A
        ├── CoverageStore: repository B
        └── shared service/projection layer
              ├── MCP tools and resources
              ├── REST API
              └── dashboard
```

## Data model

Coverage snapshots and completed runs are immutable. Registered command definitions are immutable approval records.
Mutable queue state exists only while work is queued or running. Coverage history survives run-retention cleanup.

Every public schema-revision 7 response carries repository, checkout, suite, and schema context. Compact projections
are the default; detailed projections are explicit exceptions. Collections use word budgets and opaque continuation
cursors, while server-side record caps exist only as defensive bounds.

## Design invariants

- One daemon per user, not per agent or worktree.
- One coverage store per canonical shared Git repository.
- No comparison across repository, suite, or worktree lineage.
- Unknown parents are errors, never empty-success responses.
- Managed command execution requires an immutable human-approved registration.
- MCP, resources, REST, and dashboard calls use the same service and projection behavior.

The embedded dashboard document lives in `coverage_mcp/dashboard.py`; transport and lifecycle assembly remain in
`coverage_mcp/app.py`. Keeping the dashboard local and dependency-free lets `uvx coverage-mcp connect` install one
small Python distribution without a separate frontend build toolchain.

`coverage_mcp/storage.py` owns DuckDB state and run scheduling. Pure response, topology, time, and bounded-log
projections live in `coverage_mcp/storage_helpers.py`, keeping data ownership separate from presentation logic and
making those algorithms independently testable.
