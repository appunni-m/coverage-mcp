# Coverage MCP

**Give coding agents the few test and coverage facts they need, without pouring entire logs, reports, and source files
into their context window.**

Coverage MCP is a local-first test-run ledger and coverage history service. It runs approved commands, retains their
complete output outside the model context, normalizes coverage into queryable DuckDB records, and returns compact MCP
projections by default. When an agent needs evidence, it searches for a literal and receives only the matching lines
and their surrounding context.

This matters because the usual agent loop is expensive: run a command, receive all of stdout and stderr, grep the
output, read broad source ranges, then repeat after every edit. Most of those tokens describe passing tests, unchanged
files, progress bars, or coverage rows the agent never uses. Coverage MCP keeps that data local and lets the agent ask
questions such as “what failed?”, “where did coverage regress?”, or “show lines 340-355 and 812-826” directly.

## Session-Calibrated Context Savings

In one real Codex development session, ten completed coverage-related jobs returned **48,267 o200k tokens** of
agent-visible run/status/diagnostic payloads before strict projection and response budgeting. Replaying the same
ten-job workflow with a conservative **500-token budget per job** gives **5,000 tokens**: a session-calibrated
reduction of **43,267 tokens, or 89.6%**.

| Completed job | Recorded payload | Budgeted replay | Tokens avoided |
| ---: | ---: | ---: | ---: |
| 1 | 4,987 | 500 | 4,487 |
| 2 | 16,117 | 500 | 15,617 |
| 3 | 1,518 | 500 | 1,018 |
| 4 | 3,601 | 500 | 3,101 |
| 5 | 1,545 | 500 | 1,045 |
| 6 | 12,512 | 500 | 12,012 |
| 7 | 1,791 | 500 | 1,291 |
| 8 | 2,611 | 500 | 2,111 |
| 9 | 1,813 | 500 | 1,313 |
| 10 | 1,772 | 500 | 1,272 |
| **Total** | **48,267** | **5,000** | **43,267 (89.6%)** |

### How the calculation was made

The recorded side is not a synthetic log-size guess. It is the exact `o200k_base` token count of the Coverage MCP
response payloads associated with ten consecutive completed jobs in a July 2026 Codex session transcript, counted
with `tiktoken` 0.13.0. Prompts, reasoning, source reads, and edits were excluded; serialized tool responses were
included because those are what entered the agent context. The replay side is an explicit budget model: 500 tokens per
job for compact state plus one relevant diagnostic excerpt. The arithmetic is
`48,267 - (10 × 500) = 43,267`, and `43,267 / 48,267 = 89.6%`.

The 500-token replay is a target, not a promise that every repository saves exactly 89.6%. A failure needing several
independent excerpts will use more; a passing run that only needs status will use less. The session transcript and
tokenizer determine the recorded number, while the caller controls the replay with `detailed=false`, `max_words`, a
specific log search, and cursor pagination. This makes the benchmark reproducible and the assumption visible.

Compared with ordinary shell `grep` and broad file reads, the difference is where filtering happens. Shell output is
returned to the agent first and consumes context before the agent can inspect it. Coverage MCP filters retained logs,
coverage rows, and source ranges inside the local service, then sends only the bounded result. Full evidence remains on
disk and can be queried again without making it permanent conversation history.

## Getting Started

### Recommended: install the agent plugin

The [`testing` plugin in codegen-marketplace](https://github.com/appunni-m/codegen-marketplace/tree/main/plugins/testing)
installs the agent workflow and a lightweight stdio connector. The connector uses `uvx` to obtain Coverage MCP from
this public HTTPS repository, starts the shared HTTP daemon if needed, and proxies stdio to it.

For Codex:

```bash
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Start a new Codex thread after installation. The installed connector is equivalent to:

```bash
uvx --from git+https://github.com/appunni-m/coverage-mcp.git@main \
  coverage-mcp connect
```

You do not need to clone either repository. `uvx` owns an isolated Python environment and the connector starts or
reuses the daemon on demand.

### Standalone server

Python 3.12 or newer is required.

```bash
python -m pip install \
  "coverage-mcp @ git+https://github.com/appunni-m/coverage-mcp.git@main"
coverage-mcp serve
```

Then open <http://127.0.0.1:59471/> or connect an MCP client to
<http://127.0.0.1:59471/mcp/>. For stdio clients, use:

```json
{
  "command": "coverage-mcp",
  "args": ["connect"]
}
```

### What starts on your machine

```text
Codex / Claude / another MCP client
              │ stdio
              ▼
   tiny coverage-mcp connect process
              │ loopback HTTP
              ▼
 one user-level Coverage MCP daemon
              │
              ├── common registry: ~/.coverage-mcp/common.duckdb
              └── one repository store: <shared-git-root>/.coverage-mcp/coverage.duckdb
```

Ten agents may create ten small stdio proxies, but they reuse one HTTP daemon. Repository selection is lazy. A Git
repository and all of its linked worktrees share one repository DuckDB, while separate repositories get separate
stores. Add `.coverage-mcp/` to Git ignore rules.

## Documentation Map

- [First managed run](#first-run)
- [Approved command and run ledger](#approved-run-ledger)
- [MCP usage guide](#mcp-usage-guide)
- [Worktree baselines](#worktree-baselines)
- [Dashboard](#dashboard)
- [REST API](#rest-api)
- [Storage model](#storage-model)
- [Security and privacy](#security-and-privacy)
- [Contributing](#contributing)

## At A Glance

Coverage MCP is for people who run tests often and need to know:

- did coverage go up or down after this run?
- which files changed coverage?
- which exact lines regressed or improved?
- what was the baseline when this worktree started?
- can an LLM answer coverage questions without reading huge coverage reports or source files?

It runs locally, stores coverage snapshots in DuckDB, exposes a dashboard, and provides MCP tools for LLM clients.

| Surface | Default |
| --- | --- |
| Dashboard | `http://127.0.0.1:59471/` |
| MCP endpoint | `http://127.0.0.1:59471/mcp/` |
| Database | `<shared-git-root>/.coverage-mcp/coverage.duckdb` |
| Run concurrency | Four managed commands, FIFO assignment |
| Run retention | Newest 100 terminal runs per registered command |

## What Problem It Solves

Coverage reports are usually large, format-specific artifacts. They are hard for humans to diff and wasteful for LLMs to read directly.

Coverage MCP turns those reports into small queryable records:

- overall snapshot coverage
- file-level coverage
- exact line-level coverage
- branch, function, and region counters when the report has them
- time-series history by minute-level timestamp
- frozen baseline references for worktrees

There are two supported workflows: let Coverage MCP run an explicitly approved command and retain its logs/artifacts,
or ingest a coverage report generated by another test runner. Both produce the same normalized snapshot queries.

## What Coverage Details Include

Coverage details are normalized into four levels:

- project: repo path/key, latest snapshot, snapshot count, branch count, and latest available coverage rates
- snapshot: timestamp, branch, commit SHA, suite, format, totals, rates, parser warnings
- file: covered/total lines, branches, functions, and regions, with per-file rates
- line: line number, hits when available, covered/missed state, branch/function counters on that line

When the report format is lossy, the snapshot keeps a warning. For example, Go coverprofiles report blocks, Istanbul reports statements, and LLVM reports segments.

## Installation And Updates

For development from a source checkout:

```bash
python -m pip install -e '.[dev]'
```

### Update The Server

The agent plugin and the Python server are updated separately. Updating
`testing@codegen-marketplace` does not download or restart Coverage MCP.

For a Git-installed server, stop the running process and update it:

```bash
python -m pip install --upgrade \
  "coverage-mcp @ git+https://github.com/appunni-m/coverage-mcp.git@main"
```

For an editable development checkout:

```bash
git -C /path/to/coverage-mcp pull --ff-only
python -m pip install -e '/path/to/coverage-mcp[dev]'
```

Restart `coverage-mcp` after updating. The existing `.coverage-mcp/coverage.duckdb`
is not replaced, so snapshots, run history, approvals, and worktree baselines
remain available.

Confirm the new process loaded the expected release:

```bash
curl http://127.0.0.1:59471/health
```

The response `version` must match the package version reported by `python -m pip show coverage-mcp`. Confirm that
`common_db_path` is the intended daemon registry before ingesting data.

## Start The Shared Server

Coverage MCP runs one loopback HTTP daemon per user and lazily opens one DuckDB per Git repository. Start it explicitly
with:

```bash
coverage-mcp serve
```

Verify it:

```bash
curl http://127.0.0.1:59471/health
```

Dashboard:

```text
http://127.0.0.1:59471/
```

MCP endpoint:

```text
http://127.0.0.1:59471/mcp/
```

Default database:

```text
<repository>/.coverage-mcp/coverage.duckdb
```

The daemon's common registry is `~/.coverage-mcp/common.duckdb`. Coverage MCP resolves Git's shared repository root
before opening a repository database, so the main checkout and its linked worktrees share one DuckDB. This preserves
baseline and worktree lineage without committing local history. The repository `.coverage-mcp/` directory should remain
ignored by Git.

Add `.coverage-mcp/` to the repository's `.gitignore` or local Git exclude before the first run. This repository
already ignores it.

Do not start another daemon for each worktree or repository. Every connector reuses the same HTTP daemon; it remains
the sole owner of each repository's DuckDB connection.

Override host, port, or DB:

```bash
COVERAGE_MCP_HOST=127.0.0.1 \
COVERAGE_MCP_PORT=8765 \
COVERAGE_MCP_RUN_CONCURRENCY=4 \
coverage-mcp serve
```

`COVERAGE_MCP_RUN_CONCURRENCY` accepts 1-32 workers and defaults to `4`. Use
`1` for suites that share non-isolated build outputs and cannot safely overlap.
The active value is returned by `/health`.

Retain more or fewer terminal test runs per approved command:

```bash
COVERAGE_MCP_RUN_RETENTION=250 coverage-mcp
```

The default is `100`. The active value is returned by `/health`.

## Install In An Agent

The `testing` plugin in
[codegen-marketplace](https://github.com/appunni-m/codegen-marketplace) includes the Coverage MCP connection and
agent instructions for approved test runs, bounded summaries, artifact ingestion, and worktree comparisons.

Configure agents with the stdio connector. It starts or reuses the loopback daemon and selects the connector's Git
repository automatically:

```json
{
  "command": "coverage-mcp",
  "args": ["connect"]
}
```

For an ephemeral public-GitHub installation, use `uvx`:

```json
{
  "command": "uvx",
  "args": ["--from", "git+https://github.com/appunni-m/coverage-mcp.git", "coverage-mcp", "connect"]
}
```

The plugin installs only:

- the `use-coverage-mcp` skill
- the stdio MCP connection metadata
- plugin documentation and prompts

It does not install the Python server or copy the DuckDB. The connector starts the background daemon on demand.
Upgrade the plugin for agent instructions and connection changes;
upgrade Coverage MCP for parser, storage, API, dashboard, or performance
changes.

### Codex

```bash
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Start a new Codex thread after installation and configure its MCP connection with the stdio command above.

### Claude Code

```bash
claude plugin marketplace add appunni-m/codegen-marketplace
claude plugin install testing@codegen-marketplace
```

Start a new Claude Code session after installation and configure its MCP connection with the stdio command above.

### Pi

Pi intentionally has no built-in MCP client, so install the testing skill and the MCP adapter:

```bash
git clone https://github.com/appunni-m/codegen-marketplace.git
pi install ./codegen-marketplace/plugins/testing
pi install npm:pi-mcp-adapter
node ./codegen-marketplace/plugins/testing/scripts/install-pi-mcp.mjs
```

Restart Pi after installing the adapter. The installer merges Coverage MCP into
`~/.config/mcp/mcp.json`; it does not replace other configured MCP servers. In Pi, MCP tools are accessed through the
adapter's `mcp` proxy tool.

### Confirm Agent Access

Ask the agent:

```text
Use coverage-mcp to list the registered test commands for this project.
```

If no command is registered, give the agent the complete test command, working directory, and artifact paths, then
explicitly approve that exact registration. After a run, ask:

```text
Run the approved test suite, verify its declared coverage artifact was automatically ingested, and tell me whether
this worktree improved against its frozen baseline.
```

## First Run

### Managed Test And Coverage

This is the preferred agent workflow:

1. Call `project_context(detailed=false)` before running anything.
2. Reuse an exact approved command. If none exists, present its complete command, cwd, shell, and artifacts for human
   approval, then call `register_test_command`.
3. Submit it with `run_test`, save the returned run ID, and poll `test_run(action="status", detailed=false)` no faster than
   `poll_after_ms`. Reuse one `idempotency_key` for retries of the same intended run.
4. When the run is terminal, inspect `coverage_ingest.status` and `coverage_ingest.snapshot_ids`. A declared artifact
   with `coverage_format` is automatically ingested only when that run created or modified it.
5. Query the returned snapshot with `coverage_query` and `coverage_compare`, or open the
   dashboard. Do not call `ingest_coverage` again for an automatically ingested artifact.

### Ingest An Existing Report

Use this path when another tool already generated coverage:

1. Generate the report in one of the supported formats.
2. Call `ingest_coverage` or `POST /api/ingest` with the actual checkout path and a stable suite name.
3. Query the stored snapshot through MCP, REST, or the dashboard.

LCOV through REST:

```bash
curl -X POST http://127.0.0.1:59471/api/ingest \
  -H 'content-type: application/json' \
  -d '{
    "report_path": "coverage/lcov.info",
    "format": "lcov",
    "repo_path": "/path/to/repo",
    "branch": "main",
    "suite": "unit"
  }'
```

The same ingest through MCP:

```text
ingest_coverage(
  report_path="coverage/lcov.info",
  format="lcov",
  branch="main",
  suite="unit",
  max_words=500
)
```

Use `format="auto"` to detect the report type.

## `AGENTS.md` Snippet

Projects using Coverage MCP can place this small policy in their `AGENTS.md`:

```md
## Coverage MCP

- Reuse the repository's single Coverage MCP server and shared DuckDB. Never copy the database into a worktree or set
  `COVERAGE_MCP_DB` to a worktree-local path.
- Create reference-branch snapshots before calling `register_worktree(path, base_ref, name)`. Keep the returned
  `worktree_id`; its registration time freezes suite baselines for that worktree.
- Keep `detailed=false` unless exact audit/provenance fields are necessary. Set a task-sized `max_words`, continue
  collections with `cursor`, and search logs by literal text with small surrounding windows.
- Run tests through an existing human-approved command with `run_test`. Record the returned run id and
  use one stable `idempotency_key` for that intended run. Poll `test_run(action="status", detailed=false)` no faster than `poll_after_ms`; retries
  must reuse the same key. If no approved command exists, ask for explicit approval before registering one.
- Declare managed coverage artifacts with `coverage_format` and a stable `suite`. On terminal runs, require
  `coverage_ingest.status` to report `ingested` and use its `snapshot_ids`; never ingest that artifact a second time.
- Use `ingest_coverage` only for reports produced outside the Coverage MCP managed runner.
- Use `coverage_compare(view="progress", worktree_id=..., suite=..., detailed=false)` to report whether line, branch,
  function, and region coverage improved. No current snapshot means "not measured", not "unchanged"; never compare
  unrelated worktrees.
```

## Approved Run Ledger

For managed workflows, Coverage MCP stores test runs without requiring a YAML suite file.

Instead, a human explicitly registers the full command string once, including cwd and expected artifacts. Registration requires approval fields:

```text
register_test_command(
  name="condition",
  command="make -C pillow-rs-freetype test-unified-condition-coverage",
  cwd="/path/to/repo",
  artifact_paths={
    "llvm_json": {
      "path": "pillow-rs-freetype/target/coverage/unified-condition-summary.json",
      "required": true,
      "coverage_format": "llvm",
      "suite": "unified-condition"
    },
    "missing_lines": "pillow-rs-freetype/target/coverage/unified-condition-missing-lines.txt"
  },
  human_approved=true,
  approved_by="your-name",
  approval_note="approved exact condition coverage command"
)
```

After that, agents run only the registered command id or name:

```text
run_test(
  command_ref="condition",
  idempotency_key="condition:<commit-sha>:coverage",
  max_words=500
)
```

Submission returns immediately with a durable run id and `status` set to `queued` or `running`. Poll that id until
`terminal` is true:

```text
test_run(run_id="returned-run-id", action="status", detailed=false, max_words=500)
```

Use `project_context(detailed=false)` to inspect active work and queue position. Set `wait=true` only for a command
known to finish quickly; the default background mode keeps the MCP call responsive during long suites. Compact
responses are the default. Use `detailed=true` once when audit/provenance metadata is necessary, and use
`search_test_logs` for targeted diagnostics instead of increasing a generic excerpt budget.

Artifacts with a non-null `coverage_format` are automatically parsed after a managed process exits normally, whether
the test status is `passed` or `failed`. The server compares file size, nanosecond modification/change times, and inode
state captured immediately before and after the command. A pre-existing report that the command did not modify is
reported as `skipped_stale` and is not ingested. Cancelled, timed-out, interrupted, or unlaunched commands do not
auto-ingest potentially partial artifacts.

A terminal run returns a top-level `coverage_ingest` summary and per-artifact results. `status: "ingested"` includes
the immutable `snapshot_ids`. `failed`, `missing`, `skipped_stale`, and `skipped_run_status` leave the test process
status unchanged and explain the ingestion outcome in each artifact's `ingest_error`. Parser failure is therefore
visible without turning a successful test command into a failed command. Reusing an idempotency key returns the same
run and snapshot links instead of creating duplicate snapshots.

Runs completed before Coverage MCP `0.3.3` may report `coverage_ingest.status: "not_recorded"`. They are historical
ledger entries with no automatic-ingestion decision; do not poll them for a snapshot or implicitly ingest their
possibly stale artifact.

```json
{
  "status": "passed",
  "coverage_ingest": {
    "status": "ingested",
    "configured_artifacts": 1,
    "ingested_artifacts": 1,
    "failed_artifacts": 0,
    "skipped_artifacts": 0,
    "snapshot_ids": ["snapshot-uuid"]
  },
  "artifact_paths": [{
    "kind": "llvm_json",
    "modified_by_run": true,
    "ingest_status": "ingested",
    "snapshot_id": "snapshot-uuid",
    "ingest_error": null
  }]
}
```

After a command has completed normally at least once, active polling also reports an ETA learned from that command's
own history. The command record stores the median duration, p90 duration, sample count, and newest contributing run
time for its latest 20 natural completions. Passed and failed test processes contribute; cancellations, timeouts,
interruptions, and launch failures do not. Queued ETA includes the estimated remaining time of FIFO jobs ahead plus
the current command's median. When required history is missing, `eta_seconds` is `null` and
`eta_unavailable_reason` explains why.

An idempotency key identifies one intended run and is scoped to the registered command. Repeating the same key returns
the existing queued, running, or terminal run with `submission_reused: true`. Use a new key only when a genuinely new
execution is intended. A key remains effective while its run is retained.

Each completed run is stored as an immutable ledger record:

- exact command
- cwd
- repo key/path
- branch and commit SHA when known
- start/end time and duration
- exit code and status
- full stdout/stderr log paths
- bounded parsed summary
- registered artifact paths

The MCP response does not return full raw logs by default. It returns a bounded summary and tells you where the full
logs are stored. While a run is active, polling is constant-time and defers log parsing until completion.

Managed commands run outside the MCP event loop. Four local workers claim jobs in FIFO order by default, so dashboards,
health checks, coverage insights, and other agents remain responsive while independent suites run concurrently. Queue
ETA models the worker lanes instead of summing every earlier job as serial work. DuckDB operations remain internally
serialized. A graceful shutdown waits for accepted work; check `project_context` before restarting when a prompt restart
matters. After an unexpected process exit, active runs are preserved as `interrupted` and queued runs resume from the
same DuckDB rather than being submitted again.

Every command starts in its own process group. `test_run(action="cancel")` and command timeouts signal the entire group, then escalate
from `SIGTERM` to `SIGKILL` after two seconds if processes remain. This prevents child test processes from continuing
after the managed run has ended.

## Run Retention

Coverage MCP keeps the newest 100 terminal run records for each registered command. Retention is count-based, never
time-based, so an older but low-volume suite keeps its own history even when another suite runs frequently.

The count includes `passed`, `failed`, `cancelled`, `timeout`, `interrupted`, and `internal_error` records. Queued and running jobs
are never retention candidates. When a record expires, Coverage MCP removes its run row, artifact-registry rows, and
managed stdout/stderr files. It does not delete registered artifact files or coverage snapshots.

Set `COVERAGE_MCP_RUN_RETENTION` before starting the server to change the per-command limit. Lowering the value prunes
existing history during startup; no scheduler or time-based cleanup task is involved.

## Object Topology

Coverage MCP does not store topology as a separate table. It computes topology from each object's own fields and returns it inline.

Examples:

- project topology: repo key/path, snapshot count, command count, run count, latest snapshot
- command topology: project, command id/name, cwd, approval metadata, artifact kinds
- run topology: project, command id/name, run id/status, runtime branch/commit, artifact paths
- snapshot topology: project, snapshot id, suite, format, report path, branch/commit
- worktree topology: project, worktree path/head, baseline ref and baseline snapshot

This means a registered command is project-specific because registration stores `repo_key` and `repo_path` from its approved `cwd`. Runs carry more detailed topology because they also store runtime branch/commit, logs, status, and artifacts.

## Supported Coverage Formats

| Format | Use `format` | Notes |
| --- | --- | --- |
| LCOV | `lcov` | Lines, branches, functions |
| coverage.py JSON | `coveragepy` or `coverage.py` | Lines and branch arcs |
| Cobertura XML | `cobertura` | Lines and branch counts |
| JaCoCo XML | `jacoco` | Java/Kotlin/JVM line, instruction, branch counters |
| Istanbul/nyc JSON | `istanbul` or `nyc` | Statements become line records; branches/functions kept separately |
| Go coverprofile | `go` | Block ranges expanded to lines |
| LLVM JSON export | `llvm` | Segments become line records; branch, function, and aggregate region coverage are preserved |

Some formats are lossy when normalized. For example, Go reports blocks, Istanbul reports statements, and LLVM reports segments. Coverage MCP stores warnings on snapshots when it has to approximate line records.

<details>
<summary>Legacy MCP v0.6 migration reference</summary>

> This inventory describes the pre-v0.7 surface for migration only. New clients should use the consolidated
> schema-revision 7 tools in the MCP Usage Guide below.

Connect your MCP client to:

```text
http://127.0.0.1:59471/mcp/
```

Schema revision 6 exposed the following 22 tools; revision 7 consolidates them into the ten-tool contract documented
in the current guide below. This section exists only to map older clients during migration. `tools/list` returns concrete JSON Schemas
for both inputs and outputs, including nested fields, required fields, nullability,
status enums, descriptions, and bounds. An agent can therefore use the MCP
contract without reading this repository or guessing response keys. Invalid
types, missing required inputs, out-of-range values, and unsupported input enum
values are returned as MCP tool errors before execution. Returned payloads are
also validated against these contracts so implementation drift fails visibly.

General conventions:

- IDs are durable UUID strings. A `command_ref` accepts either a registration ID or its latest matching name.
- `file_path` is the exact repository-relative path stored by the report. Rename history is intentionally not inferred.
- Result limits are inclusive. `limit` is tool-specific. Run operations are compact unless `detailed=true`.
- Retained logs are queried with `search_run_logs`; line context preserves locality while `max_words` controls text volume.
- Times are returned as UTC timestamps plus `age_seconds` and a human-readable `age` such as `10 minutes 3 seconds ago`.
- Lookup tools normally return MCP tool errors for missing objects; list and history queries may return an empty list as documented.

### `project_summaries`

`project_summaries(limit: integer = 100)` discovers projects known through snapshots, commands, or runs.

**Inputs:** `limit` is the maximum records to return, from 1 through 1000.

**Returns:** A list containing project identity, latest snapshot and run, counts, current rates, freshness, and inline topology. A project known only through an approved command has null coverage fields until its first snapshot; that means not measured, not zero coverage.

**Errors:** Schema validation errors only; an empty server returns an empty list.

### `register_test_command`

`register_test_command(name, command, human_approved, approved_by, approval_note, cwd = null, shell = "/bin/bash", artifact_paths = null)` creates an immutable approved command definition.

**Inputs:** `name` is a non-empty stable label; `command` is the complete shell command; `human_approved` must literally be `true`; `approved_by` identifies the approving human; `approval_note` records the exact approval; `cwd` is the project working directory or null for the server cwd; `shell` is the executable shell; `artifact_paths` maps artifact kind to either a path string or `{path, required, coverage_format, suite}`. Relative artifact paths resolve from `cwd`; `required` defaults to false and `coverage_format` defaults to null. A non-null `coverage_format` accepts the formats listed under `ingest_coverage` and enables freshness-guarded automatic ingestion. `suite` is a non-empty stable trend name and defaults to the registered command name. Give each of multiple coverage artifacts an explicit suite.

**Returns:** The registration ID, exact execution details, approval audit fields, project topology, enabled state, and learned duration fields.

**Errors:** Missing or false approval, blank required text (including an explicitly supplied blank artifact suite), or invalid cwd/artifact configuration produces a tool error. Duplicate definitions are allowed as separate immutable registrations; changing an approved command requires a new registration.

```text
register_test_command(
  name="unit-coverage",
  command="pytest --cov=src --cov-report=json:.coverage-mcp/coverage.json",
  cwd="/path/to/repo",
  artifact_paths={
    "coverage_json": {
      "path": ".coverage-mcp/coverage.json",
      "required": true,
      "coverage_format": "coveragepy",
      "suite": "unit"
    }
  },
  human_approved=true,
  approved_by="person-or-auditable-label",
  approval_note="Approved this exact command, cwd, shell, and artifact."
)
```

### `list_registered_commands`

`list_registered_commands(limit: integer = 100)` lists immutable command registrations newest first.

**Inputs:** `limit` is 1-1000.

**Returns:** Registrations with command, cwd, shell, artifacts, approval, topology, enabled state, and median/p90 duration statistics.

**Errors:** Schema validation errors only; no registrations returns an empty list.

### `run_command_profiled`

`run_command_profiled(command_ref, timeout_seconds = null, idempotency_key = null, wait = false, detailed = false)` submits an approved command to the bounded FIFO worker pool.

**Inputs:** `command_ref` is a registration ID or name; `timeout_seconds` is null or 1-86400; `idempotency_key` is null or a 1-200 character key scoped to that command; `wait` blocks until terminal only when true; `detailed` selects the full response instead of compact state.

**Returns:** Compact state contains `id`, `command_name`, `status`, `terminal`, `duration_ms`, `exit_code`, `counters`, `poll_after_ms`, queue position, contextual age, ETA, Git identity, `coverage_ingest`, cancellation/error state, and `diagnostics_available`. With `detailed: true`, it additionally returns the exact command, paths, timestamps, artifacts, full ETA internals, stored summary, and topology. Reusing the same idempotency key sets `submission_reused: true`.

**Errors:** Unknown/disabled commands, invalid timeouts, or reusing one idempotency key with different submission parameters produces a tool error. A test failure is a successful tool response with `status: "failed"`, not an MCP protocol error. Missing, stale, or malformed coverage is likewise terminal response data under `coverage_ingest`, not a transport error.

### `run_queue`

`run_queue(limit: integer = 100)` inspects active work in FIFO order.

**Inputs:** `limit` is 1-1000.

**Returns:** Compact running runs first with `queue_position: 0`, followed by queued runs with positive positions, contextual age, polling guidance, and lane-aware `eta_seconds`/`eta`.

**Errors:** Schema validation errors only; an idle worker returns an empty list.

### `cancel_run`

`cancel_run(run_id, detailed = false)` requests cancellation of queued or running work.

**Inputs:** `run_id` is the durable run UUID; `detailed` selects full metadata instead of compact state.

**Returns:** A queued run becomes terminal immediately. A running run reports `cancellation_requested: true` while Coverage MCP terminates its process group; poll `run_result` until terminal. Repeating cancellation on an already cancelled run is idempotent.

**Errors:** Unknown runs and terminal runs other than `cancelled` cannot be cancelled.

### `run_result`

`run_result(run_id, detailed = false)` polls one submitted run.

**Inputs:** `run_id` is required; `detailed` selects full metadata instead of compact state.

**Returns:** Compact state includes polling, counters, contextual age, queue/ETA, Git identity, coverage-ingestion outcome, and `diagnostics_available`; it contains no log text. With `detailed: true`, paths, artifact records, timestamps, stored summary, and topology are included. Poll no faster than `poll_after_ms`.

**Errors:** Unknown run IDs produce a tool error.

### `latest_run`

`latest_run(command_ref = null, detailed = false)` retrieves the newest active or terminal run.

**Inputs:** `command_ref` optionally restricts lookup to a command ID/name; `detailed` selects full metadata instead of compact state.

**Returns:** The same compact state as `run_result`, including automatic ingestion outcomes plus contextual `age` and `age_seconds`; detailed mode restores command, artifact, path, timing, summary, and topology fields.

**Errors:** Unknown command references or no matching runs produce a tool error.

### `search_run_logs`

`search_run_logs(run_id, query, stream = "both", context_lines = 3, max_matches = 5, max_words = 400, case_sensitive = false)` searches retained output without returning an entire generic excerpt.

**Inputs:** `run_id` selects the durable run; `query` is literal text; `stream` is `both`, `stdout`, or `stderr`; `context_lines` is 0-10 lines before and after a match; `max_matches` is 1-20 context anchors; `max_words` is the 20-2,000 word budget across all returned context; `case_sensitive` controls Unicode case-folded versus exact matching.

**Returns:** `run_id`, `query`, searched streams, total `match_count`, `returned_match_count`, `returned_line_count`, `returned_word_count`, `truncated`, and merged `contexts`. Each context identifies stdout/stderr and contains numbered lines with a `match` flag. Long lines are centered on the match, and the final line is cut at a word boundary when the budget is exhausted.

**Errors:** Unknown runs, empty queries, unsupported streams, or out-of-range context/match/line limits produce a tool error. Zero matches returns empty contexts.

### `latest_artifact`

`latest_artifact(kind, command_ref = null)` locates a run artifact without searching build directories.

**Inputs:** `kind` is the non-empty artifact key from registration; `command_ref` optionally restricts lookup to one command ID/name.

**Returns:** Artifact kind/path, existence and size, run/command identity, project identity, run status/timing, freshness, `coverage_format`, suite, freshness decision, ingestion status/error, linked snapshot ID, and topology for the newest match.

**Errors:** Unknown command references or no matching artifact produces a tool error. A registered artifact that was not generated is still returned with `exists: false` when it is the latest match.

### `object_topology`

`object_topology(object_kind, object_ref)` resolves relationships without a separate topology object.

**Inputs:** `object_kind` accepts `project`, `repo`, `repository`, `command`, `registered_command`, `test_command`, `run`, `snapshot`, `coverage_snapshot`, or `worktree`; `object_ref` is the corresponding UUID, command name, or repo path/key.

**Returns:** Inline project identity plus the object's Git, command, run, artifact, snapshot, worktree, and baseline relationships as applicable.

**Errors:** Unsupported kinds and unresolved references produce a tool error.

### `ingest_coverage`

`ingest_coverage(report_path, format = "auto", repo_path = null, suite = "default", branch = null, commit_sha = null, base_ref = null)` parses and stores one external immutable snapshot.

**Inputs:** `report_path` is a local server-readable artifact produced outside the managed runner; `format` accepts `auto`, `lcov`, `coverage.py`, `coveragepy`, `coverage-json`, `coveragepy-json`, `cobertura`, `jacoco`, `istanbul`, `nyc`, `go`, `go-cover`, `go-coverprofile`, `coverprofile`, `llvm`, or `llvm-json`; `repo_path` identifies the checkout/shared repository; `suite` is a non-empty stable trend name; `branch`, `commit_sha`, and `base_ref` override or add Git metadata when provided. Managed artifacts declaring `coverage_format` must use their run's automatic snapshot instead of this tool.

**Returns:** Snapshot identity and topology, normalized line/branch/function/region totals and rates, report metadata, parser warnings, timestamp, and freshness.

**Errors:** Missing/unreadable artifacts, unsupported or misdetected formats, malformed reports, invalid repo paths, and empty suites produce a tool error.

### `register_worktree`

`register_worktree(path, base_ref, name = null)` records a linked checkout and freezes available reference coverage.

**Inputs:** `path` is the worktree checkout path; `base_ref` is the branch/revision whose latest suite snapshots become baseline references; `name` is an optional label.

**Returns:** Worktree ID, path/head metadata, project identity, one primary `baseline_snapshot_id`, registration time,
and inline topology. Other suite baselines are resolved from snapshots that existed no later than registration.

**Errors:** Schema validation rejects blank paths or refs. When Git metadata or reference coverage is unavailable,
registration can still succeed with null Git fields or a null baseline. A null baseline never advances automatically:
ingest reference coverage and register a new worktree record before comparing. Registration never copies DuckDB into
the worktree.

### `worktree_progress`

`worktree_progress(worktree_id, suite = null, file_path = null, limit = 200)` answers whether one worktree improved from its frozen baseline.

**Inputs:** `worktree_id` is required; `suite` selects one coverage suite or the latest applicable suite; `file_path` optionally restricts the exact-path trend; `limit` is 1-2000 time-series points.

**Returns:** Worktree and baseline metadata, chronological points, `current` snapshot when measured, and
line/branch/function/region rate deltas. With no current worktree snapshot, `current` and its deltas are null rather
than being reported as unchanged. Each worktree progresses independently from the common parent history.

**Errors:** Unknown worktrees, absent suite baselines, or an exact file path missing from the baseline produce a tool error.

### `coverage_summary`

`coverage_summary(snapshot_id = null, repo_path = null, branch = null, suite = null)` returns one compact overall snapshot.

**Inputs:** `snapshot_id` directly selects an immutable snapshot. Without it, `repo_path`, `branch`, and `suite` filter the latest snapshot. If `snapshot_id` is supplied, it takes precedence and all filters are ignored.

**Returns:** Snapshot identity/topology, report metadata, line/branch/function/region totals and rates, warnings, timestamp, and freshness.

**Errors:** Unknown snapshot IDs or no snapshot matching the filters produces a tool error.

### `coverage_files`

`coverage_files(snapshot_id, limit = 100)` finds weak files without returning raw report data.

**Inputs:** `snapshot_id` is required; `limit` is 1-5000.

**Returns:** Per-file line/branch/function totals and rates ordered by lowest line coverage and then largest files.

**Errors:** Schema-invalid inputs produce a tool error. No matching file records, including an unknown snapshot ID,
returns an empty list; use `coverage_summary` first when snapshot existence must be verified.

### `coverage_file`

`coverage_file(snapshot_id, file_path, start_line = 1, max_ranges = 50, line_ranges = null, detailed = false)` drills into one exact path without dumping every covered line.

**Inputs:** `snapshot_id` and exact `file_path` are required. `start_line` continues a truncated gap response and `max_ranges` bounds it to 1-100 contiguous groups. `line_ranges` optionally accepts up to 10 inclusive `{start, end}` windows with at most 200 unique requested lines after normalization. Bounds must be positive and `end >= start`. `detailed` adds only format-specific raw file metrics when true.

**Returns:** Compact common file totals/rates plus `gaps`: counts and contiguous ranges containing only uncovered counted lines, partial branches, or uncovered functions. Each range reports its reasons and summed missed branch/function outcomes. `truncated` and `next_start_line` support focused continuation. Requested windows are sorted and merge exact duplicates, nesting, overlap, and adjacency. `selected_lines` contains compact exact coverage records—including covered lines—deduplicated and sorted. `line_selection` reports the normalized windows plus unique requested, returned, and unrecorded line counts. Repeated snapshot/path fields and per-line parser details are never embedded.

Use `source_context` for a small source window around a returned range. Use `detailed=true` only when parser-specific file counters are required.

**Errors:** Unknown snapshots or paths produce a tool error.

### `coverage_insights`

`coverage_insights(snapshot_id, baseline_snapshot_id = null, limit = 10)` returns deterministic investigation priorities.

**Inputs:** `snapshot_id` is current coverage; `baseline_snapshot_id` optionally enables regression analysis; `limit` is 1-50 primary items (the categorized response can contain up to four times this value).

**Returns:** Current/baseline summaries, severity counts, and prioritized zero/low-covered files, weak branches, parser warnings, overall/file regressions, and newly uncovered lines.

**Errors:** Unknown snapshots or invalid limits produce a tool error.

### `compare_to_baseline`

`compare_to_baseline(snapshot_id = null, baseline_snapshot_id = null, worktree_id = null, file_limit = 100, line_limit = 500)` supports two explicit modes.

**Inputs:** Direct mode requires both `snapshot_id` and `baseline_snapshot_id`. Worktree mode requires `worktree_id`, optionally accepts `snapshot_id` as the current point, and resolves the frozen suite baseline itself. `baseline_snapshot_id` is forbidden in worktree mode. `file_limit` is 1-1000 and `line_limit` is 1-5000.

**Returns:** Current and baseline summaries, overall line/branch/function/region deltas, bounded per-file comparison records in `files`, and bounded exact records in `changed_lines`. Worktree mode also returns `worktree` metadata.

**Errors:** Mixing comparison modes, omitting required direct IDs, unknown snapshots/worktrees, or a missing frozen baseline produces a tool error.

### `changed_lines`

`changed_lines(snapshot_id, baseline_snapshot_id, file_path = null, only_regressions = false, limit = 500)` answers exact line-change questions directly.

**Inputs:** Current `snapshot_id` and `baseline_snapshot_id` are required; `file_path` optionally selects one exact path; `only_regressions` keeps only covered-to-uncovered changes; `limit` is 1-5000.

**Returns:** Line records with baseline/current covered state, hits, branch data, and status such as improved or regressed.

**Errors:** Invalid inputs produce a tool error; no matching changes returns an empty list.

### `line_history`

`line_history(file_path, line_number, repo_path = null, branch = null, limit = 100)` follows one path and line over time.

**Inputs:** Exact `file_path` and one-based positive `line_number` are required; `repo_path` and `branch` optionally restrict project history; `limit` is 1-1000.

**Returns:** Chronological snapshot/time, Git, suite, hit, and covered-state points. History is path-based and does not follow renames.

**Errors:** Invalid inputs produce a tool error; no recorded points returns an empty list.

### `source_context`

`source_context(snapshot_id, file_path, start, end)` reads only the source range needed to interpret coverage.

**Inputs:** `snapshot_id` locates the repository, `file_path` is a repository-relative source path, and `start`/`end` are positive inclusive one-based boundaries. An `end` below `start` resolves to the single start line.

**Returns:** Up to 200 `{line_number, text}` records. A larger requested range is bounded rather than returning the whole file.

**Errors:** Unknown snapshots/files, paths escaping the repository, or unreadable source produces a tool error.

### MCP Resources

Read-only discovery is also available through two exact resources and three templates:

| URI | Result |
| --- | --- |
| `coverage://projects` | Up to 100 project summaries |
| `coverage://snapshots/latest` | Latest snapshot, or an error object when none exists |
| `coverage://snapshot/{snapshot_id}/summary` | One snapshot summary |
| `coverage://snapshot/{snapshot_id}/insights` | Deterministic insights for one snapshot |
| `coverage://snapshot/{snapshot_id}/files` | Up to 500 per-file records |

</details>

## MCP Usage Guide

Connect to `http://127.0.0.1:59471/mcp/`, or run `coverage-mcp connect` as a stdio proxy. Schema revision 7 exposes ten tools. Every result uses the same `{context, data, page}` envelope as REST and resources. `context` identifies `repo_key`, the exact `checkout_path`, the applicable `suite`, and `schema_revision` without repeating the full topology.

`max_words` is the primary response budget. Collections continue through opaque `cursor`/`next_cursor` values; numeric offsets are not public. Internal item caps are defensive only. Agents should omit `detailed` or leave it `false`. Only `project_context`, `test_run`, `coverage_query`, and `coverage_compare` expose it, for specifically requested audit or raw-provenance fields; it is never a way to retrieve logs. Parent lookups fail for unknown IDs, and comparisons reject mismatched repositories, suites, checkout lineage, or snapshots predating worktree registration.

### `project_context`

**Inputs:** `cursor` continues approved commands and `max_words` budgets the page. Keep `detailed` false; use true only for approval audit fields and full project chronology.

**Returns:** Project metrics and freshness, complete executable command definitions, latest run, active runs, and cursor metadata.

**Errors:** Invalid or query-mismatched `cursor` values and invalid `max_words` budgets are tool errors.

### `register_test_command`

**Inputs:** `name`, exact `command`, literal `human_approved` set to true, `approved_by`, and `approval_note` are required. `cwd` defaults to the selected checkout; `shell` defaults to `/bin/bash`; `artifact_paths` declares outputs. `max_words` budgets the response.

**Returns:** The immutable registration with complete `command`, `cwd`, `shell`, artifact specifications, and duration estimates. No expanded mode is needed.

**Errors:** False approval, blank audit fields, a `cwd` outside the selected repository, bad artifacts, or an undersized `max_words` budget fails.

### `run_test`

**Inputs:** `command_ref` selects an approved registration; `timeout_seconds`, `idempotency_key`, and `wait` control submission. `max_words` budgets output.

**Returns:** Durable compact run state, queue position, polling guidance, age and duration estimates, counters, and coverage-ingestion state. Use `test_run` for subsequent state or exceptional audit detail.

**Errors:** Unknown or disabled commands, conflicting idempotent submissions, invalid timeouts, or an insufficient `max_words` budget fails. Test failure remains result data.

### `test_run`

**Inputs:** `run_id`, `action` (`status` or `cancel`), and `max_words`. Keep `detailed` false; use true only when exact artifact records/paths, timestamps, or execution audit metadata are required.

**Returns:** Current durable run state or cancellation state.

**Errors:** Unknown `run_id`, unsupported `action`, invalid cancellation state, or an insufficient `max_words` budget fails.

### `search_test_logs`

**Inputs:** `run_id`, literal `query`, `stream`, `context_lines`, defensive `max_matches`, primary `max_words`, and `case_sensitive`.

**Returns:** Merged, numbered stdout/stderr windows around matches, bounded by words rather than a generic output excerpt.

**Errors:** Unknown `run_id`, invalid search options, or blank `query` fails. No matches returns an empty successful result.

### `ingest_coverage`

**Inputs:** `report_path`, `format`, `suite`, optional `branch`, `commit_sha`, and `base_ref`, plus `max_words`. Relative paths resolve inside the selected checkout.

**Returns:** The normalized immutable snapshot summary and bounded parser warnings. Raw parser metadata remains available through `coverage_query(detailed=true)` when explicitly needed.

**Errors:** Missing or malformed reports, unsupported `format`, blank `suite`, repository mismatch, or an insufficient `max_words` budget fails.

### `register_worktree`

**Inputs:** Git checkout `path`, frozen `base_ref`, optional `name`, and `max_words`.

**Returns:** Worktree ID, name, registration time, exact checkout, head/base revisions, and frozen baseline when available. Repeated repository topology is omitted.

**Errors:** Unknown paths, non-Git paths, a checkout from another repository, invalid refs, or an insufficient `max_words` budget fails.

### `coverage_query`

**Inputs:** `view` is `summary`, `files`, `file`, `insights`, or `line_history`. Selection uses `snapshot_id`, optional insights `baseline_snapshot_id`, `suite`, `branch`, `file_path`, and `line_number`; `line_ranges` accepts multiple inclusive ranges with overlap, adjacency, duplicates, and nesting normalized. `cursor` and `max_words` control paging. Keep `detailed` false; use true only for report/parser provenance on summary or insights, raw file metrics, or unabridged line-history records.

**Returns:** The selected compact projection. Collection views include a `page.next_cursor`; file view returns gaps plus only explicitly requested covered-line ranges.

**Errors:** Unknown parent IDs or paths, invalid range bounds or combined span, missing view-specific inputs, mismatched cursors, and invalid budgets fail.

### `coverage_compare`

**Inputs:** `view` is `overview`, `files`, `lines`, or `progress`. Direct mode uses `snapshot_id` and `baseline_snapshot_id`; worktree mode uses `worktree_id`. `suite`, `file_path`, `only_regressions`, `cursor`, and `max_words` refine the result. Keep `detailed` false; use true only when raw baseline/current snapshot provenance is explicitly required.

**Returns:** Overall deltas or a word-budgeted page of changed files, changed lines, or worktree progress points.

**Errors:** Unknown parents, mixed modes, repository/suite/worktree lineage mismatches, pre-registration snapshots, missing frozen baselines, invalid cursors, or invalid budgets fail.

### `source_context`

**Inputs:** `snapshot_id`, exact `file_path`, inclusive one-based `start` and `end`, `cursor`, and `max_words`.

**Returns:** Snapshot commit identity and a word-budgeted page of numbered source lines. Several coverage ranges can be fetched first with `coverage_query`, then source windows requested as needed.

**Errors:** Unknown snapshots/files, escaping paths, reversed bounds, invalid cursors, and invalid budgets fail.

### MCP Resources

- `coverage://context` returns the same compact project projection as `project_context`.
- `coverage://snapshot/{snapshot_id}/summary` returns the same compact summary projection as `coverage_query`.

## Worktree Baselines

The design goal is reproducible baseline comparison.

Create the needed reference-branch suite snapshots first. When you register a worktree, Coverage MCP stores one
primary baseline snapshot and the registration timestamp:

```bash
curl -X POST http://127.0.0.1:59471/api/worktrees/register \
  -H 'content-type: application/json' \
  -d '{
    "path": "/path/to/worktree",
    "base_ref": "main",
    "name": "feature-login"
  }'
```

MCP equivalent:

```text
register_worktree(
  path="/path/to/worktree",
  base_ref="main",
  name="feature-login"
)
```

The stored `baseline_snapshot_id` selects the primary suite. For another suite, Coverage MCP resolves the latest
matching reference snapshot that existed no later than the worktree's registration. Later uploads to `main` cannot
move either baseline. If registration returns a null baseline, ingest reference coverage and create a new worktree
registration before comparing.

This lets you ask, "what changed compared with the base coverage when this worktree started?"

All linked worktrees share one project identity and one database, but each registered worktree is a separate progress
lane. A lane contains its frozen reference snapshot followed only by snapshots ingested from that exact worktree path
after registration. Branch names alone are not used to join lanes.

## Dashboard

The dashboard at `http://127.0.0.1:59471/` shows:

- project selector with latest coverage for each project
- latest snapshot summary
- lineage-scoped multi-series trends for every available dimension: line, branch, function, and region
- investigation queue with high/medium/info items
- searchable file navigator ranked by missed lines, branch gaps, and baseline regressions
- editor-style source coverage with hit counts, branch gaps, and baseline changes in the gutter
- focused views for uncovered regions, partial branches, and changed coverage, with surrounding source context
- coverage overview rail and previous/next gap navigation for large files
- file diagnosis with jumpable uncovered regions and per-line history across snapshots
- automatic comparison with the preceding project snapshot, with an explicit baseline selector when another reference is needed

It uses the same REST API and DuckDB storage as the MCP tools.

The trend selector has two kinds of views:

- **Reference: `main`** shows only that branch and suite over time. It represents the health of the common parent tree.
- **Worktree: `<name>`** starts at the worktree's frozen baseline and then shows only runs from that worktree. Its label
  reports independent metric deltas such as `Line +1.2 pp` or `Branch -0.5 pp`. Baselines are frozen separately for
  each suite.

The graph never connects points from different worktrees. This follows the same reference-branch model used by
[GitHub coverage comparisons](https://docs.github.com/en/code-security/reference/code-quality/metrics-and-ratings).

The investigation layout follows established coverage workflows rather than treating coverage as a spreadsheet:

- an explorer plus editor gutter and uncovered-region navigation, as used by [VS Code test coverage](https://code.visualstudio.com/docs/debugtest/testing)
- changed-code coverage beside the source diff, as used by [Codacy's coverage view](https://docs.codacy.com/repositories/pull-requests/#coverage-tab)
- separate line and condition coverage, following [Sonar's coverage metric definitions](https://docs.sonarsource.com/sonarqube-server/2025.1/user-guide/code-metrics/metrics-definition/#coverage)
- explicit visibility for coverage changes outside edited lines, based on [Codecov's indirect-change model](https://docs.codecov.com/docs/unexpected-coverage-changes)

The first viewport is designed to answer the main operational questions in one pass:

- which project am I looking at?
- what is the latest line and branch coverage?
- is coverage moving up or down?
- what should I investigate first?

## REST API

REST uses the same schema-7 service projections as MCP, resources, and the dashboard. All API responses carry
`{context, data, page}`; `max_words` is the public budget, `cursor` is the only collection continuation mechanism,
and `detailed=false` is the default. Numeric `limit`, `offset`, `file_limit`, and `line_limit` parameters are no longer
part of the public API.

Useful endpoints:

- `GET /docs`
- `GET /api/projects`
- `POST /api/commands/register`
- `GET /api/commands`
- `GET /api/commands/{command_ref}`
- `POST /api/runs/profiled`
- `GET /api/runs/queue`
- `POST /api/runs/{run_id}/cancel`
- `GET /api/runs/latest`
- `GET /api/runs/{run_id}/logs/search`
- `GET /api/runs/{run_id}`
- `GET /api/artifacts/latest`
- `GET /api/topology/{object_kind}/{object_ref:path}`
- `POST /api/ingest`
- `POST /api/worktrees/register`
- `GET /api/worktrees`
- `GET /api/worktrees/{worktree_id}/progress`
- `GET /api/worktrees/{worktree_id}/compare`
- `GET /api/snapshots`
- `GET /api/snapshots/latest`
- `GET /api/snapshots/{snapshot_id}`
- `GET /api/snapshots/{snapshot_id}/insights`
- `GET /api/snapshots/{snapshot_id}/files`
- `GET /api/snapshots/{snapshot_id}/files/{file_path:path}`
- `GET /api/trend`
- `POST /api/compare`
- `GET /api/compare`
- `GET /api/changed-lines`
- `GET /api/line-history`
- `GET /api/source-lines`

## Storage Model

Snapshots are immutable. Each ingest creates a new snapshot with:

- timestamp and minute bucket
- repo path and repo key
- branch and commit SHA when known
- suite name
- normalized file records
- normalized line records
- line, branch, function, and region totals when supplied by the report format

Registered commands and completed runs are immutable too. Changing a command means registering a new approved command
record. Mutable queue state is stored separately until a run reaches a terminal state. Run stdout/stderr are written
under the database directory's `runs/` folder, and the database stores the paths plus bounded parsed summaries.
Fresh managed coverage artifacts create immutable snapshots and store their snapshot IDs on the run artifact registry;
run retention can remove the run linkage but does not delete coverage history.

Topology is derived, not separately stored. The same immutable rows power both direct object responses and `object_topology`.

DuckDB is used because this is local-first and query-heavy. A bounded in-process worker pool drains the durable command
queue; there is no broker or external time-series database.

## Security And Privacy

Coverage MCP binds to loopback by default and does not upload source, logs, coverage, or DuckDB data. Treat a registered
test command as local code execution: registration requires an explicit human approval record, working directories
must stay inside the selected repository, and agents can only run immutable registered commands. Do not expose the
HTTP port to an untrusted network. Review commands and artifact paths before approving them, especially in repositories
you do not control.

## Test Coverage

The test suite covers:

- parser normalization for all supported formats
- exact line, branch, function, and aggregate region counters
- auto-detection for all supported formats
- lossy-format warnings
- parser error paths
- DuckDB snapshot storage and baseline comparison
- approved command registration and run ledger storage
- bounded profiled command summaries
- artifact registration and lookup
- freshness-guarded managed artifact auto-ingestion and run-to-snapshot linkage
- computed topology for projects, commands, runs, snapshots, and artifacts
- FastAPI ingest/list endpoints
- project summaries and coverage insights
- the exact 10-tool MCP inventory, every input description, enum, required field, and numeric/string bound
- both MCP resources and templates
- in-process MCP execution for every workflow and validation failure
- a real Streamable HTTP session through the official MCP client, including initialization, schemas, calls, and resources
- README contract coverage that requires every MCP tool to document all inputs, outputs, and errors
- REST route parity that prevents API endpoints from drifting out of the README

Local quality gates:

```bash
pytest -q
ruff check .
mypy coverage_mcp
coverage run -m pytest -q
coverage report -m
```

The coverage configuration measures the `coverage_mcp` package and fails below 100%.

To run the same gates through tox:

```bash
tox
```

## Contributing

Issues and pull requests are welcome. Keep changes scoped, add regression coverage for behavior changes, preserve the
shared service/projection contract across MCP, resources, REST, and the dashboard, and run the quality gates above.
Public API changes should update this README and include contract tests. Please report security-sensitive problems
privately to the repository owner instead of opening a public issue.

## License

MIT. See [LICENSE](LICENSE).
