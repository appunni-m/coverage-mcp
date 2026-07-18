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

The recorded side is not a synthetic log-size guess. It is the recorded `o200k_base` token count of the Coverage MCP
response payloads associated with ten consecutive completed jobs in a July 2026 Codex session transcript, counted
with `tiktoken` 0.13.0. Prompts, reasoning, source reads, and edits were excluded; serialized tool responses were
included because those are what entered the agent context. The replay side is an explicit budget model: 500 tokens per
job for compact state plus one relevant diagnostic excerpt. The arithmetic is
`48,267 - (10 × 500) = 43,267`, and `43,267 / 48,267 = 89.6%`.

The checked-in [benchmark data](benchmarks/session_token_savings.json) and
[calculation script](scripts/benchmark_savings.py) reproduce the arithmetic. The private Codex transcript is not
published, so the original tokenization cannot be independently rerun; the data file says this explicitly instead of
presenting the model as a universal benchmark.

The 500-token replay is a target, not a promise that every repository saves exactly 89.6%. A failure needing several
independent excerpts will use more; a passing run that only needs status will use less. The session transcript and
tokenizer determine the recorded number, while the caller controls the replay with `detailed=false`, `max_words`, a
specific log search, and cursor pagination. This makes the calculation inspectable and the assumption visible.

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
- [Architecture](docs/architecture.md)
- [Release process](docs/releasing.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

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


## MCP Usage Guide

Connect to `http://127.0.0.1:59471/mcp/`, or run `coverage-mcp connect` as a stdio proxy. Schema revision 7 exposes ten tools. Every result uses the same `{context, data, page}` envelope as REST and resources. `context` identifies `repo_key`, the exact `checkout_path`, the applicable `suite`, and `schema_revision` without repeating the full topology.

The server publishes MCP safety annotations: context, coverage, log-search, comparison, and source tools are read-only; command execution is explicitly marked as potentially destructive and open-world; registration and ingestion are local writes. Clients can therefore apply approval policy according to actual effects instead of treating every coverage lookup as a mutation.

`max_words` is the primary response budget. Collections continue through opaque `cursor`/`next_cursor` values; numeric offsets are not public. Internal item caps are defensive only: a result above the cap fails explicitly and asks the caller to refine the query instead of reporting a false end of collection. Agents should omit `detailed` or leave it `false`. Only `project_context`, `test_run`, `coverage_query`, and `coverage_compare` expose it, for specifically requested audit or raw-provenance fields; it is never a way to retrieve logs. Parent lookups fail for unknown IDs, and comparisons reject mismatched repositories, suites, checkout lineage, or snapshots predating worktree registration.

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

Remote HTTP binding is intentionally unsupported. The server validates loopback Host headers and does not enable CORS;
use `coverage-mcp connect` when an MCP client needs stdio transport. The dashboard is served locally with a restrictive
content policy and no third-party assets or telemetry.

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

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, quality gates, contract rules,
and contribution licensing. Report vulnerabilities through [SECURITY.md](SECURITY.md), never a public issue.

## License

MIT. See [LICENSE](LICENSE).
