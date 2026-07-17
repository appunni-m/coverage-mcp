# Coverage MCP

Coverage MCP is a local coverage history server for people who run tests often and need to know:

- did coverage go up or down after this run?
- which files changed coverage?
- which exact lines regressed or improved?
- what was the baseline when this worktree started?
- can an LLM answer coverage questions without reading huge coverage reports or source files?

It runs locally, stores coverage snapshots in DuckDB, exposes a dashboard, and provides MCP tools for LLM clients.

## What Problem It Solves

Coverage reports are usually large, format-specific artifacts. They are hard for humans to diff and wasteful for LLMs to read directly.

Coverage MCP turns those reports into small queryable records:

- overall snapshot coverage
- file-level coverage
- exact line-level coverage
- branch and function counters when the report has them
- time-series history by minute-level timestamp
- frozen baseline references for worktrees

The intended workflow is explicit: run tests, generate coverage, then call the MCP tool or REST endpoint to ingest that report.

## What Coverage Details Include

Coverage details are normalized into four levels:

- project: repo path/key, latest snapshot, snapshot count, branch count, latest line and branch rates
- snapshot: timestamp, branch, commit SHA, suite, format, totals, rates, parser warnings
- file: covered/total lines, covered/total branches, covered/total functions, per-file rates
- line: line number, hits when available, covered/missed state, branch/function counters on that line

When the report format is lossy, the snapshot keeps a warning. For example, Go coverprofiles report blocks, Istanbul reports statements, and LLVM reports segments.

## Install The Server

Coverage MCP currently installs from GitHub:

```bash
python -m pip install "coverage-mcp @ git+https://github.com/appunni-m/coverage-mcp.git@main"
```

For development from this checkout:

```bash
python -m pip install -e '.[dev]'
```

Python 3.12 or newer is required.

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

The response must report `version: "0.3.2"` and includes the active `db_path`.

## Start The Shared Server

Start one server from the main checkout, not one server per worktree:

```bash
cd /path/to/main-checkout
coverage-mcp
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
<main-repository>/.coverage-mcp/coverage.duckdb
```

Coverage MCP resolves Git's shared repository root before choosing the default database. Starting it from `main` or
from any linked worktree therefore opens the same DuckDB. This preserves baseline and worktree lineage without
committing local history. The `.coverage-mcp/` directory should remain ignored by Git.

Do not start another Coverage MCP process from each worktree. Every agent and worktree should connect to this one
server so DuckDB has one writer and one continuous project history.

Override host, port, or DB:

```bash
COVERAGE_MCP_HOST=127.0.0.1 \
COVERAGE_MCP_PORT=8765 \
COVERAGE_MCP_DB=/path/to/coverage.duckdb \
coverage-mcp
```

Retain more or fewer terminal test runs per approved command:

```bash
COVERAGE_MCP_RUN_RETENTION=250 coverage-mcp
```

The default is `100`. The active value is returned by `/health`.

## Install In An Agent

The `testing` plugin in
[codegen-marketplace](https://github.com/appunni-m/codegen-marketplace) includes the Coverage MCP connection and
agent instructions for approved test runs, bounded summaries, artifact ingestion, and worktree comparisons.

The plugin expects Coverage MCP at `http://127.0.0.1:59471/mcp/`. Start the server before opening a new agent session.

The plugin installs only:

- the `use-coverage-mcp` skill
- the HTTP MCP connection metadata
- plugin documentation and prompts

It does not install the Python server, start a background process, or copy the
DuckDB. Upgrade the plugin for agent instructions and connection changes;
upgrade Coverage MCP for parser, storage, API, dashboard, or performance
changes.

### Codex

```bash
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Start a new Codex thread after installation. To install only the MCP connection without the testing skill:

```bash
codex mcp add coverage-mcp --url http://127.0.0.1:59471/mcp/
```

### Claude Code

```bash
claude plugin marketplace add appunni-m/codegen-marketplace
claude plugin install testing@codegen-marketplace
```

Start a new Claude Code session after installation. To install only the MCP connection:

```bash
claude mcp add --transport http --scope user coverage-mcp http://127.0.0.1:59471/mcp/
```

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
Run the approved test suite, ingest its coverage artifact, and tell me whether this worktree improved against its
frozen baseline.
```

## Quick Workflow

1. Generate a coverage report from your test tool.
2. Ingest the report into Coverage MCP.
3. Open the dashboard or ask your MCP client for summaries, file details, changed lines, and history.

Example with LCOV:

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

## `AGENTS.md` Snippet

Projects using Coverage MCP can place this small policy in their `AGENTS.md`:

```md
## Coverage MCP

- Reuse the repository's single Coverage MCP server and shared DuckDB. Never copy the database into a worktree or set
  `COVERAGE_MCP_DB` to a worktree-local path.
- Register a new worktree once with `register_worktree(path, base_ref, name)` before its first coverage run. Keep the
  returned `worktree_id`; its frozen baseline defines the lineage for that worktree.
- Run tests through an existing human-approved command with `run_command_profiled`. Record the returned run id and
  use one stable `idempotency_key` for that intended run. Poll `run_result` no faster than `poll_after_ms`; retries
  must reuse the same key. If no approved command exists, ask for explicit approval before registering one.
- Use `eta_seconds` to schedule the next status check when available. Treat it as a median-based estimate, use
  `duration_p90_ms` as the conservative reference, and keep polling normally when the estimate is exceeded or absent.
- Ingest generated coverage with the actual worktree path as `repo_path`, its branch/commit, and a stable suite name.
- Use `worktree_progress(worktree_id, suite)` or `compare_to_baseline(worktree_id=...)` to report whether line, branch,
  function, and region coverage improved. Do not compare one worktree's snapshots to another worktree.
- Keep suite names stable: each suite is compared with the matching base snapshot that existed when the worktree was
  registered, never with a later reference-branch run.
- Treat the reference branch trend (normally `main`) as project health. Treat each worktree trend as independent
  progress from its frozen reference baseline.
```

The same operation through MCP:

```text
ingest_coverage(
  report_path="coverage/lcov.info",
  format="lcov",
  repo_path="/path/to/repo",
  branch="main",
  suite="unit"
)
```

Use `format="auto"` when you want Coverage MCP to detect the report type.

## Approved Run Ledger

Coverage MCP can also record test runs. It does not require a YAML suite file.

Instead, a human explicitly registers the full command string once, including cwd and expected artifacts. Registration requires approval fields:

```text
register_test_command(
  name="condition",
  command="make -C pillow-rs-freetype test-unified-condition-coverage",
  cwd="/path/to/repo",
  artifact_paths={
    "llvm_json": "pillow-rs-freetype/target/coverage/unified-condition-summary.json",
    "missing_lines": "pillow-rs-freetype/target/coverage/unified-condition-missing-lines.txt"
  },
  human_approved=true,
  approved_by="your-name",
  approval_note="approved exact condition coverage command"
)
```

After that, agents run only the registered command id or name:

```text
run_command_profiled(
  command_ref="condition",
  max_summary_lines=80,
  idempotency_key="condition:<commit-sha>:coverage"
)
```

Submission returns immediately with a durable run id and `status` set to `queued` or `running`. Poll that id until
`terminal` is true:

```text
run_result(run_id="returned-run-id", max_summary_lines=80)
```

Use `run_queue()` to inspect FIFO position. Set `wait=true` only for a command known to finish quickly; the default
background mode keeps the MCP call responsive during long suites.

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

Managed commands run outside the MCP event loop. One local worker executes them in FIFO order, so dashboards, health
checks, coverage insights, and other agents remain responsive without running several expensive suites concurrently.
Queued jobs survive a clean restart and resume from the same DuckDB. Graceful shutdown lets the current command
finish. If the server exits unexpectedly during a command, restart recovery preserves that run as `interrupted`
instead of rerunning it automatically.

Every command starts in its own process group. `cancel_run` and command timeouts signal the entire group, then escalate
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

Connect your MCP client to:

```text
http://127.0.0.1:59471/mcp/
```

Coverage MCP `0.3.2` exposes 21 tools. Their MCP JSON Schemas carry the same
descriptions, required fields, enums, and bounds documented below. Invalid
types, missing required inputs, out-of-range values, and unsupported enum
values are returned as MCP tool errors before execution.

General conventions:

- IDs are durable UUID strings. A `command_ref` accepts either a registration ID or its latest matching name.
- `file_path` is the exact repository-relative path stored by the report. Rename history is intentionally not inferred.
- Result limits are inclusive. `limit` is tool-specific; `max_summary_lines` is always 1-500.
- Times are returned as UTC timestamps plus `age_seconds` and a human-readable `age` such as `10 minutes 3 seconds ago`.
- Missing IDs, files, reports, artifacts, or applicable baselines are MCP tool errors, not empty success objects unless stated otherwise.

### `project_summaries`

`project_summaries(limit: integer = 100)` discovers projects known through snapshots, commands, or runs.

**Inputs:** `limit` is the maximum records to return, from 1 through 1000.

**Returns:** A list containing project identity, latest snapshot and run, counts, current rates, freshness, and inline topology.

**Errors:** Schema validation errors only; an empty server returns an empty list.

### `register_test_command`

`register_test_command(name, command, human_approved, approved_by, approval_note, cwd = null, shell = "/bin/bash", artifact_paths = null)` creates an immutable approved command definition.

**Inputs:** `name` is a non-empty stable label; `command` is the complete shell command; `human_approved` must literally be `true`; `approved_by` identifies the approving human; `approval_note` records the exact approval; `cwd` is the project working directory or null for the server cwd; `shell` is the executable shell; `artifact_paths` maps artifact kind to either a path string or `{path, required, coverage_format}`. Relative artifact paths resolve from `cwd`; `required` defaults to false and `coverage_format` defaults to null. When present, `coverage_format` accepts the formats listed under `ingest_coverage`.

**Returns:** The registration ID, exact execution details, approval audit fields, project topology, enabled state, and learned duration fields.

**Errors:** Missing or false approval, blank required text, or invalid cwd/artifact configuration produces a tool error. Duplicate definitions are allowed as separate immutable registrations; changing an approved command requires a new registration.

```text
register_test_command(
  name="unit-coverage",
  command="pytest --cov=src --cov-report=json:.coverage-mcp/coverage.json",
  cwd="/path/to/repo",
  artifact_paths={
    "coverage_json": {
      "path": ".coverage-mcp/coverage.json",
      "required": true,
      "coverage_format": "coveragepy"
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

`run_command_profiled(command_ref, max_summary_lines = 80, timeout_seconds = null, idempotency_key = null, wait = false)` submits an approved command to the single FIFO worker.

**Inputs:** `command_ref` is a registration ID or name; `max_summary_lines` is 1-500; `timeout_seconds` is null or 1-86400; `idempotency_key` is null or a 1-200 character key scoped to that command; `wait` blocks until terminal only when true.

**Returns:** Normally a durable run with `status` queued/running, `terminal: false`, `poll_after_ms`, queue position, log paths, and historical ETA fields. A terminal result also contains exit code, timing, bounded excerpts/counters, artifacts, freshness, and status `passed`, `failed`, `cancelled`, `timeout`, `interrupted`, or `internal_error`. Reusing the same idempotency key returns the original run with `submission_reused: true`.

**Errors:** Unknown/disabled commands, invalid limits/timeouts, or reusing one idempotency key with different submission parameters produces a tool error. A test failure is a successful tool response with `status: "failed"`, not an MCP protocol error.

### `run_queue`

`run_queue(limit: integer = 100)` inspects active work in FIFO order.

**Inputs:** `limit` is 1-1000.

**Returns:** The running run first with `queue_position: 0`, followed by queued runs with positive positions, polling guidance, and ETA. If history is insufficient, `eta_unavailable_reason` explains why.

**Errors:** Schema validation errors only; an idle worker returns an empty list.

### `cancel_run`

`cancel_run(run_id, max_summary_lines = 80)` requests cancellation of queued or running work.

**Inputs:** `run_id` is the durable run UUID; `max_summary_lines` is 1-500.

**Returns:** A queued run becomes terminal immediately. A running run reports `cancellation_requested: true` while Coverage MCP terminates its process group; poll `run_result` until terminal. Repeating cancellation on an already cancelled run is idempotent.

**Errors:** Unknown runs and terminal runs other than `cancelled` cannot be cancelled.

### `run_result`

`run_result(run_id, max_summary_lines = 80)` polls one submitted run.

**Inputs:** `run_id` is required; `max_summary_lines` is 1-500.

**Returns:** Active state avoids reading full logs and includes `poll_after_ms`, queue/ETA details, and log paths. Terminal state includes bounded output summaries, counters, artifacts, exact timing, and freshness. Poll no faster than `poll_after_ms`; ETA is historical, while timeout is an execution limit.

**Errors:** Unknown run IDs or invalid limits produce a tool error.

### `latest_run`

`latest_run(command_ref = null, max_summary_lines = 80)` retrieves the newest active or terminal run.

**Inputs:** `command_ref` optionally restricts lookup to a command ID/name; `max_summary_lines` is 1-500.

**Returns:** The same bounded state as `run_result`, including `age` and `age_seconds` so callers can decide whether it is stale.

**Errors:** Unknown command references, no matching runs, or invalid limits produce a tool error.

### `latest_artifact`

`latest_artifact(kind, command_ref = null)` locates a run artifact without searching build directories.

**Inputs:** `kind` is the non-empty artifact key from registration; `command_ref` optionally restricts lookup to one command ID/name.

**Returns:** Artifact kind/path, existence and size, run/command identity, project identity, run status/timing, freshness, and topology for the newest match.

**Errors:** Unknown command references or no matching artifact produces a tool error. A registered artifact that was not generated is still returned with `exists: false` when it is the latest match.

### `object_topology`

`object_topology(object_kind, object_ref)` resolves relationships without a separate topology object.

**Inputs:** `object_kind` accepts `project`, `repo`, `repository`, `command`, `registered_command`, `test_command`, `run`, `snapshot`, `coverage_snapshot`, or `worktree`; `object_ref` is the corresponding UUID, command name, or repo path/key.

**Returns:** Inline project identity plus the object's Git, command, run, artifact, snapshot, worktree, and baseline relationships as applicable.

**Errors:** Unsupported kinds and unresolved references produce a tool error.

### `ingest_coverage`

`ingest_coverage(report_path, format = "auto", repo_path = null, suite = "default", branch = null, commit_sha = null, base_ref = null)` parses and stores one immutable snapshot.

**Inputs:** `report_path` is a local server-readable artifact; `format` accepts `auto`, `lcov`, `coverage.py`, `coveragepy`, `coverage-json`, `coveragepy-json`, `cobertura`, `jacoco`, `istanbul`, `nyc`, `go`, `go-cover`, `go-coverprofile`, `coverprofile`, `llvm`, or `llvm-json`; `repo_path` identifies the checkout/shared repository; `suite` is a non-empty stable trend name; `branch`, `commit_sha`, and `base_ref` override or add Git metadata when provided.

**Returns:** Snapshot identity and topology, normalized line/branch/function/region totals and rates, report metadata, parser warnings, timestamp, and freshness.

**Errors:** Missing/unreadable artifacts, unsupported or misdetected formats, malformed reports, invalid repo paths, and empty suites produce a tool error.

### `register_worktree`

`register_worktree(path, base_ref, name = null)` records a linked checkout and freezes available reference coverage.

**Inputs:** `path` is the worktree checkout path; `base_ref` is the branch/revision whose latest suite snapshots become baseline references; `name` is an optional label.

**Returns:** Worktree ID, path/head metadata, project identity, frozen suite-to-snapshot baseline map, and inline topology.

**Errors:** Invalid/non-Git paths produce a tool error. If no reference coverage exists, registration succeeds with a null baseline so lineage is preserved, but progress/comparison tools fail clearly until reference coverage is ingested. Registration never copies DuckDB into the worktree.

### `worktree_progress`

`worktree_progress(worktree_id, suite = null, file_path = null, limit = 200)` answers whether one worktree improved from its frozen baseline.

**Inputs:** `worktree_id` is required; `suite` selects one coverage suite or the latest applicable suite; `file_path` optionally restricts the exact-path trend; `limit` is 1-2000 time-series points.

**Returns:** Worktree and baseline metadata, chronological points, latest snapshot, and line/branch/function/region rate deltas. Each worktree progresses independently while its baseline still belongs to the common parent history.

**Errors:** Unknown worktrees, absent suite baselines/current snapshots, or unknown exact file paths produce a tool error.

### `coverage_summary`

`coverage_summary(snapshot_id = null, repo_path = null, branch = null, suite = null)` returns one compact overall snapshot.

**Inputs:** `snapshot_id` directly selects an immutable snapshot. Without it, `repo_path`, `branch`, and `suite` filter the latest snapshot. If `snapshot_id` is supplied, it takes precedence and all filters are ignored.

**Returns:** Snapshot identity/topology, report metadata, line/branch/function/region totals and rates, warnings, timestamp, and freshness.

**Errors:** Unknown snapshot IDs or no snapshot matching the filters produces a tool error.

### `coverage_files`

`coverage_files(snapshot_id, limit = 100)` finds weak files without returning raw report data.

**Inputs:** `snapshot_id` is required; `limit` is 1-5000.

**Returns:** Per-file line/branch/function totals and rates ordered by lowest line coverage and then largest files.

**Errors:** Invalid inputs produce a tool error; a valid snapshot with no file records returns an empty list.

### `coverage_file`

`coverage_file(snapshot_id, file_path, include_lines = true)` drills into one exact path.

**Inputs:** `snapshot_id` and exact `file_path` are required; `include_lines` controls whether exact line records are included.

**Returns:** A `file` object with totals/rates and, by default, a `lines` array capped at 5000 records containing line number, hits, covered state, and branch/function counters when available.

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

## Worktree Baselines

The design goal is reproducible baseline comparison.

When you register a worktree, Coverage MCP stores a reference to one baseline snapshot:

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

That worktree stores `baseline_snapshot_id`. Later uploads to `main` do not change this baseline automatically. This lets you ask, "what changed compared with the base coverage when this worktree started?"

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

Useful endpoints:

- `GET /api/projects`
- `POST /api/commands/register`
- `GET /api/commands`
- `GET /api/commands/{command_ref}`
- `POST /api/runs/profiled`
- `GET /api/runs/queue`
- `POST /api/runs/{run_id}/cancel`
- `GET /api/runs/latest`
- `GET /api/runs/{run_id}`
- `GET /api/artifacts/latest`
- `GET /api/topology/{object_kind}/{object_ref}`
- `POST /api/ingest`
- `POST /api/worktrees/register`
- `GET /api/worktrees/{worktree_id}/progress`
- `GET /api/snapshots`
- `GET /api/snapshots/latest`
- `GET /api/snapshots/{snapshot_id}/insights`
- `GET /api/snapshots/{snapshot_id}/files`
- `GET /api/snapshots/{snapshot_id}/files/{file_path}`
- `GET /api/trend`
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

Topology is derived, not separately stored. The same immutable rows power both direct object responses and `object_topology`.

DuckDB is used because this is local-first and query-heavy. A lightweight in-process worker drains the durable command
queue; there is no broker or external time-series database.

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
- computed topology for projects, commands, runs, snapshots, and artifacts
- FastAPI ingest/list endpoints
- project summaries and coverage insights
- the exact 21-tool MCP inventory, every input description, enum, required field, and numeric/string bound
- all five MCP resources and templates
- in-process MCP execution for every workflow and validation failure
- a real Streamable HTTP session through the official MCP client, including initialization, schemas, calls, and resources
- README contract coverage that requires every MCP tool to document all inputs, outputs, and errors

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

## License

MIT. See [LICENSE](LICENSE).
