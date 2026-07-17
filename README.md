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

The response includes the running `version` and active `db_path`.

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

The most common tools are:

### `project_summaries`

Lists known projects with their latest coverage and snapshot counts.

```text
project_summaries(limit=100)
```

Use this first when you want to know which project has what coverage.

### `register_test_command`

Registers an approved command that can be run later by id or name.

```text
register_test_command(
  name="unit",
  command="pytest tests/unit",
  cwd="/path/to/repo",
  artifact_paths={"lcov": "coverage/lcov.info"},
  human_approved=true,
  approved_by="your-name",
  approval_note="approved unit test command"
)
```

Registration fails unless `human_approved` is true and approval metadata is present.

### `list_registered_commands`

Lists approved command records, newest first.

```text
list_registered_commands(limit=50)
```

### `run_command_profiled`

Queues a registered command and immediately returns its durable run id.

```text
run_command_profiled(
  command_ref="unit",
  max_summary_lines=80,
  idempotency_key="unit:<commit-sha>:requested-check"
)
```

The normal response has `status: "queued"` or `status: "running"`, `terminal: false`, `queue_position`, and
`poll_after_ms`. With sufficient command history it also has `eta_seconds`, a human-readable `eta`,
`estimated_completion_at`, `duration_estimate_ms`, `duration_p90_ms`, and `duration_sample_count`. Queued runs add
`estimated_start_at` and `queue_wait_estimate_seconds`; running jobs report `estimate_overrun_seconds` after exceeding
their historical median. Save the run id and poll `run_result`; do not call `run_command_profiled` again for the same
intended run. Retrying with the same `idempotency_key` reuses that run instead of creating a duplicate. Use `wait=true`
only for a command known to be short when a single blocking call is explicitly useful.

Once complete, the response includes pass/fail, exit code, execution and queue duration, key counters, selected
error/tail excerpts, full log paths, and artifact paths. It also includes the exact completion time plus freshness
fields such as `age_seconds: 603` and `age: "10 minutes 3 seconds ago"`.

### `run_queue`

Lists the currently running command followed by queued commands in FIFO order.

```text
run_queue(limit=50)
```

There is at most one running command per server. `queue_position: 0` means running; positive positions are waiting.
Queue ETAs are composed from the stored median duration of each command in FIFO order. A missing estimate anywhere
ahead produces `eta_unavailable_reason: "queue_history_incomplete"` rather than an unreliable ETA.

### `cancel_run`

Cancels a queued or running command by run id.

```text
cancel_run(run_id="...")
```

Queued runs become terminal immediately. Running runs expose `cancellation_requested: true`; poll `run_result` until
their status becomes `cancelled`. Repeating cancellation for an already cancelled run is safe. Other terminal runs
cannot be cancelled.

### `latest_run`

Returns the latest bounded run result, optionally for one registered command.

```text
latest_run(command_ref="unit", max_summary_lines=80)
```

Use this before rerunning a suite. The `age` and `age_seconds` fields make it
clear whether the previous result is fresh enough for the current task.

### `run_result`

Returns current status or a bounded final summary for a submitted run.

```text
run_result(run_id="...", max_summary_lines=80)
```

Poll no faster than the returned `poll_after_ms`. Active responses intentionally defer log scanning and expose the
full log paths. Treat ETA as a historical estimate, not a timeout: p90 is the conservative reference, and
`estimate_overrun_seconds` shows when a running command has exceeded its median. Stop polling when `terminal` is true.
Terminal statuses are `passed`, `failed`, `cancelled`, `timeout`, `interrupted`, and `internal_error`.

### `latest_artifact`

Finds the latest artifact of a given kind for a command.

```text
latest_artifact(command_ref="unit", kind="lcov")
```

### `object_topology`

Returns the computed topology for an object.

```text
object_topology(object_kind="command", object_ref="unit")
object_topology(object_kind="run", object_ref="run-id")
object_topology(object_kind="snapshot", object_ref="snapshot-id")
object_topology(object_kind="project", object_ref="/path/to/repo")
```

### `ingest_coverage`

Stores a coverage report as an immutable snapshot.

```text
ingest_coverage(report_path, format="auto", repo_path=None, suite="default")
```

Use this after every test run that produces a coverage artifact.

### `coverage_summary`

Gets a compact overall summary for a snapshot or the latest known snapshot.

```text
coverage_summary()
coverage_summary(snapshot_id="...")
coverage_summary(repo_path="/path/to/repo", branch="main")
```

### `coverage_files`

Lists files in a snapshot, lowest line coverage first.

```text
coverage_files(snapshot_id="...", limit=50)
```

Use this when you want the LLM to find weak files without reading the whole report.

### `coverage_file`

Shows one file's coverage and, by default, its line records.

```text
coverage_file(snapshot_id="...", file_path="src/app.py")
```

### `coverage_insights`

Returns prioritized investigation items for a snapshot.

```text
coverage_insights(snapshot_id="...")
coverage_insights(
  snapshot_id="current-snapshot-id",
  baseline_snapshot_id="baseline-snapshot-id"
)
```

The insight output is deterministic and data-driven. It can flag:

- files with zero line coverage
- files with low line coverage
- files with low branch coverage
- parser warnings from lossy formats
- overall regressions against a baseline
- files that regressed against a baseline
- exact lines that became uncovered

### `compare_to_baseline`

Compares two snapshots or compares a worktree's current snapshot against its frozen baseline.

```text
compare_to_baseline(
  snapshot_id="current-snapshot-id",
  baseline_snapshot_id="baseline-snapshot-id"
)
```

For a registered worktree:

```text
compare_to_baseline(worktree_id="...")
```

### `worktree_progress`

Returns one registered worktree's frozen baseline, exact-path trend, latest point, and line/branch/function/region
deltas. This is the preferred compact answer to "did this worktree improve coverage?"

```text
worktree_progress(worktree_id="...", suite="unit")
```

### `changed_lines`

Returns exact line-level coverage changes between two snapshots.

```text
changed_lines(
  snapshot_id="current-snapshot-id",
  baseline_snapshot_id="baseline-snapshot-id",
  only_regressions=true
)
```

Use this when you want the LLM to report only the lines that became uncovered.

### `line_history`

Shows coverage history for one file path and line number.

```text
line_history(file_path="src/app.py", line_number=42)
```

History is path-based. Renames are not tracked.

### `source_context`

Reads a bounded source range from the repository for a covered file.

```text
source_context(
  snapshot_id="...",
  file_path="src/app.py",
  start=35,
  end=50
)
```

Use this only when coverage metadata is not enough. The response is capped so an LLM does not pull a large source file by accident.

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
- MCP tool calls for command registration/runs, ingest, summary, project listing, insights, file listing, and file drill-down

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
