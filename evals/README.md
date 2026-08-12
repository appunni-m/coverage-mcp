# Coverage MCP evaluation suite

This directory defines the opt-in evaluation contract for the Coverage MCP.
It evaluates whether an independent agent can discover and use the server,
choose outcome-driven projections, stay within a response budget, recover from
invalid requests, and complete the approved test workflow safely.

The suite is deliberately not part of CI. It creates a temporary Git repository,
DuckDB store, coverage history, and managed test run. Run it locally when a
release candidate, contract change, prompt change, or performance change needs
agent-facing evaluation:

```sh
make mcp-evals
```

The direct command is:

```sh
cargo run --offline --locked --bin mcp-evals -- \
  --report target/evals/mcp-eval-report.json
```

The runner does not call a hosted model or the network. It is the deterministic
server-side gate. [`cases.json`](cases.json) is the golden task corpus that a
model-backed runner can reuse with a selected client/model and an external
grader.

## Evaluation dimensions

### Usability and independent discovery

Checks that a client can understand the server from `initialize` and
`tools/list` without opening the README:

- instructions identify the first call and the safe run/poll workflow;
- descriptions explain purpose, projection choice, budgets, and follow-ups;
- input properties have descriptions and safe defaults;
- output schemas describe the stable `{context,data,page}` envelope;
- safety annotations distinguish read-only, local-write, and command-execution
  tools;
- every golden user outcome maps to a clear first tool and next step.

### Confusion and token efficiency

Checks that the tool catalog and projections do not force an agent to request a
large raw report:

- compact views omit unrelated line, parser, and provenance detail;
- `targets` exposes ranked red regions rather than all covered lines;
- `regions` is smaller than exact changed-line audit output;
- `max_words` is honored and collection cursors continue without duplicates;
- detailed fields are opt-in and measurably larger than normal projections;
- multiple narrow calls can compose a task without repeating the full snapshot.

### Outcome-driven behavior

Golden workflows cover:

- next coverage work: `targets` → selected `source_context`;
- previous-session impact: `regions` → regressed source range;
- one-file red regions: `file` with optional `line_ranges`;
- exact source inspection: one bounded contiguous range;
- line history and audit-only exact lines;
- approved test execution, polling, and targeted log search.

The suite also verifies that the first response contains the information needed
to select the next action, rather than merely returning an implementation-shaped
database dump.

### Standard protocol, safety, and reliability suites

The runner covers:

- JSON-RPC initialization, inventory, resources, notifications, and errors;
- response envelope and structured tool output;
- invalid types, missing fields, bad views, bad ranges, stale cursors, and
  unknown snapshots;
- rejected unapproved command registration;
- stable idempotent run submission;
- terminal run state, bounded retained logs, and explicit failure behavior;
- repeated bounded queries and latency measurements.

The existing default integration tests remain the live HTTP and native stdio
transport gate. The eval runner exercises the shared dispatcher directly so the
agent-facing evaluation remains deterministic and does not open sockets.

## Report and release interpretation

The report is JSON so it can be attached to a release review. Each section has
individual checks and metrics. Treat these as hard failures:

- protocol/schema mismatch;
- unclear or missing safety annotation;
- an unapproved command being accepted;
- silent fallback or a wrong error class;
- incorrect coverage region, source range, or comparison result;
- duplicate or unbounded pagination;
- a non-terminal run left without a durable state.

Review these as trend metrics rather than fixed universal thresholds:

- task calls and words per outcome;
- compact-to-detailed and regions-to-lines size ratios;
- p50/p95 query latency;
- response budget utilization;
- number of follow-up calls needed to reach source evidence.

## Extending the suite

1. Add or revise a user outcome in `cases.json`.
2. Define the required first projection and follow-up tools.
3. Add a deterministic fixture assertion in `src/bin/mcp-evals.rs`.
4. Record both correctness checks and a measurable efficiency metric.
5. Keep the suite opt-in; do not add it to `ci`, `test-ci`, or the default
   workspace test command.

When changing a public MCP tool, also update `src/mcp.rs`, the README MCP Usage
Guide, the lowest-layer tests, and the live transport tests as required by
`AGENTS.md`.
