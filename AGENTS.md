# Coverage MCP Agent Notes

This repository owns the Coverage MCP server, REST API, MCP tool contract,
storage behavior, dashboard, and primary README. When changing MCP behavior,
keep the server contract, user docs, marketplace plugin docs, and installed
tooling in sync.

## MCP Contract Change Checklist

Use this checklist for any change that affects MCP tool names, inputs, output
shape, descriptions, server instructions, safety annotations, resources, or the
agent workflow.

1. Update server code in `coverage_mcp/app.py`.
   - `create_mcp()` instructions must be sufficient for an agent without
     reading the README.
   - Tool docstrings must explain when to use the tool, important mode choices,
     pagination/budget behavior, and where to go next.
   - Safety annotations must match actual side effects.

2. Update typed contract descriptions in `coverage_mcp/contracts.py`.
   - Input descriptions are what clients see in `tools/list`.
   - Keep names consistent with actual tool names. For example, polling uses
     `test_run`, not internal storage names such as `run_result`.
   - Describe non-obvious semantics such as OR matching, cursor ownership, or
     detailed-mode limits.

3. Update implementation layers that share the behavior.
   - Storage and helpers own validation and core semantics.
   - Service owns compact envelopes and fields hidden from default MCP output.
   - REST should remain consistent with MCP when it exposes the same behavior.

4. Update tests that lock the public contract.
   - `tests/test_mcp.py` must assert tool inventory, input names, generated
     schema descriptions, server instructions, resources, and README coverage.
   - Add behavior tests at the lowest useful layer, then one REST or MCP path
     test when public input/output changed.

5. Update `README.md`.
   - The MCP Usage Guide must list every tool, every input, returns, errors,
     resources, and the effective workflow.
   - State that MCP instructions plus `tools/list` are intended to be
     sufficient without the README.

6. Update `codegen-marketplace` when agent instructions or connector guidance
   changes.
   - Root: `/Users/lazytrot/work/codegen-marketplace/README.md` when examples
     or user-facing marketplace instructions change.
   - Plugin docs: `plugins/testing/README.md`.
   - Skill workflow: `plugins/testing/skills/use-coverage-mcp/SKILL.md`.
   - If generated marketplace artifacts are affected, follow that repo's
     `CLAUDE.md` and run its build/check workflow. Do not edit generated root
     artifacts directly.

7. Reinstall and restart after merging or pushing.
   - Reinstall the local Codex plugin when marketplace docs/skills change:
     `codex plugin add testing@codegen-marketplace`.
   - Reinstall the server from this checkout when server code changes:
     `/Users/lazytrot/Library/Python/3.9/bin/uv pip install --python /Users/lazytrot/work/coverage-mcp/.venv/bin/python -e '/Users/lazytrot/work/coverage-mcp[dev]'`.
   - Restart the daemon with the built-in helper:
     `/Users/lazytrot/work/coverage-mcp/.venv/bin/python -c 'from coverage_mcp.app import ensure_daemon; print(ensure_daemon())'`.

8. Verify the live contract, not only local unit tests.
   - `/health` must report schema revision 7 and the expected daemon path.
   - A live MCP `tools/list` call over `http://127.0.0.1:59471/mcp/` must show
     updated instructions, tool descriptions, and input schema.
   - Run the repo gate relevant to the change. For MCP changes, at minimum run
     `ruff check .`, `ruff format --check .`, `mypy coverage_mcp`, and
     `pytest -q tests/test_mcp.py`; use the approved full coverage command
     before finalizing broad or public-contract changes.

## Current MCP Workflow

Agents should:

1. Call `project_context` first.
2. Run only exact approved registrations returned by `project_context`, or call
   `register_test_command` only after human approval of the exact command, cwd,
   shell, and artifacts.
3. Submit with `run_test(wait=false)` and a stable `idempotency_key`.
4. Poll `test_run(action="status", detailed=false)` no sooner than
   `poll_after_ms` until `terminal` is true.
5. Use `search_test_logs` for targeted retained stdout/stderr evidence; never
   use `detailed` to retrieve logs.
6. Inspect `coverage_ingest.status` and `snapshot_ids` before making coverage
   claims.
7. Use `coverage_query` for snapshot reads, `coverage_compare` only for
   compatible lineage or registered worktrees, and `source_context` only for
   bounded source ranges already identified by coverage data.
