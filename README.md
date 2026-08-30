# Coverage MCP

[![CI](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/appunni-m/coverage-mcp/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/coverage-mcp.svg)](https://crates.io/crates/coverage-mcp)
[![Rust API documentation](https://docs.rs/coverage-mcp/badge.svg)](https://docs.rs/coverage-mcp)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

Use Coverage MCP when you want a coding agent to run approved tests, remember
coverage across sessions and Git worktrees, explain what changed, and point to
the next useful test without pasting a large coverage report into the model's
context.

Coverage MCP turns a project-generated coverage report into an immutable
snapshot. An agent can then ask focused questions such as:

- Which changed lines are still untested?
- Did this worktree improve or regress coverage?
- What are the highest-priority gaps?
- What did one smaller test case add to a fixed full-suite baseline?
- Which named tests cover exactly the same regions and deserve human review?

The normal user experience is the `testing` plugin from
[codegen-marketplace](https://github.com/appunni-m/codegen-marketplace). The
plugin gives the agent a safe coverage-review workflow and starts the native
Coverage MCP runtime automatically.

> **Safety note:** Coverage MCP can run commands in your repository with your user account's
> permissions. A new command must be shown to you and explicitly approved
> before it can be registered or run.

## Install for Codex

Do not clone this development repository for normal use. Install the published
`testing` plugin instead:

```sh
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Start a new Codex task in the repository you want to review. The marketplace
plugin downloads and verifies a compatible Coverage MCP release on supported
macOS, Linux, and WSL hosts. If a prebuilt release is unavailable and Cargo is
already installed, the plugin can build the exact published crate as a
fallback.

For another MCP host, native Windows, or a deliberate manual installation, use
the package published on crates.io:

```sh
cargo install coverage-mcp --locked
coverage-mcp --version
```

Manual Cargo installation requires Rust 1.85 or newer. See the
[installation and first-review guide](docs/getting-started.md) for platform
details, upgrades, and removal.

## Get your first coverage review

Open a new Codex task in your project and ask:

> Use the coverage-review workflow. Inspect this repository's current coverage
> and show me the three highest-priority gaps. Reuse a fresh approved run if one
> exists. If a coverage command needs approval, show me the exact command,
> working directory, shell, report path, format, and suite before running it.

If the project has not been set up before, the agent will inspect its existing
test tooling and propose a coverage command. Review that command and its report
path before approving it. A successful first run reports:

- the command's terminal state and coverage-artifact status;
- an immutable snapshot ID;
- available line, branch, function, and region metrics;
- a bounded list of coverage gaps and one suggested next action.

After the daemon starts, the local dashboard is available at
<http://127.0.0.1:59471/>. It shows stored snapshots, comparisons, source gaps,
test runs, and retention status. You do not need to start the dashboard or
daemon separately when using the Codex plugin.

## How the workflow fits together

```text
your approved test command
          │
          ▼
project coverage report ──► immutable snapshot ──► bounded agent review
                                      │
                                      └──────────► local dashboard and history
```

Coverage MCP does not generate language-specific coverage by itself. Your
project's existing test tool produces LLVM JSON, LCOV, coverage.py JSON,
Cobertura XML, JaCoCo XML, Istanbul JSON, or a Go cover profile. Coverage MCP
validates and stores that evidence, preserves its Git and run provenance, and
returns only the detail needed for the current question.

The system is local-first:

- the service binds only to loopback;
- each Git repository keeps its own coverage history;
- linked worktrees share the same repository history;
- approved commands and completed snapshots are durable;
- compact responses keep large reports out of the agent context.

## Choose the guide you need

| Goal | Guide |
| --- | --- |
| Install the plugin and complete one review | [Getting started](docs/getting-started.md) |
| Ask common coverage questions and understand the result | [User guide](docs/user-guide.md) |
| Configure the daemon, storage, upgrades, or troubleshooting | [Operations guide](docs/operations.md) |
| Integrate an MCP client or inspect all eight tools | [MCP reference](docs/mcp-reference.md) |
| Use the loopback JSON API | [HTTP API reference](docs/http-api.md) |
| Understand processes, storage, locking, and compaction | [Architecture](docs/architecture.md) |
| Build or change Coverage MCP | [Contributing](https://github.com/appunni-m/coverage-mcp/blob/main/CONTRIBUTING.md) |
| Publish a release | [Release guide](https://github.com/appunni-m/coverage-mcp/blob/main/docs/releasing.md) |

The MCP server is self-describing: its `initialize` and `tools/list` responses
contain the live tool schemas and workflow constraints. The documentation is a
human-readable guide; the executable response is authoritative for the
installed version.

## Status and compatibility

Coverage MCP is pre-1.0 software. The current source contract is schema
revision 16 with eight agent-facing tools. Before 1.0, a minor release may make
a breaking public-contract change; the marketplace pins a compatible runtime
for each plugin release.

The source checkout pins Rust 1.85.1 and supports a manual Cargo install with
Rust 1.85 or newer. Marketplace prebuilt releases target ARM64 and x86-64 macOS
and Linux, including WSL. Native Windows currently uses the published Cargo
package and an explicit `coverage-mcp.exe connect` client configuration.

The repository's required gate checks formatting, Clippy, tests, rustdoc, a
bundled release build, and 100% measured line, function, and region coverage
for the configured Rust library/runtime target set. That gate is a contributor
policy, not a claim that every possible deployment or input is defect-free.

## Security, support, and license

Coverage reports, repositories, retained logs, and approved commands are local
inputs and may contain sensitive information. Keep the service on loopback and
do not commit `.coverage-mcp/` databases. See the [security policy](SECURITY.md)
for the trust boundary and private vulnerability-reporting route.

Use the [support guide](SUPPORT.md) for sanitized bug reports and feature
requests. Coverage MCP is maintained on a best-effort basis without a
commercial support SLA.

Coverage MCP is released under the [MIT License](LICENSE).
