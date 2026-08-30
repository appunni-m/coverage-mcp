# Install Coverage MCP and complete your first review

This guide is for someone who wants a coding agent to find useful coverage
gaps. You do not need to understand MCP, DuckDB, daemon processes, or Coverage
MCP's wire format before starting.

By the end, your agent will have either reused an existing coverage snapshot or
run one command that you approved, stored the result, and returned a small list
of concrete gaps.

## Before you start

You need:

- Codex with plugin support;
- a Git repository containing the project to review;
- a test setup that can produce a supported coverage report.

You do not have to know the coverage command in advance. The agent can inspect
the repository's existing scripts, Makefile targets, package configuration, and
CI workflow, then propose the exact command and output file for your approval.

Coverage MCP accepts these report families:

| Ecosystem or tool | Accepted report |
| --- | --- |
| Rust, C, C++, Swift, and other LLVM-instrumented code | LLVM coverage JSON |
| Tools that emit LCOV | LCOV tracefile |
| Python | coverage.py JSON |
| XML-based tools | Cobertura XML or JaCoCo XML |
| JavaScript and TypeScript | Istanbul/NYC JSON |
| Go | Go cover profile |

## 1. Install the testing plugin

Add the codegen marketplace once, then install its `testing` plugin:

```sh
codex plugin marketplace add appunni-m/codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Confirm that Codex sees the plugin:

```sh
codex plugin list
```

Look for `testing@codegen-marketplace` with status `installed, enabled`. Start
a new Codex task after installation so the task receives the plugin's workflow
and Coverage MCP server.

### What the plugin installs

The plugin supplies two pieces that work together:

- `coverage-review`, which tells the agent how to request approval, run tests,
  select valid baselines, and report bounded evidence;
- a required Coverage MCP connector, which obtains the exact runtime version
  selected by the plugin and connects it to the current repository.

On ARM64 or x86-64 macOS and Linux, including WSL, the connector first looks
for the matching binary on `PATH` or in its versioned cache. Otherwise it
downloads a checksummed GitHub Release archive. If that path is unavailable and
Cargo already exists, it installs the same published crate version as a
fallback. It does not clone or run this development checkout.

Native Windows does not currently use the bundled POSIX bootstrap. Follow
[Manual installation](#manual-installation-for-other-mcp-hosts) and configure
the host to launch `coverage-mcp.exe connect`.

## 2. Open the project you want to review

Start the new Codex task from the Git checkout whose tests you want to inspect.
Coverage MCP uses that checkout to select the repository, commit, branch, and
shared worktree history.

Ask for a first review in plain language:

> Use the coverage-review workflow. Inspect this repository's current coverage
> and show me the three highest-priority gaps. Reuse a fresh approved run if one
> exists. If a coverage command needs approval, show me the exact command,
> working directory, shell, report path, format, and suite before running it.

The first check is always read-only. The agent asks Coverage MCP for the active
repository, existing approved commands, recent runs, and available snapshots.

If Coverage MCP reports another repository, stop and reopen the task from the
intended checkout. Do not let the agent substitute an unrelated raw coverage
command.

## 3. Review the proposed command

When no suitable command has already been approved, the agent must show you:

- the exact shell command;
- the working directory;
- the shell executable;
- every expected artifact path;
- the coverage format and logical suite name.

Check that the command is appropriate for the repository and that its artifact
path stays inside the checkout. Ask for changes if the command is too broad,
uses the wrong test target, writes somewhere unexpected, or has side effects
beyond producing test and coverage output.

After you approve, Coverage MCP stores that exact registration. Later reviews
can reuse it, but a changed command or artifact definition requires another
approval. Case-specific test filters are allowed only through an argument
boundary that was part of the approved command.

## 4. Read the result

A successful run should include all of the following:

- a terminal run state such as `completed`;
- an artifact-ingestion outcome;
- one or more immutable snapshot IDs;
- the repository, commit, suite, and report provenance;
- every available line, branch, function, and region metric;
- `claim_status` and any limiting `reasons`;
- a bounded set of grouped gaps and one next action.

Do not treat a test pass as proof that coverage was measured. A missing, stale,
oversized, or malformed artifact is a separate outcome. Likewise, a
`not_measured`, `stale`, `limited`, or `invalid` claim must be reported with its
reason instead of being summarized as unchanged coverage.

Coverage MCP intentionally returns a small result first. Ask the agent to open
source context for one selected gap rather than requesting the complete raw
report.

## 5. Open the dashboard when useful

Once the connector has started the shared daemon, open:

<http://127.0.0.1:59471/>

The dashboard lets you browse projects, snapshots, coverage history, changed
lines, file gaps, managed runs, composite coverage, and retention status. It is
an optional local view; normal agent reviews do not require it to be open.

The dedicated source graph is available at <http://127.0.0.1:59471/graph>.

## Try the next common task

After the first snapshot exists, try one of these prompts:

> Compare the current worktree with its parent commit and show only changed
> executable lines that still need coverage.

> Show the recent coverage history, explain any limited claims, and identify
> the single best next test target.

> Show bounded source context around the top uncovered region. Do not return an
> audit-sized raw line list.

> Find named tests with exactly duplicate coverage and give me review
> candidates. Do not delete or merge tests.

The [user guide](user-guide.md) explains these tasks and the terms used in the
result.

## Upgrade the installed plugin

Refresh the marketplace snapshot and reinstall the plugin from that snapshot:

```sh
codex plugin marketplace upgrade codegen-marketplace
codex plugin add testing@codegen-marketplace
```

Start a new Codex task afterward. Each marketplace release pins a compatible
Coverage MCP runtime; do not replace that pin with a moving Git branch.

## Remove the plugin

Remove the Codex plugin with:

```sh
codex plugin remove testing@codegen-marketplace
```

This removes the plugin, not your stored coverage history or the versioned
runtime cache. The [operations guide](operations.md#uninstall-and-data-removal)
lists the retained files and safe cleanup considerations.

## Manual installation for other MCP hosts

The published Cargo package is the supported alternative to the Codex
marketplace path:

```sh
cargo install coverage-mcp --locked
coverage-mcp --version
```

Configure the MCP host to launch:

```sh
coverage-mcp connect --repo /absolute/path/to/repository
```

The connector speaks newline-delimited JSON-RPC on standard input and output.
It starts or reuses the loopback daemon; do not start a second per-project
daemon. A generic client configuration looks like:

```json
{
  "mcpServers": {
    "coverage-mcp": {
      "command": "coverage-mcp",
      "args": ["connect", "--repo", "/absolute/path/to/repository"]
    }
  }
}
```

See the [MCP reference](mcp-reference.md) for transport behavior and the
[operations guide](operations.md) for configuration and troubleshooting.
