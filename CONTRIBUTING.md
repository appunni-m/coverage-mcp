# Contributing to Coverage MCP

Thank you for improving Coverage MCP. Small, focused pull requests are easiest to review.

## Before opening a change

- Search existing issues and discussions.
- Open an issue before changing a public contract, storage schema, security boundary, or supported platform.
- Never commit coverage databases, run logs, credentials, or private repository data.

## Local setup

Coverage MCP requires Python 3.12 or newer and Git.

```bash
git clone https://github.com/appunni-m/coverage-mcp.git
cd coverage-mcp
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
```

Run the complete local gate before submitting:

```bash
ruff check .
ruff format --check .
mypy coverage_mcp
coverage run -m pytest -q
coverage report -m
python -m build
python -m twine check dist/*
```

Behavior changes require regression tests. Public contract changes must update the shared service projection, REST,
MCP, resources, dashboard, documentation, and contract tests together.

## Pull requests

- Explain the user-visible problem and why the chosen design solves it.
- Keep refactors separate from behavior changes when practical.
- Preserve one shared daemon, one common registry, and one lazily opened coverage store per canonical Git repository.
- Keep `detailed=false`, word budgets, and bounded queries as the default agent experience.
- Add a changelog entry for user-visible behavior.

By submitting a contribution, you agree that it is licensed under this repository's MIT License. Contributors retain
copyright in their work. Coverage MCP does not require a contributor license agreement.

Security reports follow [SECURITY.md](SECURITY.md), not public issues.
