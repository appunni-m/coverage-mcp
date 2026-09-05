# Security Policy

## Supported versions

Security fixes are provided for the latest released minor version. Older releases may be used to reproduce a report,
but users should upgrade after a fix is published.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting page:

<https://github.com/appunni-m/coverage-mcp/security/advisories/new>

Include the affected version, operating system, reproduction steps, expected impact, and whether untrusted repositories
or network access are required. Remove source code, logs, tokens, and database contents that are not necessary to
reproduce the problem.

The maintainer will acknowledge a report as soon as practical, coordinate validation and remediation privately, and
credit reporters who want attribution. Please allow time for a release before public disclosure.

## Trust boundary

Coverage MCP is a local developer tool. Its 0.16 MCP/HTTP query surface does not execute tests.
Legacy Rust execution-adapter APIs can still run authorized local code when explicitly used by embedders.
Loopback binding is not permission isolation, and repositories, coverage reports, historical logs, and metadata should be treated
as potentially hostile inputs. Remote binding is unsupported. The server validates loopback Host headers, does not
enable CORS, and serves no third-party dashboard assets, but other processes running as the same user remain inside the
local trust boundary.
