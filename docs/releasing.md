# Releasing

Coverage MCP releases are built once in GitHub Actions and published to PyPI through trusted publishing. Maintainers
must configure a protected `pypi` environment and register `.github/workflows/release.yml` as the PyPI trusted
publisher before creating a release tag. No long-lived PyPI token belongs in GitHub secrets.

## Checklist

1. Confirm `main` is clean and protected and all required checks pass.
2. Update `coverage_mcp.__version__` and move the changelog's Unreleased entries into that version.
3. Run the complete local gate from `CONTRIBUTING.md`.
4. Install the built wheel in a clean environment and verify `coverage-mcp --version`, HTTP health, ten MCP tools, and
   the ten-connector shared-daemon check.
5. Create one annotated `v<version>` tag. Never move a published release tag.
6. Verify the release workflow's package metadata, Trusted Publisher identity, and PyPI attestations.
7. Upgrade codegen-marketplace to the released version, reinstall the testing plugin, and verify from a fresh Codex
   thread.

If a release is bad, publish a corrected version. Do not replace distributions or retarget the original tag.
