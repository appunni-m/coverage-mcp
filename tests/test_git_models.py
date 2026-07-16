from __future__ import annotations

import subprocess

from coverage_mcp import git_utils
from coverage_mcp.models import CoverageBuilder, LineCoverage, normalize_report_path, rate


def test_git_helpers_with_real_repository(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    (repo / "file.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repo, check=True, capture_output=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(["git", "checkout", "-b", "feature"], cwd=repo, check=True, capture_output=True)
    (repo / "file.txt").write_text("base\nfeature\n", encoding="utf-8")
    subprocess.run(["git", "commit", "-am", "feature"], cwd=repo, check=True, capture_output=True)

    info = git_utils.inspect_git(repo.as_posix())

    assert info.repo_path == repo.as_posix()
    assert info.branch == "feature"
    assert info.commit_sha is not None
    assert git_utils.merge_base(repo.as_posix(), "main") == base_sha
    assert git_utils.is_ancestor(repo.as_posix(), "main", "HEAD") is True
    assert git_utils.is_ancestor(repo.as_posix(), "HEAD", "main") is False


def test_git_private_runner_handles_failures(monkeypatch, tmp_path):
    class Failed:
        returncode = 1
        stdout = ""

    def failed_run(*args, **kwargs):
        return Failed()

    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="git", timeout=10)

    monkeypatch.setattr(git_utils.subprocess, "run", failed_run)
    assert git_utils._git(tmp_path, ["status"]) is None
    monkeypatch.setattr(git_utils.subprocess, "run", timeout_run)
    assert git_utils._git(tmp_path, ["status"]) is None


def test_model_helpers_and_merge_paths(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path / "other.py"
    outside.write_text("", encoding="utf-8")

    assert rate(1, 2) == 0.5
    assert rate(1, 0) is None
    assert normalize_report_path("././src/a.py") == "src/a.py"
    assert normalize_report_path(outside.as_posix(), repo.as_posix()).endswith("other.py")

    line = LineCoverage("a.py", 1, hits=0, covered=False, count_line=False)
    line.merge(LineCoverage("a.py", 1, hits=3, covered=True, details={"source": "line"}))
    line.merge(LineCoverage("a.py", 1, total_branches=2, covered_branches=1, count_line=False))
    assert line.hits == 3
    assert line.covered is True
    assert line.count_line is True
    assert line.covered_branches == 1
    assert line.details == {"source": "line"}

    builder = CoverageBuilder()
    builder.add_line("a.py", 0, 1)
    builder.add_line("a.py", 1, -3)
    report = builder.build(format="test", report_path="report")
    assert report.total_lines == 1
    assert report.covered_lines == 0
