from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class GitInfo:
    path: str
    repo_path: str
    repo_key: str
    branch: str | None
    commit_sha: str | None


def inspect_git(path: str | None) -> GitInfo:
    root = Path(path or ".").expanduser().resolve()
    repo_path = _git(root, ["rev-parse", "--show-toplevel"])
    if repo_path is None:
        resolved = root.as_posix()
        return GitInfo(
            path=resolved,
            repo_path=resolved,
            repo_key=resolved,
            branch=None,
            commit_sha=None,
        )

    common_dir = _git(root, ["rev-parse", "--git-common-dir"])
    repo = Path(repo_path).resolve()
    repo_key = repo.as_posix()
    if common_dir:
        common = Path(common_dir)
        if not common.is_absolute():
            common = repo / common
        common = common.resolve()
        repo_key = (common.parent if common.name == ".git" else common).as_posix()

    branch = _git(root, ["branch", "--show-current"]) or None
    commit_sha = _git(root, ["rev-parse", "HEAD"]) or None
    return GitInfo(
        path=root.as_posix(),
        repo_path=repo.as_posix(),
        repo_key=repo_key,
        branch=branch,
        commit_sha=commit_sha,
    )


def merge_base(repo_path: str, base_ref: str, head_ref: str = "HEAD") -> str | None:
    return _git(Path(repo_path), ["merge-base", base_ref, head_ref])


def is_ancestor(repo_path: str, ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ["git", "-C", repo_path, "merge-base", "--is-ancestor", ancestor, descendant],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=10,
    )
    return result.returncode == 0


def _git(path: Path, args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", path.as_posix(), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None
