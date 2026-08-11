use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::AppResult;

/// Git identity attached to a coverage measurement or project.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct GitInfo {
    /// Resolved checkout path.
    pub path: String,
    /// Git worktree root, or the resolved path outside Git.
    pub repo_path: String,
    /// Shared repository key; linked worktrees use their common root.
    pub repo_key: String,
    /// Current branch, if available.
    pub branch: Option<String>,
    /// Current commit, if available.
    pub commit_sha: Option<String>,
}

/// Inspects Git without making Git a storage dependency.
pub fn inspect_git(path: &Path) -> AppResult<GitInfo> {
    if path.as_os_str().to_string_lossy().contains('\0') {
        return Err(crate::error::AppError::Validation(
            "repository path contains a NUL byte".to_owned(),
        ));
    }
    let root = resolve_path(path);
    let Some(repo_path) = run_git(&root, &["rev-parse", "--show-toplevel"]) else {
        let resolved = root.to_string_lossy().into_owned();
        return Ok(GitInfo {
            path: resolved.clone(),
            repo_path: resolved.clone(),
            repo_key: resolved,
            ..GitInfo::default()
        });
    };
    let repo = resolve_path(Path::new(&repo_path));
    let repo_key = repository_key_for_git_common(
        &root,
        &repo,
        run_git(&root, &["rev-parse", "--git-common-dir"]),
    );
    Ok(GitInfo {
        path: root.to_string_lossy().into_owned(),
        repo_path: repo.to_string_lossy().into_owned(),
        repo_key: repo_key.to_string_lossy().into_owned(),
        branch: run_git(&root, &["branch", "--show-current"]),
        commit_sha: run_git(&root, &["rev-parse", "HEAD"]),
    })
}

/// Returns the merge base of two revisions when Git can resolve it.
pub fn merge_base(repo_path: &str, base_ref: &str, head_ref: &str) -> Option<String> {
    run_git(Path::new(repo_path), &["merge-base", base_ref, head_ref])
}

/// Returns whether `ancestor` is an ancestor of `descendant`.
pub fn is_ancestor(repo_path: &str, ancestor: &str, descendant: &str) -> bool {
    command_succeeded(
        Command::new("git")
            .args([
                "-C",
                repo_path,
                "merge-base",
                "--is-ancestor",
                ancestor,
                descendant,
            ])
            .output(),
    )
}

fn command_succeeded(result: std::io::Result<std::process::Output>) -> bool {
    match result {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

fn resolve_path(path: &Path) -> PathBuf {
    if let Ok(resolved) = path.canonicalize() {
        return resolved;
    }
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    }
}

fn resolve_git_path(root: &Path, value: &str) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        resolve_path(path)
    } else {
        resolve_path(&root.join(path))
    }
}

fn repository_key(common_path: PathBuf, repo: &Path) -> PathBuf {
    match common_path.file_name() {
        Some(name) if name == ".git" => common_path.parent().unwrap_or(repo).to_path_buf(),
        _ => common_path,
    }
}

fn repository_key_for_git_common(root: &Path, repo: &Path, common: Option<String>) -> PathBuf {
    match common {
        Some(common) => repository_key(resolve_git_path(root, &common), repo),
        None => repo.to_path_buf(),
    }
}

fn run_git(path: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn run_git_checked(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .output()
            .expect("git");
        assert!(output.status.success(), "git failed: {:?}", output);
    }

    #[test]
    fn inspects_git_and_handles_non_git_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let outside = inspect_git(directory.path()).expect("outside identity");
        assert!(
            outside
                .repo_key
                .ends_with(directory.path().to_str().unwrap())
        );
        let relative_outside = inspect_git(Path::new("missing/relative/repository"))
            .expect("relative outside identity");
        assert!(Path::new(&relative_outside.path).is_absolute());
        assert!(merge_base(&outside.repo_path, "main", "HEAD").is_none());
        assert!(!is_ancestor(&outside.repo_path, "main", "HEAD"));
        assert!(inspect_git(Path::new("\0")).is_err());

        run_git_checked(directory.path(), &["init", "-b", "main"]);
        run_git_checked(
            directory.path(),
            &["config", "user.email", "rust@example.com"],
        );
        run_git_checked(directory.path(), &["config", "user.name", "Rust Tests"]);
        std::fs::write(directory.path().join("file.txt"), "content\n").expect("write");
        run_git_checked(directory.path(), &["add", "."]);
        run_git_checked(directory.path(), &["commit", "-m", "initial"]);
        let info = inspect_git(directory.path()).expect("git identity");
        assert!(info.repo_path.ends_with(directory.path().to_str().unwrap()));
        assert!(info.commit_sha.is_some());
        assert_eq!(info.branch.as_deref(), Some("main"));
        assert!(merge_base(&info.repo_path, "main", "HEAD").is_some());
        assert!(is_ancestor(
            &info.repo_path,
            info.commit_sha.as_deref().expect("commit"),
            "HEAD"
        ));
        assert!(!is_ancestor(&info.repo_path, "missing", "HEAD"));
        assert!(run_git(directory.path(), &["definitely-not-a-git-subcommand"]).is_none());
    }

    #[test]
    fn relative_paths_are_resolved_without_canonicalization() {
        let relative = Path::new(".");
        let resolved = resolve_path(relative);
        assert!(resolved.is_absolute());
        assert_eq!(
            resolve_path(Path::new("missing/path")),
            std::env::current_dir().unwrap().join("missing/path")
        );
        assert_eq!(
            resolve_git_path(Path::new("/tmp/repository"), ".git"),
            PathBuf::from("/tmp/repository/.git")
        );
        assert_eq!(
            resolve_git_path(Path::new("/tmp/repository"), "/tmp/shared/.git"),
            PathBuf::from("/tmp/shared/.git")
        );
        assert_eq!(
            repository_key(PathBuf::from("/tmp/shared"), Path::new("/tmp/repository")),
            PathBuf::from("/tmp/shared")
        );
        assert_eq!(
            repository_key(
                PathBuf::from("/tmp/repository/.git"),
                Path::new("/tmp/repository")
            ),
            PathBuf::from("/tmp/repository")
        );
        assert_eq!(
            repository_key_for_git_common(
                Path::new("/tmp/repository"),
                Path::new("/tmp/repository"),
                None,
            ),
            PathBuf::from("/tmp/repository")
        );
        assert!(!command_succeeded(Err(std::io::Error::other("injected"))));
    }
}
