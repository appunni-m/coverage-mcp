use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::{AppError, AppResult};

/// One inclusive range of lines added to the current Git revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedLineRange {
    /// Repository-relative file path.
    pub file_path: String,
    /// One-based first added line.
    pub start: i64,
    /// Number of added lines in the range.
    pub line_count: i64,
}

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

/// Returns whether a Git checkout has no tracked, staged, or untracked changes.
pub fn is_clean(path: &Path) -> bool {
    let root = resolve_path(path);
    let Ok(output) = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .output()
    else {
        return false;
    };
    output.status.success() && output.stdout.is_empty()
}

/// Returns the merge base of two revisions when Git can resolve it.
pub fn merge_base(repo_path: &str, base_ref: &str, head_ref: &str) -> Option<String> {
    run_git(Path::new(repo_path), &["merge-base", base_ref, head_ref])
}

/// Returns the first parent commit for one revision when Git can resolve it.
pub fn parent_commit(repo_path: &str, commit: &str) -> Option<String> {
    let parent = format!("{commit}^");
    run_git(Path::new(repo_path), &["rev-parse", &parent])
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

/// Returns added-line ranges between two Git revisions.
pub fn changed_line_ranges(
    repo_path: &str,
    baseline: &str,
    current: &str,
) -> AppResult<Vec<ChangedLineRange>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args([
            "diff",
            "--no-ext-diff",
            "--unified=0",
            baseline,
            current,
            "--",
        ])
        .output()
        .map_err(AppError::from)?;
    if !output.status.success() {
        return Err(AppError::Validation(format!(
            "git diff could not compare {baseline} with {current}"
        )));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    parse_changed_line_ranges(&text)
}

fn parse_changed_line_ranges(text: &str) -> AppResult<Vec<ChangedLineRange>> {
    let mut path = None;
    let mut ranges = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("+++ b/") {
            path = Some(value.to_owned());
            continue;
        }
        let Some(header) = line.strip_prefix("@@") else {
            continue;
        };
        let Some(path) = path.as_ref() else {
            continue;
        };
        let Some(plus_range) = header
            .split_whitespace()
            .find(|value| value.starts_with('+'))
        else {
            continue;
        };
        let range = plus_range.trim_start_matches('+');
        let mut parts = range.splitn(2, ',');
        let start = parts
            .next()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| {
                AppError::Validation("git diff has an invalid added range".to_owned())
            })?;
        let line_count = parts
            .next()
            .map(|value| value.parse::<i64>())
            .transpose()
            .map_err(|_| AppError::Validation("git diff has an invalid added count".to_owned()))?
            .unwrap_or(1);
        if line_count > 0 {
            ranges.push(ChangedLineRange {
                file_path: path.clone(),
                start,
                line_count,
            });
        }
    }
    Ok(ranges)
}

/// Reads a repository-relative file from a Git commit when that object exists.
pub fn read_file_at_commit(
    repo_path: &str,
    commit_sha: &str,
    file_path: &str,
) -> AppResult<Option<String>> {
    let path = Path::new(file_path);
    if path.is_absolute()
        || path
            .components()
            .any(|part| part == std::path::Component::ParentDir)
    {
        return Err(AppError::Validation(
            "file_path must remain inside the repository".to_owned(),
        ));
    }
    let object = format!("{commit_sha}:{file_path}");
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(["show", &object])
        .output()
        .map_err(AppError::from)?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&output.stdout).into_owned()))
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
        assert!(!is_clean(Path::new("\0")));

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
        assert!(is_clean(directory.path()));
        assert!(!is_clean(&directory.path().join("missing")));
        std::fs::write(directory.path().join("file.txt"), "changed\n").expect("dirty");
        assert!(!is_clean(directory.path()));
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

    #[test]
    fn parses_added_line_ranges_from_git_diff() {
        let directory = tempfile::tempdir().expect("tempdir");
        run_git_checked(directory.path(), &["init", "-b", "main"]);
        run_git_checked(
            directory.path(),
            &["config", "user.email", "rust@example.com"],
        );
        run_git_checked(directory.path(), &["config", "user.name", "Rust Tests"]);
        std::fs::write(directory.path().join("file.rs"), "one\ntwo\n").expect("base");
        run_git_checked(directory.path(), &["add", "."]);
        run_git_checked(directory.path(), &["commit", "-m", "base"]);
        let baseline = run_git(directory.path(), &["rev-parse", "HEAD"]).expect("baseline");
        std::fs::write(directory.path().join("file.rs"), "one\nadded\ntwo\n").expect("change");
        run_git_checked(directory.path(), &["add", "."]);
        run_git_checked(directory.path(), &["commit", "-m", "add line"]);
        let current = run_git(directory.path(), &["rev-parse", "HEAD"]).expect("current");
        assert_eq!(
            changed_line_ranges(
                directory.path().to_str().expect("path"),
                &baseline,
                &current
            )
            .unwrap(),
            vec![ChangedLineRange {
                file_path: "file.rs".to_owned(),
                start: 2,
                line_count: 1
            }]
        );
        assert_eq!(
            read_file_at_commit(
                directory.path().to_str().expect("path"),
                &current,
                "file.rs"
            )
            .unwrap()
            .as_deref(),
            Some("one\nadded\ntwo\n")
        );
        assert_eq!(
            read_file_at_commit(
                directory.path().to_str().expect("path"),
                &current,
                "missing.rs"
            )
            .unwrap(),
            None
        );
        assert!(
            read_file_at_commit(
                directory.path().to_str().expect("path"),
                &current,
                "../file.rs"
            )
            .is_err()
        );
        assert!(
            changed_line_ranges(
                directory.path().to_str().expect("path"),
                "missing",
                &current
            )
            .is_err()
        );
        assert!(
            parse_changed_line_ranges("@@ -1 +1 @@\n")
                .unwrap()
                .is_empty()
        );
        assert!(
            parse_changed_line_ranges("+++ b/file.rs\n@@ -1 1 @@\n")
                .unwrap()
                .is_empty()
        );
        assert!(parse_changed_line_ranges("+++ b/file.rs\n@@ -1 +bad @@\n").is_err());
        assert!(parse_changed_line_ranges("+++ b/file.rs\n@@ -1 +1,bad @@\n").is_err());
        std::fs::remove_file(directory.path().join("file.rs")).expect("remove");
        run_git_checked(directory.path(), &["add", "."]);
        run_git_checked(directory.path(), &["commit", "-m", "delete file"]);
        let deleted = run_git(directory.path(), &["rev-parse", "HEAD"]).expect("deleted");
        assert!(
            changed_line_ranges(directory.path().to_str().expect("path"), &current, &deleted)
                .unwrap()
                .is_empty()
        );

        assert!(
            parse_changed_line_ranges("+++ b/file.rs\n@@ -1,1 +1,0 @@\n")
                .unwrap()
                .is_empty()
        );
        assert!(changed_line_ranges("\0", &current, &deleted).is_err());
        assert!(read_file_at_commit("\0", &current, "file.rs").is_err());
        assert!(run_git(Path::new("\0"), &["status"]).is_none());
        let failed_helper = std::panic::catch_unwind(|| {
            run_git_checked(directory.path(), &["definitely-not-a-git-subcommand"]);
        });
        assert!(failed_helper.is_err());
    }
}
