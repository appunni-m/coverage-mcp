//! Process-lifetime file leases used to serialize daemon and database owners.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::error::{AppError, AppResult};

/// A cross-platform advisory lease held for the lifetime of an owner.
#[derive(Debug)]
pub struct FileLease {
    file: File,
    path: PathBuf,
}

impl FileLease {
    /// Acquires an exclusive lease, retaining the file descriptor until drop.
    pub fn acquire(path: PathBuf, resource: &str) -> AppResult<Self> {
        Self::acquire_with(path, resource, try_lock_exclusive)
    }

    fn acquire_with(
        path: PathBuf,
        resource: &str,
        try_lock: fn(&File) -> std::io::Result<()>,
    ) -> AppResult<Self> {
        let parent = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        match try_lock(&file) {
            Ok(()) => {
                write_metadata(&mut file, resource)?;
                Ok(Self { file, path })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let holder = read_metadata(&mut file);
                Err(AppError::Busy {
                    resource: resource.to_owned(),
                    holder,
                })
            }
            Err(error) => Err(AppError::Io(error)),
        }
    }

    /// Returns the path used for diagnostics and operator tooling.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
    file.try_lock_exclusive()
}

impl Drop for FileLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Returns the daemon-wide single-instance lock path.
pub fn daemon_lock_path(common_db_path: &Path) -> PathBuf {
    common_db_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("daemon.lock")
}

/// Returns the per-database lock path used by standalone and project stores.
pub fn database_lock_path(db_path: &Path) -> PathBuf {
    let mut value = db_path.as_os_str().to_os_string();
    value.push(".lock");
    PathBuf::from(value)
}

fn write_metadata(file: &mut File, resource: &str) -> AppResult<()> {
    let executable = executable_path(std::env::current_exe());
    let metadata = format!(
        "pid={}\nresource={}\nexecutable={}\n",
        std::process::id(),
        resource,
        executable
    );
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(metadata.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn executable_path(result: std::io::Result<PathBuf>) -> String {
    result
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn read_metadata(file: &mut File) -> String {
    let mut metadata = String::new();
    match file
        .seek(SeekFrom::Start(0))
        .and_then(|_| file.read_to_string(&mut metadata))
        .ok()
        .filter(|_| !metadata.trim().is_empty())
    {
        Some(_) => format!(" ({})", metadata.trim()),
        None => " (owner metadata unavailable)".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_are_exclusive_and_released_on_drop() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.lock");
        let first = FileLease::acquire(path.clone(), "test daemon").expect("first lease");
        let second = FileLease::acquire(path.clone(), "test daemon");
        assert!(matches!(second, Err(AppError::Busy { .. })));
        drop(first);
        let reacquired = FileLease::acquire(path, "test daemon").expect("reacquire");
        assert!(reacquired.path().exists());
    }

    #[test]
    fn an_unlocked_stale_file_is_reusable_without_pid_guessing() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("database.duckdb.lock");
        std::fs::write(&path, "pid=999999\nresource=old\n").expect("stale metadata");
        let lease = FileLease::acquire(path.clone(), "database").expect("stale lease recovery");
        let metadata = std::fs::read_to_string(path).expect("metadata");
        assert!(metadata.contains(&format!("pid={}", std::process::id())));
        assert!(lease.path().ends_with("database.duckdb.lock"));
    }

    #[test]
    fn lock_parent_creation_and_unexpected_lock_errors_are_preserved() {
        let directory = tempfile::tempdir().expect("tempdir");
        let nested = directory.path().join("new").join("nested").join("lock");
        let lease = FileLease::acquire(nested.clone(), "nested").expect("nested lease");
        assert!(nested.exists());
        drop(lease);

        let error = FileLease::acquire_with(
            directory.path().join("unexpected.lock"),
            "unexpected",
            |_| Err(std::io::Error::other("lock provider failure")),
        )
        .expect_err("unexpected lock error");
        assert!(matches!(error, AppError::Io(_)));
    }

    #[test]
    fn busy_lock_without_metadata_has_a_safe_fallback_message() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("empty.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .expect("lock file");
        file.try_lock_exclusive().expect("raw lease");
        let error = FileLease::acquire(path, "empty").expect_err("busy raw lease");
        assert!(matches!(error, AppError::Busy { holder, .. } if holder.contains("unavailable")));
        FileExt::unlock(&file).expect("unlock raw lease");
    }

    #[test]
    fn lock_paths_are_stable_and_scoped() {
        let common = Path::new("/tmp/coverage/common.duckdb");
        assert_eq!(
            daemon_lock_path(common),
            PathBuf::from("/tmp/coverage/daemon.lock")
        );
        assert_eq!(
            database_lock_path(common),
            PathBuf::from("/tmp/coverage/common.duckdb.lock")
        );
    }

    #[test]
    fn metadata_uses_a_safe_executable_fallback() {
        assert_eq!(
            executable_path(Err(std::io::Error::other("executable unavailable"))),
            "unknown"
        );
        assert_eq!(
            executable_path(Ok(PathBuf::from("/usr/bin/coverage-mcp"))),
            "/usr/bin/coverage-mcp"
        );
    }
}
