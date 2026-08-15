//! Process-lifetime file leases used to serialize daemon and database owners.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::error::{AppError, AppResult};

/// Identity recorded by a running shared daemon in its ownership lease.
///
/// `instance_id` and `handoff_token` are absent for daemons released before
/// authenticated handoff support. The token must only be sent back to the
/// loopback daemon that owns this exact lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonLeaseOwner {
    /// Operating-system process identifier recorded when the lease was acquired.
    pub pid: u32,
    /// Human-readable daemon resource name, including the configured port.
    pub resource: String,
    /// Executable that acquired the lease.
    pub executable: PathBuf,
    /// Per-process identifier exposed by the daemon health response.
    pub instance_id: Option<String>,
    /// Per-process capability used to request a graceful version handoff.
    pub handoff_token: Option<String>,
}

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

    /// Acquires the shared-daemon lease and records authenticated handoff data.
    pub fn acquire_daemon(
        path: PathBuf,
        resource: &str,
        instance_id: &str,
        handoff_token: &str,
    ) -> AppResult<Self> {
        Self::acquire_with_metadata(
            path,
            resource,
            Some((instance_id, handoff_token)),
            try_lock_exclusive,
        )
    }

    fn acquire_with(
        path: PathBuf,
        resource: &str,
        try_lock: fn(&File) -> std::io::Result<()>,
    ) -> AppResult<Self> {
        Self::acquire_with_metadata(path, resource, None, try_lock)
    }

    fn acquire_with_metadata(
        path: PathBuf,
        resource: &str,
        daemon_identity: Option<(&str, &str)>,
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
        restrict_lock_permissions(&file)?;
        match try_lock(&file) {
            Ok(()) => {
                write_metadata(&mut file, resource, daemon_identity)?;
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

/// Returns the per-database lock path used by daemon-owned project stores.
pub fn database_lock_path(db_path: &Path) -> PathBuf {
    let mut value = db_path.as_os_str().to_os_string();
    value.push(".lock");
    PathBuf::from(value)
}

/// Returns the daemon identity only while the daemon lease is actively held.
///
/// An unlocked leftover file returns `None`; callers must never infer process
/// ownership from file contents alone.
pub fn held_daemon_owner(path: &Path) -> AppResult<Option<DaemonLeaseOwner>> {
    held_daemon_owner_with(
        OpenOptions::new().read(true).write(true).open(path),
        try_lock_exclusive,
    )
}

fn held_daemon_owner_with(
    file: std::io::Result<File>,
    try_lock: fn(&File) -> std::io::Result<()>,
) -> AppResult<Option<DaemonLeaseOwner>> {
    let mut file = match file {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AppError::Io(error)),
    };
    match try_lock(&file) {
        Ok(()) => {
            FileExt::unlock(&file)?;
            Ok(None)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            let metadata = read_metadata_text(&mut file)?;
            parse_daemon_owner(&metadata).map(Some)
        }
        Err(error) => Err(AppError::Io(error)),
    }
}

fn write_metadata(
    file: &mut File,
    resource: &str,
    daemon_identity: Option<(&str, &str)>,
) -> AppResult<()> {
    let executable = executable_path(std::env::current_exe());
    let mut metadata = format!(
        "pid={}\nresource={}\nexecutable={}\n",
        std::process::id(),
        resource,
        executable
    );
    if let Some((instance_id, handoff_token)) = daemon_identity {
        metadata.push_str(&format!(
            "instance_id={instance_id}\nhandoff_token={handoff_token}\n"
        ));
    }
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
    match read_metadata_text(file)
        .ok()
        .filter(|metadata| !metadata.trim().is_empty())
    {
        Some(metadata) => format!(
            " ({})",
            metadata
                .lines()
                .filter(|line| !line.starts_with("handoff_token="))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
        ),
        None => " (owner metadata unavailable)".to_owned(),
    }
}

fn read_metadata_text(file: &mut File) -> AppResult<String> {
    let mut metadata = String::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_string(&mut metadata)?;
    Ok(metadata)
}

fn parse_daemon_owner(metadata: &str) -> AppResult<DaemonLeaseOwner> {
    let value = |name: &str| {
        metadata
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
    };
    let pid = value("pid")
        .ok_or_else(|| AppError::Runtime("daemon lease metadata is missing pid".to_owned()))?
        .parse::<u32>()
        .map_err(|_| AppError::Runtime("daemon lease metadata has an invalid pid".to_owned()))?;
    if pid == 0 {
        return Err(AppError::Runtime(
            "daemon lease metadata has an invalid pid".to_owned(),
        ));
    }
    let executable = value("executable")
        .filter(|path| !path.is_empty() && *path != "unknown")
        .ok_or_else(|| {
            AppError::Runtime("daemon lease metadata is missing executable identity".to_owned())
        })?;
    Ok(DaemonLeaseOwner {
        pid,
        resource: value("resource")
            .filter(|resource| !resource.is_empty())
            .ok_or_else(|| {
                AppError::Runtime("daemon lease metadata is missing resource identity".to_owned())
            })?
            .to_owned(),
        executable: PathBuf::from(executable),
        instance_id: value("instance_id").map(str::to_owned),
        handoff_token: value("handoff_token").map(str::to_owned),
    })
}

#[cfg(unix)]
fn restrict_lock_permissions(file: &File) -> AppResult<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_lock_permissions(_: &File) -> AppResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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
    fn held_daemon_identity_requires_a_live_lease() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.lock");
        assert_eq!(held_daemon_owner(&path).expect("missing owner"), None);

        let lease =
            FileLease::acquire_daemon(path.clone(), "test daemon", "instance-1", "handoff-secret")
                .expect("daemon lease");
        let owner = held_daemon_owner(&path)
            .expect("held owner")
            .expect("owner metadata");
        assert_eq!(owner.pid, std::process::id());
        assert_eq!(owner.resource, "test daemon");
        assert_eq!(owner.instance_id.as_deref(), Some("instance-1"));
        assert_eq!(owner.handoff_token.as_deref(), Some("handoff-secret"));
        assert_eq!(
            owner.executable,
            std::env::current_exe().expect("executable")
        );
        let busy =
            FileLease::acquire_daemon(path.clone(), "test daemon", "instance-2", "another-secret")
                .expect_err("second daemon lease");
        let diagnostic = busy.to_string();
        assert!(diagnostic.contains("instance_id=instance-1"));
        assert!(!diagnostic.contains("handoff-secret"));

        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        drop(lease);
        assert_eq!(held_daemon_owner(&path).expect("released owner"), None);
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
    fn malformed_held_daemon_metadata_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("malformed.lock");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .expect("lock file");
        file.write_all(b"resource=unknown\n")
            .expect("metadata write");
        file.try_lock_exclusive().expect("raw lease");
        assert!(held_daemon_owner(&path).is_err());
        FileExt::unlock(&file).expect("unlock raw lease");
    }

    #[test]
    fn daemon_owner_errors_and_required_identity_fields_fail_closed() {
        assert!(matches!(
            held_daemon_owner_with(
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
                try_lock_exclusive,
            ),
            Err(AppError::Io(_))
        ));

        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("unexpected.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .expect("lock file");
        assert!(matches!(
            held_daemon_owner_with(Ok(file), |_| Err(std::io::Error::other("lock failure"))),
            Err(AppError::Io(_))
        ));

        assert!(parse_daemon_owner("pid=0\nresource=daemon\nexecutable=/bin/daemon\n").is_err());
        assert!(
            parse_daemon_owner("pid=not-a-number\nresource=daemon\nexecutable=/bin/daemon\n")
                .is_err()
        );
        assert!(parse_daemon_owner("pid=1\nresource=daemon\n").is_err());
        assert!(parse_daemon_owner("pid=1\nexecutable=/bin/daemon\n").is_err());
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
