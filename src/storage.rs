// Row projections are selected from fixed SQL column lists and validated
// schema records. Keep impossible-shape assertions local while all database,
// filesystem, and process failures propagate as AppErrors.
#![allow(clippy::expect_used, clippy::unwrap_in_result)]

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use duckdb::{Connection, OptionalExt, Row, params, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::compaction::{CompactionPolicy, CompactionResult};
use crate::config::{MAX_RUN_LOG_MAX_BYTES, MIN_RUN_LOG_MAX_BYTES, ServerConfig};
use crate::error::{AppError, AppResult};
use crate::git::{GitInfo, inspect_git, is_clean, merge_base, read_file_at_commit};
use crate::lock::{FileLease, database_lock_path};
use crate::models::CoverageReport;
use crate::parser::parse_coverage_report;
use crate::pool::{DbConnection, DbPool, QueryTracker, checkout, open_pool, run_with_timeout};
use crate::{hex_prefix, stable_project_id};

/// Defensive collection ceiling shared by all public projections.
pub const MAX_COLLECTION_RECORDS: usize = 5_000;
/// One record above the public ceiling lets callers detect truncation safely.
pub const COLLECTION_FETCH_LIMIT: usize = MAX_COLLECTION_RECORDS + 1;
/// Maximum run workers accepted by the Rust implementation.
pub const MAX_RUN_CONCURRENCY: usize = 32;

#[cfg(test)]
static FORCE_ARTIFACT_FINGERPRINT_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CLAIM_FALSE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CONTROL_POISON_BEFORE_REAP: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CONTROL_LOCK_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_EMPTY_MANAGED_CHILD: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_LOG_CAPTURE_FAILURE_CALL: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FORCE_PRUNE_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_QUEUE_POSITION_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_QUEUE_POSITION_ROW_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_REUSED_RESULT_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_REAP_CHILD_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_REAP_CONTROL_LOCK_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_RESOURCES_FINISH_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_SUMMARY_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_TIMEOUT_STATE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CANCELLATION_TERMINATE_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CANCELLATION_TERMINATE_SUCCESS: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_TIMEOUT_TERMINATE_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_TERMINATE_CHILD_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_TRY_WAIT_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CANCELLATION_STATE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CANCELLATION_FALSE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_CLEAR_ARTIFACT_BASELINES_FAILURE: AtomicBool = AtomicBool::new(false);

const UPSERT_PROJECT_SETTINGS_SQL: &str = "INSERT INTO project_settings (repo_key, repo_path, created_at, updated_at, compaction_enabled, compaction_after_days, compaction_interval_seconds, compaction_batch_size) VALUES (?, ?, ?, ?, true, ?, ?, ?) ON CONFLICT (repo_key) DO UPDATE SET repo_path = excluded.repo_path, updated_at = excluded.updated_at";
const UPSERT_REPOSITORY_SQL: &str = "INSERT INTO repositories (id, repo_key, last_seen) VALUES (?, ?, ?) ON CONFLICT (repo_key) DO UPDATE SET last_seen = excluded.last_seen";
const UPDATE_PROJECT_SETTINGS_SQL: &str = "UPDATE project_settings SET updated_at = ?, compaction_enabled = ?, compaction_after_days = ?, compaction_interval_seconds = ?, compaction_batch_size = ? WHERE repo_key = ?";
const UPDATE_COMPACTION_STATUS_SQL: &str = "UPDATE project_settings SET compaction_last_run_at = ?, compaction_last_status = ?, compaction_last_snapshot_count = ?, compaction_last_bytes_before = ?, compaction_last_bytes_after = ?, updated_at = ? WHERE repo_key = ?";
const INSERT_RUN_ARTIFACT_SQL: &str = "INSERT INTO run_artifacts (run_id, kind, path, exists, size_bytes, coverage_format, suite, modified_by_run, ingest_status, snapshot_id, ingest_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const INSERT_COMPLETED_RUN_SQL: &str = "INSERT INTO runs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at) SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ?, ?, ?, ?, stdout_path, stderr_path, ?, ?, queued_at, date_diff('millisecond', queued_at, started_at), cancellation_requested_at FROM run_jobs WHERE id = ?";
const INSERT_SNAPSHOT_SQL: &str = "INSERT INTO snapshots (id, created_at, minute_bucket, repo_path, repo_key, branch, commit_sha, base_ref, suite, format, report_path, warnings, metadata, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const INSERT_FILE_SQL: &str = "INSERT INTO files (snapshot_id, file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const INSERT_LINE_SQL: &str = "INSERT INTO lines (snapshot_id, file_path, line_number, hits, covered, count_line, total_branches, covered_branches, total_functions, covered_functions, details) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// Mutable project-settings fields accepted by REST and the CLI.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ProjectSettingsPatch {
    /// Enable or disable background compaction.
    pub compaction_enabled: Option<bool>,
    /// Age threshold in days.
    pub compaction_after_days: Option<u32>,
    /// Background interval in seconds.
    pub compaction_interval_seconds: Option<u64>,
    /// Maximum events per pass.
    pub compaction_batch_size: Option<u32>,
}

/// Persisted per-project settings and maintenance status.
#[derive(Clone, Debug, Serialize)]
pub struct ProjectSettings {
    /// Shared Git repository key.
    pub repo_key: String,
    /// Current checkout path used for the project.
    pub repo_path: String,
    /// Project creation timestamp.
    pub created_at: String,
    /// Last settings update timestamp.
    pub updated_at: String,
    /// Whether compaction runs in the background.
    pub compaction_enabled: bool,
    /// Age threshold in days.
    pub compaction_after_days: u32,
    /// Background cadence in seconds.
    pub compaction_interval_seconds: u64,
    /// Batch size per pass.
    pub compaction_batch_size: u32,
    /// Last maintenance pass timestamp.
    pub compaction_last_run_at: Option<String>,
    /// Last maintenance result.
    pub compaction_last_status: String,
    /// Last number of snapshots compacted.
    pub compaction_last_snapshot_count: u64,
    /// Last uncompressed detail byte estimate.
    pub compaction_last_bytes_before: u64,
    /// Last compressed payload byte count.
    pub compaction_last_bytes_after: u64,
}

impl ProjectSettings {
    /// Returns the policy used by the background worker.
    pub fn policy(&self) -> CompactionPolicy {
        #[rustfmt::skip]
        let policy = CompactionPolicy {
            enabled: self.compaction_enabled,
            older_than_days: self.compaction_after_days,
            interval_seconds: self.compaction_interval_seconds,
            batch_size: self.compaction_batch_size };
        policy
    }
}

struct StoreInner {
    db_path: PathBuf,
    run_dir: PathBuf,
    pool: Mutex<Option<DbPool>>,
    db_lease: Mutex<Option<FileLease>>,
    write_gate: Mutex<()>,
    query_tracker: QueryTracker,
    project: RwLock<Option<GitInfo>>,
    config: ServerConfig,
    closing: AtomicBool,
    slots: (Mutex<usize>, Condvar),
    active_processes: Mutex<HashMap<String, Arc<Mutex<Option<Child>>>>>,
    run_threads: Mutex<Vec<JoinHandle<()>>>,
    compaction_thread: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    query_fault: Mutex<Option<AppError>>,
    #[cfg(test)]
    query_fault_skip: Mutex<Option<usize>>,
    #[cfg(test)]
    query_fault_owner: Mutex<Option<thread::ThreadId>>,
}

#[derive(Clone, Debug)]
struct ArtifactFingerprint {
    exists: bool,
    size_bytes: Option<i64>,
    modified_ns: Option<i64>,
    sha256: Option<String>,
}

impl ArtifactFingerprint {
    fn changed_from(&self, before: &Self) -> bool {
        if self.sha256.is_some() && before.sha256.is_some() {
            self.exists != before.exists || self.sha256 != before.sha256
        } else {
            self.exists != before.exists
                || self.size_bytes != before.size_bytes
                || self.modified_ns != before.modified_ns
        }
    }
}

#[derive(Clone, Debug)]
struct ArtifactBaseline {
    run_id: String,
    kind: String,
    path: String,
    fingerprint: ArtifactFingerprint,
}

struct LogCaptureResult {
    bytes_written: u64,
    truncated: bool,
}

struct LogCaptureHandles {
    stdout: JoinHandle<std::io::Result<LogCaptureResult>>,
    stderr: JoinHandle<std::io::Result<LogCaptureResult>>,
}

struct ManagedRunGuard {
    inner: Arc<StoreInner>,
    run_id: String,
    control: Arc<Mutex<Option<Child>>>,
    captures: Option<LogCaptureHandles>,
    completed: bool,
}

impl ManagedRunGuard {
    fn new(
        inner: Arc<StoreInner>,
        run_id: String,
        control: Arc<Mutex<Option<Child>>>,
        captures: LogCaptureHandles,
    ) -> Self {
        Self {
            inner,
            run_id,
            control,
            captures: Some(captures),
            completed: false,
        }
    }

    fn finish(&mut self) -> AppResult<(LogCaptureResult, LogCaptureResult)> {
        #[cfg(test)]
        if FORCE_RESOURCES_FINISH_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Runtime(
                "injected managed resource finalization failure".to_owned(),
            ));
        }
        terminate_managed_process(&self.control)?;
        let captures = self
            .captures
            .take()
            .ok_or_else(|| AppError::Runtime("run log capture was already joined".to_owned()))?;
        let result = join_log_capture_handles(captures);
        self.inner
            .active_processes
            .lock()
            .map_err(lock_error)?
            .remove(&self.run_id);
        let result = result?;
        self.completed = true;
        Ok(result)
    }
}

impl Drop for ManagedRunGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Err(error) = terminate_managed_process(&self.control) {
            eprintln!(
                "coverage-mcp could not clean up failed run {}: {error}",
                self.run_id
            );
        }
        if let Some(captures) = self.captures.take() {
            if let Err(error) = join_log_capture_handles(captures) {
                eprintln!(
                    "coverage-mcp could not join failed run {} log capture: {error}",
                    self.run_id
                );
            }
        }
        match self.inner.active_processes.lock() {
            Ok(mut active) => {
                active.remove(&self.run_id);
            }
            Err(_) => eprintln!(
                "coverage-mcp could not remove failed run {} from the active process registry",
                self.run_id
            ),
        }
    }
}

/// Thread-safe repository coverage store backed by the existing DuckDB schema.
#[derive(Clone)]
pub struct CoverageStore {
    inner: Arc<StoreInner>,
}

impl std::fmt::Debug for CoverageStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoverageStore")
            .field("db_path", &self.inner.db_path)
            .finish_non_exhaustive()
    }
}

fn ensure_db_parent(db_path: &Path) -> AppResult<()> {
    match db_path.parent() {
        Some(parent) => fs::create_dir_all(parent).map_err(AppError::from),
        None => Ok(()),
    }
}

fn retain_compaction_thread(
    slot: &Mutex<Option<JoinHandle<()>>>,
    handle: std::io::Result<JoinHandle<()>>,
) -> AppResult<()> {
    let handle = handle.map_err(AppError::from)?;
    *slot.lock().map_err(lock_error)? = Some(handle);
    Ok(())
}

fn remove_compacted_detail(
    connection: &Connection,
    snapshot_id: &str,
    inserted: bool,
) -> AppResult<()> {
    if inserted {
        delete_snapshot_rows(connection, snapshot_id, "files")?;
        delete_snapshot_rows(connection, snapshot_id, "lines")?;
    }
    Ok(())
}

fn delete_snapshot_rows(connection: &Connection, snapshot_id: &str, table: &str) -> AppResult<()> {
    let sql = format!("DELETE FROM {table} WHERE snapshot_id = ?");
    connection
        .execute(&sql, params![snapshot_id])
        .map(|_| ())
        .map_err(AppError::from)
}

fn finish_transaction<T>(connection: &Connection, result: AppResult<T>) -> AppResult<T> {
    match result {
        Ok(value) => connection
            .execute_batch("COMMIT")
            .map(|_| value)
            .map_err(AppError::from),
        Err(error) => Err(rollback_transaction(connection, error)),
    }
}

fn rollback_transaction(connection: &Connection, error: AppError) -> AppError {
    match connection.execute_batch("ROLLBACK") {
        Ok(()) => error,
        Err(rollback_error) => AppError::Runtime(format!(
            "{error}; transaction rollback failed: {rollback_error}"
        )),
    }
}

impl CoverageStore {
    /// Opens or creates a repository database, reconciles interrupted/queued
    /// managed runs, and starts maintenance workers.
    pub fn open(db_path: PathBuf, config: ServerConfig) -> AppResult<Self> {
        if !(1..=MAX_RUN_CONCURRENCY).contains(&config.run_concurrency) {
            return Err(AppError::Validation(format!(
                "run_concurrency must be between 1 and {MAX_RUN_CONCURRENCY}"
            )));
        }
        if config.run_retention == 0 {
            return Err(AppError::Validation(
                "run_retention must be at least 1".to_owned(),
            ));
        }
        if !(MIN_RUN_LOG_MAX_BYTES..=MAX_RUN_LOG_MAX_BYTES).contains(&config.run_log_max_bytes) {
            return Err(AppError::Validation(format!(
                "run_log_max_bytes must be between {MIN_RUN_LOG_MAX_BYTES} and {MAX_RUN_LOG_MAX_BYTES}"
            )));
        }
        ensure_db_parent(&db_path)?;
        let run_dir = db_path.parent().unwrap_or(Path::new(".")).join("runs");
        fs::create_dir_all(&run_dir)?;
        let db_lease = FileLease::acquire(
            database_lock_path(&db_path),
            &format!("DuckDB database {}", db_path.display()),
        )?;
        let pool = open_pool(&db_path, config.db_pool_size)?;
        let inner = Arc::new(StoreInner {
            db_path,
            run_dir,
            pool: Mutex::new(Some(pool)),
            db_lease: Mutex::new(Some(db_lease)),
            write_gate: Mutex::new(()),
            query_tracker: QueryTracker::default(),
            project: RwLock::new(None),
            config,
            closing: AtomicBool::new(false),
            slots: (Mutex::new(0), Condvar::new()),
            active_processes: Mutex::new(HashMap::new()),
            run_threads: Mutex::new(Vec::new()),
            compaction_thread: Mutex::new(None),
            #[cfg(test)]
            query_fault: Mutex::new(None),
            #[cfg(test)]
            query_fault_skip: Mutex::new(None),
            #[cfg(test)]
            query_fault_owner: Mutex::new(None),
        });
        let store = Self { inner };
        let ready = store
            .init_schema()
            .and_then(|_| store.reconcile_persisted_run_jobs())
            .map(|queued_runs| store.resume_queued_runs(queued_runs))
            .and_then(|_| store.start_compaction_worker());
        ready.map(|_| store)
    }

    fn reconcile_persisted_run_jobs(&self) -> AppResult<Vec<String>> {
        let ended = Utc::now();
        self.with_connection(|connection| {
            connection
                .execute(
                    "UPDATE run_jobs SET status = 'interrupted', ended_at = ?, error = 'Coverage MCP restarted before this managed run reported a terminal result.' WHERE status = 'running'",
                    params![ended],
                )
                .map_err(AppError::from)
                .and_then(|_| {
                    connection
                        .prepare("SELECT id FROM run_jobs WHERE status = 'queued' ORDER BY queued_at, id")
                        .map_err(AppError::from)
                })
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| row.get::<_, String>(0))
                        .map_err(AppError::from)
                        .and_then(|rows| {
                            rows.map(|row| row.map_err(AppError::from)).collect()
                        })
                })
        })
    }

    fn resume_queued_runs(&self, run_ids: Vec<String>) {
        for run_id in run_ids {
            if let Err(error) = self.start_run_worker(&run_id) {
                eprintln!("coverage-mcp could not resume queued run {run_id}: {error}");
            }
        }
    }

    /// Database path owned by this store.
    pub fn db_path(&self) -> &Path {
        &self.inner.db_path
    }

    /// Associates the store with a repository and creates default project settings.
    pub fn ensure_project(&self, path: &Path) -> AppResult<GitInfo> {
        let git = inspect_git(path)?;
        {
            let mut project = self.inner.project.write().map_err(lock_error)?;
            *project = Some(git.clone());
        }
        self.ensure_project_settings(&git)?;
        Ok(git)
    }

    /// Returns the currently selected repository identity.
    pub fn project(&self) -> AppResult<GitInfo> {
        self.inner
            .project
            .read()
            .map_err(lock_error)?
            .clone()
            .ok_or_else(|| {
                AppError::Validation(
                    "a repository must be selected before using coverage data".to_owned(),
                )
            })
    }

    /// Closes workers and the database connection.
    pub fn close(&self) -> AppResult<()> {
        let mut worker_error = None;
        if !self.inner.closing.swap(true, Ordering::SeqCst) {
            self.inner.slots.1.notify_all();
            if let Err(error) = self.terminate_active_processes() {
                worker_error = Some(error);
            }
            if let Some(thread) = self
                .inner
                .compaction_thread
                .lock()
                .map_err(lock_error)?
                .take()
            {
                thread.thread().unpark();
                if let Err(error) = join_worker(thread, "compaction") {
                    worker_error.get_or_insert(error);
                }
            }
            let current = thread::current().id();
            let mut threads = self.inner.run_threads.lock().map_err(lock_error)?;
            for thread in threads.drain(..) {
                if thread.thread().id() != current {
                    if let Err(error) = join_worker(thread, "run") {
                        worker_error.get_or_insert(error);
                    }
                }
            }
            drop(threads);
        }
        self.inner.query_tracker.interrupt_all();
        let idle = self
            .inner
            .query_tracker
            .wait_for_idle(Duration::from_millis(self.inner.config.db_query_timeout_ms));
        if !idle {
            return Err(AppError::Timeout {
                operation: "DuckDB shutdown".to_owned(),
                timeout_ms: self.inner.config.db_query_timeout_ms,
            });
        }
        self.inner.pool.lock().map_err(lock_error)?.take();
        self.inner.db_lease.lock().map_err(lock_error)?.take();
        worker_error.map_or(Ok(()), Err)
    }

    fn terminate_active_processes(&self) -> AppResult<()> {
        let controls = self
            .inner
            .active_processes
            .lock()
            .map_err(lock_error)?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut first_error = None;
        for control in controls {
            record_first_process_error(&mut first_error, terminate_managed_process(&control));
        }
        first_error.map_or(Ok(()), Err)
    }

    fn write_gate(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.inner.write_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("coverage-mcp recovering the poisoned write gate");
                poisoned.into_inner()
            }
        }
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        let _write_gate = self.write_gate();
        self.with_pooled_connection(operation)
    }

    fn checkpoint_connection(connection: &Connection) -> AppResult<bool> {
        connection
            .execute_batch("CHECKPOINT")
            .map(|_| true)
            .map_err(AppError::from)
    }

    fn with_connection_mut<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        let _write_gate = self.write_gate();
        self.with_pooled_connection(operation)
    }

    fn with_read_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        self.with_pooled_connection(operation)
    }

    fn with_pooled_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        self.ensure_store_open()
            .and_then(|_| self.with_pooled_connection_allow_closing(operation))
    }

    fn with_connection_allow_closing<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        let _write_gate = self.write_gate();
        self.with_pooled_connection_allow_closing(operation)
    }

    #[cfg(test)]
    fn maybe_injected_query_fault(&self) -> AppResult<()> {
        let current_thread = thread::current().id();
        let owner = self
            .inner
            .query_fault_owner
            .lock()
            .expect("query fault owner lock");
        if *owner == Some(current_thread) {
            let mut skip = self
                .inner
                .query_fault_skip
                .lock()
                .expect("query fault skip lock");
            if let Some(remaining) = *skip {
                if remaining == 0 {
                    *skip = None;
                    drop(owner);
                    *self
                        .inner
                        .query_fault_owner
                        .lock()
                        .expect("query fault owner lock") = None;
                    let error = self
                        .inner
                        .query_fault
                        .lock()
                        .expect("query fault lock")
                        .take()
                        .expect("query fault error");
                    return Err(error);
                }
                *skip = Some(remaining - 1);
            }
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[inline]
    fn maybe_injected_query_fault(&self) -> AppResult<()> {
        Ok(())
    }

    fn with_pooled_connection_allow_closing<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        self.maybe_injected_query_fault()
            .and_then(|_| self.checkout_connection())
            .and_then(|connection| {
                self.inner
                    .query_tracker
                    .begin(connection.interrupt_handle())
                    .and_then(|query_guard| {
                        let result = run_with_timeout(
                            &connection,
                            Duration::from_millis(self.inner.config.db_query_timeout_ms),
                            "DuckDB operation",
                            operation,
                        );
                        drop(query_guard);
                        result
                    })
            })
    }

    #[cfg(test)]
    pub(crate) fn inject_query_fault(&self) {
        self.inject_query_fault_after(0);
    }

    #[cfg(test)]
    pub(crate) fn inject_query_fault_after(&self, successful_queries: usize) {
        *self.inner.query_fault_owner.lock().unwrap() = Some(thread::current().id());
        *self.inner.query_fault.lock().unwrap() = Some(AppError::Runtime(
            "injected storage query failure".to_owned(),
        ));
        *self.inner.query_fault_skip.lock().unwrap() = Some(successful_queries);
    }

    #[cfg(test)]
    pub(crate) fn clear_query_fault(&self) {
        *self.inner.query_fault_owner.lock().unwrap() = None;
        *self.inner.query_fault.lock().unwrap() = None;
        *self.inner.query_fault_skip.lock().unwrap() = None;
    }

    #[cfg(test)]
    pub(crate) fn execute_sql_for_test(&self, sql: &str) -> AppResult<()> {
        self.with_connection(|connection| {
            connection.execute_batch(sql)?;
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn clear_snapshot_commit_for_test(&self, snapshot_id: &str) -> AppResult<()> {
        self.with_connection(|connection| {
            connection
                .execute(
                    "UPDATE snapshots SET commit_sha = NULL WHERE id = ?",
                    params![snapshot_id],
                )
                .expect("snapshots table exists in the test store");
            Ok(())
        })
    }

    fn ensure_store_open(&self) -> AppResult<()> {
        if self.inner.closing.load(Ordering::SeqCst) {
            return Err(AppError::Runtime("DuckDB store is closing".to_owned()));
        }
        Ok(())
    }

    fn checkout_connection(&self) -> AppResult<DbConnection> {
        let pool = self
            .inner
            .pool
            .lock()
            .map_err(lock_error)?
            .clone()
            .ok_or_else(|| AppError::Runtime("DuckDB pool is closed".to_owned()))?;
        checkout(
            &pool,
            Duration::from_millis(self.inner.config.db_acquire_timeout_ms),
            &self.inner.db_path.display().to_string(),
        )
    }

    fn init_schema(&self) -> AppResult<()> {
        let schema = r#"
                CREATE TABLE IF NOT EXISTS snapshots (
                    id VARCHAR PRIMARY KEY, created_at TIMESTAMP NOT NULL, minute_bucket TIMESTAMP NOT NULL,
                    repo_path VARCHAR NOT NULL, repo_key VARCHAR NOT NULL, branch VARCHAR, commit_sha VARCHAR,
                    base_ref VARCHAR, suite VARCHAR NOT NULL, format VARCHAR NOT NULL, report_path VARCHAR NOT NULL,
                    warnings VARCHAR NOT NULL, metadata VARCHAR NOT NULL, total_lines INTEGER NOT NULL,
                    covered_lines INTEGER NOT NULL, total_branches INTEGER NOT NULL, covered_branches INTEGER NOT NULL,
                    total_functions INTEGER NOT NULL, covered_functions INTEGER NOT NULL, total_regions INTEGER NOT NULL,
                    covered_regions INTEGER NOT NULL, line_rate DOUBLE, branch_rate DOUBLE, function_rate DOUBLE,
                    region_rate DOUBLE
                );
                CREATE TABLE IF NOT EXISTS files (
                    snapshot_id VARCHAR NOT NULL, file_path VARCHAR NOT NULL, total_lines INTEGER NOT NULL,
                    covered_lines INTEGER NOT NULL, total_branches INTEGER NOT NULL, covered_branches INTEGER NOT NULL,
                    total_functions INTEGER NOT NULL, covered_functions INTEGER NOT NULL, total_regions INTEGER NOT NULL,
                    covered_regions INTEGER NOT NULL, line_rate DOUBLE, branch_rate DOUBLE, function_rate DOUBLE,
                    region_rate DOUBLE, raw_metrics VARCHAR NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lines (
                    snapshot_id VARCHAR NOT NULL, file_path VARCHAR NOT NULL, line_number INTEGER NOT NULL,
                    hits BIGINT NOT NULL, covered BOOLEAN NOT NULL, count_line BOOLEAN NOT NULL,
                    total_branches INTEGER NOT NULL, covered_branches INTEGER NOT NULL, total_functions INTEGER NOT NULL,
                    covered_functions INTEGER NOT NULL, details VARCHAR NOT NULL
                );
                CREATE TABLE IF NOT EXISTS worktrees (
                    id VARCHAR PRIMARY KEY, created_at TIMESTAMP NOT NULL, name VARCHAR, path VARCHAR NOT NULL,
                    repo_path VARCHAR NOT NULL, repo_key VARCHAR NOT NULL, branch VARCHAR, head_sha VARCHAR,
                    base_ref VARCHAR NOT NULL, base_sha VARCHAR, baseline_snapshot_id VARCHAR
                );
                CREATE TABLE IF NOT EXISTS registered_commands (
                    id VARCHAR PRIMARY KEY, created_at TIMESTAMP NOT NULL, name VARCHAR NOT NULL, command VARCHAR NOT NULL,
                    cwd VARCHAR NOT NULL, repo_path VARCHAR NOT NULL, repo_key VARCHAR NOT NULL, branch VARCHAR,
                    commit_sha VARCHAR, shell VARCHAR NOT NULL, approved_by VARCHAR NOT NULL, approval_note VARCHAR NOT NULL,
                    artifact_specs VARCHAR NOT NULL, enabled BOOLEAN NOT NULL, duration_estimate_ms INTEGER,
                    duration_p90_ms INTEGER, duration_sample_count INTEGER NOT NULL DEFAULT 0, duration_stats_updated_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id VARCHAR PRIMARY KEY, command_id VARCHAR NOT NULL, command_name VARCHAR NOT NULL, command VARCHAR NOT NULL,
                    idempotency_key VARCHAR, cwd VARCHAR NOT NULL, repo_path VARCHAR NOT NULL, repo_key VARCHAR NOT NULL,
                    branch VARCHAR, commit_sha VARCHAR, started_at TIMESTAMP NOT NULL, ended_at TIMESTAMP NOT NULL,
                    duration_ms INTEGER NOT NULL, exit_code INTEGER, status VARCHAR NOT NULL, stdout_path VARCHAR NOT NULL,
                    stderr_path VARCHAR NOT NULL, parsed_summary VARCHAR NOT NULL, artifact_paths VARCHAR NOT NULL,
                    queued_at TIMESTAMP, queue_duration_ms INTEGER, cancellation_requested_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS run_artifacts (
                    run_id VARCHAR NOT NULL, kind VARCHAR NOT NULL, path VARCHAR NOT NULL, exists BOOLEAN NOT NULL,
                    size_bytes BIGINT, coverage_format VARCHAR, suite VARCHAR, modified_by_run BOOLEAN NOT NULL DEFAULT false,
                    ingest_status VARCHAR, snapshot_id VARCHAR, ingest_error VARCHAR
                );
                CREATE TABLE IF NOT EXISTS run_artifact_baselines (
                    run_id VARCHAR NOT NULL, kind VARCHAR NOT NULL, path VARCHAR NOT NULL, exists BOOLEAN NOT NULL,
                    size_bytes BIGINT, modified_ns BIGINT, sha256 VARCHAR,
                    PRIMARY KEY (run_id, kind)
                );
                CREATE TABLE IF NOT EXISTS run_jobs (
                    id VARCHAR PRIMARY KEY, command_id VARCHAR NOT NULL, command_name VARCHAR NOT NULL, command VARCHAR NOT NULL,
                    idempotency_key VARCHAR, cwd VARCHAR NOT NULL, repo_path VARCHAR NOT NULL, repo_key VARCHAR NOT NULL,
                    branch VARCHAR, commit_sha VARCHAR, queued_at TIMESTAMP NOT NULL, started_at TIMESTAMP, ended_at TIMESTAMP,
                    timeout_seconds INTEGER, max_summary_lines INTEGER NOT NULL, status VARCHAR NOT NULL, stdout_path VARCHAR NOT NULL,
                    stderr_path VARCHAR NOT NULL, error VARCHAR NOT NULL, cancellation_requested_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS project_settings (
                    repo_key VARCHAR PRIMARY KEY, repo_path VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL, compaction_enabled BOOLEAN NOT NULL DEFAULT true,
                    compaction_after_days INTEGER NOT NULL DEFAULT 30, compaction_interval_seconds BIGINT NOT NULL DEFAULT 3600,
                    compaction_batch_size INTEGER NOT NULL DEFAULT 100, compaction_last_run_at TIMESTAMP,
                    compaction_last_status VARCHAR NOT NULL DEFAULT 'never_run', compaction_last_snapshot_count BIGINT NOT NULL DEFAULT 0,
                    compaction_last_bytes_before BIGINT NOT NULL DEFAULT 0, compaction_last_bytes_after BIGINT NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS coverage_compacted_payloads (
                    snapshot_id VARCHAR PRIMARY KEY, repo_key VARCHAR NOT NULL, compacted_at TIMESTAMP NOT NULL,
                    original_bytes BIGINT NOT NULL, compressed_bytes BIGINT NOT NULL, payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS repositories (
                    id VARCHAR PRIMARY KEY, repo_key VARCHAR UNIQUE NOT NULL, last_seen TIMESTAMP NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_repo_time ON snapshots(repo_key, created_at);
                CREATE INDEX IF NOT EXISTS idx_snapshots_commit ON snapshots(repo_key, commit_sha);
                CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id);
                CREATE INDEX IF NOT EXISTS idx_lines_lookup ON lines(snapshot_id, file_path, line_number);
                CREATE INDEX IF NOT EXISTS idx_worktrees_repo ON worktrees(repo_key, created_at);
                CREATE INDEX IF NOT EXISTS idx_registered_commands_name ON registered_commands(name, created_at);
                CREATE INDEX IF NOT EXISTS idx_runs_command_time ON runs(command_id, started_at);
                CREATE INDEX IF NOT EXISTS idx_run_artifacts_kind ON run_artifacts(kind);
                CREATE INDEX IF NOT EXISTS idx_run_artifact_baselines_run ON run_artifact_baselines(run_id);
                CREATE INDEX IF NOT EXISTS idx_run_jobs_status_time ON run_jobs(status, queued_at);
                CREATE INDEX IF NOT EXISTS idx_project_settings_updated ON project_settings(updated_at);
                CREATE INDEX IF NOT EXISTS idx_compacted_repo ON coverage_compacted_payloads(repo_key, compacted_at);
                "#;
        self.with_connection(|connection| {
            connection.execute_batch(schema)?;
            Ok(())
        })
    }

    fn ensure_project_settings(&self, git: &GitInfo) -> AppResult<ProjectSettings> {
        let now = Utc::now();
        let config = &self.inner.config;
        self.with_connection(|connection| {
            let settings_values = params![
                git.repo_key,
                git.repo_path,
                now,
                now,
                config.default_compaction_after_days,
                config.default_compaction_interval_seconds,
                config.default_compaction_batch_size
            ];
            connection.execute(UPSERT_PROJECT_SETTINGS_SQL, settings_values)?;
            let repository_values = params![short_hash(&git.repo_key), git.repo_key, now];
            connection.execute(UPSERT_REPOSITORY_SQL, repository_values)?;
            Ok(())
        })?;
        self.project_settings()
    }

    /// Returns project settings and latest compaction status.
    pub fn project_settings(&self) -> AppResult<ProjectSettings> {
        let project = self.project()?;
        self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT repo_key, repo_path, created_at, updated_at, compaction_enabled, compaction_after_days, compaction_interval_seconds, compaction_batch_size, compaction_last_run_at, compaction_last_status, compaction_last_snapshot_count, compaction_last_bytes_before, compaction_last_bytes_after FROM project_settings WHERE repo_key = ?")?;
            let raw = statement.query_row(params![project.repo_key], |row| {
                Ok((
                    row.get::<_, String>(0)
                        .expect("project settings projections always contain repo_key"),
                    row.get::<_, String>(1)?,
                    timestamp_string(row.get_ref(2).expect("project settings projection has created_at")),
                    timestamp_string(row.get_ref(3).expect("project settings projection has updated_at")),
                    row.get::<_, bool>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    optional_timestamp(row.get_ref(8).expect("project settings projection has last_run_at")),
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                ))
            })?;
            Ok(ProjectSettings {
                repo_key: raw.0,
                repo_path: raw.1,
                created_at: raw.2,
                updated_at: raw.3,
                compaction_enabled: raw.4,
                compaction_after_days: checked_db_u32(raw.5, "compaction_after_days")?,
                compaction_interval_seconds: checked_db_u64(
                    raw.6,
                    "compaction_interval_seconds",
                )?,
                compaction_batch_size: checked_db_u32(raw.7, "compaction_batch_size")?,
                compaction_last_run_at: raw.8,
                compaction_last_status: raw.9,
                compaction_last_snapshot_count: checked_db_u64(
                    raw.10,
                    "compaction_last_snapshot_count",
                )?,
                compaction_last_bytes_before: checked_db_u64(raw.11, "compaction_last_bytes_before")?,
                compaction_last_bytes_after: checked_db_u64(raw.12, "compaction_last_bytes_after")?,
            })
        })
    }

    /// Applies validated project settings and wakes the background worker.
    pub fn update_project_settings(
        &self,
        patch: ProjectSettingsPatch,
    ) -> AppResult<ProjectSettings> {
        let project = self.project()?;
        validate_settings_patch(&patch)?;
        let current = self.project_settings()?;
        let now = Utc::now();
        self.with_connection(|connection| {
            let values = params![
                now,
                patch
                    .compaction_enabled
                    .unwrap_or(current.compaction_enabled),
                patch
                    .compaction_after_days
                    .unwrap_or(current.compaction_after_days),
                patch
                    .compaction_interval_seconds
                    .unwrap_or(current.compaction_interval_seconds),
                patch
                    .compaction_batch_size
                    .unwrap_or(current.compaction_batch_size),
                project.repo_key
            ];
            connection.execute(UPDATE_PROJECT_SETTINGS_SQL, values)?;
            Ok(())
        })?;
        if let Some(thread) = self
            .inner
            .compaction_thread
            .lock()
            .map_err(lock_error)?
            .as_ref()
        {
            thread.thread().unpark();
        }
        self.project_settings()
    }

    /// Returns a compact project summary used by REST, MCP, and the dashboard.
    #[allow(clippy::redundant_closure)]
    pub fn project_summary(&self) -> AppResult<Value> {
        let project = self.project()?;
        let settings = self.project_settings()?;
        self.with_read_connection(|connection| {
            let snapshot_count: i64 = connection.query_row("SELECT count(*) FROM snapshots WHERE repo_key = ?", params![project.repo_key], |row| row.get(0))?;
            let command_count: i64 = connection.query_row("SELECT count(*) FROM registered_commands WHERE repo_key = ?", params![project.repo_key], |row| row.get(0))?;
            let run_count: i64 = connection.query_row("SELECT count(*) FROM runs WHERE repo_key = ?", params![project.repo_key], |row| row.get(0))?;
            let latest = connection
                .query_row(
                    &format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE repo_key = ? ORDER BY created_at DESC LIMIT 1"),
                    params![project.repo_key],
                    |row| snapshot_from_row(row),
                )
                .optional()
                .expect("snapshot summary projection has the initialized schema");
            let mut result = Map::new();
            result.insert("id".to_owned(), json!(stable_project_id(&project.repo_key)));
            result.insert("repo_key".to_owned(), json!(project.repo_key));
            result.insert("repo_path".to_owned(), json!(project.repo_path));
            result.insert("snapshot_count".to_owned(), json!(snapshot_count));
            result.insert(
                "branch_count".to_owned(),
                json!(connection
                    .query_row(
                        "SELECT count(DISTINCT branch) FROM snapshots WHERE repo_key = ?",
                        params![project.repo_key],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("snapshot summary count projection has the initialized schema")),
            );
            result.insert("command_count".to_owned(), json!(command_count));
            result.insert("run_count".to_owned(), json!(run_count));
            let latest_id = latest.as_ref().map(|value| {
                required_field(value, "id", "latest snapshot")
                    .expect("snapshot projections always contain id")
                    .clone()
            });
            result.insert(
                "latest_snapshot_id".to_owned(),
                latest_id.unwrap_or(Value::Null),
            );
            if let Some(latest) = latest {
                for key in [
                    "created_at",
                    "branch",
                    "commit_sha",
                    "suite",
                    "format",
                    "total_lines",
                    "covered_lines",
                    "line_rate",
                    "total_branches",
                    "covered_branches",
                    "branch_rate",
                    "total_functions",
                    "covered_functions",
                    "function_rate",
                    "total_regions",
                    "covered_regions",
                    "region_rate",
                ] {
                    let value = required_field(&latest, key, "latest snapshot")
                        .expect("snapshot projections always contain summary fields");
                    result.insert(format!("latest_{key}"), value.clone());
                }
            }
            result.insert(
                "compaction".to_owned(),
                serde_json::to_value(settings)
                    .expect("ProjectSettings serialization must be infallible"),
            );
            Ok(Value::Object(result))
        })
    }

    fn start_compaction_worker(&self) -> AppResult<()> {
        let store = self.clone();
        let handle = thread::Builder::new()
            .name("coverage-mcp-compactor".to_owned())
            .spawn(move || {
                while !store.inner.closing.load(Ordering::SeqCst) {
                    let settings = match store.project_settings() {
                        Ok(settings) => Some(settings),
                        Err(AppError::Validation(message))
                            if message
                                == "a repository must be selected before using coverage data" =>
                        {
                            None
                        }
                        Err(error) => {
                            eprintln!(
                                "coverage-mcp compaction worker could not read settings: {error}"
                            );
                            None
                        }
                    };
                    let wait_seconds = settings
                        .as_ref()
                        .map(|settings| settings.compaction_interval_seconds.clamp(1, 86_400))
                        .unwrap_or(1);
                    if let Some(settings) = settings {
                        if settings.compaction_enabled {
                            run_compaction_maintenance(&store, &settings);
                        }
                    }
                    thread::park_timeout(Duration::from_secs(wait_seconds.min(5)));
                }
            });
        retain_compaction_thread(&self.inner.compaction_thread, handle)
    }

    /// Compacts eligible older snapshots immediately, regardless of the enable flag.
    pub fn compact_now(&self) -> AppResult<Value> {
        let project = self.project()?;
        let settings = self.project_settings()?;
        let result = self.compact_project(&project, &settings.policy())?;
        let compacted_snapshots =
            checked_duckdb_i64(result.compacted_snapshots, "compacted snapshot count")
                .expect("bounded compaction count fits DuckDB BIGINT");
        let bytes_before = checked_duckdb_i64(result.bytes_before, "compacted byte count")
            .expect("bounded compaction bytes fit DuckDB BIGINT");
        let bytes_after = checked_duckdb_i64(result.bytes_after, "compacted byte count")
            .expect("bounded compaction bytes fit DuckDB BIGINT");
        self.with_connection(|connection| {
            let values = params![
                Utc::now(),
                result.status,
                compacted_snapshots,
                bytes_before,
                bytes_after,
                Utc::now(),
                project.repo_key
            ];
            connection.execute(UPDATE_COMPACTION_STATUS_SQL, values)?;
            Ok(())
        })?;
        Ok(serde_json::to_value(result).expect("CompactionResult serialization is infallible"))
    }

    fn compact_project(
        &self,
        project: &GitInfo,
        policy: &CompactionPolicy,
    ) -> AppResult<CompactionResult> {
        let cutoff = Utc::now() - ChronoDuration::days(i64::from(policy.older_than_days));
        let ids = self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT s.id FROM snapshots s LEFT JOIN coverage_compacted_payloads p ON p.snapshot_id = s.id WHERE s.repo_key = ? AND s.created_at < ? AND p.snapshot_id IS NULL ORDER BY s.created_at ASC LIMIT ?")?;
            let rows = statement
                .query_map(params![project.repo_key, cutoff, policy.batch_size as i64], |row| {
                    row.get::<_, String>(0)
                })
                .expect("compaction candidate projection has the initialized schema");
            let mut ids = Vec::new();
            for row in rows {
                ids.push(row.expect("compaction candidate rows have the initialized schema"));
            }
            Ok(ids)
        })?;
        let mut result = CompactionResult {
            repo_key: project.repo_key.clone(),
            completed_at: now_string(),
            status: "completed".to_owned(),
            ..CompactionResult::default()
        };
        for snapshot_id in ids {
            let (inserted, bytes_before, bytes_after) =
                self.compact_snapshot_detail(&project.repo_key, &snapshot_id)?;
            let inserted = u64::from(inserted);
            #[rustfmt::skip]
            let compacted_snapshots = checked_add_u64(result.compacted_snapshots, inserted, "compacted snapshot count").expect("bounded compaction count cannot overflow");
            result.compacted_snapshots = compacted_snapshots;
            #[rustfmt::skip]
            let bytes_before = checked_add_u64(result.bytes_before, checked_mul_u64(bytes_before, inserted, "compacted byte count").expect("bounded compaction bytes cannot overflow"), "compacted byte count").expect("bounded compaction bytes cannot overflow");
            result.bytes_before = bytes_before;
            #[rustfmt::skip]
            let bytes_after = checked_add_u64(result.bytes_after, checked_mul_u64(bytes_after, inserted, "compacted byte count").expect("bounded compaction bytes cannot overflow"), "compacted byte count").expect("bounded compaction bytes cannot overflow");
            result.bytes_after = bytes_after;
        }
        if result.compacted_snapshots > 0 {
            return self
                .with_connection(Self::checkpoint_connection)
                .map(|checkpointed| {
                    result.checkpointed = checkpointed;
                    result
                });
        }
        Ok(result)
    }

    fn compact_snapshot_detail(
        &self,
        repo_key: &str,
        snapshot_id: &str,
    ) -> AppResult<(bool, u64, u64)> {
        let payload = self.detail_payload(snapshot_id)?;
        let encoded =
            serde_json::to_vec(&payload).expect("coverage payload serialization is infallible");
        let mut encoded_reader = encoded.as_slice();
        let compressed = compress_coverage_payload(&mut encoded_reader)
            .expect("in-memory coverage payload compression cannot fail");
        let original_bytes = checked_usize_i64(encoded.len(), "coverage payload")
            .expect("bounded coverage payload fits DuckDB BIGINT");
        let compressed_bytes = checked_usize_i64(compressed.len(), "compressed payload")
            .expect("bounded compressed payload fits DuckDB BIGINT");
        let inserted = self.with_connection_mut(|connection| {
            Self::begin_compaction_transaction(connection)
                .expect("pooled compaction connections are not already transactional");
            let outcome = (|| {
                let changed = connection.execute("INSERT INTO coverage_compacted_payloads (snapshot_id, repo_key, compacted_at, original_bytes, compressed_bytes, payload) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (snapshot_id) DO NOTHING", params![snapshot_id, repo_key, Utc::now(), original_bytes, compressed_bytes, compressed])?;
                remove_compacted_detail(connection, snapshot_id, changed == 1)?;
                Ok::<bool, AppError>(changed == 1)
            })();
            finish_transaction(connection, outcome)
        })?;
        Ok((inserted, encoded.len() as u64, compressed.len() as u64))
    }

    fn begin_compaction_transaction(connection: &Connection) -> AppResult<()> {
        connection.execute_batch("BEGIN TRANSACTION")?;
        Ok(())
    }

    fn detail_payload(&self, snapshot_id: &str) -> AppResult<Value> {
        let files = self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? ORDER BY file_path")?;
            let rows = statement
                .query_map(params![snapshot_id], file_from_row)
                .expect("file detail projection has the initialized schema");
            let mut values = Vec::new();
            for row in rows {
                values.push(row.expect("file detail rows have the initialized schema"));
            }
            Ok(values)
        })?;
        let lines = self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT file_path, line_number, hits, covered, count_line, total_branches, covered_branches, total_functions, covered_functions, details FROM lines WHERE snapshot_id = ? ORDER BY file_path, line_number")?;
            Self::query_detail_lines(&mut statement, params![snapshot_id])
        })?;
        Ok(json!({"files": files, "lines": lines}))
    }

    fn query_detail_lines<P: duckdb::Params>(
        statement: &mut duckdb::Statement<'_>,
        params: P,
    ) -> AppResult<Vec<Value>> {
        let rows = statement.query_map(params, |row| {
            let value = match line_from_row_with_file(row) {
                Ok(value) => value,
                Err(error) => return Err(error),
            };
            Ok(value)
        })?;
        let mut values = Vec::new();
        for row in rows {
            values.push(row?);
        }
        Ok(values)
    }

    /// Registers a linked worktree and remembers the best available baseline snapshot.
    pub fn ensure_lineage_baseline(
        &self,
        path: &Path,
        base_ref: &str,
        name: Option<&str>,
    ) -> AppResult<Value> {
        let git = inspect_git(path)?;
        let selected = self.project()?;
        if git.commit_sha.is_none() || git.repo_key != selected.repo_key {
            return Err(AppError::Validation(
                "worktree must be a Git checkout of the selected repository".to_owned(),
            ));
        }
        let base_sha = merge_base(&git.repo_path, base_ref, "HEAD");
        let baseline = self.with_read_connection(|connection| {
            Ok(connection.query_row("SELECT id FROM snapshots WHERE repo_key = ? AND (commit_sha = ? OR branch = ?) ORDER BY CASE WHEN commit_sha = ? THEN 0 ELSE 1 END, created_at DESC LIMIT 1", params![git.repo_key, base_sha, base_ref, base_sha], |row| row.get::<_, String>(0)).optional()?)
        })?;
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now();
        self.with_connection(|connection| {
            connection.execute("INSERT INTO worktrees (id, created_at, name, path, repo_path, repo_key, branch, head_sha, base_ref, base_sha, baseline_snapshot_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", params![id, created_at, name, git.path, git.repo_path, git.repo_key, git.branch, git.commit_sha, base_ref.trim(), base_sha, baseline])?;
            Ok(())
        })?;
        self.worktree(&id)
    }

    /// Returns registered worktrees for the selected project.
    pub fn list_worktrees(&self, limit: usize) -> AppResult<Vec<Value>> {
        let project = self.project()?;
        self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT id, created_at, name, path, repo_path, repo_key, branch, head_sha, base_ref, base_sha, baseline_snapshot_id FROM worktrees WHERE repo_key = ? ORDER BY created_at DESC LIMIT ?")?;
            let rows = statement.query_map(
                params![project.repo_key, collection_limit(limit) as i64],
                worktree_from_row,
            )?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })
    }

    /// Returns one worktree.
    pub fn worktree(&self, worktree_id: &str) -> AppResult<Value> {
        self.with_read_connection(|connection| connection.query_row("SELECT id, created_at, name, path, repo_path, repo_key, branch, head_sha, base_ref, base_sha, baseline_snapshot_id FROM worktrees WHERE id = ?", params![worktree_id], worktree_from_row).optional()?.ok_or_else(|| AppError::NotFound(format!("worktree not found: {worktree_id}"))))
    }

    /// Resolves the best frozen baseline for one worktree and suite.
    pub fn worktree_baseline_snapshot(
        &self,
        worktree_id: &str,
        suite: &str,
    ) -> AppResult<Option<Value>> {
        let worktree = self.worktree(worktree_id)?;
        let stored = worktree
            .get("baseline_snapshot_id")
            .and_then(Value::as_str)
            .map(|id| self.snapshot(id))
            .transpose()?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored["suite"].as_str() == Some(suite) {
            return Ok(Some(stored));
        }
        let base_sha = worktree.get("base_sha").and_then(Value::as_str);
        let base_ref = worktree.get("base_ref").and_then(Value::as_str);
        let mut branch_match = None;
        for snapshot in self.list_snapshots(None, None, Some(suite), MAX_COLLECTION_RECORDS)? {
            if base_sha.is_some_and(|sha| snapshot["commit_sha"].as_str() == Some(sha)) {
                return Ok(Some(snapshot));
            }
            if branch_match.is_none()
                && base_ref.is_some_and(|reference| snapshot["branch"].as_str() == Some(reference))
            {
                branch_match = Some(snapshot);
            }
        }
        if branch_match.is_some() {
            return Ok(branch_match);
        }
        Ok(None)
    }

    fn worktree_file_points(
        &self,
        worktree_id: &str,
        suite: &str,
        file_path: &str,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        self.trend(
            None,
            None,
            Some(suite),
            Some(file_path),
            Some(worktree_id),
            limit,
        )
    }

    /// Computes coverage progress for snapshots captured in a registered worktree.
    pub fn worktree_progress(
        &self,
        worktree_id: &str,
        suite: &str,
        file_path: Option<&str>,
        limit: usize,
    ) -> AppResult<Value> {
        let worktree = self.worktree(worktree_id)?;
        let baseline = worktree
            .get("baseline_snapshot_id")
            .and_then(Value::as_str)
            .map(|snapshot_id| self.snapshot(snapshot_id))
            .transpose()?;
        let path = required_string_field(&worktree, "path", "worktree")
            .expect("stored worktree projections always contain path");
        let mut points = self.list_snapshots(Some(&path), None, Some(suite), limit)?;
        if let Some(file_path) = file_path {
            points = self.worktree_file_points(worktree_id, suite, file_path, limit)?;
        }
        Ok(
            json!({"worktree": worktree, "baseline": baseline, "suite": suite, "file_path": file_path, "points": points}),
        )
    }

    /// Returns time-series snapshots, optionally projected to one file.
    pub fn trend(
        &self,
        repo_path: Option<&str>,
        branch: Option<&str>,
        suite: Option<&str>,
        file_path: Option<&str>,
        worktree_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let mut snapshots = self.list_snapshots(repo_path, branch, suite, limit)?;
        if let Some(worktree_id) = worktree_id {
            let worktree = self.worktree(worktree_id)?;
            let path = required_string_field(&worktree, "path", "worktree")
                .expect("stored worktree projections always contain path");
            snapshots.retain(|snapshot| {
                snapshot.get("repo_path").and_then(Value::as_str) == Some(path.as_str())
            });
        }
        if let Some(file_path) = file_path {
            let mut values = Vec::new();
            for snapshot in snapshots {
                let id = required_string_field(&snapshot, "id", "snapshot")
                    .expect("stored snapshot projections always contain id");
                if let Some(file) = self
                    .files(&id, MAX_COLLECTION_RECORDS)?
                    .into_iter()
                    .find(|file| file.get("file_path").and_then(Value::as_str) == Some(file_path))
                {
                    let mut point = file;
                    let object = required_object_mut(&mut point, "file projection")
                        .expect("stored file projections are objects");
                    object.insert("id".to_owned(), json!(id));
                    object.insert(
                        "created_at".to_owned(),
                        required_field(&snapshot, "created_at", "snapshot")
                            .expect("stored snapshot projections always contain created_at")
                            .clone(),
                    );
                    object.insert(
                        "branch".to_owned(),
                        required_field(&snapshot, "branch", "snapshot")
                            .expect("stored snapshot projections always contain branch")
                            .clone(),
                    );
                    object.insert(
                        "commit_sha".to_owned(),
                        required_field(&snapshot, "commit_sha", "snapshot")
                            .expect("stored snapshot projections always contain commit_sha")
                            .clone(),
                    );
                    object.insert(
                        "suite".to_owned(),
                        required_field(&snapshot, "suite", "snapshot")
                            .expect("stored snapshot projections always contain suite")
                            .clone(),
                    );
                    values.push(point);
                }
            }
            return Ok(values);
        }
        Ok(snapshots)
    }

    /// Compares two compatible snapshots by metrics, files, and changed lines.
    pub fn compare(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        file_limit: usize,
        line_limit: usize,
    ) -> AppResult<Value> {
        let (current, baseline) =
            self.compatible_snapshot_pair(snapshot_id, baseline_snapshot_id)?;
        let files = self.compare_files(snapshot_id, baseline_snapshot_id, file_limit)?;
        let changed_lines =
            self.changed_lines(snapshot_id, baseline_snapshot_id, None, false, line_limit)?;
        Ok(
            json!({"baseline": baseline, "current": current, "overall": overall_delta(&current, &baseline).expect("compatible snapshots have complete metrics"), "files": files, "changed_lines": changed_lines}),
        )
    }

    /// Compares snapshots using grouped changed regions instead of line records.
    pub fn compare_regions(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        file_path: Option<&str>,
        only_regressions: bool,
        limit: usize,
    ) -> AppResult<Value> {
        let (current, baseline) =
            self.compatible_snapshot_pair(snapshot_id, baseline_snapshot_id)?;
        let regions = self.changed_regions(
            snapshot_id,
            baseline_snapshot_id,
            file_path,
            only_regressions,
            limit,
        )?;
        Ok(json!({
            "baseline": baseline,
            "current": current,
            "overall": overall_delta(&current, &baseline)
                .expect("compatible snapshots have complete metrics"),
            "regions": regions,
        }))
    }

    fn attach_worktree_to_comparison(result: &mut Value, worktree: Value) -> AppResult<()> {
        let object = result.as_object_mut().ok_or_else(|| {
            AppError::Runtime("comparison result must be a JSON object".to_owned())
        })?;
        object.insert("worktree".to_owned(), worktree);
        Ok(())
    }

    fn compatible_snapshot_pair(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
    ) -> AppResult<(Value, Value)> {
        let current = self.snapshot(snapshot_id)?;
        let baseline = self.snapshot(baseline_snapshot_id)?;
        if current.get("repo_key") != baseline.get("repo_key") {
            return Err(AppError::Validation(
                "comparisons require snapshots from the same repository".to_owned(),
            ));
        }
        if current.get("suite") != baseline.get("suite") {
            return Err(AppError::Validation(
                "comparisons require snapshots from the same suite".to_owned(),
            ));
        }
        Ok((current, baseline))
    }

    fn compare_files(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let mut baseline = HashMap::new();
        for value in self.files(baseline_snapshot_id, MAX_COLLECTION_RECORDS)? {
            let key = required_string_field(&value, "file_path", "baseline file")
                .expect("stored file projections always contain file_path");
            baseline.insert(key, value);
        }
        let mut current = HashMap::new();
        for value in self.files(snapshot_id, MAX_COLLECTION_RECORDS)? {
            let key = required_string_field(&value, "file_path", "current file")
                .expect("stored file projections always contain file_path");
            current.insert(key, value);
        }
        let mut paths: Vec<String> = baseline.keys().chain(current.keys()).cloned().collect();
        paths.sort();
        paths.dedup();
        let mut values = Vec::new();
        for path in paths.into_iter().take(collection_limit(limit)) {
            let before = baseline.get(&path);
            let after = current.get(&path);
            let mut value = Map::new();
            value.insert("file_path".to_owned(), json!(path));
            for (key, output) in [
                ("baseline_total_lines", "total_lines"),
                ("current_total_lines", "total_lines"),
                ("baseline_covered_lines", "covered_lines"),
                ("current_covered_lines", "covered_lines"),
                ("baseline_line_rate", "line_rate"),
                ("current_line_rate", "line_rate"),
                ("baseline_total_branches", "total_branches"),
                ("current_total_branches", "total_branches"),
                ("baseline_covered_branches", "covered_branches"),
                ("current_covered_branches", "covered_branches"),
                ("baseline_branch_rate", "branch_rate"),
                ("current_branch_rate", "branch_rate"),
                ("baseline_total_functions", "total_functions"),
                ("current_total_functions", "total_functions"),
                ("baseline_covered_functions", "covered_functions"),
                ("current_covered_functions", "covered_functions"),
                ("baseline_function_rate", "function_rate"),
                ("current_function_rate", "function_rate"),
                ("baseline_total_regions", "total_regions"),
                ("current_total_regions", "total_regions"),
                ("baseline_covered_regions", "covered_regions"),
                ("current_covered_regions", "covered_regions"),
                ("baseline_region_rate", "region_rate"),
                ("current_region_rate", "region_rate"),
            ] {
                let source = if key.starts_with("baseline") {
                    before
                } else {
                    after
                };
                value.insert(
                    key.to_owned(),
                    source
                        .and_then(|item| item.get(output))
                        .cloned()
                        .unwrap_or(Value::Null),
                );
            }
            for (metric, delta_key) in [
                ("line_rate", "line_rate_delta"),
                ("branch_rate", "branch_rate_delta"),
                ("function_rate", "function_rate_delta"),
                ("region_rate", "region_rate_delta"),
            ] {
                value.insert(
                    delta_key.to_owned(),
                    delta(
                        after.and_then(|item| item.get(metric)),
                        before.and_then(|item| item.get(metric)),
                    ),
                );
            }
            values.push(Value::Object(value));
        }
        values.sort_by(|left, right| {
            left.get("line_rate_delta")
                .and_then(Value::as_f64)
                .partial_cmp(&right.get("line_rate_delta").and_then(Value::as_f64))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left.get("file_path")
                        .and_then(Value::as_str)
                        .cmp(&right.get("file_path").and_then(Value::as_str))
                })
        });
        Ok(values)
    }

    /// Computes line-level changes between snapshots.
    pub fn changed_lines(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        file_path: Option<&str>,
        only_regressions: bool,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let mut values = self.changed_line_values(
            snapshot_id,
            baseline_snapshot_id,
            file_path,
            only_regressions,
        )?;
        values.sort_by(|left, right| {
            status_order(left).cmp(&status_order(right)).then_with(|| {
                left.get("file_path")
                    .and_then(Value::as_str)
                    .cmp(&right.get("file_path").and_then(Value::as_str))
                    .then_with(|| {
                        left.get("line_number")
                            .and_then(Value::as_i64)
                            .cmp(&right.get("line_number").and_then(Value::as_i64))
                    })
            })
        });
        values.truncate(collection_limit(limit));
        Ok(values)
    }

    /// Computes grouped changed-line regions for compact agent responses.
    pub fn changed_regions(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        file_path: Option<&str>,
        only_regressions: bool,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let lines = self.changed_line_values(
            snapshot_id,
            baseline_snapshot_id,
            file_path,
            only_regressions,
        )?;
        let mut grouped: BTreeMap<(String, String), Vec<i64>> = BTreeMap::new();
        for line in lines {
            let path = required_string_field(&line, "file_path", "changed line")
                .expect("stored changed-line projections always contain file_path");
            let status = required_string_field(&line, "status", "changed line")
                .expect("stored changed-line projections always contain status");
            let number = required_i64_field(&line, "line_number", "changed line")
                .expect("stored changed-line projections always contain line_number");
            grouped.entry((path, status)).or_default().push(number);
        }
        let mut regions = Vec::new();
        for ((path, status), mut numbers) in grouped {
            numbers.sort_unstable();
            numbers.dedup();
            for region in line_regions(&numbers) {
                regions.push(json!({
                    "file_path": path,
                    "status": status,
                    "start": region["start"],
                    "end": region["end"],
                    "line_count": region["line_count"],
                }));
            }
        }
        regions.sort_by(|left, right| {
            status_order(left).cmp(&status_order(right)).then_with(|| {
                left.get("file_path")
                    .and_then(Value::as_str)
                    .cmp(&right.get("file_path").and_then(Value::as_str))
                    .then_with(|| {
                        left.get("start")
                            .and_then(Value::as_i64)
                            .cmp(&right.get("start").and_then(Value::as_i64))
                    })
            })
        });
        regions.truncate(collection_limit(limit));
        Ok(regions)
    }

    fn changed_line_values(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        file_path: Option<&str>,
        only_regressions: bool,
    ) -> AppResult<Vec<Value>> {
        let current_snapshot = self.snapshot(snapshot_id)?;
        let baseline_snapshot = self.snapshot(baseline_snapshot_id)?;
        if current_snapshot.get("repo_key") != baseline_snapshot.get("repo_key") {
            return Err(AppError::Validation(
                "changed lines require snapshots from the same repository".to_owned(),
            ));
        }
        if current_snapshot.get("suite") != baseline_snapshot.get("suite") {
            return Err(AppError::Validation(
                "changed lines require snapshots from the same suite".to_owned(),
            ));
        }
        let before = self.lines_all(baseline_snapshot_id)?;
        let after = self.lines_all(snapshot_id)?;
        let mut keys: Vec<(String, i64)> = before.keys().chain(after.keys()).cloned().collect();
        keys.sort();
        keys.dedup();
        let mut values = Vec::new();
        for (path, number) in keys {
            if file_path.is_some_and(|wanted| wanted != path) {
                continue;
            }
            let left = before.get(&(path.clone(), number));
            let right = after.get(&(path.clone(), number));
            let left_covered = left.map(|value| {
                required_bool_field(value, "covered", "baseline line")
                    .expect("stored baseline line projections always contain covered")
            });
            let right_covered = right.map(|value| {
                required_bool_field(value, "covered", "current line")
                    .expect("stored current line projections always contain covered")
            });
            let left_hits = left.map(|value| {
                required_i64_field(value, "hits", "baseline line")
                    .expect("stored baseline line projections always contain hits")
            });
            let right_hits = right.map(|value| {
                required_i64_field(value, "hits", "current line")
                    .expect("stored current line projections always contain hits")
            });
            let left_branches = left.map(|value| {
                required_i64_field(value, "covered_branches", "baseline line")
                    .expect("stored baseline line projections always contain covered_branches")
            });
            let right_branches = right.map(|value| {
                required_i64_field(value, "covered_branches", "current line")
                    .expect("stored current line projections always contain covered_branches")
            });
            let left_total_branches = left.map(|value| {
                required_i64_field(value, "total_branches", "baseline line")
                    .expect("stored baseline line projections always contain total_branches")
            });
            let right_total_branches = right.map(|value| {
                required_i64_field(value, "total_branches", "current line")
                    .expect("stored current line projections always contain total_branches")
            });
            if left_covered == right_covered
                && left_hits == right_hits
                && left_branches == right_branches
                && left_total_branches == right_total_branches
            {
                continue;
            }
            let branch_gap = |covered: Option<i64>, total: Option<i64>| {
                total.zip(covered).map(|(total, covered)| total - covered)
            };
            let branch_regressed = branch_gap(right_branches, right_total_branches)
                .zip(branch_gap(left_branches, left_total_branches))
                .is_some_and(|(current, baseline)| current > baseline);
            let branch_improved = branch_gap(right_branches, right_total_branches)
                .zip(branch_gap(left_branches, left_total_branches))
                .is_some_and(|(current, baseline)| current < baseline);
            let status = if left.is_none() {
                "new"
            } else if right.is_none() {
                "removed"
            } else if left_covered == Some(true) && right_covered == Some(false) {
                "regressed"
            } else if left_covered == Some(false) && right_covered == Some(true) {
                "improved"
            } else if branch_regressed {
                "regressed"
            } else if branch_improved {
                "improved"
            } else {
                "changed"
            };
            if only_regressions && status != "regressed" {
                continue;
            }
            values.push(json!({
                "file_path": path,
                "line_number": number,
                "baseline_covered": left.map(|value| value["covered"].clone()).unwrap_or(Value::Null),
                "current_covered": right.map(|value| value["covered"].clone()).unwrap_or(Value::Null),
                "baseline_hits": left.map(|value| value["hits"].clone()).unwrap_or(Value::Null),
                "current_hits": right.map(|value| value["hits"].clone()).unwrap_or(Value::Null),
                "baseline_total_branches": left.map(|value| value["total_branches"].clone()).unwrap_or(Value::Null),
                "current_total_branches": right.map(|value| value["total_branches"].clone()).unwrap_or(Value::Null),
                "baseline_covered_branches": left.map(|value| value["covered_branches"].clone()).unwrap_or(Value::Null),
                "current_covered_branches": right.map(|value| value["covered_branches"].clone()).unwrap_or(Value::Null),
                "status": status
            }));
        }
        Ok(values)
    }

    fn lines_all(&self, snapshot_id: &str) -> AppResult<HashMap<(String, i64), Value>> {
        let rows = self.with_connection(|connection| line_rows(connection, snapshot_id))?;
        if !rows.is_empty() {
            let mut values = HashMap::new();
            for line in rows {
                let path = required_string_field(&line, "file_path", "stored line")
                    .expect("stored line projections always contain file_path");
                append_line_with_path(&mut values, &path, line)
                    .expect("stored line projections are valid line objects");
            }
            return Ok(values);
        }
        let mut values = HashMap::new();
        if let Some(payload) = self.compacted_detail(snapshot_id)? {
            for line in payload
                .get("lines")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .cloned()
            {
                let path = required_string_field(&line, "file_path", "compacted line")
                    .expect("compacted line projections always contain file_path");
                append_line_with_path(&mut values, &path, line)
                    .expect("compacted line projections are valid line objects");
            }
        }
        Ok(values)
    }

    /// Returns ranked file and uncovered-line targets without raw line JSON.
    pub fn targets(
        &self,
        snapshot_id: &str,
        order_by: &str,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        if !matches!(
            order_by,
            "priority" | "uncovered_lines" | "line_rate" | "file_path"
        ) {
            return Err(AppError::Validation(
                "order_by must be priority, uncovered_lines, line_rate, or file_path".to_owned(),
            ));
        }
        let mut file_values = self.files(snapshot_id, MAX_COLLECTION_RECORDS)?;
        let lines = self.lines_all(snapshot_id)?;
        let mut uncovered_by_file: HashMap<String, Vec<i64>> = HashMap::new();
        for ((path, number), line) in lines {
            let count_line = required_bool_field(&line, "count_line", "coverage line")
                .expect("stored line projections always contain count_line");
            let covered = required_bool_field(&line, "covered", "coverage line")
                .expect("stored line projections always contain covered");
            if count_line && !covered {
                uncovered_by_file.entry(path).or_default().push(number);
            }
        }
        let mut values = Vec::new();
        for file in file_values.drain(..) {
            let path = required_string_field(&file, "file_path", "coverage file")
                .expect("stored file projections always contain file_path");
            let total_lines = required_i64_field(&file, "total_lines", "coverage file")
                .expect("stored file projections always contain total_lines");
            let covered_lines = required_i64_field(&file, "covered_lines", "coverage file")
                .expect("stored file projections always contain covered_lines");
            let uncovered_lines = uncovered_metric(total_lines, covered_lines, "lines")
                .expect("stored line metrics are non-negative");
            let total_branches = required_i64_field(&file, "total_branches", "coverage file")
                .expect("stored file projections always contain total_branches");
            let covered_branches = required_i64_field(&file, "covered_branches", "coverage file")
                .expect("stored file projections always contain covered_branches");
            let uncovered_branches = uncovered_metric(total_branches, covered_branches, "branches")
                .expect("stored branch metrics are non-negative");
            let total_functions = required_i64_field(&file, "total_functions", "coverage file")
                .expect("stored file projections always contain total_functions");
            let covered_functions = required_i64_field(&file, "covered_functions", "coverage file")
                .expect("stored file projections always contain covered_functions");
            let uncovered_functions =
                uncovered_metric(total_functions, covered_functions, "functions")
                    .expect("stored function metrics are non-negative");
            if uncovered_lines == 0 && uncovered_branches == 0 && uncovered_functions == 0 {
                continue;
            }
            let mut numbers = uncovered_by_file.remove(&path).unwrap_or_default();
            numbers.sort_unstable();
            numbers.dedup();
            let priority =
                coverage_target_priority(uncovered_lines, uncovered_branches, uncovered_functions)
                    .expect("stored coverage metrics fit target priority");
            values.push(json!({
                "file_path": path,
                "line_rate": required_field(&file, "line_rate", "coverage file")
                    .expect("stored file projections always contain line_rate"),
                "uncovered_lines": uncovered_lines,
                "uncovered_branches": uncovered_branches,
                "uncovered_functions": uncovered_functions,
                "priority": priority,
                "regions": line_regions(&numbers),
            }));
        }
        values.sort_by(|left, right| target_order(left, right, order_by));
        values.truncate(collection_limit(limit));
        Ok(values)
    }

    /// Builds prioritized coverage insights for one snapshot.
    pub fn insights(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Value> {
        let snapshot = self.snapshot(snapshot_id)?;
        let mut items = Vec::new();
        for warning in snapshot
            .get("warnings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            items.push(json!({"severity":"info","category":"parser-warning","title":"Coverage format has lossy detail","detail":warning,"snapshot_id":snapshot_id}));
        }
        for file in self.files(snapshot_id, MAX_COLLECTION_RECORDS)? {
            let path = required_string_field(&file, "file_path", "coverage file")
                .expect("stored file projections always contain file_path");
            let total = required_i64_field(&file, "total_lines", "coverage file")
                .expect("stored file projections always contain total_lines");
            let covered = required_i64_field(&file, "covered_lines", "coverage file")
                .expect("stored file projections always contain covered_lines");
            let uncovered = uncovered_metric(total, covered, "lines")
                .expect("stored line metrics are non-negative");
            let rate = required_field(&file, "line_rate", "coverage file")
                .expect("stored file projections always contain line_rate")
                .as_f64();
            if total > 0 && covered == 0 {
                items.push(json!({"severity": if total >= 20 {"high"} else {"medium"},"category":"zero-coverage-file","title":"File has no covered lines","detail":format!("{path} has 0/{total} covered lines."),"file_path":path,"uncovered_lines":uncovered,"line_rate":rate}));
            }
            if total >= 5 && covered > 0 {
                if let Some(rate_value) = rate.filter(|value| *value < 0.6) {
                    items.push(json!({"severity":"medium","category":"low-line-coverage","title":"File has low line coverage","detail":format!("{path} is {:.1}% covered with {uncovered} uncovered lines.", rate_value*100.0),"file_path":path,"uncovered_lines":uncovered,"line_rate":rate}));
                }
            }
            let total_branches = required_i64_field(&file, "total_branches", "coverage file")
                .expect("stored file projections always contain total_branches");
            let covered_branches = required_i64_field(&file, "covered_branches", "coverage file")
                .expect("stored file projections always contain covered_branches");
            let uncovered_branches = uncovered_metric(total_branches, covered_branches, "branches")
                .expect("stored branch metrics are non-negative");
            let branch_rate = required_field(&file, "branch_rate", "coverage file")
                .expect("stored file projections always contain branch_rate")
                .as_f64();
            if total_branches >= 2 && branch_rate.is_none_or(|value| value < 0.7) {
                items.push(json!({"severity":"medium","category":"low-branch-coverage","title":"Branch coverage needs attention","detail":format!("{path} covers {covered_branches}/{total_branches} branches."),"file_path":path,"uncovered_branches":uncovered_branches,"branch_rate":branch_rate}));
            }
        }
        let baseline = if let Some(baseline) = baseline_snapshot_id {
            let comparison =
                self.compare(snapshot_id, baseline, limit, limit.saturating_mul(20))?;
            let overall = required_field(&comparison, "overall", "comparison")
                .expect("comparison projections always contain overall")
                .clone();
            if overall
                .get("line_rate_delta")
                .and_then(Value::as_f64)
                .is_some_and(|value| value < 0.0)
            {
                items.push(json!({"severity":"high","category":"overall-regression","title":"Overall line coverage regressed","detail":"Overall line coverage decreased.","line_rate_delta":overall.get("line_rate_delta"),"covered_lines_delta":overall.get("covered_lines_delta")}));
            }
            let files = required_array_field(&comparison, "files", "comparison")
                .expect("comparison projections always contain files");
            for file in files.iter().take(limit) {
                let path = required_string_field(file, "file_path", "comparison file")
                    .expect("comparison file projections always contain file_path");
                let line_rate_delta = required_field(file, "line_rate_delta", "comparison file")
                    .expect("comparison file projections always contain line_rate_delta");
                if file
                    .get("line_rate_delta")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value < 0.0)
                {
                    items.push(json!({"severity":"high","category":"file-regression","title":"File coverage regressed","detail":format!("{path} changed coverage."),"file_path":path,"line_rate_delta":line_rate_delta}));
                }
            }
            let changed_lines = required_array_field(&comparison, "changed_lines", "comparison")
                .expect("comparison projections always contain changed_lines");
            for line in changed_lines
                .iter()
                .filter(|line| line.get("status").and_then(Value::as_str) == Some("regressed"))
                .take(limit)
            {
                let path = required_string_field(line, "file_path", "changed line")
                    .expect("changed-line projections always contain file_path");
                let number = required_i64_field(line, "line_number", "changed line")
                    .expect("changed-line projections always contain line_number");
                items.push(json!({"severity":"high","category":"line-regression","title":"Line became uncovered","detail":format!("{path}:{number} was covered and is now missed."),"file_path":path,"line_number":number}));
            }
            Some(comparison)
        } else {
            None
        };
        items.sort_by(|left, right| {
            insight_order(left)
                .cmp(&insight_order(right))
                .then_with(|| {
                    left.get("file_path")
                        .and_then(Value::as_str)
                        .cmp(&right.get("file_path").and_then(Value::as_str))
                })
        });
        let selected: Vec<Value> = items
            .into_iter()
            .take(limit.saturating_mul(4).max(limit))
            .collect();
        Ok(
            json!({"snapshot":snapshot,"baseline":baseline.as_ref().and_then(|value| value.get("baseline")).cloned(),"summary":{"item_count":selected.len(),"high_count":selected.iter().filter(|item| item.get("severity").and_then(Value::as_str)==Some("high")).count(),"medium_count":selected.iter().filter(|item| item.get("severity").and_then(Value::as_str)==Some("medium")).count(),"info_count":selected.iter().filter(|item| item.get("severity").and_then(Value::as_str)==Some("info")).count()},"items":selected}),
        )
    }

    /// Compares a worktree snapshot with its frozen baseline.
    pub fn compare_worktree(
        &self,
        worktree_id: &str,
        snapshot_id: Option<&str>,
        file_limit: usize,
        line_limit: usize,
    ) -> AppResult<Value> {
        let (worktree, current, baseline) =
            self.worktree_snapshot_pair(worktree_id, snapshot_id)?;
        let current_id = required_string_field(&current, "id", "snapshot")
            .expect("snapshot projections always contain id");
        let mut result = self.compare(&current_id, &baseline, file_limit, line_limit)?;
        Self::attach_worktree_to_comparison(&mut result, worktree)
            .expect("comparison projections are JSON objects");
        Ok(result)
    }

    /// Compares a worktree baseline with grouped changed regions.
    pub fn compare_worktree_regions(
        &self,
        worktree_id: &str,
        snapshot_id: Option<&str>,
        file_path: Option<&str>,
        only_regressions: bool,
        limit: usize,
    ) -> AppResult<Value> {
        let (worktree, current, baseline) =
            self.worktree_snapshot_pair(worktree_id, snapshot_id)?;
        let current_id = required_string_field(&current, "id", "snapshot")
            .expect("snapshot projections always contain id");
        let mut result =
            self.compare_regions(&current_id, &baseline, file_path, only_regressions, limit)?;
        Self::attach_worktree_to_comparison(&mut result, worktree)
            .expect("comparison projections are JSON objects");
        Ok(result)
    }

    fn worktree_snapshot_pair(
        &self,
        worktree_id: &str,
        snapshot_id: Option<&str>,
    ) -> AppResult<(Value, Value, String)> {
        let worktree = self.worktree(worktree_id)?;
        let current_id = if let Some(snapshot_id) = snapshot_id {
            snapshot_id.to_owned()
        } else {
            let path = required_string_field(&worktree, "path", "worktree")
                .expect("stored worktree projections always contain path");
            let current = self
                .trend(Some(&path), None, None, None, Some(worktree_id), 1)?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    AppError::NotFound(format!(
                        "no current snapshot found for worktree: {worktree_id}"
                    ))
                })?;
            required_string_field(&current, "id", "worktree snapshot")
                .expect("snapshot projections always contain id")
        };
        let current = self.snapshot(&current_id)?;
        let current_repo_key = required_field(&current, "repo_key", "snapshot")
            .expect("snapshot projections always contain repo_key");
        let worktree_repo_key = required_field(&worktree, "repo_key", "worktree")
            .expect("worktree projections always contain repo_key");
        let current_repo_path = required_field(&current, "repo_path", "snapshot")
            .expect("snapshot projections always contain repo_path");
        let worktree_path = required_field(&worktree, "path", "worktree")
            .expect("worktree projections always contain path");
        if current_repo_key != worktree_repo_key || current_repo_path != worktree_path {
            return Err(AppError::Validation(
                "current snapshot does not belong to the selected worktree".to_owned(),
            ));
        }
        let suite = required_string_field(&current, "suite", "current snapshot")
            .expect("snapshot projections always contain suite");
        let baseline = self
            .worktree_baseline_snapshot(worktree_id, &suite)?
            .and_then(|snapshot| snapshot["id"].as_str().map(str::to_owned))
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "worktree has no baseline snapshot for suite {suite}"
                ))
            })?;
        Ok((worktree, current, baseline))
    }

    pub(crate) fn compare_worktree_default_limits(
        &self,
        worktree_id: &str,
        snapshot_id: Option<&str>,
    ) -> AppResult<Value> {
        self.compare_worktree(
            worktree_id,
            snapshot_id,
            COLLECTION_FETCH_LIMIT,
            COLLECTION_FETCH_LIMIT,
        )
    }

    /// Registers one human-approved command.
    #[allow(clippy::too_many_arguments)]
    pub fn register_command(
        &self,
        name: &str,
        command: &str,
        cwd: Option<&Path>,
        shell: &str,
        artifact_paths: Option<Value>,
        human_approved: bool,
        approved_by: &str,
        approval_note: &str,
        enabled: bool,
    ) -> AppResult<Value> {
        if !human_approved {
            return Err(AppError::Validation(
                "human_approved must be true to register a command".to_owned(),
            ));
        }
        let name = name.trim();
        let command = command.trim();
        let approved_by = approved_by.trim();
        let approval_note = approval_note.trim();
        if name.is_empty() {
            return Err(AppError::Validation("command name is required".to_owned()));
        }
        if command.is_empty() {
            return Err(AppError::Validation("command is required".to_owned()));
        }
        if approved_by.is_empty() {
            return Err(AppError::Validation("approved_by is required".to_owned()));
        }
        if approval_note.is_empty() {
            return Err(AppError::Validation("approval_note is required".to_owned()));
        }
        let cwd = cwd.unwrap_or(Path::new(".")).canonicalize()?;
        if !cwd.is_dir() {
            return Err(AppError::Validation(format!(
                "cwd does not exist or is not a directory: {}",
                cwd.display()
            )));
        }
        let git = self.ensure_project(&cwd)?;
        let artifacts = normalize_artifact_specs(artifact_paths.unwrap_or(Value::Null))?;
        let id = Uuid::new_v4().to_string();
        self.with_connection(|connection| {
            connection.execute("INSERT INTO registered_commands (id, created_at, name, command, cwd, repo_path, repo_key, branch, commit_sha, shell, approved_by, approval_note, artifact_specs, enabled, duration_estimate_ms, duration_p90_ms, duration_sample_count, duration_stats_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL)", params![id, Utc::now(), name, command, cwd.to_string_lossy(), git.repo_path, git.repo_key, git.branch, git.commit_sha, shell, approved_by, approval_note, serde_json::to_string(&artifacts).expect("artifact specification serialization is infallible"), enabled])?;
            Ok(())
        })?;
        self.registered_command(&id)
    }

    /// Resolves a command by UUID or latest matching name.
    pub fn registered_command(&self, reference: &str) -> AppResult<Value> {
        self.with_connection(|connection| {
            let by_id = connection.query_row("SELECT id, created_at, name, command, cwd, repo_path, repo_key, branch, commit_sha, shell, approved_by, approval_note, artifact_specs, enabled, duration_estimate_ms, duration_p90_ms, duration_sample_count FROM registered_commands WHERE id = ?", params![reference], command_from_row).optional()?;
            if let Some(value) = by_id { return Ok(value); }
            connection.query_row("SELECT id, created_at, name, command, cwd, repo_path, repo_key, branch, commit_sha, shell, approved_by, approval_note, artifact_specs, enabled, duration_estimate_ms, duration_p90_ms, duration_sample_count FROM registered_commands WHERE name = ? ORDER BY created_at DESC LIMIT 1", params![reference], command_from_row).optional()?.ok_or_else(|| AppError::NotFound(format!("registered command not found: {reference}")))
        })
    }

    /// Lists registered commands newest first.
    pub fn list_registered_commands(&self, limit: usize) -> AppResult<Vec<Value>> {
        let project = self.project()?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT id, created_at, name, command, cwd, repo_path, repo_key, branch, commit_sha, shell, approved_by, approval_note, artifact_specs, enabled, duration_estimate_ms, duration_p90_ms, duration_sample_count FROM registered_commands WHERE repo_key = ? ORDER BY created_at DESC LIMIT ?")?;
            let rows = statement.query_map(
                params![project.repo_key, collection_limit(limit) as i64],
                command_from_row,
            )?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })
    }

    fn retain_run_thread(&self, handle: std::io::Result<JoinHandle<()>>) -> AppResult<()> {
        let handle = handle.map_err(AppError::from)?;
        self.inner
            .run_threads
            .lock()
            .map_err(lock_error)?
            .push(handle);
        Ok(())
    }

    fn start_run_worker(&self, run_id: &str) -> AppResult<()> {
        let store = self.clone();
        let worker_run_id = run_id.to_owned();
        let handle = thread::Builder::new()
            .name(format!("coverage-mcp-run-{run_id}"))
            .spawn(move || {
                report_background_run_error(&store, &worker_run_id);
            });
        if let Err(error) = self.retain_run_thread(handle) {
            self.finalize_failed_job_or_log(run_id, &error, "finalize unretained run");
            return Err(error);
        }
        Ok(())
    }

    /// Submits one approved command without state-based reuse.
    pub fn submit_command(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
    ) -> AppResult<Value> {
        self.submit_command_with_options(
            command_ref,
            timeout_seconds,
            idempotency_key,
            max_summary_lines,
            false,
        )
    }

    /// Submits one approved command to the background runner.
    pub fn submit_command_with_options(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
        reuse_if_unchanged: bool,
    ) -> AppResult<Value> {
        if max_summary_lines == 0 || max_summary_lines > 500 {
            return Err(AppError::Validation(
                "max_summary_lines must be between 1 and 500".to_owned(),
            ));
        }
        if timeout_seconds.is_some_and(|value| !(1..=86_400).contains(&value)) {
            return Err(AppError::Validation(
                "timeout_seconds must be between 1 and 86400".to_owned(),
            ));
        }
        let command = self.registered_command(command_ref)?;
        if !required_bool_field(&command, "enabled", "registered command")
            .expect("registered command projections always contain enabled")
        {
            return Err(AppError::Validation(format!(
                "registered command is disabled: {command_ref}"
            )));
        }
        let key = idempotency_key
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_owned);
        if idempotency_key.is_some() && key.is_none() {
            return Err(AppError::Validation(
                "idempotency_key must not be blank".to_owned(),
            ));
        }
        if key.as_ref().is_some_and(|value| value.len() > 200) {
            return Err(AppError::Validation(
                "idempotency_key must not exceed 200 characters".to_owned(),
            ));
        }
        let command_id = required_string_field(&command, "id", "registered command")
            .expect("registered command projections always contain id");
        if let Some(existing) = self.idempotent_run_id(&command_id, key.as_deref())? {
            let mut value = self.run_result(&existing, max_summary_lines)?;
            #[allow(clippy::option_map_unit_fn)]
            value.as_object_mut().map(|object| {
                object.insert("submission_reused".to_owned(), json!(true));
                object.insert("reuse_reason".to_owned(), json!("idempotency_key"));
            });
            return Ok(value);
        }
        if reuse_if_unchanged && key.is_none() {
            if let Some(existing) = self.reusable_run_id(&command, &command_id)? {
                let mut value = self.reused_run_result(&existing, max_summary_lines)?;
                #[allow(clippy::option_map_unit_fn)]
                value.as_object_mut().map(|object| {
                    object.insert("submission_reused".to_owned(), json!(true));
                    object.insert("reuse_reason".to_owned(), json!("unchanged_checkout"));
                });
                return Ok(value);
            }
        }
        let id = Uuid::new_v4().to_string();
        let run_path = self.inner.run_dir.join(&id);
        let (stdout, stderr) = create_run_log_files(&run_path)?;
        let git = inspect_git(Path::new(
            command.get("cwd").and_then(Value::as_str).unwrap_or("."),
        ))?;
        let baselines = artifact_baselines(
            &id,
            &command,
            command.get("cwd").and_then(Value::as_str).unwrap_or("."),
        )?;
        self.with_connection(|connection| {
            connection.execute("INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 'queued', ?, ?, '', NULL)", params![id, command.get("id").and_then(Value::as_str), command.get("name").and_then(Value::as_str), command.get("command").and_then(Value::as_str), key, command.get("cwd").and_then(Value::as_str), git.repo_path, git.repo_key, git.branch, git.commit_sha, Utc::now(), timeout_seconds.map(|value| value as i64), max_summary_lines as i64, stdout.to_string_lossy(), stderr.to_string_lossy()])?;
            for baseline in &baselines {
                connection.execute(
                    "INSERT INTO run_artifact_baselines (run_id, kind, path, exists, size_bytes, modified_ns, sha256) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![
                        baseline.run_id,
                        baseline.kind,
                        baseline.path,
                        baseline.fingerprint.exists,
                        baseline.fingerprint.size_bytes,
                        baseline.fingerprint.modified_ns,
                        baseline.fingerprint.sha256,
                    ],
                )?;
            }
            Ok(())
        })?;
        self.start_run_worker(&id)?;
        let mut result = self.run_result(&id, max_summary_lines)?;
        #[allow(clippy::option_map_unit_fn)]
        result.as_object_mut().map(|object| {
            object.insert("submission_reused".to_owned(), json!(false));
        });
        Ok(result)
    }

    fn submitted_run_id(submitted: &Value) -> AppResult<&str> {
        submitted
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Runtime("run submission did not return an id".to_owned()))
    }

    /// Submits and waits for one command to reach a terminal state.
    pub fn run_command(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
    ) -> AppResult<Value> {
        self.run_command_with_options(
            command_ref,
            timeout_seconds,
            idempotency_key,
            max_summary_lines,
            false,
        )
    }

    /// Submits and waits for one command, optionally reusing unchanged state.
    pub fn run_command_with_options(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
        reuse_if_unchanged: bool,
    ) -> AppResult<Value> {
        let submission = (
            command_ref,
            timeout_seconds,
            idempotency_key,
            max_summary_lines,
        );
        let submitted = self.submit_command_with_options(
            submission.0,
            submission.1,
            submission.2,
            submission.3,
            reuse_if_unchanged,
        )?;
        let id = Self::submitted_run_id(&submitted)
            .expect("managed run submission always includes its run id")
            .to_owned();
        let mut result = submitted;
        while !result
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            thread::sleep(Duration::from_millis(20));
            result = self.run_result(&id, max_summary_lines)?;
        }
        Ok(result)
    }

    fn reused_run_result(&self, run_id: &str, max_summary_lines: usize) -> AppResult<Value> {
        #[cfg(test)]
        if FORCE_REUSED_RESULT_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Runtime(
                "injected unchanged-checkout result failure".to_owned(),
            ));
        }
        self.run_result(run_id, max_summary_lines)
    }

    fn idempotent_run_id(&self, command_id: &str, key: Option<&str>) -> AppResult<Option<String>> {
        let Some(key) = key else {
            return Ok(None);
        };
        self.with_connection(|connection| Ok(connection.query_row("SELECT id FROM run_jobs WHERE command_id = ? AND idempotency_key = ? UNION ALL SELECT id FROM runs WHERE command_id = ? AND idempotency_key = ? LIMIT 1", params![command_id, key, command_id, key], |row| row.get::<_, String>(0)).optional()?))
    }

    fn reusable_run_id(&self, command: &Value, command_id: &str) -> AppResult<Option<String>> {
        let cwd = required_string_field(command, "cwd", "registered command")?;
        let current = inspect_git(Path::new(&cwd))?;
        let Some(commit_sha) = current.commit_sha.as_deref() else {
            return Ok(None);
        };
        if !is_clean(Path::new(&cwd)) {
            return Ok(None);
        }
        let Some(latest) = self.latest_run(Some(command_id))? else {
            return Ok(None);
        };
        let matches =
            |key: &str, expected: &str| latest.get(key).and_then(Value::as_str) == Some(expected);
        if !matches("command_id", command_id)
            || !matches("cwd", &cwd)
            || !matches("repo_key", &current.repo_key)
            || !matches("repo_path", &current.repo_path)
            || !matches("commit_sha", commit_sha)
            || latest.get("branch").and_then(Value::as_str) != current.branch.as_deref()
        {
            return Ok(None);
        }
        let status = latest.get("status").and_then(Value::as_str).unwrap_or("");
        let has_snapshot = latest
            .get("coverage_ingest")
            .and_then(|value| value.get("snapshot_ids"))
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty());
        if status == "passed" || has_snapshot {
            Ok(latest.get("id").and_then(Value::as_str).map(str::to_owned))
        } else {
            Ok(None)
        }
    }

    /// Executes one queued managed run synchronously.
    pub fn execute_run(&self, run_id: &str) -> AppResult<()> {
        let (slot_lock, slot_cv) = &self.inner.slots;
        let mut active = slot_lock.lock().map_err(lock_error)?;
        while *active >= self.inner.config.run_concurrency
            && !self.inner.closing.load(Ordering::SeqCst)
        {
            active = slot_cv.wait(active).map_err(lock_error)?;
        }
        if self.inner.closing.load(Ordering::SeqCst) {
            drop(active);
            let error = AppError::Runtime("DuckDB store is closing".to_owned());
            self.finalize_failed_job_or_log(run_id, &error, "finalize shutdown run");
            return Err(error);
        }
        *active += 1;
        drop(active);
        let result = self.execute_run_with_slot(run_id);
        let release_result = release_run_slot(slot_lock, slot_cv);
        let result = combine_run_results(result, release_result);
        if let Err(error) = &result {
            self.finalize_failed_job_or_log(run_id, error, "finalize failed run");
        }
        result
    }

    fn execute_run_with_slot(&self, run_id: &str) -> AppResult<()> {
        let job = self
            .job(run_id)?
            .ok_or_else(|| AppError::NotFound(format!("run not found: {run_id}")))?;
        if required_string_field(&job, "status", "queued run")
            .expect("queued run projections always contain status")
            != "queued"
        {
            return Ok(());
        }
        let started = Utc::now();
        let claimed = self.with_connection(|connection| claim_run(connection, run_id, started))?;
        if !claimed {
            return Ok(());
        }
        self.execute_run_with_claimed_job(run_id, &job, started)
    }

    fn execute_run_with_claimed_job(
        &self,
        run_id: &str,
        job: &Value,
        started: DateTime<Utc>,
    ) -> AppResult<()> {
        let command = required_string_field(job, "command", "queued run")?;
        let cwd = required_string_field(job, "cwd", "queued run")?;
        let command_id = required_string_field(job, "command_id", "queued run")?;
        let shell = self.registered_command(&command_id)?;
        let shell = required_string_field(&shell, "shell", "registered command")
            .expect("registered command projections always contain shell");
        let stdout_path = PathBuf::from(required_string_field(job, "stdout_path", "queued run")?);
        let stderr_path = PathBuf::from(required_string_field(job, "stderr_path", "queued run")?);
        let stdout_file = File::create(&stdout_path)?;
        let stderr_file = File::create(&stderr_path)?;
        let mut process = Command::new(&shell);
        process
            .arg("-c")
            .arg(&command)
            .current_dir(&cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        process.process_group(0);
        let mut child = process.spawn()?;
        let stdout_pipe = child.stdout.take();
        let stdout = take_child_stream(&mut child, stdout_pipe, "stdout")
            .expect("managed child stdout is piped");
        let stderr_pipe = child.stderr.take();
        let stderr = take_child_stream(&mut child, stderr_pipe, "stderr")
            .expect("managed child stderr is piped");
        #[rustfmt::skip]
        let stdout_capture = capture_handle_or_cleanup(&mut child, spawn_log_capture(stdout, stdout_file, self.inner.config.run_log_max_bytes, "stdout"))?;
        #[rustfmt::skip]
        let (stdout_capture, stderr_capture) = capture_second_handle_or_cleanup(&mut child, spawn_log_capture(stderr, stderr_file, self.inner.config.run_log_max_bytes, "stderr"), stdout_capture)?;
        let captures = LogCaptureHandles {
            stdout: stdout_capture,
            stderr: stderr_capture,
        };
        let control = Arc::new(Mutex::new(Some(child)));
        if let Err(error) = self
            .inner
            .active_processes
            .lock()
            .map_err(lock_error)
            .map(|mut active| active.insert(run_id.to_owned(), control.clone()))
        {
            return cleanup_unregistered_run(run_id, &control, captures, error);
        }
        let mut resources = ManagedRunGuard::new(
            Arc::clone(&self.inner),
            run_id.to_owned(),
            Arc::clone(&control),
            captures,
        );
        let timeout = timeout_duration(optional_i64_field(job, "timeout_seconds", "queued run")?)?;
        let started_instant = Instant::now();
        let mut exit_code = Option::<i32>::default();
        let mut status = "failed".to_owned();
        let mut finished = false;
        while !finished {
            let mut guard = lock_managed_control(&control)?;
            #[cfg(test)]
            if FORCE_EMPTY_MANAGED_CHILD.swap(false, Ordering::SeqCst) {
                *guard = None;
            }
            let child = required_managed_child(&mut guard)?;
            if let Some(result) = try_wait_managed_child(child)? {
                exit_code = result.code();
                status = if result.success() { "passed" } else { "failed" }.to_owned();
                finished = true;
                continue;
            }
            let cancelled = cancellation_state(self, run_id)?;
            if cancelled {
                terminate_cancelled_child_group(child)?;
                status = "cancelled".to_owned();
            } else if timeout_reached(started_instant, timeout) {
                terminate_timeout_child_group(child)?;
                status = "timeout".to_owned();
            }
            drop(guard);
            if status == "cancelled" || status == "timeout" {
                #[cfg(test)]
                if FORCE_CONTROL_POISON_BEFORE_REAP.swap(false, Ordering::SeqCst) {
                    let control_for_poison = Arc::clone(&control);
                    let _ = thread::spawn(move || {
                        let _guard = control_for_poison.lock().unwrap();
                        panic!("injected managed control poison before reap");
                    })
                    .join();
                }
                let mut guard = lock_managed_control_for_reap(&control)?;
                reap_child(&mut guard)?;
                exit_code = None;
                finished = true;
                continue;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let (stdout_capture, stderr_capture) = resources.finish()?;
        let ended = Utc::now();
        let duration_ms = ended
            .signed_duration_since(started)
            .num_milliseconds()
            .max(0);
        let exit_code = exit_code.map(i64::from);
        let command_row = self.registered_command(&command_id)?;
        let artifacts = self.collect_artifacts(run_id, &command_row, &cwd, status == "passed")?;
        let max_summary_lines = summary_line_limit(job.get("max_summary_lines"))?;
        #[rustfmt::skip]
        let summary = summarize_logs(&stdout_path, &stderr_path, &status, exit_code, duration_ms, max_summary_lines, stdout_capture, stderr_capture)?;
        #[rustfmt::skip]
        let persist_result = self.with_connection(|connection| persist_completed_run(connection, run_id, ended, duration_ms, exit_code, &status, &summary, &artifacts));
        persist_result?;
        self.clear_artifact_baselines(run_id)?;
        let command_id = required_string_field(&command_row, "id", "registered command")
            .expect("registered command projections always contain id");
        self.prune_runs(&command_id)?;
        Ok(())
    }

    fn finalize_failed_job(&self, run_id: &str, error: &AppError) -> AppResult<()> {
        let message = error.to_string();
        self.with_connection_allow_closing(|connection| {
            connection.execute(
                "UPDATE run_jobs SET status = 'failed', ended_at = ?, error = ? WHERE id = ? AND status IN ('queued', 'running')",
                params![Utc::now(), message, run_id],
            )?;
            Ok(())
        })
    }

    fn finalize_failed_job_or_log(&self, run_id: &str, error: &AppError, context: &str) {
        if let Err(recovery_error) = self.finalize_failed_job(run_id, error) {
            eprintln!("coverage-mcp could not {context} {run_id}: {recovery_error}");
        }
    }

    fn job(&self, run_id: &str) -> AppResult<Option<Value>> {
        self.with_connection(|connection| Ok(connection.query_row("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at FROM run_jobs WHERE id = ?", params![run_id], job_from_row).optional()?))
    }

    /// Fetches current or terminal run state without advancing it.
    pub fn run_result(&self, run_id: &str, max_summary_lines: usize) -> AppResult<Value> {
        let job = if let Some(job) = self.job(run_id)? {
            job
        } else {
            let terminal = self.with_connection(|connection| Ok(connection.query_row("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at FROM runs WHERE id = ?", params![run_id], run_from_row).optional()?))?;
            if let Some(value) = terminal {
                return Self::decorate_terminal_run(value);
            }
            return Err(AppError::NotFound(format!("run not found: {run_id}")));
        };
        let status = required_string_field(&job, "status", "queued run")
            .expect("queued run projections always contain status");
        let terminal = !matches!(status.as_str(), "queued" | "running");
        let queue_position = if status == "queued" {
            Some(self.queued_position(run_id)?)
        } else {
            None
        };
        let _ = max_summary_lines;
        Self::decorate_queued_run(job, status, terminal, queue_position)
    }

    fn queued_position(&self, run_id: &str) -> AppResult<i64> {
        #[cfg(test)]
        if FORCE_QUEUE_POSITION_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Runtime(
                "injected queue position failure".to_owned(),
            ));
        }
        self.with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT count(*) FROM run_jobs WHERE status = 'queued' AND (queued_at < (SELECT queued_at FROM run_jobs WHERE id = ?) OR (queued_at = (SELECT queued_at FROM run_jobs WHERE id = ?) AND id <= ?))",
                params![run_id, run_id, run_id],
                queued_position_count,
            )?)
        })
    }

    fn decorate_terminal_run(mut value: Value) -> AppResult<Value> {
        let object = value.as_object_mut().ok_or_else(|| {
            AppError::Runtime("terminal run projection is not an object".to_owned())
        })?;
        object.insert("terminal".to_owned(), json!(true));
        object.insert("poll_after_ms".to_owned(), Value::Null);
        object.insert("queue_position".to_owned(), Value::Null);
        Ok(value)
    }

    fn decorate_queued_run(
        mut result: Value,
        status: String,
        terminal: bool,
        queue_position: Option<i64>,
    ) -> AppResult<Value> {
        let object = result.as_object_mut().ok_or_else(|| {
            AppError::Runtime("queued run projection is not an object".to_owned())
        })?;
        let duration_ms = run_duration_ms(object.get("started_at"), object.get("queued_at"))?;
        let stdout_path = object
            .get("stdout_path")
            .cloned()
            .ok_or_else(|| AppError::Runtime("queued run is missing stdout_path".to_owned()))?;
        let stderr_path = object
            .get("stderr_path")
            .cloned()
            .ok_or_else(|| AppError::Runtime("queued run is missing stderr_path".to_owned()))?;
        let cancellation_requested = object
            .get("cancellation_requested_at")
            .is_some_and(|value| !value.is_null());
        object.insert("terminal".to_owned(), json!(terminal));
        object.insert("exit_code".to_owned(), Value::Null);
        object.insert("duration_ms".to_owned(), json!(duration_ms));
        object.insert("artifact_paths".to_owned(), json!([]));
        object.insert(
            "coverage_ingest".to_owned(),
            json!({"status":"pending","snapshot_ids":[],"configured":0,"ingested":0,"failed":0}),
        );
        object.insert(
            "queue_position".to_owned(),
            queue_position.map_or(Value::Null, |value| json!(value)),
        );
        object.insert(
            "poll_after_ms".to_owned(),
            if terminal { Value::Null } else { json!(1000) },
        );
        object.insert("execution_mode".to_owned(), json!("background"));
        object.insert(
            "cancellation_requested".to_owned(),
            json!(cancellation_requested),
        );
        object.insert("parsed_summary".to_owned(), json!({"status":status,"exit_code":null,"duration_ms":duration_ms,"stdout_line_count":null,"stderr_line_count":null,"counters":{},"excerpts":[],"truncated":false,"stdout_path":stdout_path,"stderr_path":stderr_path,"summary_deferred":true}));
        Ok(result)
    }

    /// Lists queued and running jobs.
    pub fn list_run_queue(&self, limit: usize) -> AppResult<Vec<Value>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at FROM run_jobs WHERE status IN ('queued', 'running') ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, queued_at LIMIT ?")?;
            let rows = statement
                .query_map(params![collection_limit(limit) as i64], job_from_row)
                ?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })
    }

    /// Requests cancellation for a queued or running job.
    pub fn cancel_run(&self, run_id: &str, max_summary_lines: usize) -> AppResult<Value> {
        if let Some(value) = self.with_connection(|connection| {
            Ok(connection
                .query_row(
                    "SELECT status FROM runs WHERE id = ?",
                    params![run_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?)
        })? {
            return Err(AppError::Validation(format!(
                "run is already terminal: {value}"
            )));
        }
        let status = self
            .job(run_id)?
            .and_then(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or_else(|| AppError::NotFound(format!("run not found: {run_id}")))?;
        if !matches!(status.as_str(), "queued" | "running") {
            return Err(AppError::Validation(format!(
                "run is already terminal: {status}"
            )));
        }
        self.with_connection(|connection| {
            if status == "queued" { connection.execute("UPDATE run_jobs SET status = 'cancelled', ended_at = ?, cancellation_requested_at = ?, error = 'Run cancelled before execution.' WHERE id = ?", params![Utc::now(), Utc::now(), run_id])?; } else { connection.execute("UPDATE run_jobs SET cancellation_requested_at = ?, error = 'Cancellation requested.' WHERE id = ?", params![Utc::now(), run_id])?; }
            Ok(())
        })?;
        self.run_result(run_id, max_summary_lines)
    }

    /// Returns the newest terminal run, optionally scoped to a command reference.
    pub fn latest_run(&self, command_ref: Option<&str>) -> AppResult<Option<Value>> {
        let command_id = if let Some(reference) = command_ref {
            let command = self.registered_command(reference)?;
            Some(
                required_command_id(&command, reference)
                    .expect("registered command projections always contain id"),
            )
        } else {
            None
        };
        self.with_connection(|connection| {
            if let Some(command_id) = command_id {
                connection.query_row("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at FROM runs WHERE command_id = ? ORDER BY started_at DESC LIMIT 1", params![command_id], run_from_row).optional().map_err(AppError::from)
            } else {
                connection.query_row("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at FROM runs ORDER BY started_at DESC LIMIT 1", [], run_from_row).optional().map_err(AppError::from)
            }
        })
    }

    /// Searches retained stdout/stderr literally with OR semantics.
    #[allow(clippy::too_many_arguments)]
    pub fn search_run_logs(
        &self,
        run_id: &str,
        queries: &[String],
        stream: &str,
        context_lines: usize,
        max_matches: usize,
        case_sensitive: bool,
        max_words: usize,
    ) -> AppResult<Value> {
        if queries.is_empty() || queries.len() > 20 {
            return Err(AppError::Validation(
                "query must contain between 1 and 20 terms".to_owned(),
            ));
        }
        let result = self.run_result(run_id, 80)?;
        let mut matches = Vec::new();
        for (stream_name, path_key) in [("stdout", "stdout_path"), ("stderr", "stderr_path")] {
            if stream != "both" && stream != stream_name {
                continue;
            }
            let lines = read_log_lines(&result, run_id, stream_name, path_key)?;
            append_log_matches(
                &mut matches,
                stream_name,
                &lines,
                queries,
                context_lines,
                max_matches,
                case_sensitive,
            );
            if matches.len() >= max_matches {
                break;
            }
        }
        let query_value = if queries.len() == 1 {
            json!(queries[0])
        } else {
            json!(queries)
        };
        Ok(
            json!({"run_id":run_id,"query":query_value,"queries":queries,"match_count":matches.len(),"returned_line_count":matches.iter().map(|item| item.get("context").and_then(Value::as_array).map_or(1, Vec::len)).sum::<usize>(),"matches":matches,"max_words":max_words,"case_sensitive":case_sensitive}),
        )
    }

    /// Finds the newest retained artifact of a kind.
    pub fn latest_artifact(
        &self,
        kind: &str,
        command_ref: Option<&str>,
    ) -> AppResult<Option<Value>> {
        let command_id = if let Some(reference) = command_ref {
            Some(required_string_field(
                &self.registered_command(reference)?,
                "id",
                "registered command",
            )?)
        } else {
            None
        };
        self.with_connection(|connection| {
            let row = if let Some(command_id) = command_id { connection.query_row("SELECT a.run_id, a.kind, a.path, a.exists, a.size_bytes, a.coverage_format, a.suite, a.modified_by_run, a.ingest_status, a.snapshot_id, a.ingest_error, r.command_id, r.command_name, r.repo_key, r.repo_path, r.started_at, r.ended_at, r.status, r.exit_code FROM run_artifacts a JOIN runs r ON r.id = a.run_id WHERE a.kind = ? AND r.command_id = ? ORDER BY r.started_at DESC LIMIT 1", params![kind, command_id], artifact_from_row).optional()? } else { connection.query_row("SELECT a.run_id, a.kind, a.path, a.exists, a.size_bytes, a.coverage_format, a.suite, a.modified_by_run, a.ingest_status, a.snapshot_id, a.ingest_error, r.command_id, r.command_name, r.repo_key, r.repo_path, r.started_at, r.ended_at, r.status, r.exit_code FROM run_artifacts a JOIN runs r ON r.id = a.run_id WHERE a.kind = ? ORDER BY r.started_at DESC LIMIT 1", params![kind], artifact_from_row).optional()? }; Ok(row)
        })
    }

    fn artifact_baseline(
        &self,
        run_id: &str,
        kind: &str,
    ) -> AppResult<Option<ArtifactFingerprint>> {
        self.with_read_connection(|connection| {
            connection
                .query_row(
                    "SELECT exists, size_bytes, modified_ns, sha256 FROM run_artifact_baselines WHERE run_id = ? AND kind = ?",
                    params![run_id, kind],
                    |row| {
                        Ok(ArtifactFingerprint {
                            exists: row.get(0)?,
                            size_bytes: row.get(1)?,
                            modified_ns: row.get(2)?,
                            sha256: row.get(3)?,
                        })
                    },
                )
                .optional()
                .map_err(AppError::from)
        })
    }

    fn clear_artifact_baselines(&self, run_id: &str) -> AppResult<()> {
        #[cfg(test)]
        if FORCE_CLEAR_ARTIFACT_BASELINES_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Runtime(
                "injected artifact baseline cleanup failure".to_owned(),
            ));
        }
        self.with_connection(|connection| {
            connection.execute(
                "DELETE FROM run_artifact_baselines WHERE run_id = ?",
                params![run_id],
            )?;
            Ok(())
        })
    }

    fn collect_artifacts(
        &self,
        run_id: &str,
        command: &Value,
        cwd: &str,
        eligible: bool,
    ) -> AppResult<Vec<Value>> {
        let specs = required_array_field(command, "artifact_specs", "registered command")?.to_vec();
        let command_repo_path = required_string_field(command, "repo_path", "registered command")
            .expect("registered command projections always contain repo_path");
        let command_name = required_string_field(command, "name", "registered command")
            .expect("registered command projections always contain name");
        let mut artifacts = Vec::new();
        for spec in specs {
            let kind = required_string_field(&spec, "kind", "artifact specification")?;
            let raw_path = required_string_field(&spec, "path", "artifact specification")?;
            let path = resolve_artifact_path(cwd, &raw_path);
            let metadata = match fs::metadata(&path) {
                Ok(metadata) => Some(metadata),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let coverage_format = spec.get("coverage_format").and_then(Value::as_str);
            let required = required_bool_field(&spec, "required", "artifact specification")?;
            let suite = spec.get("suite").and_then(Value::as_str);
            let mut artifact = Map::new();
            artifact.insert("kind".to_owned(), json!(kind));
            artifact.insert("path".to_owned(), json!(path.to_string_lossy()));
            artifact.insert("exists".to_owned(), json!(metadata.is_some()));
            artifact.insert(
                "size_bytes".to_owned(),
                json!(metadata.as_ref().map(|value| value.len())),
            );
            artifact.insert("required".to_owned(), json!(required));
            artifact.insert("coverage_format".to_owned(), json!(coverage_format));
            artifact.insert("suite".to_owned(), json!(suite));
            let hash_contents = coverage_format.is_some();
            let fingerprint = artifact_fingerprint(&path, hash_contents)?;
            let baseline = self.artifact_baseline(run_id, &kind)?;
            let modified_by_run = baseline
                .as_ref()
                .is_some_and(|before| fingerprint.changed_from(before));
            artifact.insert("modified_by_run".to_owned(), json!(modified_by_run));
            artifact.insert("ingest_status".to_owned(), Value::Null);
            artifact.insert("snapshot_id".to_owned(), Value::Null);
            artifact.insert("ingest_error".to_owned(), Value::Null);
            artifact.insert(
                "fingerprint".to_owned(),
                json!({
                    "exists": fingerprint.exists,
                    "size_bytes": fingerprint.size_bytes,
                    "modified_ns": fingerprint.modified_ns,
                    "sha256": fingerprint.sha256,
                }),
            );
            if let Some(format) = coverage_format {
                let status: (String, String) = if !eligible {
                    (
                        "skipped_run_status".to_owned(),
                        "run did not complete with a process exit code".to_owned(),
                    )
                } else if metadata.is_none() {
                    (
                        "missing".to_owned(),
                        "coverage artifact does not exist".to_owned(),
                    )
                } else if baseline.is_none() {
                    (
                        "skipped_stale".to_owned(),
                        "no pre-run artifact fingerprint was recorded".to_owned(),
                    )
                } else if !modified_by_run {
                    (
                        "skipped_stale".to_owned(),
                        "coverage artifact was not created or modified by this run".to_owned(),
                    )
                } else {
                    match self.ingest_report(
                        &path,
                        format,
                        Some(Path::new(&command_repo_path)),
                        command.get("branch").and_then(Value::as_str),
                        command.get("commit_sha").and_then(Value::as_str),
                        None,
                        suite.unwrap_or(&command_name),
                    ) {
                        Ok(snapshot) => {
                            artifact.insert(
                                "snapshot_id".to_owned(),
                                required_field(&snapshot, "id", "ingested snapshot")
                                    .expect("ingested snapshot projections always contain id")
                                    .clone(),
                            );
                            ("ingested".to_owned(), String::new())
                        }
                        Err(error) => ("failed".to_owned(), error.to_string()),
                    }
                };
                artifact.insert("ingest_status".to_owned(), json!(status.0));
                if !status.1.is_empty() {
                    artifact.insert("ingest_error".to_owned(), json!(status.1));
                }
            }
            artifacts.push(Value::Object(artifact));
        }
        Ok(artifacts)
    }

    fn prune_runs(&self, command_id: &str) -> AppResult<()> {
        #[cfg(test)]
        if FORCE_PRUNE_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Runtime("injected prune failure".to_owned()));
        }
        let ids = self.with_connection(|connection| {
            query_pruned_run_ids(
                connection,
                command_id,
                self.inner.config.run_retention as i64,
            )
        })?;
        if ids.is_empty() {
            return Ok(());
        }
        self.with_connection(|connection| {
            for id in &ids {
                connection.execute("DELETE FROM run_artifacts WHERE run_id = ?", params![id])?;
                connection.execute("DELETE FROM runs WHERE id = ?", params![id])?;
            }
            Ok(())
        })?;
        for id in ids {
            remove_run_directory(&self.inner.run_dir, &id)?;
        }
        Ok(())
    }

    /// Parses and stores one immutable coverage snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn ingest_report(
        &self,
        report_path: &Path,
        format: &str,
        repo_path: Option<&Path>,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        suite: &str,
    ) -> AppResult<Value> {
        let selected_path = repo_path.unwrap_or(report_path.parent().unwrap_or(Path::new(".")));
        let git = self.ensure_project(selected_path)?;
        let report = parse_coverage_report(report_path, format, Some(&git.repo_path))?;
        let selected_branch = branch.map(str::to_owned).or(git.branch.clone());
        let selected_commit = commit_sha.map(str::to_owned).or(git.commit_sha.clone());
        let snapshot_id = self.store_report(
            &report,
            &git,
            selected_branch.as_deref(),
            selected_commit.as_deref(),
            base_ref,
            suite,
        )?;
        self.snapshot(&snapshot_id)
    }

    fn store_report(
        &self,
        report: &CoverageReport,
        git: &GitInfo,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        suite: &str,
    ) -> AppResult<String> {
        let suite = suite.trim();
        if suite.is_empty() {
            return Err(AppError::Validation("suite must not be blank".to_owned()));
        }
        let snapshot_id = Uuid::new_v4().to_string();
        let created_at = Utc::now();
        self.with_connection_mut(|connection| {
            connection
                .execute_batch("BEGIN TRANSACTION")
                .expect("pooled report connections are not already transactional");
            let result = (|| {
                let warnings = serde_json::to_string(&report.warnings)
                    .expect("coverage warnings serialization is infallible");
                let metadata = serde_json::to_string(&report.metadata)
                    .expect("coverage metadata serialization is infallible");
                let snapshot_values = params![
                    snapshot_id,
                    created_at,
                    created_at,
                    git.repo_path,
                    git.repo_key,
                    branch,
                    commit_sha,
                    base_ref,
                    suite,
                    report.format,
                    report.report_path,
                    warnings,
                    metadata,
                    report.total_lines(),
                    report.covered_lines(),
                    report.total_branches(),
                    report.covered_branches(),
                    report.total_functions(),
                    report.covered_functions(),
                    report.total_regions(),
                    report.covered_regions(),
                    report.line_rate(),
                    report.branch_rate(),
                    report.function_rate(),
                    report.region_rate()
                ];
                connection.execute(INSERT_SNAPSHOT_SQL, snapshot_values)?;
                for file in &report.files {
                    let raw_metrics = serde_json::to_string(&file.raw_metrics)
                        .expect("file metrics serialization is infallible");
                    let file_values = params![
                        snapshot_id,
                        file.file_path,
                        file.total_lines,
                        file.covered_lines,
                        file.total_branches,
                        file.covered_branches,
                        file.total_functions,
                        file.covered_functions,
                        file.total_regions,
                        file.covered_regions,
                        file.line_rate(),
                        file.branch_rate(),
                        file.function_rate(),
                        file.region_rate(),
                        raw_metrics
                    ];
                    connection.execute(INSERT_FILE_SQL, file_values)?;
                }
                for line in &report.lines {
                    let details = serde_json::to_string(&line.details)
                        .expect("line details serialization is infallible");
                    let line_values = params![
                        snapshot_id,
                        line.file_path,
                        line.line_number,
                        line.hits,
                        line.covered,
                        line.count_line,
                        line.total_branches,
                        line.covered_branches,
                        line.total_functions,
                        line.covered_functions,
                        details
                    ];
                    connection.execute(INSERT_LINE_SQL, line_values)?;
                }
                Ok::<(), AppError>(())
            })();
            finish_transaction(connection, result)
        })?;
        Ok(snapshot_id)
    }

    /// Reads one snapshot with full provenance fields.
    pub fn snapshot(&self, snapshot_id: &str) -> AppResult<Value> {
        self.with_connection(|connection| {
            let sql = format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE id = ?");
            connection
                .query_row(&sql, params![snapshot_id], snapshot_from_row)
                .optional()?
                .ok_or_else(|| AppError::NotFound(format!("snapshot not found: {snapshot_id}")))
        })
    }

    /// Finds the newest snapshot recorded at one compatible commit.
    pub fn snapshot_for_commit(
        &self,
        repo_path: &str,
        branch: Option<&str>,
        suite: &str,
        commit_sha: &str,
    ) -> AppResult<Option<Value>> {
        let matches_commit = |snapshots: Vec<Value>| {
            snapshots.into_iter().find(|snapshot| {
                snapshot.get("commit_sha").and_then(Value::as_str) == Some(commit_sha)
            })
        };
        let scoped =
            self.list_snapshots(Some(repo_path), branch, Some(suite), MAX_COLLECTION_RECORDS)?;
        if let Some(snapshot) = matches_commit(scoped) {
            return Ok(Some(snapshot));
        }
        if branch.is_some() {
            return Ok(matches_commit(self.list_snapshots(
                Some(repo_path),
                None,
                Some(suite),
                MAX_COLLECTION_RECORDS,
            )?));
        }
        Ok(None)
    }

    /// Lists snapshots for the selected project.
    pub fn list_snapshots(
        &self,
        repo_path: Option<&str>,
        branch: Option<&str>,
        suite: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let project = self.project()?;
        let limit = collection_limit(limit);
        self.with_connection(|connection| {
            let mut sql = format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE repo_key = ?");
            let mut args = vec![project.repo_key.clone()];
            if let Some(repo_path) = repo_path {
                sql.push_str(" AND repo_path = ?");
                args.push(repo_path.to_owned());
            }
            if let Some(branch) = branch {
                sql.push_str(" AND branch = ?");
                args.push(branch.to_owned());
            }
            if let Some(suite) = suite {
                sql.push_str(" AND suite = ?");
                args.push(suite.to_owned());
            }
            sql.push_str(&format!(" ORDER BY created_at DESC LIMIT {limit}"));
            let mut statement = connection.prepare(&sql)?;
            let rows = statement.query_map(duckdb::params_from_iter(args), snapshot_from_row)?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })
    }

    /// Finds the latest matching snapshot.
    pub fn latest_snapshot(
        &self,
        repo_path: Option<&str>,
        branch: Option<&str>,
        suite: Option<&str>,
    ) -> AppResult<Option<Value>> {
        Ok(self
            .list_snapshots(repo_path, branch, suite, 1)?
            .into_iter()
            .next())
    }

    /// Finds the snapshot immediately before one snapshot in the same suite.
    pub fn previous_snapshot(&self, snapshot_id: &str) -> AppResult<Option<Value>> {
        let current = self.snapshot(snapshot_id)?;
        let branch = current.get("branch").and_then(Value::as_str);
        let suite = required_string_field(&current, "suite", "snapshot")
            .expect("snapshot projections always contain suite");
        let repo_path = required_string_field(&current, "repo_path", "snapshot")
            .expect("snapshot projections always contain repo_path");
        let branch_value = required_field(&current, "branch", "snapshot")
            .expect("snapshot projections always contain branch")
            .clone();
        let mut snapshots = Vec::new();
        for snapshot in self.list_snapshots(
            Some(&repo_path),
            branch,
            Some(&suite),
            MAX_COLLECTION_RECORDS,
        )? {
            if required_field(&snapshot, "branch", "snapshot")
                .expect("snapshot projections always contain branch")
                == &branch_value
            {
                snapshots.push(snapshot);
            }
        }
        let position = snapshots
            .iter()
            .position(|snapshot| snapshot.get("id").and_then(Value::as_str) == Some(snapshot_id));
        Ok(position.and_then(|position| snapshots.get(position + 1).cloned()))
    }

    /// Returns file summaries, transparently restoring compacted detail payloads.
    pub fn files(&self, snapshot_id: &str, limit: usize) -> AppResult<Vec<Value>> {
        self.snapshot(snapshot_id)?;
        let rows = self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? ORDER BY file_path LIMIT ?")?;
            let rows = statement.query_map(
                params![snapshot_id, collection_limit(limit) as i64],
                file_from_row,
            )?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })?;
        if !rows.is_empty() {
            return Ok(rows);
        }
        let Some(payload) = self.compacted_detail(snapshot_id)? else {
            return Ok(Vec::new());
        };
        let files = payload
            .get("files")
            .and_then(Value::as_array)
            .ok_or_else(|| AppError::Runtime("compacted coverage is missing files".to_owned()))?;
        for file in files {
            required_string_field(file, "file_path", "compacted file")
                .expect("compacted file projections always contain file_path");
        }
        Ok(files.clone())
    }

    /// Returns one file summary.
    pub fn file_coverage(&self, snapshot_id: &str, file_path: &str) -> AppResult<Value> {
        let file = self.with_connection(|connection| Ok(connection.query_row("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? AND file_path = ?", params![snapshot_id, file_path], file_from_row).optional()?))?;
        if let Some(file) = file {
            return Ok(file);
        }
        if let Some(payload) = self.compacted_detail(snapshot_id)? {
            let files = payload
                .get("files")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AppError::Runtime("compacted coverage is missing files".to_owned())
                })?;
            for file in files {
                let path = required_string_field(file, "file_path", "compacted file")
                    .expect("compacted file projections always contain file_path");
                if path == file_path {
                    return Ok(file.clone());
                }
            }
        }
        Err(AppError::NotFound(format!("file not found: {file_path}")))
    }

    /// Returns exact line records for one file.
    pub fn lines(&self, snapshot_id: &str, file_path: &str, limit: usize) -> AppResult<Vec<Value>> {
        let rows = self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT line_number, hits, covered, count_line, total_branches, covered_branches, total_functions, covered_functions, details FROM lines WHERE snapshot_id = ? AND file_path = ? ORDER BY line_number LIMIT ?")?;
            let rows = statement.query_map(
                params![snapshot_id, file_path, collection_limit(limit) as i64],
                line_from_row,
            )?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })?;
        if !rows.is_empty() {
            return Ok(rows);
        }
        let Some(payload) = self.compacted_detail(snapshot_id)? else {
            return Ok(Vec::new());
        };
        let lines = payload
            .get("lines")
            .and_then(Value::as_array)
            .ok_or_else(|| AppError::Runtime("compacted coverage is missing lines".to_owned()))?;
        let mut selected = Vec::new();
        for line in lines {
            let path = required_string_field(line, "file_path", "compacted line")
                .expect("compacted line projections always contain file_path");
            required_i64_field(line, "line_number", "compacted line")
                .expect("compacted line projections always contain line_number");
            if path == file_path {
                selected.push(line.clone());
            }
        }
        Ok(selected)
    }

    /// Returns exact line records in normalized inclusive ranges.
    pub fn lines_in_ranges(
        &self,
        snapshot_id: &str,
        file_path: &str,
        ranges: &[LineRange],
    ) -> AppResult<Value> {
        let ranges = normalize_line_ranges(ranges)?;
        let all = self.lines(snapshot_id, file_path, MAX_COLLECTION_RECORDS)?;
        let mut selected = Vec::new();
        for line in all {
            let number = required_i64_field(&line, "line_number", "coverage line")
                .expect("stored line projections always contain line_number");
            if ranges
                .iter()
                .any(|(start, end)| number >= *start && number <= *end)
            {
                selected.push(line);
            }
        }
        Ok(
            json!({"lines": selected, "requested_ranges": ranges.iter().map(|(start,end)| json!({"start":start,"end":end})).collect::<Vec<_>>(), "line_count": selected.len()}),
        )
    }

    /// Returns contiguous uncovered line ranges.
    pub fn file_gaps(
        &self,
        snapshot_id: &str,
        file_path: &str,
        max_ranges: usize,
    ) -> AppResult<Value> {
        let lines = self.lines(snapshot_id, file_path, MAX_COLLECTION_RECORDS)?;
        let mut gaps = Vec::new();
        let mut current: Option<(i64, i64)> = None;
        let mut uncovered_line_count = 0usize;
        for line in &lines {
            let count_line = required_bool_field(line, "count_line", "coverage line")
                .expect("stored line projections always contain count_line");
            let number = required_i64_field(line, "line_number", "coverage line")
                .expect("stored line projections always contain line_number");
            let uncovered = !required_bool_field(line, "covered", "coverage line")
                .expect("stored line projections always contain covered");
            if !count_line {
                continue;
            }
            if uncovered {
                uncovered_line_count = uncovered_line_count.saturating_add(1);
                if let Some((start, end)) = current.as_mut() {
                    if number <= end.saturating_add(1) {
                        *end = number;
                        continue;
                    }
                    gaps.push(json!({"start":start,"end":end}));
                }
                current = Some((number, number));
            } else if let Some((start, end)) = current.take() {
                gaps.push(json!({"start":start,"end":end}));
            }
        }
        if let Some((start, end)) = current {
            gaps.push(json!({"start":start,"end":end}));
        }
        let truncated = gaps.len() > max_ranges;
        gaps.truncate(max_ranges);
        let returned_range_count = gaps.len();
        Ok(
            json!({"file_path": file_path, "uncovered_line_count": uncovered_line_count, "ranges": gaps, "returned_range_count": returned_range_count, "truncated": truncated}),
        )
    }

    /// Returns a file/line history across compatible snapshots.
    pub fn line_history(
        &self,
        file_path: &str,
        line_number: i64,
        branch: Option<&str>,
        suite: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let project = self.project()?;
        let snapshots = self.list_snapshots(None, branch, suite, collection_limit(limit))?;
        let mut values = Vec::new();
        for snapshot in snapshots.into_iter().rev() {
            let id = required_string_field(&snapshot, "id", "snapshot")
                .expect("stored snapshot projections always contain id");
            if let Some(line) = self
                .lines(&id, file_path, MAX_COLLECTION_RECORDS)?
                .into_iter()
                .find(|line| line.get("line_number").and_then(Value::as_i64) == Some(line_number))
            {
                let mut point = Map::new();
                point.insert("snapshot_id".to_owned(), json!(id));
                point.insert(
                    "created_at".to_owned(),
                    required_field(&snapshot, "created_at", "snapshot")
                        .expect("stored snapshot projections always contain created_at")
                        .clone(),
                );
                point.insert(
                    "branch".to_owned(),
                    required_field(&snapshot, "branch", "snapshot")
                        .expect("stored snapshot projections always contain branch")
                        .clone(),
                );
                point.insert(
                    "commit_sha".to_owned(),
                    required_field(&snapshot, "commit_sha", "snapshot")
                        .expect("stored snapshot projections always contain commit_sha")
                        .clone(),
                );
                let snapshot_suite = required_string_field(&snapshot, "suite", "snapshot")
                    .expect("stored snapshot projections always contain suite");
                let suite_value = suite.unwrap_or(&snapshot_suite);
                point.insert("suite".to_owned(), json!(suite_value));
                point.insert("file_path".to_owned(), json!(file_path));
                point.insert("line_number".to_owned(), json!(line_number));
                for key in ["hits", "covered", "total_branches", "covered_branches"] {
                    point.insert(
                        key.to_owned(),
                        required_field(&line, key, "coverage line")
                            .expect("stored line projections contain requested metric")
                            .clone(),
                    );
                }
                values.push(Value::Object(point));
            }
        }
        let _ = project;
        Ok(values)
    }

    /// Reads a bounded repository-relative source range.
    pub fn source_lines(
        &self,
        snapshot_id: &str,
        file_path: &str,
        start: i64,
        end: i64,
    ) -> AppResult<Vec<Value>> {
        if start < 1 {
            return Err(AppError::Validation(
                "start must be a positive line number".to_owned(),
            ));
        }
        if end < start {
            return Err(AppError::Validation(
                "end must be greater than or equal to start".to_owned(),
            ));
        }
        let line_count = end - start + 1;
        if line_count > 200 {
            return Err(AppError::Validation(
                "source ranges may contain at most 200 lines".to_owned(),
            ));
        }
        let snapshot = self.snapshot(snapshot_id)?;
        let root = PathBuf::from(
            required_string_field(&snapshot, "repo_path", "snapshot")
                .expect("snapshot projections always contain repo_path"),
        )
        .canonicalize()?;
        let committed_source = snapshot
            .get("commit_sha")
            .and_then(Value::as_str)
            .map(|commit_sha| read_file_at_commit(&root.to_string_lossy(), commit_sha, file_path))
            .transpose()?
            .flatten();
        if let Some(contents) = committed_source {
            return Ok(source_lines_from_text(&contents, start, end));
        }
        let source = root.join(file_path).canonicalize()?;
        if !source.starts_with(&root) {
            return Err(AppError::Validation(
                "file_path escapes repository root".to_owned(),
            ));
        }
        let file = match File::open(&source) {
            Ok(file) => file,
            Err(_) => {
                return Err(AppError::NotFound(format!("file not found: {file_path}")));
            }
        };
        let mut text = String::new();
        let mut file = file;
        file.read_to_string(&mut text)?;
        Ok(source_lines_from_text(&text, start, end))
    }

    /// Identifies whether source evidence was resolved at the measured commit
    /// or had to fall back to the current checkout.
    pub fn source_resolution(&self, snapshot_id: &str, file_path: &str) -> AppResult<&'static str> {
        let snapshot = self.snapshot(snapshot_id)?;
        let root = PathBuf::from(
            required_string_field(&snapshot, "repo_path", "snapshot")
                .expect("snapshot projections always contain repo_path"),
        )
        .canonicalize()?;
        if let Some(commit_sha) = snapshot.get("commit_sha").and_then(Value::as_str) {
            if read_file_at_commit(&root.to_string_lossy(), commit_sha, file_path)?.is_some() {
                return Ok("snapshot_commit");
            }
            return Ok(if root.join(file_path).canonicalize().is_ok() {
                "current_checkout_fallback"
            } else {
                "unavailable"
            });
        }
        Ok(if root.join(file_path).canonicalize().is_ok() {
            "current_checkout"
        } else {
            "unavailable"
        })
    }

    fn compacted_detail(&self, snapshot_id: &str) -> AppResult<Option<Value>> {
        self.with_connection(|connection| {
            let payload: Option<Vec<u8>> = connection
                .query_row(
                    "SELECT payload FROM coverage_compacted_payloads WHERE snapshot_id = ?",
                    params![snapshot_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(payload) = payload else {
                return Ok(None);
            };
            let decoded = zstd::decode_all(payload.as_slice()).map_err(|error| {
                AppError::Runtime(format!(
                    "compacted coverage payload could not be decoded: {error}"
                ))
            })?;
            Ok(Some(serde_json::from_slice(&decoded)?))
        })
    }
}

fn source_lines_from_text(text: &str, start: i64, end: i64) -> Vec<Value> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let number = index as i64 + 1;
            (number >= start && number <= end)
                .then(|| json!({"line_number": number, "text": line.to_owned()}))
        })
        .collect()
}

fn run_compaction_maintenance(store: &CoverageStore, settings: &ProjectSettings) {
    match maintenance_due(settings) {
        Ok(true) => {
            if let Err(error) = store.compact_now() {
                eprintln!("coverage-mcp background compaction failed: {error}");
            }
        }
        Ok(false) => {}
        Err(error) => {
            eprintln!("coverage-mcp compaction worker rejected invalid maintenance state: {error}")
        }
    }
}

/// Inclusive line range used by coverage projections.
pub type LineRange = (i64, i64);

const SNAPSHOT_COLUMNS: &str = "id, created_at, repo_path, repo_key, branch, commit_sha, base_ref, suite, format, report_path, warnings, metadata, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate";

fn bool_column(row: &Row<'_>) -> duckdb::Result<bool> {
    row.get::<_, bool>(0)
}

fn line_rows(connection: &Connection, snapshot_id: &str) -> AppResult<Vec<Value>> {
    let mut statement = connection.prepare("SELECT file_path, line_number, hits, covered, count_line, total_branches, covered_branches, total_functions, covered_functions, details FROM lines WHERE snapshot_id = ? ORDER BY file_path, line_number")?;
    let rows = statement
        .query_map(params![snapshot_id], line_from_row_with_file)
        .expect("line projection has the initialized schema");
    let mut values = Vec::new();
    for row in rows {
        values.push(row.expect("line rows have the initialized schema"));
    }
    Ok(values)
}

fn snapshot_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let mut value = Map::new();
    value.insert("id".to_owned(), json!(row.get::<_, String>(0)?));
    value.insert(
        "created_at".to_owned(),
        json!(timestamp_string(
            row.get_ref(1).expect("snapshot projection has created_at")
        )),
    );
    value.insert("repo_path".to_owned(), json!(row.get::<_, String>(2)?));
    value.insert("repo_key".to_owned(), json!(row.get::<_, String>(3)?));
    value.insert("branch".to_owned(), json!(row.get::<_, Option<String>>(4)?));
    value.insert(
        "commit_sha".to_owned(),
        json!(row.get::<_, Option<String>>(5)?),
    );
    value.insert(
        "base_ref".to_owned(),
        json!(row.get::<_, Option<String>>(6)?),
    );
    value.insert("suite".to_owned(), json!(row.get::<_, String>(7)?));
    value.insert("format".to_owned(), json!(row.get::<_, String>(8)?));
    value.insert("report_path".to_owned(), json!(row.get::<_, String>(9)?));
    value.insert(
        "warnings".to_owned(),
        json_string(row.get::<_, String>(10)?),
    );
    value.insert(
        "metadata".to_owned(),
        json_string(row.get::<_, String>(11)?),
    );
    for (index, key) in [
        (12, "total_lines"),
        (13, "covered_lines"),
        (14, "total_branches"),
        (15, "covered_branches"),
        (16, "total_functions"),
        (17, "covered_functions"),
        (18, "total_regions"),
        (19, "covered_regions"),
    ] {
        value.insert(key.to_owned(), json!(row.get::<_, i64>(index)?));
    }
    for (index, key) in [
        (20, "line_rate"),
        (21, "branch_rate"),
        (22, "function_rate"),
        (23, "region_rate"),
    ] {
        value.insert(key.to_owned(), json!(row.get::<_, Option<f64>>(index)?));
    }
    Ok(Value::Object(value))
}

fn file_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let mut value = Map::new();
    value.insert("file_path".to_owned(), json!(row.get::<_, String>(0)?));
    for (index, key) in [
        (1, "total_lines"),
        (2, "covered_lines"),
        (3, "total_branches"),
        (4, "covered_branches"),
        (5, "total_functions"),
        (6, "covered_functions"),
        (7, "total_regions"),
        (8, "covered_regions"),
    ] {
        value.insert(key.to_owned(), json!(row.get::<_, i64>(index)?));
    }
    for (index, key) in [
        (9, "line_rate"),
        (10, "branch_rate"),
        (11, "function_rate"),
        (12, "region_rate"),
    ] {
        value.insert(key.to_owned(), json!(row.get::<_, Option<f64>>(index)?));
    }
    value.insert(
        "raw_metrics".to_owned(),
        json_string(row.get::<_, String>(13)?),
    );
    Ok(Value::Object(value))
}

fn line_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let mut value = Map::new();
    value.insert("line_number".to_owned(), json!(row.get::<_, i64>(0)?));
    value.insert("hits".to_owned(), json!(row.get::<_, i64>(1)?));
    value.insert("covered".to_owned(), json!(row.get::<_, bool>(2)?));
    value.insert("count_line".to_owned(), json!(row.get::<_, bool>(3)?));
    for (index, key) in [
        (4, "total_branches"),
        (5, "covered_branches"),
        (6, "total_functions"),
        (7, "covered_functions"),
    ] {
        value.insert(key.to_owned(), json!(row.get::<_, i64>(index)?));
    }
    value.insert("details".to_owned(), json_string(row.get::<_, String>(8)?));
    Ok(Value::Object(value))
}

fn timestamp_string(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        ValueRef::Timestamp(unit, raw) => {
            match DateTime::<Utc>::from_timestamp_micros(unit.to_micros(raw)) {
                Some(date) => date.to_rfc3339(),
                None => raw.to_string(),
            }
        }
        ValueRef::Null => String::new(),
        other => format!("{other:?}"),
    }
}

fn now_string() -> String {
    Utc::now().to_rfc3339()
}

fn optional_timestamp(value: ValueRef<'_>) -> Option<String> {
    if matches!(value, ValueRef::Null) {
        None
    } else {
        Some(timestamp_string(value))
    }
}

fn json_string(value: String) -> Value {
    match serde_json::from_str(&value) {
        Ok(value) => value,
        Err(_) => Value::String(value),
    }
}

fn required_field<'a>(value: &'a Value, key: &str, context: &str) -> AppResult<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| AppError::Runtime(format!("{context} is missing required field '{key}'")))
}

fn required_object_mut<'a>(
    value: &'a mut Value,
    context: &str,
) -> AppResult<&'a mut Map<String, Value>> {
    value
        .as_object_mut()
        .ok_or_else(|| AppError::Runtime(format!("{context} must be an object")))
}

fn required_string_field(value: &Value, key: &str, context: &str) -> AppResult<String> {
    required_field(value, key, context)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::Runtime(format!(
                "{context} field '{key}' must be a non-empty string"
            ))
        })
}

fn required_i64_field(value: &Value, key: &str, context: &str) -> AppResult<i64> {
    required_field(value, key, context)?
        .as_i64()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be an integer")))
}

fn required_bool_field(value: &Value, key: &str, context: &str) -> AppResult<bool> {
    required_field(value, key, context)?
        .as_bool()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be a boolean")))
}

fn required_array_field<'a>(
    value: &'a Value,
    key: &str,
    context: &str,
) -> AppResult<&'a Vec<Value>> {
    required_field(value, key, context)?
        .as_array()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be an array")))
}

fn optional_i64_field(value: &Value, key: &str, context: &str) -> AppResult<Option<i64>> {
    let field = required_field(value, key, context)?;
    if field.is_null() {
        Ok(None)
    } else {
        field.as_i64().map(Some).ok_or_else(|| {
            AppError::Runtime(format!(
                "{context} field '{key}' must be an integer or null"
            ))
        })
    }
}

fn uncovered_metric(total: i64, covered: i64, metric: &str) -> AppResult<i64> {
    if total < 0 || covered < 0 || covered > total {
        return Err(AppError::Runtime(format!(
            "coverage {metric} metrics are inconsistent: total={total}, covered={covered}"
        )));
    }
    Ok(total - covered)
}

fn required_command_id(value: &Value, reference: &str) -> AppResult<String> {
    match value.get("id").and_then(Value::as_str) {
        Some(value) => Ok(value.to_owned()),
        None => Err(AppError::NotFound(format!(
            "registered command not found: {reference}"
        ))),
    }
}

fn compress_coverage_payload(reader: &mut dyn Read) -> AppResult<Vec<u8>> {
    match zstd::stream::encode_all(reader, 3) {
        Ok(payload) => Ok(payload),
        Err(error) => Err(AppError::Runtime(format!(
            "coverage detail compression failed: {error}"
        ))),
    }
}

fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex_prefix(&digest, 8)
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> AppError {
    AppError::Runtime("coverage store lock was poisoned".to_owned())
}

fn claim_run(connection: &Connection, run_id: &str, started: DateTime<Utc>) -> AppResult<bool> {
    #[cfg(test)]
    if FORCE_CLAIM_FALSE.swap(false, Ordering::SeqCst) {
        return Ok(false);
    }
    let changed = match connection.execute(
        "UPDATE run_jobs SET status = 'running', started_at = ?, error = '' WHERE id = ? AND status = 'queued'",
        params![started, run_id],
    ) {
        Ok(changed) => changed,
        Err(error) => return Err(AppError::from(error)),
    };
    Ok(changed == 1)
}

fn record_first_process_error(first_error: &mut Option<AppError>, result: AppResult<()>) {
    if let Err(error) = result {
        if first_error.is_none() {
            *first_error = Some(error);
        }
    }
}

fn cancellation_state(store: &CoverageStore, run_id: &str) -> AppResult<bool> {
    #[cfg(test)]
    if FORCE_CANCELLATION_STATE.swap(false, Ordering::SeqCst) {
        return Ok(true);
    }
    #[cfg(test)]
    if FORCE_CANCELLATION_FALSE.swap(false, Ordering::SeqCst) {
        return Ok(false);
    }
    if store.inner.closing.load(Ordering::SeqCst) {
        Ok(true)
    } else {
        cancellation_requested(store, run_id)
    }
}

fn timeout_reached(started: Instant, timeout: Option<Duration>) -> bool {
    #[cfg(test)]
    if FORCE_TIMEOUT_STATE.swap(false, Ordering::SeqCst) {
        return true;
    }
    timeout.is_some_and(|value| started.elapsed() >= value)
}

fn queued_position_count(row: &Row<'_>) -> duckdb::Result<i64> {
    #[cfg(test)]
    if FORCE_QUEUE_POSITION_ROW_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(duckdb::Error::InvalidColumnIndex(99));
    }
    row.get::<_, i64>(0)
}

fn combine_run_results(result: AppResult<()>, release_result: AppResult<()>) -> AppResult<()> {
    match (result, release_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn persist_completed_run(
    connection: &Connection,
    run_id: &str,
    ended: DateTime<Utc>,
    duration_ms: i64,
    exit_code: Option<i64>,
    status: &str,
    summary: &Value,
    artifacts: &[Value],
) -> AppResult<()> {
    let run_values = params![
        ended,
        duration_ms,
        exit_code,
        status,
        serde_json::to_string(summary).expect("run summary serialization is infallible"),
        serde_json::to_string(artifacts).expect("run artifact serialization is infallible"),
        run_id
    ];
    connection.execute(INSERT_COMPLETED_RUN_SQL, run_values)?;
    for artifact in artifacts {
        let values = params![
            run_id,
            required_string_field(artifact, "kind", "run artifact")
                .expect("collected run artifacts always contain kind"),
            required_string_field(artifact, "path", "run artifact")
                .expect("collected run artifacts always contain path"),
            required_bool_field(artifact, "exists", "run artifact")
                .expect("collected run artifacts always contain exists"),
            artifact.get("size_bytes").and_then(Value::as_i64),
            artifact.get("coverage_format").and_then(Value::as_str),
            artifact.get("suite").and_then(Value::as_str),
            required_bool_field(artifact, "modified_by_run", "run artifact")
                .expect("collected run artifacts always contain modified_by_run"),
            artifact.get("ingest_status").and_then(Value::as_str),
            artifact.get("snapshot_id").and_then(Value::as_str),
            artifact.get("ingest_error").and_then(Value::as_str),
        ];
        connection.execute(INSERT_RUN_ARTIFACT_SQL, values)?;
    }
    connection.execute("DELETE FROM run_jobs WHERE id = ?", params![run_id])?;
    Ok(())
}

fn checked_db_u32(value: i64, field: &str) -> AppResult<u32> {
    u32::try_from(value).map_err(|_| AppError::Runtime(persisted_value_out_of_range(field, value)))
}

fn checked_db_u64(value: i64, field: &str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| AppError::Runtime(persisted_value_out_of_range(field, value)))
}

fn checked_duckdb_i64(value: u64, field: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::Runtime(format!("{field} exceeds DuckDB BIGINT")))
}

fn checked_usize_i64(value: usize, field: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::Runtime(format!("{field} exceeds DuckDB BIGINT")))
}

fn checked_add_u64(left: u64, right: u64, field: &str) -> AppResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| AppError::Runtime(format!("{field} overflowed")))
}

fn checked_mul_u64(left: u64, right: u64, field: &str) -> AppResult<u64> {
    left.checked_mul(right)
        .ok_or_else(|| AppError::Runtime(format!("{field} overflowed")))
}

fn persisted_value_out_of_range(field: &str, value: i64) -> String {
    format!("persisted {field} value is out of range: {value}")
}

type LogCaptureHandle = JoinHandle<std::io::Result<LogCaptureResult>>;
type LogCaptureTask = Box<dyn FnOnce() -> std::io::Result<LogCaptureResult> + Send + 'static>;

fn release_run_slot(slot_lock: &Mutex<usize>, slot_cv: &Condvar) -> AppResult<()> {
    let mut active = slot_lock.lock().map_err(lock_error)?;
    *active = active.saturating_sub(1);
    slot_cv.notify_one();
    Ok(())
}

fn lock_managed_control(
    control: &Arc<Mutex<Option<Child>>>,
) -> AppResult<std::sync::MutexGuard<'_, Option<Child>>> {
    #[cfg(test)]
    if FORCE_CONTROL_LOCK_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(AppError::Runtime(
            "injected managed control lock failure".to_owned(),
        ));
    }
    control.lock().map_err(lock_error)
}

fn lock_managed_control_for_reap(
    control: &Arc<Mutex<Option<Child>>>,
) -> AppResult<std::sync::MutexGuard<'_, Option<Child>>> {
    #[cfg(test)]
    if FORCE_REAP_CONTROL_LOCK_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(AppError::Runtime(
            "injected managed reap control lock failure".to_owned(),
        ));
    }
    lock_managed_control(control)
}

fn try_wait_managed_child(child: &mut Child) -> std::io::Result<Option<std::process::ExitStatus>> {
    #[cfg(test)]
    if FORCE_TRY_WAIT_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other("injected child polling failure"));
    }
    child.try_wait()
}

fn take_child_stream<R>(child: &mut Child, stream: Option<R>, name: &str) -> AppResult<R> {
    stream.ok_or_else(|| {
        let _ = terminate_child_group(child);
        let _ = child.wait();
        AppError::Runtime(format!("managed run did not expose {name} capture"))
    })
}

fn capture_handle_or_cleanup(
    child: &mut Child,
    result: AppResult<LogCaptureHandle>,
) -> AppResult<LogCaptureHandle> {
    match result {
        Ok(handle) => Ok(handle),
        Err(error) => {
            let _ = terminate_child_group(child);
            let _ = child.wait();
            Err(error)
        }
    }
}

fn capture_second_handle_or_cleanup(
    child: &mut Child,
    result: AppResult<LogCaptureHandle>,
    previous: LogCaptureHandle,
) -> AppResult<(LogCaptureHandle, LogCaptureHandle)> {
    match result {
        Ok(handle) => Ok((previous, handle)),
        Err(error) => {
            let _ = terminate_child_group(child);
            let _ = child.wait();
            let _ = previous.join();
            Err(error)
        }
    }
}

fn cleanup_unregistered_run(
    run_id: &str,
    control: &Arc<Mutex<Option<Child>>>,
    captures: LogCaptureHandles,
    registry_error: AppError,
) -> AppResult<()> {
    let terminate_error = terminate_managed_process(control).err();
    let capture_error = join_log_capture_handles(captures).err();
    if let Some(cleanup_error) = terminate_error.or(capture_error) {
        return Err(AppError::Runtime(format!(
            "run {run_id} registration failed: {registry_error}; cleanup failed: {cleanup_error}"
        )));
    }
    Err(registry_error)
}

fn spawn_log_capture<R>(
    reader: R,
    output: File,
    max_bytes: u64,
    stream: &str,
) -> AppResult<LogCaptureHandle>
where
    R: Read + Send + 'static,
{
    #[cfg(test)]
    {
        let remaining = FORCE_LOG_CAPTURE_FAILURE_CALL.load(Ordering::SeqCst);
        if remaining > 0 && FORCE_LOG_CAPTURE_FAILURE_CALL.fetch_sub(1, Ordering::SeqCst) == 1 {
            return Err(AppError::Runtime(
                "injected log capture thread spawn failure".to_owned(),
            ));
        }
    }
    let name = format!("coverage-mcp-log-{stream}");
    let task: LogCaptureTask = Box::new(move || capture_log(reader, output, max_bytes));
    spawn_log_capture_task(name, task, |name, task| {
        thread::Builder::new().name(name).spawn(task)
    })
}

fn spawn_log_capture_task(
    name: String,
    task: LogCaptureTask,
    spawn: impl FnOnce(String, LogCaptureTask) -> std::io::Result<LogCaptureHandle>,
) -> AppResult<LogCaptureHandle> {
    spawn(name, task).map_err(AppError::from)
}

fn capture_log<R, W>(
    mut reader: R,
    mut output: W,
    max_bytes: u64,
) -> std::io::Result<LogCaptureResult>
where
    R: Read,
    W: Write,
{
    capture_log_stream(&mut reader, &mut output, max_bytes)
}

fn capture_log_stream(
    reader: &mut dyn Read,
    output: &mut dyn Write,
    max_bytes: u64,
) -> std::io::Result<LogCaptureResult> {
    let mut buffer = [0_u8; 8 * 1024];
    let mut bytes_written = 0_u64;
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(bytes_written);
        let write_len = remaining.min(read as u64) as usize;
        write_capture_chunk(output, &buffer, write_len, &mut bytes_written)?;
        if write_len < read {
            truncated = true;
        }
    }
    Ok(LogCaptureResult {
        bytes_written,
        truncated,
    })
}

fn write_capture_chunk(
    output: &mut dyn Write,
    buffer: &[u8],
    write_len: usize,
    bytes_written: &mut u64,
) -> std::io::Result<()> {
    if write_len > 0 {
        output.write_all(&buffer[..write_len])?;
        *bytes_written += write_len as u64;
    }
    Ok(())
}

fn join_log_capture(
    handle: JoinHandle<std::io::Result<LogCaptureResult>>,
    stream: &str,
) -> AppResult<LogCaptureResult> {
    handle
        .join()
        .map_err(|_| AppError::Runtime(format!("{stream} log capture thread panicked")))?
        .map_err(AppError::from)
}

fn join_log_capture_handles(
    handles: LogCaptureHandles,
) -> AppResult<(LogCaptureResult, LogCaptureResult)> {
    let stdout = join_log_capture(handles.stdout, "stdout");
    let stderr = join_log_capture(handles.stderr, "stderr");
    match (stdout, stderr) {
        (Ok(stdout), Ok(stderr)) => Ok((stdout, stderr)),
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
    }
}

fn terminate_managed_process(control: &Arc<Mutex<Option<Child>>>) -> AppResult<()> {
    let mut guard = control.lock().map_err(lock_error)?;
    if let Some(child) = guard.as_mut() {
        terminate_child_group(child)?;
    }
    reap_child(&mut guard)
}

fn terminate_child_group(child: &mut Child) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_TERMINATE_CHILD_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other("injected child termination failure"));
    }
    #[cfg(unix)]
    {
        terminate_child_group_with(child, |pid| {
            let group = format!("-{pid}");
            Command::new("kill")
                // POSIX requires `--` before a negative PID operand; without
                // it Linux kill(1) may parse the process group as a signal.
                .args(["-KILL", "--", group.as_str()])
                .stderr(Stdio::null())
                .status()
        })
    }
    #[cfg(windows)]
    {
        let pid = child.id().to_string();
        let status = Command::new("taskkill")
            .args(["/PID", pid.as_str(), "/T", "/F"])
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "taskkill exited with {status}"
            )))
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        child.kill()
    }
}

fn terminate_cancelled_child_group(child: &mut Child) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_CANCELLATION_TERMINATE_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other(
            "injected cancelled child termination failure",
        ));
    }
    #[cfg(test)]
    if FORCE_CANCELLATION_TERMINATE_SUCCESS.swap(false, Ordering::SeqCst) {
        return child.kill();
    }
    terminate_child_group(child)
}

fn terminate_timeout_child_group(child: &mut Child) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_TIMEOUT_TERMINATE_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other(
            "injected timeout child termination failure",
        ));
    }
    terminate_child_group(child)
}

#[cfg(unix)]
fn terminate_child_group_with(
    child: &mut Child,
    kill_group: impl FnOnce(u32) -> std::io::Result<std::process::ExitStatus>,
) -> std::io::Result<()> {
    terminate_child_group_result(child, kill_group(child.id()), Child::try_wait)
}

#[cfg(unix)]
fn terminate_child_group_result(
    child: &mut Child,
    result: std::io::Result<std::process::ExitStatus>,
    try_wait: fn(&mut Child) -> std::io::Result<Option<std::process::ExitStatus>>,
) -> std::io::Result<()> {
    match result {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            if try_wait(child)?.is_some() {
                Ok(())
            } else {
                fallback_after_group_status(status, child.kill())
            }
        }
        Err(error) => fallback_after_group_command(error, child.kill()),
    }
}

#[cfg(unix)]
fn fallback_after_group_status(
    status: std::process::ExitStatus,
    fallback: std::io::Result<()>,
) -> std::io::Result<()> {
    if fallback.is_ok() {
        Err(std::io::Error::other(format!(
            "process-group kill exited with {status}"
        )))
    } else {
        fallback
    }
}

#[cfg(unix)]
fn fallback_after_group_command(
    error: std::io::Error,
    fallback: std::io::Result<()>,
) -> std::io::Result<()> {
    if fallback.is_ok() {
        Err(error)
    } else {
        fallback
    }
}

fn reap_child(child: &mut Option<std::process::Child>) -> AppResult<()> {
    if let Some(child) = child {
        wait_for_reap(child)?;
    }
    Ok(())
}

fn wait_for_reap(child: &mut std::process::Child) -> std::io::Result<std::process::ExitStatus> {
    #[cfg(test)]
    if FORCE_REAP_CHILD_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other("injected child reap failure"));
    }
    child.wait()
}

fn join_worker(thread: JoinHandle<()>, worker: &str) -> AppResult<()> {
    thread
        .join()
        .map_err(|_| AppError::Runtime(format!("coverage-mcp {worker} worker panicked")))
}

fn report_background_run_error(store: &CoverageStore, run_id: &str) {
    if let Err(error) = store.execute_run(run_id) {
        eprintln!("coverage-mcp background run {run_id} failed: {error}");
    }
}

fn cancellation_requested(store: &CoverageStore, run_id: &str) -> AppResult<bool> {
    store.with_connection(|connection| {
        connection
            .query_row(
                "SELECT cancellation_requested_at IS NOT NULL FROM run_jobs WHERE id = ?",
                params![run_id],
                bool_column,
            )
            .map_err(AppError::from)
    })
}

fn summary_line_limit(value: Option<&Value>) -> AppResult<usize> {
    let value = value
        .ok_or_else(|| AppError::Runtime("queued run is missing max_summary_lines".to_owned()))?;
    let value = value.as_u64().ok_or_else(|| {
        AppError::Runtime("max_summary_lines must be an unsigned integer".to_owned())
    })?;
    if value == 0 || value > 500 {
        return Err(AppError::Runtime(
            "max_summary_lines is outside the allowed range".to_owned(),
        ));
    }
    Ok(value as usize)
}

fn timeout_duration(value: Option<i64>) -> AppResult<Option<Duration>> {
    value
        .map(|value| {
            u64::try_from(value).map(Duration::from_secs).map_err(|_| {
                AppError::Runtime("queued run timeout_seconds must not be negative".to_owned())
            })
        })
        .transpose()
}

fn read_log_lines(
    result: &Value,
    run_id: &str,
    stream_name: &str,
    path_key: &str,
) -> AppResult<Vec<String>> {
    let path = result
        .get(path_key)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Runtime(format!(
                "run {run_id} is missing its {stream_name} log path"
            ))
        })?;
    let bytes = fs::read(path)?;
    Ok(String::from_utf8_lossy(&bytes)
        .lines()
        .map(str::to_owned)
        .collect())
}

fn remove_run_directory(run_dir: &Path, run_id: &str) -> AppResult<()> {
    let path = run_dir.join(run_id);
    match fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn create_run_log_files(run_path: &Path) -> AppResult<(PathBuf, PathBuf)> {
    fs::create_dir_all(run_path)?;
    let stdout = run_path.join("stdout.log");
    let stderr = run_path.join("stderr.log");
    File::create(&stdout)?;
    File::create(&stderr)?;
    Ok((stdout, stderr))
}

fn collection_limit(limit: usize) -> usize {
    limit.clamp(1, COLLECTION_FETCH_LIMIT)
}

fn validate_settings_patch(patch: &ProjectSettingsPatch) -> AppResult<()> {
    if let Some(value) = patch.compaction_after_days {
        if !(1..=36_500).contains(&value) {
            return Err(AppError::Validation(
                "compaction_after_days must be between 1 and 36500".to_owned(),
            ));
        }
    }
    if let Some(value) = patch.compaction_interval_seconds {
        if !(1..=86_400).contains(&value) {
            return Err(AppError::Validation(
                "compaction_interval_seconds must be between 1 and 86400".to_owned(),
            ));
        }
    }
    if let Some(value) = patch.compaction_batch_size {
        if !(1..=10_000).contains(&value) {
            return Err(AppError::Validation(
                "compaction_batch_size must be between 1 and 10000".to_owned(),
            ));
        }
    }
    Ok(())
}

fn normalize_line_ranges(ranges: &[LineRange]) -> AppResult<Vec<LineRange>> {
    if ranges.len() > 10 {
        return Err(AppError::Validation(
            "line_ranges accepts at most 10 ranges".to_owned(),
        ));
    }
    let mut ordered = ranges.to_vec();
    for (start, end) in &ordered {
        if *start < 1 || *end < 1 {
            return Err(AppError::Validation(format!(
                "line range bounds must be positive: {start}-{end}"
            )));
        }
        if end < start {
            return Err(AppError::Validation(format!(
                "line range end must be at least start: {start}-{end}"
            )));
        }
    }
    ordered.sort_unstable();
    let mut normalized: Vec<LineRange> = Vec::new();
    for (start, end) in ordered {
        if let Some((_, previous_end)) = normalized.last_mut() {
            if start <= previous_end.saturating_add(1) {
                *previous_end = (*previous_end).max(end);
                continue;
            }
        }
        normalized.push((start, end));
    }
    let count = normalized.iter().try_fold(0_i64, |count, (start, end)| {
        let range_count = end - start + 1;
        if range_count > 200 {
            return Err(AppError::Validation(
                "line_ranges combined unique span may contain at most 200 lines".to_owned(),
            ));
        }
        Ok(count + range_count)
    })?;
    if count > 200 {
        return Err(AppError::Validation(
            "line_ranges combined unique span may contain at most 200 lines".to_owned(),
        ));
    }
    Ok(normalized)
}

fn worktree_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"created_at":timestamp_string(row.get_ref(1).expect("worktree projection has created_at")),"name":row.get::<_, Option<String>>(2)?,"path":row.get::<_, String>(3)?,"repo_path":row.get::<_, String>(4)?,"repo_key":row.get::<_, String>(5)?,"branch":row.get::<_, Option<String>>(6)?,"head_sha":row.get::<_, Option<String>>(7)?,"base_ref":row.get::<_, String>(8)?,"base_sha":row.get::<_, Option<String>>(9)?,"baseline_snapshot_id":row.get::<_, Option<String>>(10)?}),
    )
}

fn command_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"created_at":timestamp_string(row.get_ref(1).expect("command projection has created_at")),"name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"cwd":row.get::<_, String>(4)?,"repo_path":row.get::<_, String>(5)?,"repo_key":row.get::<_, String>(6)?,"branch":row.get::<_, Option<String>>(7)?,"commit_sha":row.get::<_, Option<String>>(8)?,"shell":row.get::<_, String>(9)?,"approved_by":row.get::<_, String>(10)?,"approval_note":row.get::<_, String>(11)?,"artifact_specs":json_string(row.get::<_, String>(12)?),"enabled":row.get::<_, bool>(13)?,"duration_estimate_ms":row.get::<_, Option<i64>>(14)?,"duration_p90_ms":row.get::<_, Option<i64>>(15)?,"duration_sample_count":row.get::<_, i64>(16)?}),
    )
}

fn job_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"command_id":row.get::<_, String>(1)?,"command_name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"idempotency_key":row.get::<_, Option<String>>(4)?,"cwd":row.get::<_, String>(5)?,"repo_path":row.get::<_, String>(6)?,"repo_key":row.get::<_, String>(7)?,"branch":row.get::<_, Option<String>>(8)?,"commit_sha":row.get::<_, Option<String>>(9)?,"queued_at":timestamp_string(row.get_ref(10).expect("job projection has queued_at")),"started_at":optional_timestamp(row.get_ref(11).expect("job projection has started_at")),"ended_at":optional_timestamp(row.get_ref(12).expect("job projection has ended_at")),"timeout_seconds":row.get::<_, Option<i64>>(13)?,"max_summary_lines":row.get::<_, i64>(14)?,"status":row.get::<_, String>(15)?,"stdout_path":row.get::<_, String>(16)?,"stderr_path":row.get::<_, String>(17)?,"error":row.get::<_, String>(18)?,"cancellation_requested_at":optional_timestamp(row.get_ref(19).expect("job projection has cancellation_requested_at"))}),
    )
}

fn run_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let parsed_summary = json_string(row.get::<_, String>(17)?);
    let artifact_paths = json_string(row.get::<_, String>(18)?);
    Ok(
        json!({"id":row.get::<_, String>(0)?,"command_id":row.get::<_, String>(1)?,"command_name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"idempotency_key":row.get::<_, Option<String>>(4)?,"cwd":row.get::<_, String>(5)?,"repo_path":row.get::<_, String>(6)?,"repo_key":row.get::<_, String>(7)?,"branch":row.get::<_, Option<String>>(8)?,"commit_sha":row.get::<_, Option<String>>(9)?,"started_at":timestamp_string(row.get_ref(10).expect("run projection has started_at")),"ended_at":timestamp_string(row.get_ref(11).expect("run projection has ended_at")),"duration_ms":row.get::<_, i64>(12)?,"exit_code":row.get::<_, Option<i64>>(13)?,"status":row.get::<_, String>(14)?,"stdout_path":row.get::<_, String>(15)?,"stderr_path":row.get::<_, String>(16)?,"parsed_summary":parsed_summary,"artifact_paths":artifact_paths,"queued_at":optional_timestamp(row.get_ref(19).expect("run projection has queued_at")),"queue_duration_ms":row.get::<_, Option<i64>>(20)?,"cancellation_requested_at":optional_timestamp(row.get_ref(21).expect("run projection has cancellation_requested_at")),"terminal":true,"poll_after_ms":Value::Null,"queue_position":Value::Null,"execution_mode":"background","cancellation_requested":!matches!(row.get_ref(21).expect("run projection has cancellation_requested_at"), ValueRef::Null),"coverage_ingest":coverage_ingest(&artifact_paths)}),
    )
}

fn artifact_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"run_id":row.get::<_, String>(0)?,"kind":row.get::<_, String>(1)?,"path":row.get::<_, String>(2)?,"exists":row.get::<_, bool>(3)?,"size_bytes":row.get::<_, Option<i64>>(4)?,"coverage_format":row.get::<_, Option<String>>(5)?,"suite":row.get::<_, Option<String>>(6)?,"modified_by_run":row.get::<_, bool>(7)?,"ingest_status":row.get::<_, Option<String>>(8)?,"snapshot_id":row.get::<_, Option<String>>(9)?,"ingest_error":row.get::<_, Option<String>>(10)?,"command_id":row.get::<_, String>(11)?,"command_name":row.get::<_, String>(12)?,"repo_key":row.get::<_, String>(13)?,"repo_path":row.get::<_, String>(14)?,"started_at":timestamp_string(row.get_ref(15).expect("artifact projection has started_at")),"ended_at":timestamp_string(row.get_ref(16).expect("artifact projection has ended_at")),"status":row.get::<_, String>(17)?,"exit_code":row.get::<_, Option<i64>>(18)?}),
    )
}

fn line_from_row_with_file(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let mut object = line_from_row_offset(row)?;
    let file_path = row.get::<_, String>(0)?;
    object.insert("file_path".to_owned(), json!(file_path));
    Ok(Value::Object(object))
}

fn required_managed_child(guard: &mut Option<Child>) -> AppResult<&mut Child> {
    guard.as_mut().ok_or_else(|| {
        AppError::Runtime("managed run process control was removed unexpectedly".to_owned())
    })
}

fn append_line_with_path(
    values: &mut HashMap<(String, i64), Value>,
    path: &str,
    mut line: Value,
) -> AppResult<()> {
    let object = line
        .as_object_mut()
        .ok_or_else(|| AppError::Runtime("coverage line must be an object".to_owned()))?;
    let number = object
        .get("line_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::Runtime("coverage line number must be an integer".to_owned()))?;
    object.insert("file_path".to_owned(), json!(path));
    values.insert((path.to_owned(), number), line);
    Ok(())
}

fn line_from_row_offset(row: &duckdb::Row<'_>) -> duckdb::Result<Map<String, Value>> {
    Ok(Map::from_iter([
        ("line_number".to_owned(), json!(row.get::<_, i64>(1)?)),
        ("hits".to_owned(), json!(row.get::<_, i64>(2)?)),
        ("covered".to_owned(), json!(row.get::<_, bool>(3)?)),
        ("count_line".to_owned(), json!(row.get::<_, bool>(4)?)),
        ("total_branches".to_owned(), json!(row.get::<_, i64>(5)?)),
        ("covered_branches".to_owned(), json!(row.get::<_, i64>(6)?)),
        ("total_functions".to_owned(), json!(row.get::<_, i64>(7)?)),
        ("covered_functions".to_owned(), json!(row.get::<_, i64>(8)?)),
        ("details".to_owned(), json_string(row.get::<_, String>(9)?)),
    ]))
}

fn normalize_artifact_specs(value: Value) -> AppResult<Vec<Value>> {
    let object = match value {
        Value::Null => return Ok(Vec::new()),
        Value::Object(object) => object,
        _ => {
            return Err(AppError::Validation(
                "artifact_specs must be an object keyed by artifact kind".to_owned(),
            ));
        }
    };
    let mut specs = Vec::new();
    for (kind, raw) in object {
        let mut spec = match raw {
            Value::String(path) => json!({"kind":kind,"path":path,"required":false}),
            Value::Object(map) => {
                let mut value = Value::Object(map.clone());
                #[allow(clippy::option_map_unit_fn)]
                value.as_object_mut().map(|object| {
                    object.insert("kind".to_owned(), json!(kind));
                    object.entry("required".to_owned()).or_insert(json!(false));
                });
                value
            }
            _ => {
                return Err(AppError::Validation(format!(
                    "artifact spec must be a path string or object: {kind}"
                )));
            }
        };
        if spec
            .get("path")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(AppError::Validation(format!(
                "artifact path is required: {kind}"
            )));
        }
        specs.push(spec.take());
    }
    Ok(specs)
}

fn resolve_artifact_path(cwd: &str, raw_path: &str) -> PathBuf {
    let path = PathBuf::from(raw_path);
    if path.is_absolute() {
        path
    } else {
        PathBuf::from(cwd).join(path)
    }
}

fn artifact_fingerprint(path: &Path, hash_contents: bool) -> AppResult<ArtifactFingerprint> {
    #[cfg(test)]
    if FORCE_ARTIFACT_FINGERPRINT_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(AppError::Runtime(
            "injected artifact fingerprint failure".to_owned(),
        ));
    }
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ArtifactFingerprint {
                exists: false,
                size_bytes: None,
                modified_ns: None,
                sha256: None,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .and_then(|value| i64::try_from(value.as_nanos()).ok());
    let sha256 = if hash_contents && metadata.is_file() && metadata.len() <= 64 * 1024 * 1024 {
        Some(hash_file(path)?)
    } else {
        None
    };
    Ok(ArtifactFingerprint {
        exists: true,
        size_bytes: i64::try_from(metadata.len()).ok(),
        modified_ns,
        sha256,
    })
}

fn hash_file(path: &Path) -> AppResult<String> {
    let mut reader = File::open(path)?;
    hash_reader(&mut reader)
}

fn hash_reader(reader: &mut dyn Read) -> AppResult<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex_prefix(&digest.finalize(), 32))
}

fn artifact_baselines(
    run_id: &str,
    command: &Value,
    cwd: &str,
) -> AppResult<Vec<ArtifactBaseline>> {
    let specs = required_array_field(command, "artifact_specs", "registered command")
        .expect("registered command projections always contain artifact_specs");
    let mut baselines = Vec::new();
    for spec in specs {
        let kind = required_string_field(spec, "kind", "artifact specification")
            .expect("normalized artifact specifications always contain kind");
        let raw_path = required_string_field(spec, "path", "artifact specification")
            .expect("normalized artifact specifications always contain path");
        let path = resolve_artifact_path(cwd, &raw_path);
        let hash_contents = spec
            .get("coverage_format")
            .is_some_and(|value| !value.is_null());
        baselines.push(ArtifactBaseline {
            run_id: run_id.to_owned(),
            kind,
            path: path.to_string_lossy().into_owned(),
            fingerprint: artifact_fingerprint(&path, hash_contents)?,
        });
    }
    Ok(baselines)
}

fn coverage_ingest(artifacts: &Value) -> Value {
    let values = artifacts.as_array().cloned().unwrap_or_default();
    let configured = values
        .iter()
        .filter(|value| {
            value
                .get("coverage_format")
                .is_some_and(|format| !format.is_null())
        })
        .count();
    let ingested = values
        .iter()
        .filter(|value| value.get("ingest_status").and_then(Value::as_str) == Some("ingested"))
        .count();
    let failed = values
        .iter()
        .filter(|value| {
            matches!(
                value.get("ingest_status").and_then(Value::as_str),
                Some("failed" | "missing")
            )
        })
        .count();
    let stale = values
        .iter()
        .filter(|value| value.get("ingest_status").and_then(Value::as_str) == Some("skipped_stale"))
        .count();
    let skipped = values
        .iter()
        .filter(|value| {
            value.get("ingest_status").and_then(Value::as_str) == Some("skipped_run_status")
        })
        .count();
    let snapshot_ids: Vec<Value> = values
        .iter()
        .filter_map(|value| value.get("snapshot_id"))
        .filter(|value| !value.is_null())
        .cloned()
        .collect();
    let status = if configured == 0 {
        "not_configured"
    } else if failed > 0 {
        "failed"
    } else if ingested == configured {
        "ingested"
    } else if stale > 0 && ingested == 0 {
        "stale"
    } else {
        "partial"
    };
    json!({"status":status,"configured":configured,"ingested":ingested,"failed":failed,"stale":stale,"skipped":skipped,"snapshot_ids":snapshot_ids})
}

#[allow(clippy::too_many_arguments)]
fn summarize_logs(
    stdout_path: &Path,
    stderr_path: &Path,
    status: &str,
    exit_code: Option<i64>,
    duration_ms: i64,
    max_lines: usize,
    stdout_capture: LogCaptureResult,
    stderr_capture: LogCaptureResult,
) -> AppResult<Value> {
    #[cfg(test)]
    if FORCE_SUMMARY_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(AppError::Runtime("injected log summary failure".to_owned()));
    }
    let stdout = String::from_utf8_lossy(&fs::read(stdout_path)?).into_owned();
    let stderr = String::from_utf8_lossy(&fs::read(stderr_path)?).into_owned();
    let mut counters = Map::new();
    for (name, needle) in [
        ("passed", "passed"),
        ("failed", "failed"),
        ("error", "error"),
        ("warning", "warning"),
    ] {
        counters.insert(
            name.to_owned(),
            json!(
                stdout
                    .lines()
                    .chain(stderr.lines())
                    .filter(|line| line.to_lowercase().contains(needle))
                    .count()
            ),
        );
    }
    let mut excerpts = Vec::new();
    for (stream, text) in [("stdout", stdout.as_str()), ("stderr", stderr.as_str())] {
        for (index, line) in text.lines().take(max_lines).enumerate() {
            excerpts.push(json!({"stream":stream,"line_number":index+1,"text":line}));
        }
    }
    Ok(
        json!({"status":status,"exit_code":exit_code,"duration_ms":duration_ms,"stdout_line_count":stdout.lines().count(),"stderr_line_count":stderr.lines().count(),"stdout_bytes":stdout_capture.bytes_written,"stderr_bytes":stderr_capture.bytes_written,"counters":counters,"excerpts":excerpts,"truncated":stdout_capture.truncated || stderr_capture.truncated,"stdout_path":stdout_path,"stderr_path":stderr_path}),
    )
}

fn context_window(lines: &[String], index: usize, context: usize) -> Vec<Value> {
    let start = index.saturating_sub(context);
    let end = (index + context + 1).min(lines.len());
    (start..end)
        .map(|position| json!({"line_number":position+1,"text":lines[position]}))
        .collect()
}

fn append_log_matches(
    matches: &mut Vec<Value>,
    stream_name: &str,
    lines: &[String],
    queries: &[String],
    context_lines: usize,
    max_matches: usize,
    case_sensitive: bool,
) {
    for (index, line) in lines.iter().enumerate() {
        let haystack = if case_sensitive {
            line.clone()
        } else {
            line.to_lowercase()
        };
        let found = queries.iter().any(|query| {
            let needle = if case_sensitive {
                query.clone()
            } else {
                query.to_lowercase()
            };
            haystack.contains(&needle)
        });
        match found {
            true => {
                matches.push(json!({"stream":stream_name,"line_number":index+1,"text":line,"context":context_window(lines,index,context_lines)}));
                if matches.len() >= max_matches {
                    break;
                }
            }
            false => continue,
        }
    }
}

fn query_pruned_run_ids(
    connection: &Connection,
    command_id: &str,
    retention: i64,
) -> AppResult<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT id FROM runs WHERE command_id = ? ORDER BY started_at DESC OFFSET ?")
        .map_err(AppError::from)?;
    let rows = statement
        .query_map(params![command_id, retention], |row| {
            row.get::<_, String>(0)
        })
        .map_err(AppError::from)?;
    rows.map(|row| row.map_err(AppError::from)).collect()
}

fn run_duration_ms(started: Option<&Value>, queued: Option<&Value>) -> AppResult<i64> {
    let selected = started
        .filter(|value| !value.is_null())
        .or(queued.filter(|value| !value.is_null()));
    let Some(selected) = selected else {
        return Ok(0);
    };
    let value = selected
        .as_str()
        .ok_or_else(|| AppError::Runtime("run timestamp must be an RFC3339 string".to_owned()))?;
    let value = DateTime::parse_from_rfc3339(value)
        .map_err(|error| AppError::Runtime(format!("run timestamp is invalid: {error}")))?;
    Ok(Utc::now()
        .signed_duration_since(value.with_timezone(&Utc))
        .num_milliseconds()
        .max(0))
}

fn maintenance_due(settings: &ProjectSettings) -> AppResult<bool> {
    let Some(value) = settings.compaction_last_run_at.as_ref() else {
        return Ok(true);
    };
    let last = DateTime::parse_from_rfc3339(value)
        .map_err(|error| AppError::Runtime(format!("compaction timestamp is invalid: {error}")))?;
    Ok(Utc::now()
        .signed_duration_since(last.with_timezone(&Utc))
        .num_seconds()
        >= settings.compaction_interval_seconds as i64)
}

fn delta(current: Option<&Value>, baseline: Option<&Value>) -> Value {
    match (
        current.and_then(Value::as_f64),
        baseline.and_then(Value::as_f64),
    ) {
        (Some(current), Some(baseline)) => json!(current - baseline),
        _ => Value::Null,
    }
}

fn overall_delta(current: &Value, baseline: &Value) -> AppResult<Value> {
    let count_delta = |key: &str| -> AppResult<i64> {
        required_i64_field(current, key, "current snapshot")?
            .checked_sub(required_i64_field(baseline, key, "baseline snapshot")?)
            .ok_or_else(|| AppError::Runtime(format!("snapshot metric {key} delta overflows")))
    };
    let current_line_rate = required_field(current, "line_rate", "current snapshot")?;
    let baseline_line_rate = required_field(baseline, "line_rate", "baseline snapshot")?;
    let current_branch_rate = required_field(current, "branch_rate", "current snapshot")?;
    let baseline_branch_rate = required_field(baseline, "branch_rate", "baseline snapshot")?;
    let current_function_rate = required_field(current, "function_rate", "current snapshot")?;
    let baseline_function_rate = required_field(baseline, "function_rate", "baseline snapshot")?;
    let current_region_rate = required_field(current, "region_rate", "current snapshot")?;
    let baseline_region_rate = required_field(baseline, "region_rate", "baseline snapshot")?;
    Ok(json!({
        "line_rate_delta": delta(Some(current_line_rate), Some(baseline_line_rate)),
        "covered_lines_delta": count_delta("covered_lines")?,
        "total_lines_delta": count_delta("total_lines")?,
        "branch_rate_delta": delta(Some(current_branch_rate), Some(baseline_branch_rate)),
        "covered_branches_delta": count_delta("covered_branches")?,
        "total_branches_delta": count_delta("total_branches")?,
        "function_rate_delta": delta(Some(current_function_rate), Some(baseline_function_rate)),
        "covered_functions_delta": count_delta("covered_functions")?,
        "total_functions_delta": count_delta("total_functions")?,
        "region_rate_delta": delta(Some(current_region_rate), Some(baseline_region_rate)),
        "covered_regions_delta": count_delta("covered_regions")?,
        "total_regions_delta": count_delta("total_regions")?,
    }))
}

fn line_regions(numbers: &[i64]) -> Vec<Value> {
    let mut numbers = numbers.to_vec();
    numbers.sort_unstable();
    numbers.dedup();
    let mut regions = Vec::new();
    let mut current: Option<(i64, i64)> = None;
    for number in numbers {
        if let Some((start, end)) = current.as_mut() {
            if number <= end.saturating_add(1) {
                *end = (*end).max(number);
                continue;
            }
            let line_count = *end - *start + 1;
            regions.push(json!({
                "start": *start,
                "end": *end,
                "line_count": line_count,
            }));
        }
        current = Some((number, number));
    }
    if let Some((start, end)) = current {
        let line_count = end - start + 1;
        regions.push(json!({
            "start": start,
            "end": end,
            "line_count": line_count,
        }));
    }
    regions
}

fn target_order(left: &Value, right: &Value, order_by: &str) -> std::cmp::Ordering {
    let path_order = || {
        left.get("file_path")
            .and_then(Value::as_str)
            .cmp(&right.get("file_path").and_then(Value::as_str))
    };
    let order = match order_by {
        "uncovered_lines" => right
            .get("uncovered_lines")
            .and_then(Value::as_i64)
            .cmp(&left.get("uncovered_lines").and_then(Value::as_i64)),
        "line_rate" => match (
            left.get("line_rate").and_then(Value::as_f64),
            right.get("line_rate").and_then(Value::as_f64),
        ) {
            (Some(left), Some(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
        },
        "file_path" => path_order(),
        _ => right
            .get("priority")
            .and_then(Value::as_i64)
            .cmp(&left.get("priority").and_then(Value::as_i64)),
    };
    order.then_with(path_order)
}

fn coverage_target_priority(
    uncovered_lines: i64,
    uncovered_branches: i64,
    uncovered_functions: i64,
) -> AppResult<i64> {
    uncovered_lines
        .checked_mul(100)
        .and_then(|value| value.checked_add(uncovered_branches.checked_mul(10)?))
        .and_then(|value| value.checked_add(uncovered_functions.checked_mul(5)?))
        .ok_or_else(|| AppError::Runtime("coverage target priority overflows".to_owned()))
}

fn status_order(value: &Value) -> u8 {
    match value.get("status").and_then(Value::as_str) {
        Some("regressed") => 0,
        Some("improved") => 1,
        _ => 2,
    }
}
fn insight_order(value: &Value) -> u8 {
    match value.get("severity").and_then(Value::as_str) {
        Some("high") => 0,
        Some("medium") => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::{checkout, inject_watchdog_failure};
    use std::fmt::Write as _;
    use std::io::{self, Write};

    #[test]
    fn policy_validation_and_range_normalization_are_deterministic() {
        let settings = ProjectSettings {
            repo_key: "repo".to_owned(),
            repo_path: "/repo".to_owned(),
            created_at: String::new(),
            updated_at: String::new(),
            compaction_enabled: true,
            compaction_after_days: 30,
            compaction_interval_seconds: 60,
            compaction_batch_size: 10,
            compaction_last_run_at: None,
            compaction_last_status: "never_run".to_owned(),
            compaction_last_snapshot_count: 0,
            compaction_last_bytes_before: 0,
            compaction_last_bytes_after: 0,
        };
        assert!(settings.policy().enabled);
        assert_eq!(settings.policy().batch_size, 10);
        assert!(validate_settings_patch(&ProjectSettingsPatch::default()).is_ok());
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_after_days: Some(0),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_after_days: Some(36_501),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_interval_seconds: Some(0),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_interval_seconds: Some(86_401),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_batch_size: Some(0),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_settings_patch(&ProjectSettingsPatch {
                compaction_batch_size: Some(10_001),
                ..Default::default()
            })
            .is_err()
        );

        assert_eq!(collection_limit(0), 1);
        assert_eq!(
            collection_limit(COLLECTION_FETCH_LIMIT + 1),
            COLLECTION_FETCH_LIMIT
        );
        assert_eq!(normalize_line_ranges(&[]).unwrap(), Vec::<LineRange>::new());
        assert_eq!(
            normalize_line_ranges(&[(5, 8), (1, 2), (3, 5), (20, 20)]).unwrap(),
            vec![(1, 8), (20, 20)]
        );
        assert!(normalize_line_ranges(&[(0, 1)]).is_err());
        assert!(normalize_line_ranges(&[(4, 3)]).is_err());
        assert!(normalize_line_ranges(&[(1, 1); 11]).is_err());
        assert!(normalize_line_ranges(&[(1, 201)]).is_err());
        assert!(normalize_line_ranges(&[(1, 100), (300, 400)]).is_err());
    }

    #[test]
    fn artifact_fingerprints_distinguish_hash_and_metadata_fallbacks() {
        let directory = tempfile::tempdir().unwrap();
        assert!(artifact_fingerprint(Path::new("\0"), false).is_err());
        let missing = artifact_fingerprint(&directory.path().join("missing"), true).unwrap();
        assert!(!missing.exists);
        assert!(!missing.changed_from(&missing));

        let report = directory.path().join("coverage.lcov");
        std::fs::write(&report, "TN:\n").unwrap();
        let hashed = artifact_fingerprint(&report, true).unwrap();
        assert!(hashed.exists);
        assert!(hashed.sha256.is_some());
        assert!(!hashed.changed_from(&hashed));
        let mut changed_hash = hashed.clone();
        changed_hash.sha256 = Some("different".to_owned());
        assert!(changed_hash.changed_from(&hashed));

        let metadata_only = artifact_fingerprint(&report, false).unwrap();
        assert!(metadata_only.sha256.is_none());
        let mut changed_metadata = metadata_only.clone();
        changed_metadata.size_bytes = Some(metadata_only.size_bytes.unwrap_or_default() + 1);
        assert!(changed_metadata.changed_from(&metadata_only));
        changed_metadata.size_bytes = metadata_only.size_bytes;
        changed_metadata.modified_ns = Some(
            metadata_only
                .modified_ns
                .unwrap_or_default()
                .saturating_add(1),
        );
        assert!(changed_metadata.changed_from(&metadata_only));

        struct FailingHashReader;
        impl Read for FailingHashReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("hash reader failure"))
            }
        }
        let mut failing_reader = FailingHashReader;
        assert!(hash_reader(&mut failing_reader).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let unreadable = directory.path().join("unreadable");
            std::fs::write(&unreadable, "secret").unwrap();
            let mut permissions = std::fs::metadata(&unreadable).unwrap().permissions();
            permissions.set_mode(0o0);
            std::fs::set_permissions(&unreadable, permissions).unwrap();
            assert!(artifact_fingerprint(&unreadable, true).is_err());
            let mut permissions = std::fs::metadata(&unreadable).unwrap().permissions();
            permissions.set_mode(0o600);
            std::fs::set_permissions(&unreadable, permissions).unwrap();
        }

        assert_eq!(
            source_lines_from_text("one\ntwo\nthree\n", 2, 3),
            vec![
                json!({"line_number":2,"text":"two"}),
                json!({"line_number":3,"text":"three"})
            ]
        );
    }

    #[test]
    fn artifact_and_log_helpers_report_all_statuses() {
        assert!(normalize_artifact_specs(Value::Null).unwrap().is_empty());
        let specs = normalize_artifact_specs(
            json!({"coverage":"coverage.lcov","log":{"path":"run.log","required":true}}),
        )
        .unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0]["required"], false);
        assert_eq!(specs[1]["required"], true);
        assert!(normalize_artifact_specs(json!({"bad": 1})).is_err());
        assert!(normalize_artifact_specs(json!({"bad":""})).is_err());

        assert_eq!(coverage_ingest(&Value::Null)["status"], "not_configured");
        assert_eq!(
            coverage_ingest(
                &json!([{"coverage_format":"lcov","ingest_status":"ingested","snapshot_id":"s"}])
            )["status"],
            "ingested"
        );
        assert_eq!(
            coverage_ingest(&json!([{"coverage_format":"lcov","ingest_status":"failed"}]))["status"],
            "failed"
        );
        assert_eq!(
            coverage_ingest(&json!([{"coverage_format":"lcov","ingest_status":"missing"}]))["status"],
            "failed"
        );
        assert_eq!(
            coverage_ingest(&json!([{"coverage_format":"lcov","ingest_status":"pending"}]))["status"],
            "partial"
        );
        let stale =
            coverage_ingest(&json!([{"coverage_format":"lcov","ingest_status":"skipped_stale"}]));
        assert_eq!(stale["status"], "stale");
        assert_eq!(stale["stale"], 1);
        let skipped = coverage_ingest(
            &json!([{"coverage_format":"lcov","ingest_status":"skipped_run_status"}]),
        );
        assert_eq!(skipped["skipped"], 1);

        let directory = tempfile::tempdir().unwrap();
        let stdout_path = directory.path().join("stdout");
        let stderr_path = directory.path().join("stderr");
        std::fs::write(&stdout_path, "passed\nwarning\n").unwrap();
        std::fs::write(&stderr_path, "failed\nerror\n").unwrap();
        let bounded_path = directory.path().join("bounded");
        let capture = capture_log(
            b"0123456789".as_slice(),
            File::create(&bounded_path).unwrap(),
            4,
        )
        .unwrap();
        let direct_write_path = directory.path().join("direct-write");
        let mut direct_write_bytes = 0;
        let mut direct_write_file = File::create(&direct_write_path).unwrap();
        write_capture_chunk(
            &mut direct_write_file,
            b"direct",
            6,
            &mut direct_write_bytes,
        )
        .unwrap();
        write_capture_chunk(
            &mut direct_write_file,
            b"direct",
            0,
            &mut direct_write_bytes,
        )
        .unwrap();
        assert_eq!(direct_write_bytes, 6);
        assert_eq!(capture.bytes_written, 4);
        assert!(capture.truncated);
        assert_eq!(std::fs::read(&bounded_path).unwrap(), b"0123");
        let complete_path = directory.path().join("complete");
        let complete_capture =
            capture_log(b"x".as_slice(), File::create(&complete_path).unwrap(), 10).unwrap();
        assert_eq!(complete_capture.bytes_written, 1);
        assert!(!complete_capture.truncated);
        struct FailingReader;
        impl io::Read for FailingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("capture reader failure"))
            }
        }
        struct FailingWriter;
        impl io::Write for FailingWriter {
            fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("capture writer failure"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut successful_writer = FailingWriter;
        assert!(successful_writer.flush().is_ok());
        assert!(capture_log(FailingReader, FailingWriter, 10).is_err());
        assert!(capture_log(b"x".as_slice(), FailingWriter, 10).is_err());
        #[cfg(unix)]
        {
            let stdout_capture_path = directory.path().join("child-stdout");
            let mut stdout_child = Command::new("sh")
                .args(["-c", "printf 012345"])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout_pipe = stdout_child.stdout.take().unwrap();
            let stdout_capture =
                capture_log(stdout_pipe, File::create(&stdout_capture_path).unwrap(), 1).unwrap();
            assert!(stdout_capture.truncated);
            stdout_child.wait().unwrap();

            let stderr_capture_path = directory.path().join("child-stderr");
            let mut stderr_child = Command::new("sh")
                .args(["-c", "printf 012345 >&2"])
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let stderr_pipe = stderr_child.stderr.take().unwrap();
            let stderr_capture =
                capture_log(stderr_pipe, File::create(&stderr_capture_path).unwrap(), 1).unwrap();
            assert!(stderr_capture.truncated);
            stderr_child.wait().unwrap();
        }
        let stdout_error = thread::spawn(|| Err(io::Error::other("stdout capture failure")));
        let stderr_success = thread::spawn(|| {
            Ok(LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            })
        });
        assert!(
            join_log_capture_handles(LogCaptureHandles {
                stdout: stdout_error,
                stderr: stderr_success,
            })
            .is_err()
        );
        let stdout_success = thread::spawn(|| {
            Ok(LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            })
        });
        let stderr_error = thread::spawn(|| Err(io::Error::other("stderr capture failure")));
        assert!(
            join_log_capture_handles(LogCaptureHandles {
                stdout: stdout_success,
                stderr: stderr_error,
            })
            .is_err()
        );
        let panicked_capture = thread::spawn(|| -> io::Result<LogCaptureResult> {
            panic!("injected capture panic");
        });
        assert!(join_log_capture(panicked_capture, "stdout").is_err());
        fn successful_capture_task() -> io::Result<LogCaptureResult> {
            Ok(LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            })
        }
        let task: LogCaptureTask = Box::new(successful_capture_task);
        assert!(
            spawn_log_capture_task("spawn-error".to_owned(), task, |_, _| {
                Err(io::Error::other("injected log thread spawn failure"))
            })
            .is_err()
        );
        let successful_task: LogCaptureTask = Box::new(successful_capture_task);
        let successful_handle =
            spawn_log_capture_task("spawn-success".to_owned(), successful_task, |_, task| {
                Ok(thread::spawn(task))
            })
            .unwrap();
        assert!(join_log_capture(successful_handle, "spawn-success").is_ok());
        let mut missing_stdout = Command::new("sleep").arg("5").spawn().unwrap();
        assert!(
            take_child_stream(
                &mut missing_stdout,
                None::<std::process::ChildStdout>,
                "stdout"
            )
            .is_err()
        );
        let mut missing_stderr = Command::new("sleep").arg("5").spawn().unwrap();
        assert!(
            take_child_stream(
                &mut missing_stderr,
                None::<std::process::ChildStderr>,
                "stderr"
            )
            .is_err()
        );
        let mut first_capture_failure = Command::new("sleep").arg("5").spawn().unwrap();
        assert!(
            capture_handle_or_cleanup(
                &mut first_capture_failure,
                Err(AppError::Runtime("first capture failure".to_owned())),
            )
            .is_err()
        );
        let mut second_capture_failure = Command::new("sleep").arg("5").spawn().unwrap();
        let previous_capture = thread::spawn(|| {
            Ok(LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            })
        });
        assert!(
            capture_second_handle_or_cleanup(
                &mut second_capture_failure,
                Err(AppError::Runtime("second capture failure".to_owned())),
                previous_capture,
            )
            .is_err()
        );
        #[cfg(unix)]
        {
            fn completed_child_status(
                _: &mut Child,
            ) -> io::Result<Option<std::process::ExitStatus>> {
                use std::os::unix::process::ExitStatusExt;

                Ok(Some(std::process::ExitStatus::from_raw(0)))
            }

            let mut fallback_child = Command::new("sleep").arg("5").spawn().unwrap();
            let fallback_result =
                terminate_child_group_with(&mut fallback_child, |_| Command::new("false").status());
            assert!(fallback_result.is_err());
            let _ = fallback_child.wait();
            let mut command_error_child = Command::new("sleep").arg("5").spawn().unwrap();
            let command_error = terminate_child_group_with(&mut command_error_child, |_| {
                Err(io::Error::other("injected process-group command failure"))
            });
            assert!(command_error.is_err());
            let _ = command_error_child.wait();
            fn failing_child_poll(_: &mut Child) -> io::Result<Option<std::process::ExitStatus>> {
                Err(io::Error::other("injected child poll failure"))
            }
            let mut poll_error_child = Command::new("sleep").arg("5").spawn().unwrap();
            let status = Command::new("false").status().unwrap();
            assert!(
                terminate_child_group_result(
                    &mut poll_error_child,
                    Ok(status),
                    failing_child_poll,
                )
                .is_err()
            );
            let _ = poll_error_child.kill();
            let _ = poll_error_child.wait();
            let status = Command::new("false").status().unwrap();
            assert!(
                terminate_child_group_result(
                    &mut command_error_child,
                    Ok(status),
                    completed_child_status,
                )
                .is_ok()
            );
            let status = Command::new("false").status().unwrap();
            assert!(
                fallback_after_group_status(status, Err(io::Error::other("fallback"))).is_err()
            );
            assert!(
                fallback_after_group_command(
                    io::Error::other("command"),
                    Err(io::Error::other("fallback")),
                )
                .is_err()
            );
        }
        let summary = summarize_logs(
            &stdout_path,
            &stderr_path,
            "failed",
            Some(1),
            10,
            1,
            LogCaptureResult {
                bytes_written: 15,
                truncated: false,
            },
            LogCaptureResult {
                bytes_written: 13,
                truncated: false,
            },
        )
        .unwrap();
        assert_eq!(summary["counters"]["passed"], 1);
        assert_eq!(summary["counters"]["failed"], 1);
        assert_eq!(summary["stdout_line_count"], 2);
        assert_eq!(summary["stdout_bytes"], 15);
        assert_eq!(summary["truncated"], false);
        assert_eq!(summary["excerpts"].as_array().unwrap().len(), 2);
        let lines = vec!["one".to_owned(), "two".to_owned(), "three".to_owned()];
        assert_eq!(context_window(&lines, 1, 1).len(), 3);
        assert_eq!(context_window(&lines, 0, 0)[0]["line_number"], 1);
        let mut no_matches = Vec::new();
        append_log_matches(
            &mut no_matches,
            "stdout",
            &["present".to_owned()],
            &["absent".to_owned()],
            0,
            5,
            false,
        );
        assert!(no_matches.is_empty());
        let mut empty = tempfile::NamedTempFile::new().unwrap();
        writeln!(empty, "no matching output").unwrap();
        let missing = summarize_logs(
            directory.path(),
            &directory.path().join("missing"),
            "passed",
            None,
            0,
            0,
            LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            },
            LogCaptureResult {
                bytes_written: 0,
                truncated: false,
            },
        );
        assert!(missing.is_err());
        let stderr_directory = directory.path().join("stderr-directory");
        std::fs::create_dir(&stderr_directory).unwrap();
        assert!(
            summarize_logs(
                &stdout_path,
                &stderr_directory,
                "passed",
                None,
                0,
                0,
                LogCaptureResult {
                    bytes_written: 0,
                    truncated: false,
                },
                LogCaptureResult {
                    bytes_written: 0,
                    truncated: false,
                },
            )
            .is_err()
        );
        let mut reaped_child = Command::new("true").spawn().unwrap();
        reaped_child.wait().unwrap();
        let mut reaped = Some(reaped_child);
        FORCE_REAP_CHILD_FAILURE.store(true, Ordering::SeqCst);
        assert!(reap_child(&mut reaped).is_err());

        let cleanup_control = Arc::new(Mutex::new(None));
        let cleanup_poison = Arc::clone(&cleanup_control);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = cleanup_poison.lock().unwrap();
            panic!("injected unregistered-process lock poison");
        }));
        assert!(
            cleanup_unregistered_run(
                "unregistered",
                &cleanup_control,
                LogCaptureHandles {
                    stdout: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                    stderr: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                },
                AppError::Runtime("registry failure".to_owned()),
            )
            .is_err()
        );

        let guard_store =
            CoverageStore::open(directory.path().join("guard-errors.duckdb"), test_config())
                .unwrap();
        stop_compaction_worker(&guard_store);
        let poisoned_control = Arc::new(Mutex::new(None));
        let control_for_poison = Arc::clone(&poisoned_control);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = control_for_poison.lock().unwrap();
            panic!("injected managed-process lock poison");
        }));
        {
            let _guard = ManagedRunGuard::new(
                Arc::clone(&guard_store.inner),
                "drop-terminate-error".to_owned(),
                poisoned_control,
                LogCaptureHandles {
                    stdout: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                    stderr: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                },
            );
        }
        let capture_failure_control = Arc::new(Mutex::new(None));
        {
            let _guard = ManagedRunGuard::new(
                Arc::clone(&guard_store.inner),
                "drop-capture-error".to_owned(),
                capture_failure_control,
                LogCaptureHandles {
                    stdout: thread::spawn(|| Err(io::Error::other("drop stdout failure"))),
                    stderr: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                },
            );
        }
        let mut no_captures = ManagedRunGuard::new(
            Arc::clone(&guard_store.inner),
            "drop-without-captures".to_owned(),
            Arc::new(Mutex::new(None)),
            LogCaptureHandles {
                stdout: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
                stderr: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
            },
        );
        no_captures.captures = None;
        assert!(no_captures.finish().is_err());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = guard_store.inner.active_processes.lock().unwrap();
            panic!("injected active-process lock poison");
        }));
        {
            let _guard = ManagedRunGuard::new(
                Arc::clone(&guard_store.inner),
                "drop-registry-error".to_owned(),
                Arc::new(Mutex::new(None)),
                LogCaptureHandles {
                    stdout: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                    stderr: thread::spawn(|| {
                        Ok(LogCaptureResult {
                            bytes_written: 0,
                            truncated: false,
                        })
                    }),
                },
            );
        }
        guard_store.inner.active_processes.clear_poison();
        guard_store.close().unwrap();
        let active_error_store = CoverageStore::open(
            directory.path().join("active-process-error.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&active_error_store);
        let active_control = Arc::new(Mutex::new(None));
        let active_control_poison = Arc::clone(&active_control);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = active_control_poison.lock().unwrap();
            panic!("injected active child lock poison");
        }));
        active_error_store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .insert("poisoned".to_owned(), active_control);
        assert!(active_error_store.close().is_err());
    }

    #[test]
    fn maintenance_helpers_cover_empty_and_error_arms() {
        assert!(ensure_db_parent(Path::new("coverage.duckdb")).is_ok());
        assert!(ensure_db_parent(Path::new("")).is_ok());
        let mut invalid_log_config = test_config();
        invalid_log_config.run_log_max_bytes = MIN_RUN_LOG_MAX_BYTES - 1;
        assert!(
            CoverageStore::open(
                tempfile::tempdir()
                    .unwrap()
                    .path()
                    .join("invalid-log.duckdb"),
                invalid_log_config,
            )
            .is_err()
        );

        let mut values = HashMap::new();
        append_line_with_path(&mut values, "a.py", json!({"line_number": 1})).unwrap();
        assert!(append_line_with_path(&mut values, "a.py", json!({"covered": true})).is_err());
        assert!(append_line_with_path(&mut values, "a.py", json!("scalar")).is_err());
        assert_eq!(values.len(), 1);

        let connection = Connection::open_in_memory().unwrap();
        assert!(remove_compacted_detail(&connection, "missing", false).is_ok());
        connection.execute_batch("BEGIN TRANSACTION").unwrap();
        assert!(
            finish_transaction(
                &connection,
                Err::<(), AppError>(AppError::Validation("rollback".to_owned()))
            )
            .is_err()
        );
        assert!(
            finish_transaction(
                &connection,
                Err::<(), AppError>(AppError::Validation("rollback failure".to_owned()))
            )
            .is_err()
        );
        assert!(
            retain_compaction_thread(
                &Mutex::new(None),
                Err(std::io::Error::other("spawn failure")),
            )
            .is_err()
        );

        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("helpers.duckdb"), test_config()).unwrap();
        assert!(CoverageStore::submitted_run_id(&json!({})).is_err());
        assert_eq!(
            CoverageStore::submitted_run_id(&json!({"id":"run-id"})).unwrap(),
            "run-id"
        );
        report_background_run_error(&store, "missing-background-run");
        assert!(summary_line_limit(None).is_err());
        assert_eq!(summary_line_limit(Some(&json!(7))).unwrap(), 7);
        assert!(summary_line_limit(Some(&json!(0))).is_err());
        assert!(store.source_lines("missing", "a.py", 1, 201).is_err());
        assert!(read_log_lines(&json!({}), "missing-run", "stdout", "stdout_path").is_err());
        assert!(
            read_log_lines(
                &json!({"stdout_path": directory.path()}),
                "directory-run",
                "stdout",
                "stdout_path",
            )
            .is_err()
        );
        assert!(remove_run_directory(directory.path(), "missing-run-directory").is_ok());
        let run_file = directory.path().join("run-file");
        std::fs::write(&run_file, "not a directory").unwrap();
        assert!(remove_run_directory(directory.path(), "run-file").is_err());
        assert!(create_run_log_files(&run_file).is_err());
        let stdout_conflict = directory.path().join("stdout-conflict");
        std::fs::create_dir_all(stdout_conflict.join("stdout.log")).unwrap();
        assert!(create_run_log_files(&stdout_conflict).is_err());
        let stderr_conflict = directory.path().join("stderr-conflict");
        std::fs::create_dir_all(stderr_conflict.join("stderr.log")).unwrap();
        assert!(create_run_log_files(&stderr_conflict).is_err());
        assert!(
            store
                .retain_run_thread(Err(std::io::Error::other("spawn failure")))
                .is_err()
        );
        let mut no_child = None;
        reap_child(&mut no_child).unwrap();
        assert!(required_managed_child(&mut no_child).is_err());
        assert!(terminate_managed_process(&Arc::new(Mutex::new(None))).is_ok());

        let mut child = Command::new("sleep").arg("5").spawn().unwrap();
        let _ = terminate_child_group(&mut child);
        let _ = child.wait();
        let command = store
            .register_command(
                "close-join",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        store
            .submit_command(command["id"].as_str().unwrap(), None, None, 20)
            .unwrap();
        let report = directory.path().join("compact.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n").unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
        let command_projection = json!({
            "repo_path": project.repo_path,
            "name": "artifact-test",
            "artifact_specs": [{}]
        });
        assert!(
            store
                .collect_artifacts(
                    "missing",
                    &command_projection,
                    directory.path().to_str().unwrap(),
                    true
                )
                .is_err()
        );
        let command_path_projection = json!({
            "repo_path": project.repo_path,
            "name": "artifact-test",
            "artifact_specs": [{"kind":"coverage"}]
        });
        assert!(
            store
                .collect_artifacts(
                    "missing",
                    &command_path_projection,
                    directory.path().to_str().unwrap(),
                    true
                )
                .is_err()
        );
        let command_required_projection = json!({
            "repo_path": project.repo_path,
            "name": "artifact-test",
            "artifact_specs": [{"kind":"coverage","path":"missing.coverage"}]
        });
        assert!(
            store
                .collect_artifacts(
                    "missing",
                    &command_required_projection,
                    directory.path().to_str().unwrap(),
                    true
                )
                .is_err()
        );
        let snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                Some("known-commit"),
                None,
                "unit",
            )
            .unwrap();
        let source_snapshot_id = snapshot["id"].as_str().unwrap();
        assert!(
            store
                .source_resolution(source_snapshot_id, "../src/a.py")
                .is_err()
        );
        store
            .clear_snapshot_commit_for_test(source_snapshot_id)
            .unwrap();
        assert!(
            store
                .source_lines(source_snapshot_id, "missing.py", 1, 1)
                .is_err()
        );
        let gap_report = directory.path().join("gap.lcov");
        std::fs::write(
            &gap_report,
            "TN:\nSF:src/a.py\nBRDA:2,0,0,-\nend_of_record\n",
        )
        .unwrap();
        let gap_snapshot = store
            .ingest_report(
                &gap_report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "gap",
            )
            .unwrap();
        let _other_branch_snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("other"),
                None,
                None,
                "unit",
            )
            .unwrap();
        assert!(
            store
                .file_gaps(gap_snapshot["id"].as_str().unwrap(), "src/a.py", 10)
                .is_ok()
        );
        let first = store
            .compact_snapshot_detail(&project.repo_key, snapshot["id"].as_str().unwrap())
            .unwrap();
        assert!(first.0);
        let second = store
            .compact_snapshot_detail(&project.repo_key, snapshot["id"].as_str().unwrap())
            .unwrap();
        assert!(!second.0);
        let malformed_payload = {
            let bytes = serde_json::to_vec(&json!({"lines":[]})).unwrap();
            let mut reader = bytes.as_slice();
            compress_coverage_payload(&mut reader).unwrap()
        };
        let update_malformed_payload = || {
            store.with_connection(|connection| {
                connection.execute(
                    "UPDATE coverage_compacted_payloads SET payload = ? WHERE snapshot_id = ?",
                    params![&malformed_payload, snapshot["id"].as_str().unwrap()],
                )?;
                Ok(())
            })
        };
        update_malformed_payload().unwrap();
        assert!(store.files(snapshot["id"].as_str().unwrap(), 10).is_err());
        assert!(
            store
                .file_coverage(snapshot["id"].as_str().unwrap(), "src/a.py")
                .is_err()
        );
        let missing_lines_payload = {
            let bytes = serde_json::to_vec(&json!({"files":[]})).unwrap();
            let mut reader = bytes.as_slice();
            compress_coverage_payload(&mut reader).unwrap()
        };
        let update_missing_lines_payload = || {
            store.with_connection(|connection| {
                connection.execute(
                    "UPDATE coverage_compacted_payloads SET payload = ? WHERE snapshot_id = ?",
                    params![&missing_lines_payload, snapshot["id"].as_str().unwrap()],
                )?;
                Ok(())
            })
        };
        update_missing_lines_payload().unwrap();
        assert!(
            store
                .lines(snapshot["id"].as_str().unwrap(), "src/a.py", 10)
                .is_err()
        );
        let unmatched_lines_payload = {
            let bytes = serde_json::to_vec(&json!({
                "lines": [{"file_path":"other.py","line_number":1}]
            }))
            .unwrap();
            let mut reader = bytes.as_slice();
            compress_coverage_payload(&mut reader).unwrap()
        };
        let update_unmatched_lines_payload = || {
            store.with_connection(|connection| {
                connection.execute(
                    "UPDATE coverage_compacted_payloads SET payload = ? WHERE snapshot_id = ?",
                    params![&unmatched_lines_payload, snapshot["id"].as_str().unwrap()],
                )?;
                Ok(())
            })
        };
        update_unmatched_lines_payload().unwrap();
        assert!(
            store
                .lines(snapshot["id"].as_str().unwrap(), "src/a.py", 10)
                .unwrap()
                .is_empty()
        );
        store
            .with_connection(|connection| {
                connection
                    .execute_batch("DROP TABLE coverage_compacted_payloads")
                    .expect("drop compacted payload table");
                Ok(())
            })
            .unwrap();
        assert!(update_unmatched_lines_payload().is_err());
        assert!(update_malformed_payload().is_err());
        assert!(update_missing_lines_payload().is_err());
        assert!(
            store
                .compact_snapshot_detail(&project.repo_key, snapshot["id"].as_str().unwrap())
                .is_err()
        );

        let malformed = store
            .register_command(
                "nul-cwd",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let update_cwd = |sql: &str| {
            store.with_connection(|connection| {
                connection.execute(sql, params!["\0", malformed["id"].as_str().unwrap()])?;
                Ok(())
            })
        };
        update_cwd("UPDATE registered_commands SET cwd = ? WHERE id = ?").unwrap();
        assert!(update_cwd("THIS IS NOT VALID SQL").is_err());
        assert!(
            store
                .submit_command(malformed["id"].as_str().unwrap(), None, None, 20)
                .is_err()
        );
        assert!(
            store
                .collect_artifacts(
                    "invalid-artifact-run",
                    &json!({"artifact_specs":[{"kind":"invalid","path":"\0","required":false}],"repo_path":directory.path().to_string_lossy(),"name":"invalid"}),
                    &directory.path().to_string_lossy(),
                    true,
                )
                .is_err()
        );
        let make_broken_command_view = || {
            store.with_connection(|connection| {
                connection.execute_batch(
                    "DROP INDEX IF EXISTS idx_registered_commands_name;
                     ALTER TABLE registered_commands RENAME TO registered_commands_base;
                     CREATE VIEW registered_commands AS
                     SELECT '' AS id, created_at, 'broken' AS name, command, cwd,
                            repo_path, repo_key, branch, commit_sha, shell, approved_by,
                            approval_note, artifact_specs, enabled, duration_estimate_ms,
                            duration_p90_ms, duration_sample_count
                     FROM registered_commands_base
                     LIMIT 1;",
                )?;
                Ok(())
            })
        };
        make_broken_command_view().unwrap();
        assert!(make_broken_command_view().is_err());
        assert!(store.latest_artifact("coverage", Some("broken")).is_err());

        let current_snapshot_id = snapshot["id"].as_str().unwrap();
        let clear_snapshot_branch = || {
            store.with_connection(|connection| {
                connection.execute(
                    "UPDATE snapshots SET branch = NULL WHERE id = ?",
                    params![current_snapshot_id],
                )?;
                Ok(())
            })
        };
        clear_snapshot_branch().unwrap();
        let broken_snapshot_view = format!(
            "DROP INDEX IF EXISTS idx_snapshots_repo_time;
             DROP INDEX IF EXISTS idx_snapshots_commit;
             ALTER TABLE snapshots RENAME TO snapshots_base;
             CREATE VIEW snapshots AS
             SELECT {SNAPSHOT_COLUMNS} FROM snapshots_base
             UNION ALL
             SELECT CAST('broken-previous' AS VARCHAR) AS id, created_at, repo_path, repo_key,
                    CAST('other' AS VARCHAR) AS branch,
                    commit_sha, base_ref, suite, format, report_path, warnings, metadata,
                    total_lines, covered_lines, total_branches, covered_branches,
                    total_functions, covered_functions, total_regions, covered_regions,
                    line_rate, branch_rate, function_rate, region_rate
             FROM snapshots_base
             WHERE id = '{current_snapshot_id}';",
        );
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&broken_snapshot_view)
                    .expect("install broken snapshot view");
                Ok(())
            })
            .unwrap();
        assert!(clear_snapshot_branch().is_err());
        assert!(store.previous_snapshot(current_snapshot_id).is_ok());
        let install_malformed_snapshot_view = |sql: &str| {
            store.with_connection(|connection| {
                connection.execute_batch(sql)?;
                Ok(())
            })
        };
        let malformed_snapshot_sql = format!(
            "DROP VIEW snapshots;
             CREATE VIEW snapshots AS
             SELECT {SNAPSHOT_COLUMNS} FROM snapshots_base
             UNION ALL
             SELECT CAST(NULL AS VARCHAR) AS id, created_at, repo_path, repo_key,
                    CAST('other' AS VARCHAR) AS branch,
                    commit_sha, base_ref, suite, format, report_path, warnings, metadata,
                    total_lines, covered_lines, total_branches, covered_branches,
                    total_functions, covered_functions, total_regions, covered_regions,
                    line_rate, branch_rate, function_rate, region_rate
             FROM snapshots_base
             WHERE id = '{current_snapshot_id}';"
        );
        install_malformed_snapshot_view(&malformed_snapshot_sql).unwrap();
        assert!(install_malformed_snapshot_view("THIS IS NOT VALID SQL").is_err());
        assert!(store.previous_snapshot(current_snapshot_id).is_err());
        store.close().unwrap();

        let no_worker =
            CoverageStore::open(directory.path().join("no-worker.duckdb"), test_config()).unwrap();
        no_worker.inner.compaction_thread.lock().unwrap().take();
        no_worker.close().unwrap();

        let self_closing =
            CoverageStore::open(directory.path().join("self-closing.duckdb"), test_config())
                .unwrap();
        let (start_sender, start_receiver) = std::sync::mpsc::channel();
        let (done_sender, done_receiver) = std::sync::mpsc::channel();
        let worker_store = self_closing.clone();
        let worker = std::thread::spawn(move || {
            start_receiver.recv().unwrap();
            worker_store.close().unwrap();
            done_sender.send(()).unwrap();
        });
        self_closing.inner.run_threads.lock().unwrap().push(worker);
        start_sender.send(()).unwrap();
        done_receiver.recv().unwrap();

        let panicking = CoverageStore::open(
            directory.path().join("panicking-worker.duckdb"),
            test_config(),
        )
        .unwrap();
        panicking
            .inner
            .run_threads
            .lock()
            .unwrap()
            .push(std::thread::spawn(|| panic!("injected worker panic")));
        assert!(panicking.close().is_err());

        let panicking_compaction = CoverageStore::open(
            directory.path().join("panicking-compaction-worker.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&panicking_compaction);
        *panicking_compaction.inner.compaction_thread.lock().unwrap() =
            Some(std::thread::spawn(|| panic!("injected compaction panic")));
        assert!(panicking_compaction.close().is_err());

        let settings_error = CoverageStore::open(
            directory.path().join("settings-worker-error.duckdb"),
            test_config(),
        )
        .unwrap();
        settings_error.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&settings_error);
        make_broken_view(&settings_error, "project_settings");
        settings_error.start_compaction_worker().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        settings_error.close().unwrap();

        let invalid_maintenance = CoverageStore::open(
            directory.path().join("invalid-maintenance-worker.duckdb"),
            test_config(),
        )
        .unwrap();
        invalid_maintenance
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&invalid_maintenance);
        let rewrite_invalid_settings = || {
            invalid_maintenance.with_connection(|connection| {
                connection.execute_batch(
                    "DROP INDEX IF EXISTS idx_project_settings_updated;
                     ALTER TABLE project_settings RENAME TO project_settings_base;
                     CREATE VIEW project_settings AS
                     SELECT repo_key, repo_path, created_at, updated_at, compaction_enabled,
                            compaction_after_days, compaction_interval_seconds, compaction_batch_size,
                            CAST('bad' AS VARCHAR) AS compaction_last_run_at,
                            compaction_last_status, compaction_last_snapshot_count,
                            compaction_last_bytes_before,
                            compaction_last_bytes_after
                     FROM project_settings_base;",
                )?;
                Ok(())
            })
        };
        rewrite_invalid_settings().unwrap();
        assert!(rewrite_invalid_settings().is_err());
        assert!(invalid_maintenance.project_settings().is_ok());
        invalid_maintenance.start_compaction_worker().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        stop_compaction_worker(&invalid_maintenance);
        let rewrite_invalid_type_settings = |sql: &str| {
            invalid_maintenance.with_connection(|connection| {
                connection.execute_batch(sql)?;
                Ok(())
            })
        };
        rewrite_invalid_type_settings(
            "DROP VIEW project_settings;
             CREATE VIEW project_settings AS
             SELECT repo_key, repo_path, created_at, updated_at,
                    CAST('bad' AS VARCHAR) AS compaction_enabled,
                    compaction_after_days, compaction_interval_seconds,
                    compaction_batch_size, compaction_last_run_at,
                    compaction_last_status, compaction_last_snapshot_count,
                    compaction_last_bytes_before, compaction_last_bytes_after
             FROM project_settings_base;",
        )
        .unwrap();
        assert!(invalid_maintenance.project_settings().is_err());
        let invalid_project_settings_column = |column: &str, expression: &str| {
            let columns = [
                "repo_key",
                "repo_path",
                "created_at",
                "updated_at",
                "compaction_enabled",
                "compaction_after_days",
                "compaction_interval_seconds",
                "compaction_batch_size",
                "compaction_last_run_at",
                "compaction_last_status",
                "compaction_last_snapshot_count",
                "compaction_last_bytes_before",
                "compaction_last_bytes_after",
            ];
            let projection = columns
                .iter()
                .map(|name| {
                    if *name == column {
                        format!("{expression} AS {name}")
                    } else {
                        (*name).to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "DROP VIEW project_settings;
                 CREATE VIEW project_settings AS
                 SELECT {projection} FROM project_settings_base;"
            );
            invalid_maintenance.with_connection(|connection| {
                connection
                    .execute_batch(&sql)
                    .expect("test settings projection should be created");
                Ok(())
            })
        };
        for (column, expression) in [
            ("repo_key", "CAST(7 AS INTEGER)"),
            ("repo_key", "CAST(NULL AS VARCHAR)"),
            ("repo_path", "CAST(7 AS INTEGER)"),
            ("compaction_after_days", "CAST('bad' AS VARCHAR)"),
            ("compaction_interval_seconds", "CAST('bad' AS VARCHAR)"),
            ("compaction_batch_size", "CAST('bad' AS VARCHAR)"),
            ("compaction_last_status", "CAST(1 AS INTEGER)"),
            ("compaction_last_snapshot_count", "CAST('bad' AS VARCHAR)"),
            ("compaction_last_bytes_before", "CAST('bad' AS VARCHAR)"),
            ("compaction_last_bytes_after", "CAST('bad' AS VARCHAR)"),
        ] {
            invalid_project_settings_column(column, expression).unwrap();
            assert!(invalid_maintenance.project_settings().is_err());
        }
        assert!(rewrite_invalid_type_settings("CREATE VIEW project_settings AS SELECT 1").is_err());
        invalid_maintenance.close().unwrap();

        let compaction_error = CoverageStore::open(
            directory.path().join("compaction-worker-error.duckdb"),
            test_config(),
        )
        .unwrap();
        compaction_error.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&compaction_error);
        let report = directory.path().join("worker-error.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n").unwrap();
        let snapshot = compaction_error
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        compaction_error
            .with_connection(|connection| {
                #[rustfmt::skip]
                connection.execute("UPDATE snapshots SET created_at = ? WHERE id = ?", params![Utc::now() - ChronoDuration::days(31), snapshot["id"].as_str().unwrap()]).expect("age compaction snapshot");
                Ok(())
            })
            .unwrap();
        make_broken_view(&compaction_error, "lines");
        compaction_error.start_compaction_worker().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        compaction_error.close().unwrap();

        let source_root_error = CoverageStore::open(
            directory.path().join("source-root-error.duckdb"),
            test_config(),
        )
        .unwrap();
        source_root_error.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&source_root_error);
        let source_snapshot = source_root_error
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "source-root",
            )
            .unwrap();
        source_root_error
            .with_connection(|connection| {
                connection
                    .execute(
                        "UPDATE snapshots SET repo_path = ? WHERE id = ?",
                        params!["\0", source_snapshot["id"].as_str().unwrap()],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        assert!(
            source_root_error
                .source_resolution(source_snapshot["id"].as_str().unwrap(), "src/a.py")
                .is_err()
        );
        source_root_error.close().unwrap();
    }

    #[test]
    fn storage_lifecycle_and_lock_boundaries_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());

        let unselected = CoverageStore::open(
            directory.path().join("unselected-projections.duckdb"),
            test_config(),
        )
        .unwrap();
        assert!(
            unselected
                .update_project_settings(ProjectSettingsPatch::default())
                .is_err()
        );
        assert!(unselected.project_summary().is_err());
        assert!(unselected.compact_now().is_err());
        assert!(unselected.list_worktrees(10).is_err());
        assert!(unselected.list_registered_commands(10).is_err());
        unselected.close().unwrap();

        let run_directory = directory.path().join("run-directory-conflict");
        std::fs::create_dir_all(&run_directory).unwrap();
        std::fs::write(run_directory.join("runs"), "not a directory").unwrap();
        let _ = CoverageStore::open(run_directory.join("coverage.duckdb"), test_config())
            .expect_err("run directory creation should fail");

        let pool_directory = directory.path().join("pool-directory");
        std::fs::create_dir_all(&pool_directory).unwrap();
        let _ = CoverageStore::open(pool_directory, test_config())
            .expect_err("DuckDB should reject a directory path");

        let malformed_database = directory.path().join("malformed-schema.duckdb");
        let connection = Connection::open(&malformed_database).unwrap();
        connection
            .execute("CREATE TABLE snapshots (id VARCHAR)", [])
            .unwrap();
        drop(connection);
        let _ = CoverageStore::open(malformed_database, test_config())
            .expect_err("schema bootstrap should reject incompatible tables");

        let store =
            CoverageStore::open(directory.path().join("lifecycle.duckdb"), test_config()).unwrap();
        store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&store);

        let termination_child = Command::new("sleep").arg("5").spawn().unwrap();
        let termination_control = Arc::new(Mutex::new(Some(termination_child)));
        FORCE_TERMINATE_CHILD_FAILURE.store(true, Ordering::SeqCst);
        assert!(terminate_managed_process(&termination_control).is_err());
        fn reap_test_child(control: &Arc<Mutex<Option<Child>>>) {
            if let Some(child) = control.lock().unwrap().as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        reap_test_child(&termination_control);
        let empty_control = Arc::new(Mutex::new(None));
        reap_test_child(&empty_control);

        let mut terminate_error = ManagedRunGuard::new(
            Arc::clone(&store.inner),
            "terminate-error".to_owned(),
            Arc::new(Mutex::new(None)),
            LogCaptureHandles {
                stdout: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
                stderr: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
            },
        );
        let control = Arc::clone(&terminate_error.control);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = control.lock().unwrap();
            panic!("injected managed control poison");
        }));
        let _ = terminate_error
            .finish()
            .err()
            .expect("poisoned managed control should fail finish");
        terminate_error.control.clear_poison();

        let mut capture_error = ManagedRunGuard::new(
            Arc::clone(&store.inner),
            "capture-error".to_owned(),
            Arc::new(Mutex::new(None)),
            LogCaptureHandles {
                stdout: thread::spawn(|| Err(std::io::Error::other("capture failure"))),
                stderr: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
            },
        );
        let _ = capture_error
            .finish()
            .err()
            .expect("capture join error should fail finish");

        let mut registry_error = ManagedRunGuard::new(
            Arc::clone(&store.inner),
            "registry-error".to_owned(),
            Arc::new(Mutex::new(None)),
            LogCaptureHandles {
                stdout: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
                stderr: thread::spawn(|| {
                    Ok(LogCaptureResult {
                        bytes_written: 0,
                        truncated: false,
                    })
                }),
            },
        );
        let registry_inner = Arc::clone(&registry_error.inner);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = registry_inner.active_processes.lock().unwrap();
            panic!("injected managed registry poison");
        }));
        let _ = registry_error
            .finish()
            .err()
            .expect("poisoned active registry should fail finish");
        store.inner.active_processes.clear_poison();

        let slot = Arc::new(Mutex::new(None));
        let poison_target = Arc::clone(&slot);
        let _ = std::thread::spawn(move || {
            let _guard = poison_target.lock().unwrap();
            panic!("injected compaction slot poison");
        })
        .join();
        let _ = retain_compaction_thread(&slot, Ok(thread::spawn(|| {})))
            .expect_err("poisoned compaction slot should fail");
        slot.clear_poison();

        let missing_tables = Connection::open_in_memory().unwrap();
        let _ = remove_compacted_detail(&missing_tables, "snapshot", true)
            .expect_err("missing files table should fail deletion");
        let lines_only = Connection::open_in_memory().unwrap();
        lines_only
            .execute("CREATE TABLE files (snapshot_id VARCHAR)", [])
            .unwrap();
        let _ = remove_compacted_detail(&lines_only, "snapshot", true)
            .expect_err("missing lines table should fail deletion");

        let _ = store
            .ensure_project(Path::new("\0"))
            .expect_err("NUL repository path should fail");

        let project_poison = CoverageStore::open(
            directory.path().join("project-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        let project_inner = Arc::clone(&project_poison.inner);
        let _ = std::thread::spawn(move || {
            let _guard = project_inner.project.write().unwrap();
            panic!("injected project lock poison");
        })
        .join();
        assert!(project_poison.list_snapshots(None, None, None, 10).is_err());
        assert!(
            project_poison
                .line_history("a.py", 1, None, None, 10)
                .is_err()
        );
        let _ = project_poison
            .ensure_project(directory.path())
            .expect_err("poisoned project lock should fail selection");
        project_poison.inner.project.clear_poison();
        project_poison.close().unwrap();

        let gate_poison =
            CoverageStore::open(directory.path().join("gate-poison.duckdb"), test_config())
                .unwrap();
        fn no_op_connection(_: &Connection) -> AppResult<()> {
            Ok(())
        }
        assert!(gate_poison.with_connection_mut(no_op_connection).is_ok());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = gate_poison.inner.write_gate.lock().unwrap();
            panic!("injected write gate poison");
        }));
        assert!(gate_poison.with_connection(no_op_connection).is_ok());
        assert!(gate_poison.with_connection_mut(no_op_connection).is_ok());
        gate_poison
            .ensure_project(directory.path())
            .expect("poisoned write gate should be recoverable");
        gate_poison.inner.write_gate.clear_poison();
        gate_poison.close().unwrap();

        let compaction_thread_poison = CoverageStore::open(
            directory
                .path()
                .join("compaction-thread-update-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        compaction_thread_poison
            .ensure_project(directory.path())
            .unwrap();
        let compaction_thread = &compaction_thread_poison.inner.compaction_thread;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = compaction_thread.lock().unwrap();
            panic!("injected compaction-thread update lock poison");
        }));
        assert!(
            compaction_thread_poison
                .update_project_settings(ProjectSettingsPatch::default())
                .is_err()
        );
        compaction_thread_poison
            .inner
            .compaction_thread
            .clear_poison();
        compaction_thread_poison.close().unwrap();

        let active_process_poison = CoverageStore::open(
            directory.path().join("active-process-lock-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        let active_processes = &active_process_poison.inner.active_processes;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = active_processes.lock().unwrap();
            panic!("injected active-process lock poison");
        }));
        assert!(active_process_poison.close().is_err());
        active_process_poison.inner.active_processes.clear_poison();
        active_process_poison.close().unwrap();

        let tracker_poison = CoverageStore::open(
            directory.path().join("tracker-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        tracker_poison.ensure_project(directory.path()).unwrap();
        tracker_poison.inner.query_tracker.poison_active_for_test();
        let _ = tracker_poison
            .project_settings()
            .expect_err("poisoned query tracker should fail");
        tracker_poison
            .inner
            .query_tracker
            .clear_active_poison_for_test();
        tracker_poison.close().unwrap();

        let pool_poison =
            CoverageStore::open(directory.path().join("pool-poison.duckdb"), test_config())
                .unwrap();
        pool_poison.ensure_project(directory.path()).unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = pool_poison.inner.pool.lock().unwrap();
            panic!("injected pool lock poison");
        }));
        let _ = pool_poison
            .project_settings()
            .expect_err("poisoned pool lock should fail");
        pool_poison.inner.pool.clear_poison();
        pool_poison.close().unwrap();

        let compaction_lock = CoverageStore::open(
            directory.path().join("compaction-lock-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&compaction_lock);
        let compaction_inner = Arc::clone(&compaction_lock.inner);
        let _ = std::thread::spawn(move || {
            let _guard = compaction_inner.compaction_thread.lock().unwrap();
            panic!("injected compaction thread lock poison");
        })
        .join();
        let _ = compaction_lock
            .close()
            .expect_err("poisoned compaction thread lock should fail close");
        compaction_lock.inner.compaction_thread.clear_poison();
        compaction_lock.close().unwrap();

        let run_thread_lock = CoverageStore::open(
            directory.path().join("run-thread-lock-poison.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&run_thread_lock);
        let run_inner = Arc::clone(&run_thread_lock.inner);
        let _ = std::thread::spawn(move || {
            let _guard = run_inner.run_threads.lock().unwrap();
            panic!("injected run thread lock poison");
        })
        .join();
        let _ = run_thread_lock
            .close()
            .expect_err("poisoned run thread lock should fail close");
        run_thread_lock.inner.run_threads.clear_poison();
        run_thread_lock.close().unwrap();

        store.close().unwrap();
    }

    #[test]
    fn time_and_delta_helpers_handle_missing_and_valid_values() {
        let old = (Utc::now() - ChronoDuration::seconds(5)).to_rfc3339();
        assert!(run_duration_ms(Some(&json!(old)), None).unwrap() >= 0);
        assert!(run_duration_ms(Some(&Value::Null), Some(&json!(old))).unwrap() >= 0);
        assert!(run_duration_ms(Some(&json!("bad")), None).is_err());
        assert!(run_duration_ms(Some(&json!(1)), None).is_err());
        assert_eq!(run_duration_ms(None, None).unwrap(), 0);
        assert!(
            maintenance_due(&ProjectSettings {
                compaction_last_run_at: None,
                ..test_settings()
            })
            .unwrap()
        );
        assert!(
            maintenance_due(&ProjectSettings {
                compaction_last_run_at: Some(old.clone()),
                compaction_interval_seconds: 1,
                ..test_settings()
            })
            .unwrap()
        );
        let future = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
        assert!(
            !maintenance_due(&ProjectSettings {
                compaction_last_run_at: Some(future),
                compaction_interval_seconds: 3600,
                ..test_settings()
            })
            .unwrap()
        );
        assert!(
            maintenance_due(&ProjectSettings {
                compaction_last_run_at: Some("bad".to_owned()),
                ..test_settings()
            })
            .is_err()
        );
        assert_eq!(delta(Some(&json!(3.0)), Some(&json!(1.0))), json!(2.0));
        assert!(delta(Some(&Value::Null), Some(&json!(1.0))).is_null());
        let current = json!({"line_rate":0.75,"covered_lines":3,"total_lines":4,"branch_rate":null,"covered_branches":0,"total_branches":0,"function_rate":null,"covered_functions":0,"total_functions":0,"region_rate":null,"covered_regions":0,"total_regions":0});
        let baseline = json!({"line_rate":0.5,"covered_lines":2,"total_lines":4,"branch_rate":0.2,"covered_branches":0,"total_branches":0,"function_rate":null,"covered_functions":0,"total_functions":0,"region_rate":null,"covered_regions":0,"total_regions":0});
        assert_eq!(
            overall_delta(&current, &baseline).unwrap()["covered_lines_delta"],
            1
        );
        let mut overflowing_current = current.clone();
        overflowing_current["covered_lines"] = json!(i64::MIN);
        assert!(overall_delta(&overflowing_current, &baseline).is_err());
        for key in [
            "line_rate",
            "branch_rate",
            "function_rate",
            "region_rate",
            "covered_lines",
            "total_lines",
            "covered_branches",
            "total_branches",
            "covered_functions",
            "total_functions",
            "covered_regions",
            "total_regions",
        ] {
            let mut missing_current = current.clone();
            missing_current.as_object_mut().unwrap().remove(key);
            assert!(overall_delta(&missing_current, &baseline).is_err());
            let mut missing_baseline = baseline.clone();
            missing_baseline.as_object_mut().unwrap().remove(key);
            assert!(overall_delta(&current, &missing_baseline).is_err());
        }
        assert_eq!(status_order(&json!({"status":"regressed"})), 0);
        assert_eq!(status_order(&json!({"status":"improved"})), 1);
        assert_eq!(status_order(&json!({"status":"same"})), 2);
        assert_eq!(line_regions(&[5, 1, 2, 2, 4])[0]["start"], 1);
        assert_eq!(line_regions(&[5, 1, 2, 2, 4])[1]["start"], 4);
        assert!(line_regions(&[]).is_empty());
        let target_left =
            json!({"file_path":"b.py","priority":20,"uncovered_lines":2,"line_rate":0.5});
        let target_right =
            json!({"file_path":"a.py","priority":10,"uncovered_lines":1,"line_rate":0.8});
        assert_eq!(
            target_order(&target_left, &target_right, "priority"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            target_order(&target_left, &target_right, "uncovered_lines"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            target_order(&target_left, &target_right, "line_rate"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            target_order(&target_left, &target_right, "file_path"),
            std::cmp::Ordering::Greater
        );
        let missing_rate =
            json!({"file_path":"c.py","priority":1,"uncovered_lines":0,"line_rate":null});
        assert_eq!(
            target_order(&missing_rate, &target_left, "line_rate"),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            target_order(&target_left, &missing_rate, "line_rate"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            target_order(&missing_rate, &missing_rate, "line_rate"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(insight_order(&json!({"severity":"high"})), 0);
        assert_eq!(insight_order(&json!({"severity":"medium"})), 1);
        assert_eq!(insight_order(&json!({"severity":"info"})), 2);
        assert_eq!(coverage_target_priority(2, 3, 4).unwrap(), 250);
        assert!(coverage_target_priority(i64::MAX, 0, 0).is_err());
        assert!(coverage_target_priority(0, i64::MAX, 0).is_err());
        assert!(coverage_target_priority(0, 0, i64::MAX).is_err());
        assert_eq!(json_string("{\"value\":1}".to_owned())["value"], 1);
        assert_eq!(json_string("not-json".to_owned()), json!("not-json"));
        assert!(checked_db_u32(-1, "days").is_err());
        assert!(checked_db_u64(-1, "seconds").is_err());
        assert!(persisted_value_out_of_range("days", -1).contains("days"));
        assert!(checked_duckdb_i64(i64::MAX as u64 + 1, "count").is_err());
        assert!(checked_usize_i64(usize::MAX, "payload").is_err());
        assert!(checked_add_u64(u64::MAX, 1, "count").is_err());
        assert!(checked_mul_u64(u64::MAX, 2, "bytes").is_err());
        assert!(combine_run_results(Ok(()), Err(AppError::Runtime("release".to_owned()))).is_err());
        let mut first_process_error = None;
        record_first_process_error(
            &mut first_process_error,
            Err(AppError::Runtime("first".to_owned())),
        );
        record_first_process_error(
            &mut first_process_error,
            Err(AppError::Runtime("second".to_owned())),
        );
        record_first_process_error(&mut first_process_error, Ok(()));
        assert!(first_process_error.is_some());
        let missing_claim_connection = Connection::open_in_memory().unwrap();
        assert!(claim_run(&missing_claim_connection, "missing", Utc::now()).is_err());
        let claim_schema = Connection::open_in_memory().unwrap();
        claim_schema
            .execute_batch(
                "CREATE TABLE run_jobs (id VARCHAR, status VARCHAR, started_at TIMESTAMP, error VARCHAR)",
            )
            .unwrap();
        assert!(!claim_run(&claim_schema, "missing", Utc::now()).unwrap());
        let poisoned_slots = Mutex::new(0_usize);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_slots.lock().unwrap();
            panic!("injected slot lock poison");
        }));
        assert!(release_run_slot(&poisoned_slots, &Condvar::new()).is_err());

        let settings_directory = tempfile::tempdir().unwrap();
        let settings_store = CoverageStore::open(
            settings_directory.path().join("corrupt-settings.duckdb"),
            test_config(),
        )
        .unwrap();
        settings_store
            .ensure_project(settings_directory.path())
            .unwrap();
        stop_compaction_worker(&settings_store);
        settings_store
            .with_connection(|connection| {
                #[rustfmt::skip]
                connection.execute("UPDATE project_settings SET compaction_interval_seconds = -1", []).expect("corrupt compaction interval");
                Ok(())
            })
            .unwrap();
        assert!(settings_store.project_settings().is_err());
        settings_store
            .with_connection(|connection| {
                #[rustfmt::skip]
                connection.execute("UPDATE project_settings SET compaction_interval_seconds = 3600, compaction_last_snapshot_count = -1", []).expect("corrupt compaction count");
                Ok(())
            })
            .unwrap();
        assert!(settings_store.project_settings().is_err());
        for field in [
            "compaction_after_days",
            "compaction_batch_size",
            "compaction_last_bytes_before",
            "compaction_last_bytes_after",
        ] {
            settings_store
                .with_connection(|connection| {
                    connection
                        .execute(&format!("UPDATE project_settings SET {field} = -1"), [])
                        .expect("corrupt project setting");
                    Ok(())
                })
                .unwrap();
            assert!(settings_store.project_settings().is_err());
            settings_store
                .with_connection(|connection| {
                    connection
                        .execute(&format!("UPDATE project_settings SET {field} = 0"), [])
                        .expect("restore project setting");
                    Ok(())
                })
                .unwrap();
        }
        settings_store.close().unwrap();

        let recovery_store = CoverageStore::open(
            settings_directory.path().join("recovery-error.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&recovery_store);
        recovery_store.inner.pool.lock().unwrap().take();
        recovery_store.finalize_failed_job_or_log(
            "missing",
            &AppError::Runtime("run failed".to_owned()),
            "finalize test",
        );
        recovery_store.close().unwrap();
        let finalize_error_store = CoverageStore::open(
            settings_directory
                .path()
                .join("finalize-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&finalize_error_store);
        make_broken_view(&finalize_error_store, "run_jobs");
        assert!(
            finalize_error_store
                .finalize_failed_job("missing", &AppError::Runtime("failed".to_owned()))
                .is_err()
        );
        finalize_error_store.close().unwrap();

        let retain_store = CoverageStore::open(
            settings_directory.path().join("retain-error.duckdb"),
            test_config(),
        )
        .unwrap();
        retain_store
            .ensure_project(settings_directory.path())
            .unwrap();
        stop_compaction_worker(&retain_store);
        let retain_command = retain_store
            .register_command(
                "retain-error",
                "true",
                Some(settings_directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let retain_threads = &retain_store.inner.run_threads;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = retain_threads.lock().unwrap();
            panic!("injected run-thread registry poison");
        }));
        retain_store.resume_queued_runs(vec!["missing-resumed-run".to_owned()]);
        assert!(
            retain_store
                .submit_command(retain_command["id"].as_str().unwrap(), None, None, 20)
                .is_err()
        );
        retain_store.inner.run_threads.clear_poison();
        std::thread::sleep(Duration::from_millis(100));
        retain_store.close().unwrap();
        let compact_store = CoverageStore::open(
            settings_directory.path().join("compact-project.duckdb"),
            test_config(),
        )
        .unwrap();
        compact_store
            .ensure_project(settings_directory.path())
            .unwrap();
        stop_compaction_worker(&compact_store);
        let compact_report = settings_directory.path().join("compact-project.lcov");
        std::fs::write(&compact_report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n").unwrap();
        let compact_snapshot = compact_store
            .ingest_report(
                &compact_report,
                "lcov",
                Some(settings_directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        compact_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "UPDATE snapshots SET created_at = ? WHERE id = ?",
                        params![
                            Utc::now() - ChronoDuration::days(31),
                            compact_snapshot["id"].as_str().unwrap(),
                        ],
                    )
                    .map(|_| ())
                    .map_err(AppError::from)
            })
            .unwrap();
        let compact_project = compact_store.project().unwrap();
        let compact_result = compact_store
            .compact_project(
                &compact_project,
                &compact_store.project_settings().unwrap().policy(),
            )
            .unwrap();
        assert_eq!(compact_result.compacted_snapshots, 1);
        compact_store.close().unwrap();
        assert_eq!(short_hash("repo").len(), 16);
        assert_eq!(
            required_command_id(&json!({"id":"command"}), "command").unwrap(),
            "command"
        );
        assert!(required_command_id(&json!({}), "missing").is_err());

        struct FailingReader;
        impl Read for FailingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("injected reader failure"))
            }
        }
        let mut failing_reader = FailingReader;
        assert!(compress_coverage_payload(&mut failing_reader).is_err());
    }

    #[test]
    fn strict_projection_helpers_reject_malformed_values() {
        assert!(required_field(&json!({}), "id", "projection").is_err());
        assert!(required_string_field(&json!({"id":""}), "id", "projection").is_err());
        assert!(required_i64_field(&json!({"count":"1"}), "count", "projection").is_err());
        assert!(required_bool_field(&json!({}), "enabled", "projection").is_err());
        assert!(required_bool_field(&json!({"enabled":1}), "enabled", "projection").is_err());
        assert!(required_array_field(&json!({"items":{}}), "items", "projection").is_err());
        let mut scalar_projection = json!("scalar");
        assert!(required_object_mut(&mut scalar_projection, "projection").is_err());
        let mut object_projection = json!({"id":"projection"});
        assert!(required_object_mut(&mut object_projection, "projection").is_ok());
        let mut comparison = json!({});
        assert!(CoverageStore::attach_worktree_to_comparison(
            &mut comparison,
            json!({"id":"worktree"}),
        )
        .is_ok());
        assert_eq!(comparison["worktree"]["id"], "worktree");
        let mut scalar_comparison = json!("scalar");
        assert!(
            CoverageStore::attach_worktree_to_comparison(&mut scalar_comparison, json!({}),)
                .is_err()
        );
        assert!(optional_i64_field(&json!({}), "count", "projection").is_err());
        assert_eq!(
            optional_i64_field(&json!({"count":null}), "count", "projection").unwrap(),
            None
        );
        assert!(optional_i64_field(&json!({"count":"1"}), "count", "projection").is_err());
        assert_eq!(uncovered_metric(3, 1, "lines").unwrap(), 2);
        assert!(uncovered_metric(-1, 0, "lines").is_err());
        assert!(uncovered_metric(1, 2, "lines").is_err());
        assert!(overall_delta(&json!({}), &json!({})).is_err());
        assert!(CoverageStore::decorate_terminal_run(json!("scalar")).is_err());
        assert!(
            CoverageStore::decorate_queued_run(json!("scalar"), "queued".to_owned(), false, None)
                .is_err()
        );
        assert!(
            CoverageStore::decorate_queued_run(
                json!({
                    "started_at": 1,
                    "queued_at": null,
                    "stdout_path": "stdout",
                    "stderr_path": "stderr"
                }),
                "queued".to_owned(),
                false,
                None,
            )
            .is_err()
        );
        assert!(
            CoverageStore::decorate_queued_run(
                json!({"queued_at":null,"started_at":null,"stderr_path":"stderr"}),
                "queued".to_owned(),
                false,
                None,
            )
            .is_err()
        );
        assert!(
            CoverageStore::decorate_queued_run(
                json!({"queued_at":null,"started_at":null,"stdout_path":"stdout"}),
                "queued".to_owned(),
                false,
                None,
            )
            .is_err()
        );
        assert!(
            CoverageStore::decorate_queued_run(
                json!({"queued_at":null,"started_at":null,"stdout_path":"stdout","stderr_path":"stderr"}),
                "queued".to_owned(),
                false,
                Some(1),
            )
            .is_ok()
        );
        assert!(summary_line_limit(Some(&json!("1"))).is_err());
        assert!(summary_line_limit(Some(&json!(501))).is_err());
        assert!(timeout_duration(Some(-1)).is_err());
        assert_eq!(timeout_duration(None).unwrap(), None);
        assert!(normalize_artifact_specs(json!([])).is_err());
    }

    #[test]
    fn pooled_shutdown_and_checkout_deadlines_are_explicit() {
        fn no_op_connection(_: &Connection) -> AppResult<()> {
            Ok(())
        }

        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.db_pool_size = 1;
        config.db_acquire_timeout_ms = 50;
        config.db_query_timeout_ms = 100;
        let store = CoverageStore::open(directory.path().join("busy.duckdb"), config).unwrap();
        assert!(format!("{store:?}").contains("CoverageStore"));
        let pool = store.inner.pool.lock().unwrap().clone().unwrap();
        let held = checkout(&pool, Duration::from_millis(50), "test").unwrap();
        let _ = store
            .with_read_connection(no_op_connection)
            .expect_err("held pool connection should be busy");
        drop(held);
        assert!(store.with_read_connection(no_op_connection).is_ok());
        store.close().unwrap();

        let closed_pool_store =
            CoverageStore::open(directory.path().join("closed-pool.duckdb"), test_config())
                .unwrap();
        closed_pool_store.inner.pool.lock().unwrap().take();
        let _ = closed_pool_store
            .with_read_connection(no_op_connection)
            .expect_err("closed pool should reject reads");
        closed_pool_store.close().unwrap();

        let mut timeout_config = test_config();
        // Schema bootstrap is intentionally exercised under coverage
        // instrumentation; keep this deadline above startup variance while
        // retaining a bounded shutdown assertion below.
        timeout_config.db_query_timeout_ms = 1_000;
        let timeout_store = CoverageStore::open(
            directory.path().join("shutdown-timeout.duckdb"),
            timeout_config,
        )
        .unwrap();
        let connection = Connection::open_in_memory().unwrap();
        let guard = timeout_store
            .inner
            .query_tracker
            .begin(connection.interrupt_handle())
            .unwrap();
        let error = timeout_store
            .close()
            .expect_err("active query should delay close");
        let _ = error;
        drop(guard);
        timeout_store.close().unwrap();
    }

    #[test]
    fn row_decoding_and_lock_errors_are_exercised() {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection
            .prepare("SELECT 'text', NULL, 1, TIMESTAMP '2020-01-01 00:00:00'")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(timestamp_string(row.get_ref(0).unwrap()), "text");
        assert_eq!(timestamp_string(row.get_ref(1).unwrap()), "");
        assert!(timestamp_string(row.get_ref(2).unwrap()).contains("Int"));
        assert!(timestamp_string(row.get_ref(3).unwrap()).starts_with("2020-01-01"));
        assert_eq!(
            timestamp_string(ValueRef::Timestamp(
                duckdb::types::TimeUnit::Microsecond,
                i64::MAX,
            )),
            i64::MAX.to_string()
        );
        assert!(optional_timestamp(row.get_ref(1).unwrap()).is_none());
        assert!(optional_timestamp(row.get_ref(0).unwrap()).is_some());

        let mut valid = connection
            .prepare("SELECT 'a.py', 1::BIGINT, 2::BIGINT, true, true, 0::BIGINT, 0::BIGINT, 0::BIGINT, 0::BIGINT, '{}'")
            .unwrap();
        let mut valid_rows = valid.query([]).unwrap();
        let value = line_from_row_with_file(valid_rows.next().unwrap().unwrap()).unwrap();
        assert_eq!(value["file_path"], "a.py");
        let mut invalid_file_path = connection
            .prepare("SELECT true, 1::BIGINT, 2::BIGINT, true, true, 0::BIGINT, 0::BIGINT, 0::BIGINT, 0::BIGINT, '{}'")
            .unwrap();
        let mut invalid_file_path_rows = invalid_file_path.query([]).unwrap();
        assert!(line_from_row_with_file(invalid_file_path_rows.next().unwrap().unwrap()).is_err());
        let mut invalid = connection.prepare("SELECT 1").unwrap();
        let mut invalid_rows = invalid.query([]).unwrap();
        assert!(line_from_row_with_file(invalid_rows.next().unwrap().unwrap()).is_err());
        assert!(
            lock_error(std::sync::PoisonError::new(()))
                .to_string()
                .contains("poisoned")
        );

        let project_directory = tempfile::tempdir().unwrap();
        let project_store = CoverageStore::open(
            project_directory.path().join("project-lock.duckdb"),
            test_config(),
        )
        .unwrap();
        let project_lock = &project_store.inner.project;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = project_lock.write().unwrap();
            panic!("injected project lock poison");
        }));
        assert!(project_store.project().is_err());
        project_store.inner.project.clear_poison();
        project_store.close().unwrap();

        fn assert_poisoned_mutex<T: std::fmt::Debug + Send + 'static>(value: T) {
            let lock = Arc::new(Mutex::new(value));
            let held = Arc::clone(&lock);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _guard = held.lock().unwrap();
                panic!("injected mutex poison");
            }));
            assert!(
                lock_error(lock.lock().unwrap_err())
                    .to_string()
                    .contains("poisoned")
            );
        }

        assert_poisoned_mutex(None::<JoinHandle<()>>);
        assert_poisoned_mutex(None::<Connection>);
        assert_poisoned_mutex(None::<Child>);
        assert_poisoned_mutex(Vec::<JoinHandle<()>>::new());
        assert_poisoned_mutex(HashMap::<String, Arc<Mutex<Option<Child>>>>::new());
        assert_poisoned_mutex(0usize);

        let lock = Arc::new(RwLock::new(None::<GitInfo>));
        let held = Arc::clone(&lock);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = held.write().unwrap();
            panic!("injected rwlock poison");
        }));
        assert!(
            lock_error(lock.read().unwrap_err())
                .to_string()
                .contains("poisoned")
        );
        assert!(
            lock_error(lock.write().unwrap_err())
                .to_string()
                .contains("poisoned")
        );
    }

    #[test]
    fn row_decoders_reject_each_malformed_column() {
        fn row_error<F>(connection: &Connection, values: &[String], decoder: F)
        where
            F: Fn(&Row<'_>) -> duckdb::Result<Value>,
        {
            let sql = format!("SELECT {}", values.join(", "));
            let mut statement = connection.prepare(&sql).unwrap();
            let mut rows = statement.query([]).unwrap();
            let _ = decoder(rows.next().unwrap().unwrap()).expect_err(&sql);
        }

        let connection = Connection::open_in_memory().unwrap();
        let snapshot = vec![
            "'id'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "'/repo'",
            "'repo'",
            "'main'",
            "'commit'",
            "'base'",
            "'unit'",
            "'lcov'",
            "'coverage.lcov'",
            "'[]'",
            "'{}'",
            "1",
            "1",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "1.0",
            "NULL",
            "NULL",
            "NULL",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (2, "TRUE"),
            (3, "TRUE"),
            (4, "1"),
            (5, "1"),
            (6, "1"),
            (7, "TRUE"),
            (8, "TRUE"),
            (9, "TRUE"),
            (10, "TRUE"),
            (11, "TRUE"),
            (12, "'bad'"),
            (13, "'bad'"),
            (14, "'bad'"),
            (15, "'bad'"),
            (16, "'bad'"),
            (17, "'bad'"),
            (18, "'bad'"),
            (19, "'bad'"),
            (20, "'bad'"),
            (21, "'bad'"),
            (22, "'bad'"),
            (23, "'bad'"),
        ] {
            let mut values = snapshot
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, snapshot_from_row);
        }

        let file = vec![
            "'a.py'", "1", "1", "0", "0", "0", "0", "0", "0", "1.0", "NULL", "NULL", "NULL", "'{}'",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (1, "'bad'"),
            (2, "'bad'"),
            (3, "'bad'"),
            (4, "'bad'"),
            (5, "'bad'"),
            (6, "'bad'"),
            (7, "'bad'"),
            (8, "'bad'"),
            (9, "'bad'"),
            (10, "'bad'"),
            (11, "'bad'"),
            (12, "'bad'"),
            (13, "TRUE"),
        ] {
            let mut values = file
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, file_from_row);
        }

        let line = ["1", "1", "true", "true", "0", "0", "0", "0", "'{}'"];
        for (index, bad) in [
            (0, "'bad'"),
            (1, "'bad'"),
            (2, "'bad'"),
            (3, "'bad'"),
            (4, "'bad'"),
            (5, "'bad'"),
            (6, "'bad'"),
            (7, "'bad'"),
            (8, "TRUE"),
        ] {
            let mut values = line
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, line_from_row);
        }

        let line_with_file = [
            "'a.py'", "1", "1", "true", "true", "0", "0", "0", "0", "'{}'",
        ];
        for (index, bad) in [
            (1, "'bad'"),
            (2, "'bad'"),
            (3, "'bad'"),
            (4, "'bad'"),
            (5, "'bad'"),
            (6, "'bad'"),
            (7, "'bad'"),
            (8, "'bad'"),
            (9, "TRUE"),
        ] {
            let mut values = line_with_file
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, line_from_row_with_file);
        }

        let worktree = [
            "'id'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "'name'",
            "'/path'",
            "'/repo'",
            "'repo'",
            "'main'",
            "'head'",
            "'main'",
            "'base'",
            "'snapshot'",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (2, "1"),
            (3, "TRUE"),
            (4, "TRUE"),
            (5, "TRUE"),
            (6, "1"),
            (7, "1"),
            (8, "TRUE"),
            (9, "1"),
            (10, "1"),
        ] {
            let mut values = worktree
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, worktree_from_row);
        }

        let command = vec![
            "'id'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "'name'",
            "'true'",
            "'/cwd'",
            "'/repo'",
            "'repo'",
            "'main'",
            "'commit'",
            "'/bin/sh'",
            "'tester'",
            "'note'",
            "'{}'",
            "true",
            "1",
            "1",
            "1",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (2, "1"),
            (3, "1"),
            (4, "TRUE"),
            (5, "TRUE"),
            (6, "TRUE"),
            (7, "1"),
            (8, "1"),
            (9, "1"),
            (10, "1"),
            (11, "1"),
            (12, "TRUE"),
            (13, "'bad'"),
            (14, "'bad'"),
            (15, "'bad'"),
            (16, "'bad'"),
        ] {
            let mut values = command
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, command_from_row);
        }

        let job = vec![
            "'id'",
            "'command'",
            "'name'",
            "'true'",
            "'key'",
            "'/cwd'",
            "'/repo'",
            "'repo'",
            "'main'",
            "'commit'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "NULL",
            "NULL",
            "1",
            "20",
            "'queued'",
            "'/stdout'",
            "'/stderr'",
            "''",
            "NULL",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (1, "1"),
            (2, "1"),
            (3, "1"),
            (4, "1"),
            (5, "TRUE"),
            (6, "TRUE"),
            (7, "TRUE"),
            (8, "1"),
            (9, "1"),
            (13, "'bad'"),
            (14, "'bad'"),
            (15, "1"),
            (16, "1"),
            (17, "1"),
            (18, "1"),
        ] {
            let mut values = job
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, job_from_row);
        }

        let artifact = vec![
            "'run'",
            "'coverage'",
            "'/coverage.lcov'",
            "true",
            "1",
            "'lcov'",
            "'unit'",
            "false",
            "'ingested'",
            "'snapshot'",
            "'error'",
            "'command'",
            "'name'",
            "'repo'",
            "'/repo'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "NULL",
            "'passed'",
            "0",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (1, "1"),
            (2, "1"),
            (3, "'bad'"),
            (4, "'bad'"),
            (5, "1"),
            (6, "1"),
            (7, "'bad'"),
            (8, "1"),
            (9, "1"),
            (10, "1"),
            (11, "1"),
            (12, "1"),
            (13, "1"),
            (14, "TRUE"),
            (17, "1"),
            (18, "'bad'"),
        ] {
            let mut values = artifact
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, artifact_from_row);
        }

        let run = vec![
            "'run'",
            "'command'",
            "'name'",
            "'true'",
            "'key'",
            "'/cwd'",
            "'/repo'",
            "'repo'",
            "'main'",
            "'commit'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "TIMESTAMP '2020-01-01 00:00:01'",
            "1",
            "0",
            "'passed'",
            "'/stdout'",
            "'/stderr'",
            "'{}'",
            "'[]'",
            "TIMESTAMP '2020-01-01 00:00:00'",
            "1",
            "NULL",
        ];
        for (index, bad) in [
            (0, "TRUE"),
            (1, "1"),
            (2, "1"),
            (3, "1"),
            (4, "1"),
            (5, "TRUE"),
            (6, "TRUE"),
            (7, "TRUE"),
            (8, "1"),
            (9, "1"),
            (12, "'bad'"),
            (13, "'bad'"),
            (14, "1"),
            (15, "1"),
            (16, "1"),
            (17, "TRUE"),
            (18, "TRUE"),
            (20, "'bad'"),
        ] {
            let mut values = run
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            values[index] = bad.to_owned();
            row_error(&connection, &values, run_from_row);
        }
    }

    #[test]
    fn run_scheduler_and_terminal_job_edges_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        let config = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 59_471,
            default_repository_path: None,
            common_db_path: directory.path().join("common.duckdb"),
            run_retention: 100,
            run_concurrency: 1,
            mcp_http_concurrency: 16,
            db_pool_size: 4,
            db_acquire_timeout_ms: 5_000,
            db_query_timeout_ms: 30_000,
            http_request_timeout_seconds: 60,
            http_max_body_bytes: 1_048_576,
            run_log_max_bytes: 10 * 1024 * 1024,
            default_compaction_after_days: 30,
            default_compaction_interval_seconds: 3_600,
            default_compaction_batch_size: 100,
        };
        let store = CoverageStore::open(directory.path().join("runs.duckdb"), config).unwrap();
        store.ensure_project(directory.path()).unwrap();

        let mut active = store.inner.slots.0.lock().unwrap();
        *active = 1;
        drop(active);
        let waiting_store = store.clone();
        let waiting = std::thread::spawn(move || waiting_store.execute_run("waiting"));
        std::thread::sleep(Duration::from_millis(20));
        store.inner.closing.store(true, Ordering::SeqCst);
        store.inner.slots.1.notify_all();
        assert!(waiting.join().unwrap().is_err());
        store.inner.closing.store(false, Ordering::SeqCst);
        *store.inner.slots.0.lock().unwrap() = 0;

        let slot_poison_store = store.clone();
        let _ = std::thread::spawn(move || {
            let _guard = slot_poison_store.inner.slots.0.lock().unwrap();
            panic!("injected execute slot lock poison");
        })
        .join();
        assert!(store.execute_run("slot-poison").is_err());
        store.inner.slots.0.clear_poison();

        {
            *store.inner.slots.0.lock().unwrap() = 1;
            let waiting_store = store.clone();
            let waiting = std::thread::spawn(move || waiting_store.execute_run("condvar-wait"));
            std::thread::sleep(Duration::from_millis(20));
            let condvar_poison_store = store.clone();
            let _ = std::thread::spawn(move || {
                let _guard = condvar_poison_store.inner.slots.0.lock().unwrap();
                panic!("injected condvar wait lock poison");
            })
            .join();
            store.inner.slots.1.notify_all();
            assert!(waiting.join().unwrap().is_err());
            store.inner.slots.0.clear_poison();
            *store.inner.slots.0.lock().unwrap() = 0;
        }

        let project = store.project().unwrap();
        const INSERT_EDGE_JOB_SQL: &str = "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, 'edge', 'true', NULL, ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, 20, ?, ?, ?, '', NULL)";
        let insert_job = |id: &str, command_id: &str, status: &str| {
            store.with_connection(|connection| {
                let stdout_path = directory.path().join(format!("{id}.out"));
                let stderr_path = directory.path().join(format!("{id}.err"));
                let values = params![
                    id,
                    command_id,
                    project.repo_path,
                    project.repo_path,
                    project.repo_key,
                    Utc::now(),
                    status,
                    stdout_path.to_string_lossy(),
                    stderr_path.to_string_lossy(),
                ];
                connection
                    .execute(INSERT_EDGE_JOB_SQL, values)
                    .expect("insert edge job");
                Ok(())
            })
        };

        let claim_id = Uuid::new_v4().to_string();
        insert_job(&claim_id, "claim-command", "queued").unwrap();
        assert!(
            store
                .with_connection(|connection| claim_run(connection, &claim_id, Utc::now()))
                .unwrap()
        );
        assert!(
            !store
                .with_connection(|connection| claim_run(connection, &claim_id, Utc::now()))
                .unwrap()
        );
        store
            .finalize_failed_job(&claim_id, &AppError::Runtime("claim failed".to_owned()))
            .unwrap();

        let claim_error_id = Uuid::new_v4().to_string();
        insert_job(&claim_error_id, "claim-error-command", "queued").unwrap();
        store.inject_query_fault_after(1);
        assert!(store.execute_run_with_slot(&claim_error_id).is_err());
        store.clear_query_fault();

        let claim_false_id = Uuid::new_v4().to_string();
        insert_job(&claim_false_id, "claim-false-command", "queued").unwrap();
        FORCE_CLAIM_FALSE.store(true, Ordering::SeqCst);
        assert!(store.execute_run_with_slot(&claim_false_id).is_ok());

        let nonqueued = Uuid::new_v4().to_string();
        insert_job(&nonqueued, "missing-command", "running").unwrap();
        assert!(store.execute_run_with_slot(&nonqueued).is_ok());

        let missing_command = Uuid::new_v4().to_string();
        insert_job(&missing_command, "missing-command", "queued").unwrap();
        assert!(store.execute_run_with_slot(&missing_command).is_err());

        let terminal = Uuid::new_v4().to_string();
        insert_job(&terminal, "missing-command", "cancelled").unwrap();
        assert!(store.cancel_run(&terminal, 20).is_err());

        let queued = Uuid::new_v4().to_string();
        insert_job(&queued, "missing-command", "queued").unwrap();
        store.inject_query_fault_after(1);
        assert!(store.run_result(&queued, 20).is_err());
        store.clear_query_fault();
        FORCE_QUEUE_POSITION_FAILURE.store(true, Ordering::SeqCst);
        assert!(store.run_result(&queued, 20).is_err());
        FORCE_QUEUE_POSITION_ROW_FAILURE.store(true, Ordering::SeqCst);
        assert!(store.run_result(&queued, 20).is_err());
        assert!(store.cancel_run(&queued, 20).is_ok());

        let active_id = Uuid::new_v4().to_string();
        insert_job(&active_id, "missing-command", "running").unwrap();
        let child = Command::new("sleep").arg("2").spawn().unwrap();
        store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .insert(active_id.clone(), Arc::new(Mutex::new(Some(child))));
        assert!(store.cancel_run(&active_id, 20).is_ok());
        let control = store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .remove(&active_id)
            .unwrap();
        let mut child = control.lock().unwrap().take().unwrap();
        let _ = child.wait();
        let command = store
            .register_command(
                "successful-managed-run",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let successful_run = Uuid::new_v4().to_string();
        insert_job(&successful_run, command["id"].as_str().unwrap(), "queued").unwrap();
        store.execute_run_with_slot(&successful_run).unwrap();
        let successful_result = store.run_result(&successful_run, 20).unwrap();
        assert_eq!(successful_result["terminal"], true);
        assert_eq!(successful_result["status"], "passed");
        let successful_stdout = PathBuf::from(successful_result["stdout_path"].as_str().unwrap());
        std::fs::remove_file(&successful_stdout).unwrap();
        std::fs::create_dir(&successful_stdout).unwrap();
        assert!(
            store
                .search_run_logs(
                    &successful_run,
                    &["term".to_owned()],
                    "stdout",
                    0,
                    5,
                    false,
                    10,
                )
                .is_err()
        );
        let closing_store = CoverageStore::open(
            directory.path().join("closing-managed-run.duckdb"),
            test_config(),
        )
        .unwrap();
        closing_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&closing_store);
        let closing_project = closing_store.project().unwrap();
        let closing_command = closing_store
            .register_command(
                "closing-managed-run",
                "sleep 5",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let closing_run = Uuid::new_v4().to_string();
        let closing_stdout = directory.path().join("closing.stdout");
        let closing_stderr = directory.path().join("closing.stderr");
        closing_store
            .with_connection(|connection| {
                connection
                    .execute(
                        INSERT_EDGE_JOB_SQL,
                        params![
                            closing_run,
                            closing_command["id"].as_str().unwrap(),
                            closing_project.repo_path,
                            closing_project.repo_path,
                            closing_project.repo_key,
                            Utc::now(),
                            "queued",
                            closing_stdout.to_string_lossy().to_string(),
                            closing_stderr.to_string_lossy().to_string(),
                        ],
                    )
                    .map(|_| ())
                    .map_err(AppError::from)
            })
            .unwrap();
        let closing_runner_store = closing_store.clone();
        let closing_runner_id = closing_run.clone();
        let closing_runner =
            thread::spawn(move || closing_runner_store.execute_run(&closing_runner_id));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !closing_store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .contains_key(&closing_run)
        {
            Instant::now()
                .lt(&deadline)
                .then_some(())
                .expect("queued run did not finish");
            thread::sleep(Duration::from_millis(5));
        }
        closing_store.inner.closing.store(true, Ordering::SeqCst);
        assert!(cancellation_state(&closing_store, &closing_run).unwrap());
        assert!(closing_runner.join().unwrap().is_err());
        closing_store.close().unwrap();
        let registry_error_store = CoverageStore::open(
            directory.path().join("registry-error-managed-run.duckdb"),
            test_config(),
        )
        .unwrap();
        registry_error_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&registry_error_store);
        let registry_project = registry_error_store.project().unwrap();
        let registry_command = registry_error_store
            .register_command(
                "registry-error-managed-run",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let registry_run = Uuid::new_v4().to_string();
        let registry_stdout = directory.path().join("registry.stdout");
        let registry_stderr = directory.path().join("registry.stderr");
        registry_error_store
            .with_connection(|connection| {
                connection
                    .execute(
                        INSERT_EDGE_JOB_SQL,
                        params![
                            registry_run,
                            registry_command["id"].as_str().unwrap(),
                            registry_project.repo_path,
                            registry_project.repo_path,
                            registry_project.repo_key,
                            Utc::now(),
                            "queued",
                            registry_stdout.to_string_lossy().to_string(),
                            registry_stderr.to_string_lossy().to_string(),
                        ],
                    )
                    .map(|_| ())
                    .map_err(AppError::from)
            })
            .unwrap();
        let registry_processes = &registry_error_store.inner.active_processes;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = registry_processes.lock().unwrap();
            panic!("injected active-process registry poison");
        }));
        assert!(
            registry_error_store
                .execute_run_with_slot(&registry_run)
                .is_err()
        );
        registry_error_store.inner.active_processes.clear_poison();
        registry_error_store.close().unwrap();
        store.close().unwrap();
    }

    #[test]
    fn database_leases_reject_duplicate_owners_and_release_on_close() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("leased.duckdb");
        let first = CoverageStore::open(db_path.clone(), test_config()).unwrap();
        let second = CoverageStore::open(db_path.clone(), test_config());
        let _ = second.expect_err("duplicate database lease should be busy");
        first.close().unwrap();
        let reopened = CoverageStore::open(db_path, test_config()).unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn closed_store_rejects_all_persistent_operations() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("closed.duckdb"), test_config()).unwrap();
        store.ensure_project(directory.path()).unwrap();
        store.close().unwrap();
        store.close().unwrap();

        assert!(store.project_settings().is_err());
        assert!(
            store
                .update_project_settings(ProjectSettingsPatch::default())
                .is_err()
        );
        assert!(store.project_summary().is_err());
        assert!(store.compact_now().is_err());
        assert!(store.list_worktrees(10).is_err());
        assert!(store.worktree("missing").is_err());
        assert!(
            store
                .worktree_progress("missing", "unit", None, 10)
                .is_err()
        );
        assert!(store.trend(None, None, None, None, None, 10).is_err());
        assert!(store.compare_worktree("missing", None, 10, 10).is_err());
        assert!(store.registered_command("missing").is_err());
        assert!(store.list_registered_commands(10).is_err());
        assert!(store.run_result("missing", 10).is_err());
        assert!(store.list_run_queue(10).is_err());
        assert!(store.cancel_run("missing", 10).is_err());
        assert!(store.latest_run(None).is_err());
        assert!(
            store
                .search_run_logs("missing", &["term".to_owned()], "both", 1, 10, false, 10)
                .is_err()
        );
        assert!(store.latest_artifact("coverage", None).is_err());
        assert!(store.snapshot("missing").is_err());
        assert!(store.list_snapshots(None, None, None, 10).is_err());
        assert!(store.latest_snapshot(None, None, None).is_err());
        assert!(store.files("missing", 10).is_err());
        assert!(store.file_coverage("missing", "a.py").is_err());
        assert!(store.lines("missing", "a.py", 10).is_err());
        assert!(store.lines_in_ranges("missing", "a.py", &[]).is_err());
        assert!(store.file_gaps("missing", "a.py", 10).is_err());
        assert!(store.line_history("a.py", 1, None, None, 10).is_err());
        assert!(store.source_lines("missing", "a.py", 1, 2).is_err());

        let report = CoverageReport {
            format: "lcov".to_owned(),
            report_path: "missing.lcov".to_owned(),
            files: Vec::new(),
            lines: Vec::new(),
            warnings: Vec::new(),
            metadata: Value::Null,
        };
        let git = GitInfo {
            path: directory.path().to_string_lossy().into_owned(),
            repo_path: directory.path().to_string_lossy().into_owned(),
            repo_key: directory.path().to_string_lossy().into_owned(),
            ..GitInfo::default()
        };
        assert!(
            store
                .store_report(&report, &git, None, None, None, "unit")
                .is_err()
        );
    }

    #[test]
    fn poisoned_connection_returns_lock_errors_across_projections() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("poisoned.duckdb"), test_config()).unwrap();
        store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&store);
        store.inner.closing.store(true, Ordering::SeqCst);
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.inner.write_gate.lock().unwrap();
            panic!("injected write gate lock poison");
        }));
        assert!(poisoned.is_err());

        assert!(store.ensure_project(directory.path()).is_err());
        assert!(
            store
                .compact_project(
                    &GitInfo::default(),
                    &CompactionPolicy {
                        enabled: true,
                        older_than_days: 30,
                        interval_seconds: 3_600,
                        batch_size: 100,
                    },
                )
                .is_err()
        );
        assert!(store.detail_payload("missing").is_err());
        assert!(store.project_settings().is_err());
        assert!(store.project_summary().is_err());
        assert!(store.compact_now().is_err());
        assert!(store.list_worktrees(10).is_err());
        assert!(store.worktree("missing").is_err());
        assert!(store.registered_command("missing").is_err());
        assert!(store.list_registered_commands(10).is_err());
        assert!(store.submit_command("missing", None, None, 10).is_err());
        assert!(store.run_result("missing", 10).is_err());
        assert!(store.list_run_queue(10).is_err());
        assert!(store.cancel_run("missing", 10).is_err());
        assert!(store.latest_run(None).is_err());
        assert!(store.latest_artifact("coverage", None).is_err());
        assert!(store.snapshot("missing").is_err());
        assert!(store.list_snapshots(None, None, None, 10).is_err());
        assert!(store.latest_snapshot(None, None, None).is_err());
        assert!(store.files("missing", 10).is_err());
        assert!(store.file_coverage("missing", "a.py").is_err());
        assert!(store.lines("missing", "a.py", 10).is_err());
        assert!(store.lines_in_ranges("missing", "a.py", &[]).is_err());
        assert!(store.file_gaps("missing", "a.py", 10).is_err());
        assert!(store.line_history("a.py", 1, None, None, 10).is_err());
        assert!(store.source_lines("missing", "a.py", 1, 2).is_err());

        let pool_store =
            CoverageStore::open(directory.path().join("poisoned-pool.duckdb"), test_config())
                .unwrap();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = pool_store.inner.pool.lock().unwrap();
            panic!("injected pool lock poison");
        }));
        assert!(poisoned.is_err());
        assert!(pool_store.close().is_err());
        pool_store.inner.pool.clear_poison();
        pool_store.close().unwrap();

        let lease_store = CoverageStore::open(
            directory.path().join("poisoned-lease.duckdb"),
            test_config(),
        )
        .unwrap();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lease_store.inner.db_lease.lock().unwrap();
            panic!("injected database lease lock poison");
        }));
        assert!(poisoned.is_err());
        assert!(lease_store.close().is_err());
        lease_store.inner.db_lease.clear_poison();
        lease_store.close().unwrap();
    }

    #[test]
    fn storage_connection_error_paths_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("error-paths.lcov");
        std::fs::write(&report, "TN:\nSF:a.py\nDA:1,1\nend_of_record\n").unwrap();

        let files_store =
            CoverageStore::open(directory.path().join("files-error.duckdb"), test_config())
                .unwrap();
        files_store.ensure_project(directory.path()).unwrap();
        let files_snapshot = files_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        stop_compaction_worker(&files_store);
        make_broken_view(&files_store, "files");
        assert!(
            files_store
                .files(files_snapshot["id"].as_str().unwrap(), 10)
                .is_err()
        );
        files_store.close().unwrap();

        let compact_file_store = CoverageStore::open(
            directory
                .path()
                .join("file-coverage-compacted-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        compact_file_store.ensure_project(directory.path()).unwrap();
        let compact_file_snapshot = compact_file_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        stop_compaction_worker(&compact_file_store);
        make_readonly_view(&compact_file_store, "files");
        make_broken_view(&compact_file_store, "coverage_compacted_payloads");
        assert!(
            compact_file_store
                .file_coverage(compact_file_snapshot["id"].as_str().unwrap(), "missing.py",)
                .is_err()
        );
        compact_file_store.close().unwrap();

        let latest_artifact_query_store = CoverageStore::open(
            directory.path().join("latest-artifact-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        latest_artifact_query_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&latest_artifact_query_store);
        assert!(
            latest_artifact_query_store
                .latest_artifact("coverage", Some("missing-command"))
                .is_err()
        );
        make_broken_view(&latest_artifact_query_store, "run_artifacts");
        assert!(
            latest_artifact_query_store
                .latest_artifact("coverage", None)
                .is_err()
        );
        latest_artifact_query_store.close().unwrap();

        let latest_artifact_scoped_store = CoverageStore::open(
            directory
                .path()
                .join("latest-artifact-scoped-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        latest_artifact_scoped_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&latest_artifact_scoped_store);
        let scoped_command = latest_artifact_scoped_store
            .register_command(
                "latest-artifact-scoped",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        make_broken_view(&latest_artifact_scoped_store, "run_artifacts");
        assert!(
            latest_artifact_scoped_store
                .latest_artifact("coverage", Some(scoped_command["id"].as_str().unwrap()),)
                .is_err()
        );
        latest_artifact_scoped_store.close().unwrap();

        let detail_store =
            CoverageStore::open(directory.path().join("detail-error.duckdb"), test_config())
                .unwrap();
        detail_store.ensure_project(directory.path()).unwrap();
        let detail_snapshot = detail_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        stop_compaction_worker(&detail_store);
        make_broken_view(&detail_store, "lines");
        assert!(
            detail_store
                .detail_payload(detail_snapshot["id"].as_str().unwrap())
                .is_err()
        );
        detail_store.close().unwrap();

        let detail_prepare_store = CoverageStore::open(
            directory.path().join("detail-prepare-error.duckdb"),
            test_config(),
        )
        .unwrap();
        detail_prepare_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&detail_prepare_store);
        make_broken_view(&detail_prepare_store, "files");
        assert!(
            detail_prepare_store
                .compact_snapshot_detail("repo", "missing")
                .is_err()
        );
        detail_prepare_store.close().unwrap();

        let detail_delete_store = CoverageStore::open(
            directory.path().join("detail-delete-error.duckdb"),
            test_config(),
        )
        .unwrap();
        detail_delete_store
            .ensure_project(directory.path())
            .unwrap();
        let detail_delete_snapshot = detail_delete_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                None,
                None,
                None,
                "unit",
            )
            .unwrap();
        stop_compaction_worker(&detail_delete_store);
        make_readonly_view(&detail_delete_store, "lines");
        assert!(
            detail_delete_store
                .compact_snapshot_detail("repo", detail_delete_snapshot["id"].as_str().unwrap(),)
                .is_err()
        );
        detail_delete_store.close().unwrap();

        let transaction_store = CoverageStore::open(
            directory.path().join("transaction-begin-error.duckdb"),
            test_config(),
        )
        .unwrap();
        transaction_store
            .with_connection(|connection| {
                CoverageStore::begin_compaction_transaction(connection).unwrap();
                assert!(CoverageStore::begin_compaction_transaction(connection).is_err());
                connection
                    .execute_batch("ROLLBACK")
                    .expect("transaction rollback should succeed");
                Ok(())
            })
            .unwrap();
        transaction_store.close().unwrap();

        let parsed_report =
            parse_coverage_report(&report, "lcov", Some(directory.path().to_str().unwrap()))
                .unwrap();
        let report_git = inspect_git(directory.path()).unwrap();
        for (name, table) in [
            ("snapshot", "snapshots"),
            ("file", "files"),
            ("line", "lines"),
        ] {
            let report_store = CoverageStore::open(
                directory
                    .path()
                    .join(format!("store-report-{name}-error.duckdb")),
                test_config(),
            )
            .unwrap();
            report_store.ensure_project(directory.path()).unwrap();
            stop_compaction_worker(&report_store);
            make_broken_view(&report_store, table);
            assert!(
                report_store
                    .store_report(
                        &parsed_report,
                        &report_git,
                        Some("main"),
                        report_git.commit_sha.as_deref(),
                        None,
                        "unit",
                    )
                    .is_err()
            );
            report_store.close().unwrap();
        }

        let query_map_store = CoverageStore::open(
            directory.path().join("query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        query_map_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&query_map_store);
        make_query_error_view(&query_map_store, "lines");
        query_map_store
            .with_connection(|connection| {
                let mut success_statement = connection
                    .prepare("SELECT 'a.py' AS file_path, 1 AS line_number, 1 AS hits, true AS covered, 1 AS count_line, 0 AS total_branches, 0 AS covered_branches, 0 AS total_functions, 0 AS covered_functions, '{}' AS details")
                    .unwrap();
                assert!(CoverageStore::query_detail_lines(&mut success_statement, []).is_ok());
                let mut statement = connection
                    .prepare("SELECT file_path FROM lines WHERE ? = ?")
                    .unwrap();
                assert!(CoverageStore::query_detail_lines(&mut statement, []).is_err());
                let mut params_statement = connection
                    .prepare("SELECT file_path FROM lines WHERE ? = ?")
                    .unwrap();
                assert!(
                    CoverageStore::query_detail_lines(&mut params_statement, params![]).is_err()
                );
                let mut mapper_array_statement = connection
                    .prepare("SELECT 'a.py' AS file_path, 'bad' AS line_number, 1 AS hits, true AS covered, true AS count_line, 0 AS total_branches, 0 AS covered_branches, 0 AS total_functions, 0 AS covered_functions, '{}' AS details")
                    .unwrap();
                assert!(
                    CoverageStore::query_detail_lines(&mut mapper_array_statement, []).is_err()
                );
                let mut mapper_params_statement = connection
                    .prepare("SELECT 'a.py' AS file_path, 'bad' AS line_number, 1 AS hits, true AS covered, true AS count_line, 0 AS total_branches, 0 AS covered_branches, 0 AS total_functions, 0 AS covered_functions, '{}' AS details")
                    .unwrap();
                assert!(
                    CoverageStore::query_detail_lines(&mut mapper_params_statement, params![])
                        .is_err()
                );
                Ok(())
            })
            .unwrap();
        assert!(query_map_store.detail_payload("missing").is_err());
        query_map_store.close().unwrap();

        let settings_store = CoverageStore::open(
            directory.path().join("settings-error.duckdb"),
            test_config(),
        )
        .unwrap();
        settings_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&settings_store);
        make_readonly_view(&settings_store, "project_settings");
        assert!(
            settings_store
                .update_project_settings(ProjectSettingsPatch {
                    compaction_enabled: Some(false),
                    ..Default::default()
                })
                .is_err()
        );
        settings_store.close().unwrap();

        let repository_store = CoverageStore::open(
            directory.path().join("repository-upsert-error.duckdb"),
            test_config(),
        )
        .unwrap();
        repository_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&repository_store);
        make_broken_view(&repository_store, "repositories");
        assert!(repository_store.ensure_project(directory.path()).is_err());
        repository_store.close().unwrap();

        let settings_value_store = CoverageStore::open(
            directory.path().join("settings-value-errors.duckdb"),
            test_config(),
        )
        .unwrap();
        settings_value_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&settings_value_store);
        let mut settings_base_created = false;
        let mut rewrite_settings_column = |column: &str, expression: &str| {
            if !settings_base_created {
                settings_value_store
                    .with_connection(|connection| {
                        connection
                            .execute_batch(
                                "DROP INDEX IF EXISTS idx_project_settings_updated;
                         ALTER TABLE project_settings RENAME TO project_settings_base;",
                            )
                            .expect("test settings table rename should succeed");
                        Ok(())
                    })
                    .expect("test settings table rename should succeed");
                settings_base_created = true;
            }
            let columns = [
                "repo_key",
                "repo_path",
                "created_at",
                "updated_at",
                "compaction_enabled",
                "compaction_after_days",
                "compaction_interval_seconds",
                "compaction_batch_size",
                "compaction_last_run_at",
                "compaction_last_status",
                "compaction_last_snapshot_count",
                "compaction_last_bytes_before",
                "compaction_last_bytes_after",
            ];
            let projection = columns
                .iter()
                .map(|name| {
                    if *name == column {
                        format!("{expression} AS {name}")
                    } else {
                        (*name).to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            settings_value_store.with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "CREATE VIEW project_settings AS SELECT {projection} FROM project_settings_base;"
                    ))
                    .expect("test settings projection should be created");
                Ok(())
            })
        };
        assert!(rewrite_settings_column("compaction_last_bytes_before", "-1").is_ok());
        assert!(settings_value_store.project_settings().is_err());
        settings_value_store
            .with_connection(|connection| {
                connection
                    .execute_batch("DROP VIEW project_settings")
                    .expect("test settings view teardown should succeed");
                Ok(())
            })
            .unwrap();
        assert!(rewrite_settings_column("compaction_last_bytes_after", "-1").is_ok());
        assert!(settings_value_store.project_settings().is_err());
        settings_value_store
            .with_connection(|connection| {
                connection
                    .execute_batch("DROP VIEW project_settings")
                    .expect("settings view teardown should succeed");
                Ok(())
            })
            .unwrap();
        assert!(rewrite_settings_column("repo_key", "CAST(NULL AS VARCHAR)").is_ok());
        assert!(settings_value_store.project_settings().is_err());
        settings_value_store.close().unwrap();

        let compact_store =
            CoverageStore::open(directory.path().join("compact-error.duckdb"), test_config())
                .unwrap();
        compact_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&compact_store);
        make_readonly_view(&compact_store, "project_settings");
        assert!(compact_store.compact_now().is_err());
        compact_store.close().unwrap();

        let cancel_store =
            CoverageStore::open(directory.path().join("cancel-error.duckdb"), test_config())
                .unwrap();
        cancel_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&cancel_store);
        let cancel_project = cancel_store.project().unwrap();
        let cancel_id = Uuid::new_v4().to_string();
        let cancel_stdout = directory.path().join("cancel.stdout");
        let cancel_stderr = directory.path().join("cancel.stderr");
        cancel_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, 'cancel-error', 'true', NULL, ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, 20, 'queued', ?, ?, '', NULL)",
                        params![
                            cancel_id,
                            "cancel-command",
                            cancel_project.repo_path,
                            cancel_project.repo_path,
                            cancel_project.repo_key,
                            Utc::now(),
                            cancel_stdout.to_string_lossy().to_string(),
                            cancel_stderr.to_string_lossy().to_string(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        let running_cancel_id = Uuid::new_v4().to_string();
        cancel_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, 'cancel-error-running', 'true', NULL, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL, 20, 'running', ?, ?, '', NULL)",
                        params![
                            running_cancel_id,
                            "cancel-command-running",
                            cancel_project.repo_path,
                            cancel_project.repo_path,
                            cancel_project.repo_key,
                            Utc::now(),
                            Utc::now(),
                            directory.path().join("cancel-running.stdout").to_string_lossy(),
                            directory.path().join("cancel-running.stderr").to_string_lossy(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        make_readonly_view(&cancel_store, "run_jobs");
        assert!(cancel_store.cancel_run(&cancel_id, 20).is_err());
        assert!(cancel_store.cancel_run(&running_cancel_id, 20).is_err());
        cancel_store.close().unwrap();

        let queue_query_map_store = CoverageStore::open(
            directory.path().join("queue-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        queue_query_map_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&queue_query_map_store);
        let queue_project = queue_query_map_store.project().unwrap();
        queue_query_map_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, queued_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error) VALUES ('queue-query-map', 'queue-command', 'queue', 'true', NULL, ?, ?, ?, ?, NULL, 20, 'queued', ?, ?, '')",
                        params![
                            queue_project.repo_path,
                            queue_project.repo_path,
                            queue_project.repo_key,
                            Utc::now(),
                            directory.path().join("queue.out").to_string_lossy(),
                            directory.path().join("queue.err").to_string_lossy(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        make_query_execution_view(
            &queue_query_map_store,
            "run_jobs",
            "idx_run_jobs_status_time",
        );
        assert!(queue_query_map_store.list_run_queue(10).is_err());
        queue_query_map_store.close().unwrap();

        let cancel_query_store = CoverageStore::open(
            directory.path().join("cancel-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        cancel_query_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&cancel_query_store);
        make_query_execution_view(&cancel_query_store, "runs", "idx_runs_command_time");
        assert!(cancel_query_store.cancel_run("missing", 20).is_err());
        cancel_query_store.close().unwrap();

        let cancel_terminal_query_store = CoverageStore::open(
            directory.path().join("cancel-terminal-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        cancel_terminal_query_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&cancel_terminal_query_store);
        make_broken_view(&cancel_terminal_query_store, "runs");
        assert!(
            cancel_terminal_query_store
                .cancel_run("missing", 20)
                .is_err()
        );
        cancel_terminal_query_store.close().unwrap();

        let mut prune_config = test_config();
        prune_config.run_retention = 1;
        let prune_store =
            CoverageStore::open(directory.path().join("prune-error.duckdb"), prune_config).unwrap();
        prune_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&prune_store);
        let prune_project = prune_store.project().unwrap();
        for (id, age) in [("prune-old", 2_i64), ("prune-new", 1_i64)] {
            let timestamp = Utc::now() - ChronoDuration::seconds(age);
            prune_store
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, started_at, ended_at, duration_ms, status, stdout_path, stderr_path, parsed_summary, artifact_paths) VALUES (?, 'prune-command', 'prune', 'true', ?, ?, ?, ?, ?, 1, 'passed', ?, ?, '{}', '{}')",
                            params![
                                id,
                                prune_project.repo_path,
                                prune_project.repo_path,
                                prune_project.repo_key,
                                timestamp,
                                timestamp,
                                directory.path().join(format!("{id}.stdout")).to_string_lossy().to_string(),
                                directory.path().join(format!("{id}.stderr")).to_string_lossy().to_string(),
                            ],
                        )
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        }
        make_readonly_view(&prune_store, "run_artifacts");
        assert!(prune_store.prune_runs("prune-command").is_err());
        make_broken_view(&prune_store, "runs");
        assert!(prune_store.prune_runs("prune-command").is_err());
        prune_store.close().unwrap();

        let pruned_query_store = CoverageStore::open(
            directory.path().join("pruned-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        pruned_query_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&pruned_query_store);
        let pruned_project = pruned_query_store.project().unwrap();
        pruned_query_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, started_at, ended_at, duration_ms, status, stdout_path, stderr_path, parsed_summary, artifact_paths) VALUES ('pruned-query', 'command', 'command', 'true', ?, ?, ?, ?, ?, 1, 'passed', ?, ?, '{}', '{}')",
                        params![
                            pruned_project.repo_path,
                            pruned_project.repo_path,
                            pruned_project.repo_key,
                            Utc::now(),
                            Utc::now(),
                            directory.path().join("pruned.stdout").to_string_lossy(),
                            directory.path().join("pruned.stderr").to_string_lossy(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        make_query_execution_view(&pruned_query_store, "runs", "idx_runs_command_time");
        assert!(
            pruned_query_store
                .with_connection(|connection| query_pruned_run_ids(connection, "command", 0))
                .is_err()
        );
        pruned_query_store.close().unwrap();

        let mut prune_delete_config = test_config();
        prune_delete_config.run_retention = 1;
        let prune_delete_store = CoverageStore::open(
            directory.path().join("prune-delete-runs-error.duckdb"),
            prune_delete_config,
        )
        .unwrap();
        prune_delete_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&prune_delete_store);
        let prune_delete_project = prune_delete_store.project().unwrap();
        for (id, age) in [("delete-old", 2_i64), ("delete-new", 1_i64)] {
            let timestamp = Utc::now() - ChronoDuration::seconds(age);
            prune_delete_store
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, started_at, ended_at, duration_ms, status, stdout_path, stderr_path, parsed_summary, artifact_paths) VALUES (?, 'delete-command', 'delete', 'true', ?, ?, ?, ?, ?, 1, 'passed', ?, ?, '{}', '{}')",
                            params![
                                id,
                                prune_delete_project.repo_path,
                                prune_delete_project.repo_path,
                                prune_delete_project.repo_key,
                                timestamp,
                                timestamp,
                                directory.path().join(format!("{id}.stdout")).to_string_lossy().to_string(),
                                directory.path().join(format!("{id}.stderr")).to_string_lossy().to_string(),
                            ],
                        )
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        }
        make_readonly_view(&prune_delete_store, "runs");
        assert!(prune_delete_store.prune_runs("delete-command").is_err());
        prune_delete_store.close().unwrap();

        let mut prune_directory_config = test_config();
        prune_directory_config.run_retention = 1;
        let prune_directory_store = CoverageStore::open(
            directory.path().join("prune-run-directory-error.duckdb"),
            prune_directory_config,
        )
        .unwrap();
        prune_directory_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&prune_directory_store);
        let prune_directory_project = prune_directory_store.project().unwrap();
        for (id, age) in [("directory-old", 2_i64), ("directory-new", 1_i64)] {
            let timestamp = Utc::now() - ChronoDuration::seconds(age);
            prune_directory_store
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, started_at, ended_at, duration_ms, status, stdout_path, stderr_path, parsed_summary, artifact_paths) VALUES (?, 'directory-command', 'directory', 'true', ?, ?, ?, ?, ?, 1, 'passed', ?, ?, '{}', '{}')",
                            params![
                                id,
                                prune_directory_project.repo_path,
                                prune_directory_project.repo_path,
                                prune_directory_project.repo_key,
                                timestamp,
                                timestamp,
                                directory.path().join(format!("{id}.stdout")).to_string_lossy().to_string(),
                                directory.path().join(format!("{id}.stderr")).to_string_lossy().to_string(),
                            ],
                        )
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        }
        std::fs::write(
            prune_directory_store.inner.run_dir.join("directory-old"),
            "not a run directory",
        )
        .unwrap();
        assert!(
            prune_directory_store
                .prune_runs("directory-command")
                .is_err()
        );
        prune_directory_store.close().unwrap();

        let persist_job = |store: &CoverageStore, run_id: &str, command_id: &str| {
            let project = store.project().unwrap();
            store
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO run_jobs (id, command_id, command_name, command, cwd, repo_path, repo_key, queued_at, started_at, max_summary_lines, status, stdout_path, stderr_path, error) VALUES (?, ?, 'persist', 'true', ?, ?, ?, ?, ?, 20, 'running', ?, ?, '')",
                            params![
                                run_id,
                                command_id,
                                project.repo_path,
                                project.repo_path,
                                project.repo_key,
                                Utc::now(),
                                Utc::now(),
                                directory.path().join(format!("{run_id}.stdout")).to_string_lossy().to_string(),
                                directory.path().join(format!("{run_id}.stderr")).to_string_lossy().to_string(),
                            ],
                        )
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        };
        let persist_summary = json!({"status":"passed"});
        let persist_artifact = json!({
            "kind":"coverage",
            "path":"coverage.lcov",
            "exists":true,
            "size_bytes":1,
            "coverage_format":"lcov",
            "suite":"unit",
            "modified_by_run":false,
            "ingest_status":null,
            "snapshot_id":null,
            "ingest_error":null
        });

        let persist_insert_store = CoverageStore::open(
            directory.path().join("persist-insert-error.duckdb"),
            test_config(),
        )
        .unwrap();
        persist_insert_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&persist_insert_store);
        persist_job(&persist_insert_store, "persist-insert", "persist-command");
        make_readonly_view(&persist_insert_store, "runs");
        assert!(
            persist_insert_store
                .with_connection(|connection| persist_completed_run(
                    connection,
                    "persist-insert",
                    Utc::now(),
                    1,
                    Some(0),
                    "passed",
                    &persist_summary,
                    &[],
                ))
                .is_err()
        );
        persist_insert_store.close().unwrap();

        let persist_artifact_store = CoverageStore::open(
            directory.path().join("persist-artifact-error.duckdb"),
            test_config(),
        )
        .unwrap();
        persist_artifact_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&persist_artifact_store);
        persist_job(
            &persist_artifact_store,
            "persist-artifact",
            "persist-command",
        );
        make_readonly_view(&persist_artifact_store, "run_artifacts");
        assert!(
            persist_artifact_store
                .with_connection(|connection| persist_completed_run(
                    connection,
                    "persist-artifact",
                    Utc::now(),
                    1,
                    Some(0),
                    "passed",
                    &persist_summary,
                    std::slice::from_ref(&persist_artifact),
                ))
                .is_err()
        );
        persist_artifact_store.close().unwrap();

        let persist_delete_store = CoverageStore::open(
            directory.path().join("persist-delete-error.duckdb"),
            test_config(),
        )
        .unwrap();
        persist_delete_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&persist_delete_store);
        persist_job(&persist_delete_store, "persist-delete", "persist-command");
        make_readonly_view(&persist_delete_store, "run_jobs");
        assert!(
            persist_delete_store
                .with_connection(|connection| persist_completed_run(
                    connection,
                    "persist-delete",
                    Utc::now(),
                    1,
                    Some(0),
                    "passed",
                    &persist_summary,
                    &[],
                ))
                .is_err()
        );
        persist_delete_store.close().unwrap();

        let command_store =
            CoverageStore::open(directory.path().join("command-error.duckdb"), test_config())
                .unwrap();
        command_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&command_store);
        make_readonly_view(&command_store, "registered_commands");
        assert!(
            command_store
                .register_command(
                    "command-error",
                    "true",
                    Some(directory.path()),
                    "/bin/sh",
                    None,
                    true,
                    "tester",
                    "approved",
                    true,
                )
                .is_err()
        );
        command_store.close().unwrap();

        let command_row_store = CoverageStore::open(
            directory.path().join("command-row-error.duckdb"),
            test_config(),
        )
        .unwrap();
        command_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&command_row_store);
        make_query_error_command_view(&command_row_store);
        assert!(command_row_store.list_registered_commands(10).is_err());
        assert!(command_row_store.registered_command("missing").is_err());
        command_row_store.close().unwrap();

        let command_query_map_store = CoverageStore::open(
            directory.path().join("command-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        command_query_map_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&command_query_map_store);
        command_query_map_store
            .register_command(
                "command-query-map-error",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        make_query_execution_view(
            &command_query_map_store,
            "registered_commands",
            "idx_registered_commands_name",
        );
        assert!(
            command_query_map_store
                .list_registered_commands(10)
                .is_err()
        );
        command_query_map_store.close().unwrap();

        let submit_store =
            CoverageStore::open(directory.path().join("submit-error.duckdb"), test_config())
                .unwrap();
        submit_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&submit_store);
        let submit_command = submit_store
            .register_command(
                "submit-error",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        make_broken_view(&submit_store, "run_jobs");
        assert!(
            submit_store
                .submit_command(submit_command["id"].as_str().unwrap(), None, None, 20)
                .is_err()
        );
        assert!(
            submit_store
                .idempotent_run_id("missing", Some("key"))
                .is_err()
        );
        submit_store.close().unwrap();

        let run_directory_error_store = CoverageStore::open(
            directory.path().join("run-directory-submit-error.duckdb"),
            test_config(),
        )
        .unwrap();
        run_directory_error_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&run_directory_error_store);
        let run_directory_command = run_directory_error_store
            .register_command(
                "run-directory-submit-error",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        std::fs::remove_dir_all(&run_directory_error_store.inner.run_dir).unwrap();
        std::fs::write(
            &run_directory_error_store.inner.run_dir,
            "run directory conflict",
        )
        .unwrap();
        assert!(
            run_directory_error_store
                .submit_command(
                    run_directory_command["id"].as_str().unwrap(),
                    None,
                    None,
                    20,
                )
                .is_err()
        );
        run_directory_error_store.close().unwrap();
        std::fs::remove_file(&run_directory_error_store.inner.run_dir).unwrap();
        std::fs::create_dir_all(&run_directory_error_store.inner.run_dir).unwrap();

        let idempotency_store = CoverageStore::open(
            directory.path().join("idempotency-errors.duckdb"),
            test_config(),
        )
        .unwrap();
        idempotency_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&idempotency_store);
        let idempotency_command = idempotency_store
            .register_command(
                "idempotency-errors",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let idempotency_ref = idempotency_command["id"].as_str().unwrap();
        idempotency_store.inject_query_fault_after(1);
        assert!(
            idempotency_store
                .submit_command(idempotency_ref, None, Some("first"), 20)
                .is_err()
        );
        idempotency_store.clear_query_fault();
        let _ = idempotency_store
            .submit_command(idempotency_ref, None, Some("reuse"), 20)
            .unwrap();
        idempotency_store.inject_query_fault_after(2);
        assert!(
            idempotency_store
                .submit_command(idempotency_ref, None, Some("reuse"), 20)
                .is_err()
        );
        idempotency_store.clear_query_fault();
        assert!(
            idempotency_store
                .reusable_run_id(&json!({}), idempotency_ref)
                .is_err()
        );
        assert!(
            idempotency_store
                .reusable_run_id(&json!({"cwd":"\u{0}"}), idempotency_ref)
                .is_err()
        );
        idempotency_store.inject_query_fault_after(2);
        assert!(
            idempotency_store
                .submit_command(idempotency_ref, None, None, 20)
                .is_err()
        );
        idempotency_store.clear_query_fault();
        let polling_command = idempotency_store
            .register_command(
                "polling-query-error",
                "sleep 1",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        idempotency_store.inject_query_fault_after(4);
        assert!(
            idempotency_store
                .run_command(polling_command["id"].as_str().unwrap(), None, None, 20)
                .is_err()
        );
        idempotency_store.clear_query_fault();
        idempotency_store.close().unwrap();

        let artifact_baseline_store = CoverageStore::open(
            directory
                .path()
                .join("artifact-baseline-submit-error.duckdb"),
            test_config(),
        )
        .unwrap();
        artifact_baseline_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&artifact_baseline_store);
        let artifact_baseline_command = artifact_baseline_store
            .register_command(
                "artifact-baseline-submit-error",
                "true",
                Some(directory.path()),
                "/bin/sh",
                Some(json!({"coverage":"missing.coverage"})),
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        FORCE_ARTIFACT_FINGERPRINT_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            artifact_baseline_store
                .collect_artifacts(
                    "fingerprint-error",
                    &artifact_baseline_command,
                    directory.path().to_string_lossy().as_ref(),
                    true,
                )
                .is_err()
        );
        make_broken_view(&artifact_baseline_store, "run_artifact_baselines");
        assert!(
            artifact_baseline_store
                .submit_command(
                    artifact_baseline_command["id"].as_str().unwrap(),
                    None,
                    None,
                    20,
                )
                .is_err()
        );
        assert!(
            artifact_baseline_store
                .collect_artifacts(
                    "collect-baseline-error",
                    &artifact_baseline_command,
                    directory.path().to_string_lossy().as_ref(),
                    true,
                )
                .is_err()
        );
        assert!(
            artifact_baseline_store
                .clear_artifact_baselines("missing")
                .is_err()
        );
        artifact_baseline_store.close().unwrap();

        let artifact_row_store = CoverageStore::open(
            directory.path().join("artifact-baseline-row-errors.duckdb"),
            test_config(),
        )
        .unwrap();
        artifact_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&artifact_row_store);
        artifact_row_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO run_artifact_baselines (run_id, kind, path, exists, size_bytes, modified_ns, sha256) VALUES ('row-run', 'coverage', 'coverage.lcov', true, 1, 2, 'sha')",
                        [],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        let mut artifact_baseline_base = false;
        for (column, expression) in [
            ("exists", "CAST(NULL AS BOOLEAN)"),
            ("size_bytes", "CAST(true AS BOOLEAN)"),
            ("modified_ns", "CAST(true AS BOOLEAN)"),
            ("sha256", "CAST(7 AS INTEGER)"),
        ] {
            artifact_row_store
                .with_connection(|connection| {
                    let columns = [
                        "run_id",
                        "kind",
                        "path",
                        "exists",
                        "size_bytes",
                        "modified_ns",
                        "sha256",
                    ];
                    let projection = columns
                        .iter()
                        .map(|name| {
                            if *name == column {
                                format!("{expression} AS {name}")
                            } else {
                                (*name).to_owned()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let setup = if artifact_baseline_base {
                        format!(
                            "DROP VIEW run_artifact_baselines;
                             CREATE VIEW run_artifact_baselines AS
                             SELECT {projection}
                             FROM run_artifact_baselines_base;"
                        )
                    } else {
                        artifact_baseline_base = true;
                        format!(
                            "DROP INDEX IF EXISTS idx_run_artifact_baselines_run;
                             ALTER TABLE run_artifact_baselines RENAME TO run_artifact_baselines_base;
                             CREATE VIEW run_artifact_baselines AS
                             SELECT {projection}
                             FROM run_artifact_baselines_base;"
                        )
                    };
                    connection.execute_batch(&setup).unwrap();
                    Ok(())
                })
                .unwrap();
            assert!(
                artifact_row_store
                    .artifact_baseline("row-run", "coverage")
                    .is_err()
            );
        }
        artifact_row_store.close().unwrap();

        let nul_artifact_store = CoverageStore::open(
            directory.path().join("nul-artifact-submit-error.duckdb"),
            test_config(),
        )
        .unwrap();
        nul_artifact_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&nul_artifact_store);
        let nul_artifact_command = nul_artifact_store
            .register_command(
                "nul-artifact-submit-error",
                "true",
                Some(directory.path()),
                "/bin/sh",
                Some(json!({"coverage":"\u{0}"})),
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        assert!(
            nul_artifact_store
                .submit_command(nul_artifact_command["id"].as_str().unwrap(), None, None, 20,)
                .is_err()
        );
        nul_artifact_store.close().unwrap();

        let git_directory = tempfile::tempdir().unwrap();
        init_test_git(git_directory.path());
        let worktree_query_store = CoverageStore::open(
            directory.path().join("worktree-query-error.duckdb"),
            test_config(),
        )
        .unwrap();
        worktree_query_store
            .ensure_project(git_directory.path())
            .unwrap();
        stop_compaction_worker(&worktree_query_store);
        assert!(
            worktree_query_store
                .ensure_lineage_baseline(Path::new("\0"), "main", None)
                .is_err()
        );
        make_broken_view(&worktree_query_store, "snapshots");
        assert!(
            worktree_query_store
                .ensure_lineage_baseline(git_directory.path(), "main", None)
                .is_err()
        );
        worktree_query_store.close().unwrap();

        let worktree_insert_store = CoverageStore::open(
            directory.path().join("worktree-insert-error.duckdb"),
            test_config(),
        )
        .unwrap();
        worktree_insert_store
            .ensure_project(git_directory.path())
            .unwrap();
        stop_compaction_worker(&worktree_insert_store);
        make_readonly_view(&worktree_insert_store, "worktrees");
        assert!(
            worktree_insert_store
                .ensure_lineage_baseline(git_directory.path(), "main", None)
                .is_err()
        );
        worktree_insert_store.close().unwrap();

        let worktree_row_store = CoverageStore::open(
            directory.path().join("worktree-row-error.duckdb"),
            test_config(),
        )
        .unwrap();
        worktree_row_store
            .ensure_project(git_directory.path())
            .unwrap();
        stop_compaction_worker(&worktree_row_store);
        make_query_error_worktree_view(&worktree_row_store);
        assert!(worktree_row_store.list_worktrees(10).is_err());
        worktree_row_store.close().unwrap();

        let worktree_query_map_store = CoverageStore::open(
            directory.path().join("worktree-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        worktree_query_map_store
            .ensure_project(git_directory.path())
            .unwrap();
        stop_compaction_worker(&worktree_query_map_store);
        worktree_query_map_store
            .ensure_lineage_baseline(git_directory.path(), "main", None)
            .unwrap();
        make_query_execution_view(&worktree_query_map_store, "worktrees", "idx_worktrees_repo");
        assert!(worktree_query_map_store.list_worktrees(10).is_err());
        worktree_query_map_store.close().unwrap();

        let unselected_worktree_store = CoverageStore::open(
            directory.path().join("worktree-unselected-error.duckdb"),
            test_config(),
        )
        .unwrap();
        stop_compaction_worker(&unselected_worktree_store);
        assert!(
            unselected_worktree_store
                .ensure_lineage_baseline(git_directory.path(), "main", None)
                .is_err()
        );
        unselected_worktree_store.close().unwrap();

        let execute_store =
            CoverageStore::open(directory.path().join("execute-error.duckdb"), test_config())
                .unwrap();
        execute_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&execute_store);
        let execute_project = execute_store.project().unwrap();
        let execute_command_id = Uuid::new_v4().to_string();
        let execute_run_id = Uuid::new_v4().to_string();
        let execute_stdout = directory.path().join("execute.stdout");
        let execute_stderr = directory.path().join("execute.stderr");
        std::fs::write(&execute_stdout, "").unwrap();
        std::fs::write(&execute_stderr, "").unwrap();
        execute_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO registered_commands (id, created_at, name, command, cwd, repo_path, repo_key, shell, approved_by, approval_note, artifact_specs, enabled, duration_sample_count) VALUES (?, ?, 'execute-error', 'true', ?, ?, ?, '/bin/sh', 'tester', 'approved', '{}', true, 0)",
                        params![
                            execute_command_id,
                            Utc::now(),
                            directory.path().to_string_lossy().to_string(),
                            execute_project.repo_path,
                            execute_project.repo_key,
                        ],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, queued_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error) VALUES (?, ?, 'execute-error', 'true', NULL, ?, ?, ?, ?, NULL, 20, 'queued', ?, ?, '')",
                        params![
                            execute_run_id,
                            execute_command_id,
                            directory.path().to_string_lossy().to_string(),
                            execute_project.repo_path,
                            execute_project.repo_key,
                            Utc::now(),
                            execute_stdout.to_string_lossy().to_string(),
                            execute_stderr.to_string_lossy().to_string(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        make_broken_view(&execute_store, "runs");
        assert!(execute_store.execute_run(&execute_run_id).is_err());
        execute_store.close().unwrap();
    }

    #[test]
    fn running_job_reports_connection_failure_during_polling() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("polling-error.duckdb"), test_config())
                .unwrap();
        store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&store);
        let project = store.project().unwrap();
        let command_id = Uuid::new_v4().to_string();
        let run_id = Uuid::new_v4().to_string();
        let stdout = directory.path().join("polling.stdout");
        let stderr = directory.path().join("polling.stderr");
        std::fs::write(&stdout, "").unwrap();
        std::fs::write(&stderr, "").unwrap();
        store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO registered_commands (id, created_at, name, command, cwd, repo_path, repo_key, shell, approved_by, approval_note, artifact_specs, enabled, duration_sample_count) VALUES (?, ?, 'polling-error', 'sleep 2', ?, ?, ?, '/bin/sh', 'tester', 'approved', '{}', true, 0)",
                        params![
                            command_id,
                            Utc::now(),
                            directory.path().to_string_lossy().to_string(),
                            project.repo_path,
                            project.repo_key,
                        ],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, queued_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error) VALUES (?, ?, 'polling-error', 'sleep 2', NULL, ?, ?, ?, ?, NULL, 20, 'queued', ?, ?, '')",
                        params![
                            run_id,
                            command_id,
                            directory.path().to_string_lossy().to_string(),
                            project.repo_path,
                            project.repo_key,
                            Utc::now(),
                            stdout.to_string_lossy().to_string(),
                            stderr.to_string_lossy().to_string(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();

        let runner_store = store.clone();
        let runner_id = run_id.clone();
        let runner = std::thread::spawn(move || runner_store.execute_run(&runner_id));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .contains_key(&run_id)
        {
            Instant::now()
                .lt(&deadline)
                .then_some(())
                .expect("managed process did not start");
            std::thread::sleep(Duration::from_millis(5));
        }
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.inner.write_gate.lock().unwrap();
            panic!("injected polling write gate poison");
        }));
        assert!(poisoned.is_err());
        assert!(runner.join().unwrap().is_err());
        store.inner.write_gate.clear_poison();
        assert!(
            !store
                .inner
                .active_processes
                .lock()
                .unwrap()
                .contains_key(&run_id)
        );
        let result = store.run_result(&run_id, 20).unwrap();
        assert_eq!(result["terminal"], true);
        assert_eq!(result["status"], "failed");
        store.close().unwrap();
    }

    #[test]
    fn storage_projection_query_matrix_reports_persistent_failures() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());
        let report = directory.path().join("projection-errors.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n").unwrap();
        let broken_store = |name: &str, table: &str| {
            let store = CoverageStore::open(directory.path().join(name), test_config()).unwrap();
            store.ensure_project(directory.path()).unwrap();
            stop_compaction_worker(&store);
            let snapshot = store
                .ingest_report(
                    &report,
                    "lcov",
                    Some(directory.path()),
                    Some("main"),
                    None,
                    None,
                    "unit",
                )
                .unwrap();
            let snapshot_id = snapshot["id"].as_str().unwrap().to_owned();
            make_broken_view(&store, table);
            (store, snapshot_id)
        };

        let (store, _) = broken_store("settings-projection-errors.duckdb", "project_settings");
        assert!(store.project_settings().is_err());
        assert!(
            store
                .update_project_settings(ProjectSettingsPatch::default())
                .is_err()
        );
        assert!(store.project_summary().is_err());
        assert!(store.compact_now().is_err());
        store.close().unwrap();

        let (store, snapshot_id) = broken_store("summary-projection-errors.duckdb", "snapshots");
        assert!(store.project_summary().is_err());
        assert!(store.list_snapshots(None, None, None, 10).is_err());
        assert!(store.latest_snapshot(None, None, None).is_err());
        assert!(store.snapshot(&snapshot_id).is_err());
        assert!(store.previous_snapshot(&snapshot_id).is_err());
        assert!(store.trend(None, None, None, None, None, 10).is_err());
        assert!(store.targets(&snapshot_id, "priority", 10).is_err());
        store.close().unwrap();

        let snapshot_query_map_store = CoverageStore::open(
            directory.path().join("snapshot-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        snapshot_query_map_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&snapshot_query_map_store);
        let _snapshot_query_map = snapshot_query_map_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_query_execution_view(
            &snapshot_query_map_store,
            "snapshots",
            "idx_snapshots_repo_time",
        );
        assert!(
            snapshot_query_map_store
                .list_snapshots(None, None, None, 10)
                .is_err()
        );
        snapshot_query_map_store.close().unwrap();

        let snapshot_row_store = CoverageStore::open(
            directory.path().join("snapshot-row-error.duckdb"),
            test_config(),
        )
        .unwrap();
        snapshot_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&snapshot_row_store);
        snapshot_row_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_malformed_projection_view(
            &snapshot_row_store,
            "snapshots",
            "idx_snapshots_repo_time",
            &[
                "id",
                "created_at",
                "repo_path",
                "repo_key",
                "branch",
                "commit_sha",
                "base_ref",
                "suite",
                "format",
                "report_path",
                "warnings",
                "metadata",
                "total_lines",
                "covered_lines",
                "total_branches",
                "covered_branches",
                "total_functions",
                "covered_functions",
                "total_regions",
                "covered_regions",
                "line_rate",
                "branch_rate",
                "function_rate",
                "region_rate",
            ],
            "id",
            "CAST(NULL AS VARCHAR)",
        );
        assert!(
            snapshot_row_store
                .list_snapshots(None, None, None, 10)
                .is_err()
        );
        snapshot_row_store.close().unwrap();

        let (store, _) = broken_store("summary-command-errors.duckdb", "registered_commands");
        assert!(store.project_summary().is_err());
        assert!(store.list_registered_commands(10).is_err());
        assert!(store.registered_command("missing").is_err());
        store.close().unwrap();
        let (store, _) = broken_store("summary-run-errors.duckdb", "runs");
        assert!(store.project_summary().is_err());
        assert!(store.latest_run(None).is_err());
        assert!(store.run_result("missing", 20).is_err());
        store.close().unwrap();

        let (store, snapshot_id) = broken_store("files-projection-errors.duckdb", "files");
        assert!(store.files(&snapshot_id, 10).is_err());
        assert!(store.file_coverage(&snapshot_id, "src/a.py").is_err());
        assert!(store.targets(&snapshot_id, "priority", 10).is_err());
        store.close().unwrap();

        let files_query_map_store = CoverageStore::open(
            directory.path().join("files-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        files_query_map_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&files_query_map_store);
        let files_snapshot = files_query_map_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_query_execution_view(&files_query_map_store, "files", "idx_files_snapshot");
        assert!(
            files_query_map_store
                .files(files_snapshot["id"].as_str().unwrap(), 10)
                .is_err()
        );
        files_query_map_store.close().unwrap();

        let files_row_store = CoverageStore::open(
            directory.path().join("files-row-error.duckdb"),
            test_config(),
        )
        .unwrap();
        files_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&files_row_store);
        let files_row_snapshot = files_row_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_malformed_projection_view(
            &files_row_store,
            "files",
            "idx_files_snapshot",
            &[
                "snapshot_id",
                "file_path",
                "total_lines",
                "covered_lines",
                "total_branches",
                "covered_branches",
                "total_functions",
                "covered_functions",
                "total_regions",
                "covered_regions",
                "line_rate",
                "branch_rate",
                "function_rate",
                "region_rate",
                "raw_metrics",
            ],
            "total_lines",
            "CAST('bad' AS VARCHAR)",
        );
        assert!(
            files_row_store
                .files(files_row_snapshot["id"].as_str().unwrap(), 10)
                .is_err()
        );
        files_row_store.close().unwrap();

        let (store, snapshot_id) = broken_store("lines-projection-errors.duckdb", "lines");
        assert!(store.lines(&snapshot_id, "src/a.py", 10).is_err());
        assert!(
            store
                .lines_in_ranges(&snapshot_id, "src/a.py", &[(1, 1)])
                .is_err()
        );
        assert!(store.file_gaps(&snapshot_id, "src/a.py", 10).is_err());
        assert!(
            store
                .line_history("src/a.py", 1, Some("main"), Some("unit"), 10)
                .is_err()
        );
        assert!(store.targets(&snapshot_id, "priority", 10).is_err());
        store.close().unwrap();

        let lines_query_map_store = CoverageStore::open(
            directory.path().join("lines-query-map-error.duckdb"),
            test_config(),
        )
        .unwrap();
        lines_query_map_store
            .ensure_project(directory.path())
            .unwrap();
        stop_compaction_worker(&lines_query_map_store);
        let lines_snapshot = lines_query_map_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_query_execution_view(&lines_query_map_store, "lines", "idx_lines_lookup");
        assert!(
            lines_query_map_store
                .lines(lines_snapshot["id"].as_str().unwrap(), "src/a.py", 10)
                .is_err()
        );
        lines_query_map_store.close().unwrap();

        let lines_row_store = CoverageStore::open(
            directory.path().join("lines-row-error.duckdb"),
            test_config(),
        )
        .unwrap();
        lines_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&lines_row_store);
        let lines_row_snapshot = lines_row_store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        make_malformed_projection_view(
            &lines_row_store,
            "lines",
            "idx_lines_lookup",
            &[
                "snapshot_id",
                "file_path",
                "line_number",
                "hits",
                "covered",
                "count_line",
                "total_branches",
                "covered_branches",
                "total_functions",
                "covered_functions",
                "details",
            ],
            "line_number",
            "CAST('bad' AS VARCHAR)",
        );
        assert!(
            lines_row_store
                .lines(lines_row_snapshot["id"].as_str().unwrap(), "src/a.py", 10)
                .is_err()
        );
        lines_row_store.close().unwrap();

        let (store, _) = broken_store("worktree-projection-errors.duckdb", "worktrees");
        assert!(store.list_worktrees(10).is_err());
        assert!(store.worktree("missing").is_err());
        assert!(store.worktree_baseline_snapshot("missing", "unit").is_err());
        assert!(
            store
                .worktree_progress("missing", "unit", None, 10)
                .is_err()
        );
        store.close().unwrap();

        let (store, _) = broken_store("command-projection-errors.duckdb", "registered_commands");
        assert!(store.list_registered_commands(10).is_err());
        assert!(store.registered_command("missing").is_err());
        assert!(store.submit_command("missing", None, None, 20).is_err());
        store.close().unwrap();

        let (store, snapshot_id) = broken_store("job-projection-errors.duckdb", "run_jobs");
        assert!(store.list_run_queue(10).is_err());
        assert!(store.run_result("missing", 20).is_err());
        assert!(store.cancel_run("missing", 20).is_err());
        assert!(store.job(&snapshot_id).is_err());
        assert!(
            store
                .search_run_logs("missing", &["term".to_owned()], "both", 1, 5, false, 10)
                .is_err()
        );
        store.close().unwrap();

        let job_row_store = CoverageStore::open(
            directory.path().join("job-row-projection-errors.duckdb"),
            test_config(),
        )
        .unwrap();
        job_row_store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&job_row_store);
        let job_row_project = job_row_store.project().unwrap();
        job_row_store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, cwd, repo_path, repo_key, queued_at, max_summary_lines, status, stdout_path, stderr_path, error) VALUES ('job-row', 'command', 'command', 'true', ?, ?, ?, ?, 20, 'queued', ?, ?, '')",
                        params![
                            directory.path().to_string_lossy(),
                            directory.path().to_string_lossy(),
                            job_row_project.repo_key,
                            Utc::now(),
                            directory.path().join("job.stdout").to_string_lossy(),
                            directory.path().join("job.stderr").to_string_lossy(),
                        ],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        make_malformed_projection_view(
            &job_row_store,
            "run_jobs",
            "idx_run_jobs_status_time",
            &[
                "id",
                "command_id",
                "command_name",
                "command",
                "idempotency_key",
                "cwd",
                "repo_path",
                "repo_key",
                "branch",
                "commit_sha",
                "queued_at",
                "started_at",
                "ended_at",
                "timeout_seconds",
                "max_summary_lines",
                "status",
                "stdout_path",
                "stderr_path",
                "error",
                "cancellation_requested_at",
            ],
            "id",
            "CAST(NULL AS VARCHAR)",
        );
        assert!(job_row_store.list_run_queue(10).is_err());
        job_row_store.close().unwrap();

        let (store, _) = broken_store("artifact-projection-errors.duckdb", "run_artifacts");
        assert!(store.latest_artifact("coverage", None).is_err());
        assert!(
            store
                .collect_artifacts("missing-run", &json!({}), "", true)
                .is_err()
        );
        store.close().unwrap();

        let (store, snapshot_id) = broken_store(
            "compacted-projection-errors.duckdb",
            "coverage_compacted_payloads",
        );
        assert!(store.compacted_detail(&snapshot_id).is_err());
        store.close().unwrap();
    }

    #[test]
    fn managed_run_claimed_job_validation_and_launch_errors_are_bounded() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());
        let store = CoverageStore::open(
            directory.path().join("claimed-job-errors.duckdb"),
            test_config(),
        )
        .unwrap();
        store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&store);
        let command = store
            .register_command(
                "claimed-job-errors",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let command_id = command["id"].as_str().unwrap();
        let stdout = directory.path().join("claimed.stdout");
        let stderr = directory.path().join("claimed.stderr");
        let base = json!({
            "command":"true",
            "cwd":directory.path(),
            "command_id":command_id,
            "stdout_path":stdout,
            "stderr_path":stderr,
            "timeout_seconds":null,
            "max_summary_lines":20
        });
        let mut complete_base = base.clone();
        complete_base["artifact_specs"] = json!([]);
        for key in ["command", "cwd", "command_id", "stdout_path", "stderr_path"] {
            let mut malformed = base.clone();
            malformed.as_object_mut().unwrap().remove(key);
            assert!(
                store
                    .execute_run_with_claimed_job("invalid", &malformed, Utc::now())
                    .is_err()
            );
        }
        let mut invalid_stdout = base.clone();
        invalid_stdout["stdout_path"] = json!(directory.path().join("missing").join("stdout"));
        assert!(
            store
                .execute_run_with_claimed_job("invalid-stdout", &invalid_stdout, Utc::now())
                .is_err()
        );
        let mut invalid_stderr = base.clone();
        invalid_stderr["stderr_path"] = json!(directory.path().join("missing").join("stderr"));
        assert!(
            store
                .execute_run_with_claimed_job("invalid-stderr", &invalid_stderr, Utc::now())
                .is_err()
        );
        let mut invalid_timeout = base.clone();
        invalid_timeout["timeout_seconds"] = json!(-1);
        assert!(
            store
                .execute_run_with_claimed_job("invalid-timeout", &invalid_timeout, Utc::now())
                .is_err()
        );
        let mut invalid_timeout_type = base.clone();
        invalid_timeout_type["timeout_seconds"] = json!("bad");
        assert!(
            store
                .execute_run_with_claimed_job(
                    "invalid-timeout-type",
                    &invalid_timeout_type,
                    Utc::now(),
                )
                .is_err()
        );
        let mut missing_summary_limit = complete_base.clone();
        FORCE_CANCELLATION_FALSE.store(true, Ordering::SeqCst);
        missing_summary_limit
            .as_object_mut()
            .unwrap()
            .remove("max_summary_lines");
        assert!(
            store
                .execute_run_with_claimed_job(
                    "missing-summary-limit",
                    &missing_summary_limit,
                    Utc::now(),
                )
                .is_err()
        );
        let invalid_shell = store
            .register_command(
                "invalid-shell",
                "true",
                Some(directory.path()),
                "/definitely/missing-shell",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .unwrap();
        let mut invalid_shell_job = base.clone();
        invalid_shell_job["command_id"] = invalid_shell["id"].clone();
        assert!(
            store
                .execute_run_with_claimed_job("invalid-shell", &invalid_shell_job, Utc::now())
                .is_err()
        );
        FORCE_LOG_CAPTURE_FAILURE_CALL.store(1, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("capture-first", &base, Utc::now())
                .is_err()
        );
        FORCE_LOG_CAPTURE_FAILURE_CALL.store(2, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("capture-second", &base, Utc::now())
                .is_err()
        );
        FORCE_EMPTY_MANAGED_CHILD.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("missing-child", &base, Utc::now())
                .is_err()
        );
        FORCE_TRY_WAIT_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("try-wait-error", &base, Utc::now())
                .is_err()
        );
        let mut cancellation_job = base.clone();
        cancellation_job["command"] = json!("sleep 5");
        store.inner.closing.store(false, Ordering::SeqCst);
        FORCE_CANCELLATION_STATE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_TERMINATE_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job(
                    "cancel-terminate-error",
                    &cancellation_job,
                    Utc::now()
                )
                .is_err()
        );
        store.inner.closing.store(false, Ordering::SeqCst);

        let mut timeout_job = cancellation_job.clone();
        timeout_job["timeout_seconds"] = json!(0);
        FORCE_CANCELLATION_STATE.store(false, Ordering::SeqCst);
        FORCE_CANCELLATION_FALSE.store(true, Ordering::SeqCst);
        FORCE_TIMEOUT_STATE.store(true, Ordering::SeqCst);
        FORCE_TIMEOUT_TERMINATE_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("timeout-terminate-error", &timeout_job, Utc::now())
                .is_err()
        );
        assert!(!FORCE_TIMEOUT_TERMINATE_FAILURE.load(Ordering::SeqCst));

        store.inner.closing.store(false, Ordering::SeqCst);
        FORCE_TERMINATE_CHILD_FAILURE.store(false, Ordering::SeqCst);
        FORCE_CONTROL_POISON_BEFORE_REAP.store(false, Ordering::SeqCst);
        FORCE_CANCELLATION_STATE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_TERMINATE_SUCCESS.store(true, Ordering::SeqCst);
        FORCE_CONTROL_POISON_BEFORE_REAP.store(true, Ordering::SeqCst);
        let poison_result = store.execute_run_with_claimed_job(
            "reap-control-poison",
            &cancellation_job,
            Utc::now(),
        );
        assert!(poison_result.is_err());
        assert!(!FORCE_CONTROL_POISON_BEFORE_REAP.load(Ordering::SeqCst));
        store.inner.closing.store(false, Ordering::SeqCst);
        FORCE_TERMINATE_CHILD_FAILURE.store(false, Ordering::SeqCst);
        FORCE_CONTROL_POISON_BEFORE_REAP.store(false, Ordering::SeqCst);
        FORCE_REAP_CONTROL_LOCK_FAILURE.store(false, Ordering::SeqCst);
        store.inner.closing.store(false, Ordering::SeqCst);
        FORCE_CANCELLATION_STATE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_TERMINATE_SUCCESS.store(true, Ordering::SeqCst);
        FORCE_REAP_CONTROL_LOCK_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job(
                    "reap-control-lock-error",
                    &cancellation_job,
                    Utc::now()
                )
                .is_err()
        );
        store.inner.closing.store(false, Ordering::SeqCst);
        store.inner.closing.store(false, Ordering::SeqCst);
        FORCE_CANCELLATION_STATE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_TERMINATE_SUCCESS.store(true, Ordering::SeqCst);
        FORCE_REAP_CHILD_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("reap-error", &cancellation_job, Utc::now())
                .is_err()
        );
        FORCE_REAP_CHILD_FAILURE.store(false, Ordering::SeqCst);
        store.inner.closing.store(false, Ordering::SeqCst);

        FORCE_CONTROL_LOCK_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("control-lock-error", &base, Utc::now())
                .is_err()
        );
        FORCE_RESOURCES_FINISH_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("resource-finish-error", &base, Utc::now())
                .is_err()
        );

        FORCE_SUMMARY_FAILURE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_FALSE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("summary-error", &complete_base, Utc::now())
                .is_err()
        );
        FORCE_CLEAR_ARTIFACT_BASELINES_FAILURE.store(true, Ordering::SeqCst);
        FORCE_CANCELLATION_FALSE.store(true, Ordering::SeqCst);
        assert!(
            store
                .execute_run_with_claimed_job("clear-baselines-error", &complete_base, Utc::now())
                .is_err()
        );
        store.close().unwrap();
    }

    #[test]
    fn managed_run_persistence_error_call_sites_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());

        let open_store = |name: &str| {
            let store = CoverageStore::open(directory.path().join(name), test_config()).unwrap();
            store.ensure_project(directory.path()).unwrap();
            stop_compaction_worker(&store);
            let command = store
                .register_command(
                    name,
                    "true",
                    Some(directory.path()),
                    "/bin/sh",
                    None,
                    true,
                    "tester",
                    "approved",
                    true,
                )
                .unwrap();
            (store, command)
        };
        let insert_job = |store: &CoverageStore, command: &Value, run_id: &str| {
            let project = store.project().unwrap();
            let stdout = directory.path().join(format!("{run_id}.stdout"));
            let stderr = directory.path().join(format!("{run_id}.stderr"));
            store
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO run_jobs (id, command_id, command_name, command, cwd, repo_path, repo_key, queued_at, started_at, max_summary_lines, status, stdout_path, stderr_path, error) VALUES (?, ?, ?, 'true', ?, ?, ?, ?, ?, 20, 'running', ?, ?, '')",
                            params![
                                run_id,
                                command["id"].as_str().unwrap(),
                                command["name"].as_str().unwrap(),
                                directory.path().to_string_lossy(),
                                project.repo_path,
                                project.repo_key,
                                Utc::now(),
                                Utc::now(),
                                stdout.to_string_lossy(),
                                stderr.to_string_lossy(),
                            ],
                        )
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            json!({
                "command":"true",
                "cwd":directory.path(),
                "command_id":command["id"],
                "stdout_path":stdout,
                "stderr_path":stderr,
                "timeout_seconds":null,
                "max_summary_lines":20,
                "artifact_specs":[]
            })
        };

        let (runs_store, runs_command) = open_store("managed-persist-runs");
        let runs_job = insert_job(&runs_store, &runs_command, "managed-persist-runs");
        make_readonly_view(&runs_store, "runs");
        assert!(
            runs_store
                .execute_run_with_claimed_job("managed-persist-runs", &runs_job, Utc::now())
                .is_err()
        );
        runs_store.close().unwrap();

        let (baseline_store, baseline_command) = open_store("managed-persist-baselines");
        let baseline_job = insert_job(
            &baseline_store,
            &baseline_command,
            "managed-persist-baselines",
        );
        make_readonly_view(&baseline_store, "run_artifact_baselines");
        assert!(
            baseline_store
                .execute_run_with_claimed_job(
                    "managed-persist-baselines",
                    &baseline_job,
                    Utc::now(),
                )
                .is_err()
        );
        baseline_store.close().unwrap();

        let (prune_store, prune_command) = open_store("managed-persist-prune");
        let prune_job = insert_job(&prune_store, &prune_command, "managed-persist-prune");
        FORCE_PRUNE_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            prune_store
                .execute_run_with_claimed_job("managed-persist-prune", &prune_job, Utc::now())
                .is_err()
        );
        prune_store.close().unwrap();
    }

    #[test]
    fn injected_query_faults_reach_public_storage_projections() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());
        let report = directory.path().join("fault.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n").unwrap();
        let store =
            CoverageStore::open(directory.path().join("faults.duckdb"), test_config()).unwrap();
        store.ensure_project(directory.path()).unwrap();
        stop_compaction_worker(&store);
        let snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .unwrap();
        let snapshot_id = snapshot["id"].as_str().unwrap();
        macro_rules! fault {
            ($expression:expr) => {{
                store.inject_query_fault();
                let _ = $expression
                    .err()
                    .expect("injected query fault should surface");
            }};
        }
        macro_rules! watchdog_fault {
            ($expression:expr) => {{
                inject_watchdog_failure();
                let _ = $expression
                    .err()
                    .expect("injected watchdog failure should surface");
            }};
        }

        fault!(store.ensure_project(directory.path()));
        store.inject_query_fault_after(1);
        let _ = store.project_settings();
        let _ = store.project_settings();
        fault!(store.project_settings());
        fault!(store.update_project_settings(ProjectSettingsPatch::default()));
        fault!(store.project_summary());
        fault!(store.compact_now());
        fault!(store.list_worktrees(10));
        fault!(store.worktree("missing"));
        fault!(store.worktree_baseline_snapshot("missing", "unit"));
        fault!(store.worktree_progress("missing", "unit", None, 10));
        fault!(store.trend(None, None, None, None, None, 10));
        fault!(store.compare("missing", "missing", 10, 10));
        fault!(store.compare_regions("missing", "missing", None, false, 10));
        fault!(store.changed_lines("missing", "missing", None, false, 10));
        fault!(store.changed_regions("missing", "missing", None, false, 10));
        fault!(store.list_registered_commands(10));
        fault!(store.registered_command("missing"));
        fault!(store.submit_command("missing", None, None, 20));
        fault!(store.run_command("missing", None, None, 20));
        fault!(store.run_result("missing", 20));
        fault!(store.list_run_queue(10));
        fault!(store.cancel_run("missing", 20));
        fault!(store.latest_run(None));
        fault!(store.search_run_logs("missing", &["term".to_owned()], "both", 1, 5, false, 10));
        fault!(store.latest_artifact("coverage", None));
        fault!(store.snapshot(snapshot_id));
        fault!(store.list_snapshots(None, None, None, 10));
        fault!(store.latest_snapshot(None, None, None));
        fault!(store.files(snapshot_id, 10));
        fault!(store.file_coverage(snapshot_id, "src/a.py"));
        fault!(store.lines(snapshot_id, "src/a.py", 10));
        fault!(store.lines_in_ranges(snapshot_id, "src/a.py", &[(1, 1)]));
        fault!(store.file_gaps(snapshot_id, "src/a.py", 10));
        fault!(store.line_history("src/a.py", 1, None, None, 10));
        fault!(store.source_lines(snapshot_id, "src/a.py", 1, 1));
        fault!(store.source_resolution(snapshot_id, "src/a.py"));
        fault!(store.compacted_detail(snapshot_id));
        fault!(store.targets(snapshot_id, "priority", 10));
        fault!(store.insights(snapshot_id, None, 10));
        fault!(store.compare_worktree("missing", None, 10, 10));
        fault!(store.compare_worktree_regions("missing", None, None, false, 10));
        fault!(store.ensure_lineage_baseline(directory.path(), "main", None));
        fault!(store.execute_run("missing"));
        store.inject_query_fault_after(1);
        let _ = store.with_connection(|_| Ok::<(), AppError>(()));
        store.inner.query_fault_skip.lock().unwrap().take();
        let _ = store.with_connection(|_| Ok::<(), AppError>(()));
        store.clear_query_fault();
        assert!(store.execute_sql_for_test("THIS IS NOT VALID SQL").is_err());
        watchdog_fault!(store.project_settings());
        watchdog_fault!(store.project_summary());
        watchdog_fault!(store.update_project_settings(ProjectSettingsPatch::default()));
        watchdog_fault!(store.compact_now());
        watchdog_fault!(store.list_worktrees(10));
        watchdog_fault!(store.worktree("missing"));
        watchdog_fault!(store.worktree_progress("missing", "unit", None, 10));
        watchdog_fault!(store.trend(None, None, None, None, None, 10));
        watchdog_fault!(store.compare("missing", "missing", 10, 10));
        watchdog_fault!(store.compare_regions("missing", "missing", None, false, 10));
        watchdog_fault!(store.changed_lines("missing", "missing", None, false, 10));
        watchdog_fault!(store.changed_regions("missing", "missing", None, false, 10));
        watchdog_fault!(store.list_registered_commands(10));
        watchdog_fault!(store.registered_command("missing"));
        watchdog_fault!(store.submit_command("missing", None, None, 20));
        watchdog_fault!(store.run_command("missing", None, None, 20));
        watchdog_fault!(store.run_result("missing", 20));
        watchdog_fault!(store.list_run_queue(10));
        watchdog_fault!(store.cancel_run("missing", 20));
        watchdog_fault!(store.latest_run(None));
        watchdog_fault!(store.search_run_logs(
            "missing",
            &["term".to_owned()],
            "both",
            1,
            5,
            false,
            10
        ));
        watchdog_fault!(store.latest_artifact("coverage", None));
        watchdog_fault!(store.snapshot(snapshot_id));
        watchdog_fault!(store.list_snapshots(None, None, None, 10));
        watchdog_fault!(store.latest_snapshot(None, None, None));
        watchdog_fault!(store.files(snapshot_id, 10));
        watchdog_fault!(store.file_coverage(snapshot_id, "src/a.py"));
        watchdog_fault!(store.lines(snapshot_id, "src/a.py", 10));
        watchdog_fault!(store.lines_in_ranges(snapshot_id, "src/a.py", &[(1, 1)]));
        watchdog_fault!(store.file_gaps(snapshot_id, "src/a.py", 10));
        watchdog_fault!(store.line_history("src/a.py", 1, None, None, 10));
        watchdog_fault!(store.source_lines(snapshot_id, "src/a.py", 1, 1));
        watchdog_fault!(store.source_resolution(snapshot_id, "src/a.py"));
        watchdog_fault!(store.compacted_detail(snapshot_id));
        watchdog_fault!(store.targets(snapshot_id, "priority", 10));
        watchdog_fault!(store.insights(snapshot_id, None, 10));
        watchdog_fault!(store.compare_worktree("missing", None, 10, 10));
        watchdog_fault!(store.compare_worktree_regions("missing", None, None, false, 10));
        watchdog_fault!(store.execute_run("missing"));

        macro_rules! sweep_query_boundaries {
            ($expression:expr) => {{
                for skip in 0..=40 {
                    store.inject_query_fault_after(skip);
                    let _ = $expression;
                }
            }};
        }
        sweep_query_boundaries!(store.project_settings());
        sweep_query_boundaries!(store.update_project_settings(ProjectSettingsPatch::default()));
        sweep_query_boundaries!(store.project_summary());
        sweep_query_boundaries!(store.compact_now());
        sweep_query_boundaries!(store.list_worktrees(10));
        sweep_query_boundaries!(store.worktree("missing"));
        sweep_query_boundaries!(store.worktree_baseline_snapshot("missing", "unit"));
        sweep_query_boundaries!(store.worktree_progress("missing", "unit", None, 10));
        sweep_query_boundaries!(store.trend(None, None, None, None, None, 10));
        sweep_query_boundaries!(store.compare("missing", "missing", 10, 10));
        sweep_query_boundaries!(store.compare_regions("missing", "missing", None, false, 10));
        sweep_query_boundaries!(store.changed_lines("missing", "missing", None, false, 10));
        sweep_query_boundaries!(store.changed_regions("missing", "missing", None, false, 10));
        sweep_query_boundaries!(store.list_registered_commands(10));
        sweep_query_boundaries!(store.registered_command("missing"));
        sweep_query_boundaries!(store.submit_command("missing", None, None, 20));
        sweep_query_boundaries!(store.run_command("missing", None, None, 20));
        sweep_query_boundaries!(store.run_result("missing", 20));
        sweep_query_boundaries!(store.list_run_queue(10));
        sweep_query_boundaries!(store.cancel_run("missing", 20));
        sweep_query_boundaries!(store.latest_run(None));
        sweep_query_boundaries!(store.search_run_logs(
            "missing",
            &["term".to_owned()],
            "both",
            1,
            5,
            false,
            10
        ));
        sweep_query_boundaries!(store.latest_artifact("coverage", None));
        sweep_query_boundaries!(store.snapshot(snapshot_id));
        sweep_query_boundaries!(store.list_snapshots(None, None, None, 10));
        sweep_query_boundaries!(store.latest_snapshot(None, None, None));
        sweep_query_boundaries!(store.files(snapshot_id, 10));
        sweep_query_boundaries!(store.file_coverage(snapshot_id, "src/a.py"));
        sweep_query_boundaries!(store.lines(snapshot_id, "src/a.py", 10));
        sweep_query_boundaries!(store.lines_in_ranges(snapshot_id, "src/a.py", &[(1, 1)]));
        sweep_query_boundaries!(store.file_gaps(snapshot_id, "src/a.py", 10));
        sweep_query_boundaries!(store.line_history("src/a.py", 1, None, None, 10));
        sweep_query_boundaries!(store.source_lines(snapshot_id, "src/a.py", 1, 1));
        sweep_query_boundaries!(store.source_resolution(snapshot_id, "src/a.py"));
        sweep_query_boundaries!(store.compacted_detail(snapshot_id));
        sweep_query_boundaries!(store.targets(snapshot_id, "priority", 10));
        sweep_query_boundaries!(store.insights(snapshot_id, None, 10));
        sweep_query_boundaries!(store.compare_worktree("missing", None, 10, 10));
        sweep_query_boundaries!(store.compare_worktree_regions("missing", None, None, false, 10));
        store.close().unwrap();
    }

    #[test]
    fn reopening_a_store_interrupts_running_jobs_and_resumes_queued_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("restart-recovery.duckdb");
        let store = CoverageStore::open(database.clone(), test_config()).unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
        let command = store
            .register_command(
                "restart-recovery",
                "printf resumed",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "restart recovery test",
                true,
            )
            .unwrap();
        let command_id = command["id"].as_str().unwrap();
        let queued_id = Uuid::new_v4().to_string();
        let running_id = Uuid::new_v4().to_string();
        let insert = |id: &str, status: &str, started_at: Option<DateTime<Utc>>| {
            let run_directory = database.parent().unwrap().join("runs").join(id);
            std::fs::create_dir_all(&run_directory).unwrap();
            let stdout = run_directory.join("stdout.log");
            let stderr = run_directory.join("stderr.log");
            store
                .with_connection(|connection| {
                    connection
                        .execute(
                        "INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, 'restart-recovery', 'printf resumed', NULL, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL, 20, ?, ?, ?, '', NULL)",
                        params![
                            id,
                            command_id,
                            project.repo_path,
                            project.repo_path,
                            project.repo_key,
                            Utc::now(),
                            started_at,
                            status,
                            stdout.to_string_lossy(),
                            stderr.to_string_lossy(),
                        ],
                        )
                        .expect("restart recovery test row should be inserted");
                    Ok(())
                })
                .unwrap();
        };
        insert(&queued_id, "queued", None);
        insert(&running_id, "running", Some(Utc::now()));
        store.close().unwrap();

        let reopened = CoverageStore::open(database, test_config()).unwrap();
        reopened.ensure_project(directory.path()).unwrap();
        let interrupted = reopened.run_result(&running_id, 20).unwrap();
        assert_eq!(interrupted["terminal"], true);
        assert_eq!(interrupted["status"], "interrupted");
        assert!(interrupted["error"].as_str().unwrap().contains("restarted"));

        let deadline = Instant::now() + Duration::from_secs(5);
        let resumed = loop {
            let result = reopened.run_result(&queued_id, 20).unwrap();
            if result["terminal"] == true {
                break result;
            }
            Instant::now()
                .lt(&deadline)
                .then_some(())
                .expect("queued run did not resume");
            thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(resumed["status"], "passed");
        reopened.close().unwrap();
    }

    #[test]
    fn coverage_history_worktree_and_artifact_edges_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        init_test_git(directory.path());
        let commit = String::from_utf8(
            Command::new("git")
                .arg("-C")
                .arg(directory.path())
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let report = directory.path().join("coverage.lcov");
        std::fs::write(
            &report,
            "TN:\nSF:src/a.py\nDA:1,1\nBRDA:1,0,0,1\nend_of_record\n",
        )
        .unwrap();
        let store =
            CoverageStore::open(directory.path().join("edge.duckdb"), test_config()).unwrap();
        store.ensure_project(directory.path()).unwrap();
        let baseline = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&commit),
                None,
                "unit",
            )
            .unwrap();
        let worktree = store
            .ensure_lineage_baseline(directory.path(), "main", Some("edge"))
            .unwrap();
        let worktree_id = worktree["id"].as_str().unwrap();

        std::fs::write(
            &report,
            "TN:\nSF:src/a.py\nDA:1,1\nBRDA:1,0,0,-\nend_of_record\n",
        )
        .unwrap();
        let regressed = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&commit),
                None,
                "unit",
            )
            .unwrap();
        std::fs::write(
            &report,
            "TN:\nSF:src/a.py\nDA:1,1\nBRDA:1,0,0,1\nend_of_record\n",
        )
        .unwrap();
        let improved = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&commit),
                None,
                "unit",
            )
            .unwrap();
        assert_eq!(
            store
                .changed_lines(
                    regressed["id"].as_str().unwrap(),
                    baseline["id"].as_str().unwrap(),
                    None,
                    false,
                    10,
                )
                .unwrap()
                .first()
                .and_then(|value| value["status"].as_str()),
            Some("regressed")
        );
        assert_eq!(
            store
                .changed_lines(
                    improved["id"].as_str().unwrap(),
                    regressed["id"].as_str().unwrap(),
                    None,
                    false,
                    10,
                )
                .unwrap()
                .first()
                .and_then(|value| value["status"].as_str()),
            Some("improved")
        );
        assert!(
            store
                .changed_lines(
                    regressed["id"].as_str().unwrap(),
                    baseline["id"].as_str().unwrap(),
                    Some("missing.py"),
                    false,
                    10,
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .changed_lines(
                    regressed["id"].as_str().unwrap(),
                    baseline["id"].as_str().unwrap(),
                    None,
                    true,
                    10,
                )
                .unwrap()
                .iter()
                .all(|value| value["status"] == "regressed")
        );
        for order_by in ["priority", "uncovered_lines", "line_rate", "file_path"] {
            let _ = store
                .targets(regressed["id"].as_str().unwrap(), order_by, 10)
                .unwrap();
        }
        assert!(
            store
                .insights(
                    regressed["id"].as_str().unwrap(),
                    Some(baseline["id"].as_str().unwrap()),
                    10,
                )
                .unwrap()["items"]
                .is_array()
        );

        let fallback_report = directory.path().join("fallback.lcov");
        std::fs::write(
            &fallback_report,
            "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n",
        )
        .unwrap();
        let fallback = store
            .ingest_report(
                &fallback_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some("different-commit"),
                None,
                "fallback",
            )
            .unwrap();
        let exact = store
            .ingest_report(
                &fallback_report,
                "lcov",
                Some(directory.path()),
                Some("other"),
                Some(&commit),
                None,
                "exact",
            )
            .unwrap();
        assert_eq!(
            store
                .worktree_baseline_snapshot(worktree_id, "fallback")
                .unwrap()
                .and_then(|value| value["id"].as_str().map(str::to_owned)),
            Some(fallback["id"].as_str().unwrap().to_owned())
        );
        assert_eq!(
            store
                .worktree_baseline_snapshot(worktree_id, "exact")
                .unwrap()
                .and_then(|value| value["id"].as_str().map(str::to_owned)),
            Some(exact["id"].as_str().unwrap().to_owned())
        );
        assert!(
            store
                .worktree_baseline_snapshot(worktree_id, "missing-suite")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .worktree_progress(worktree_id, "unit", Some("src/a.py"), 10)
                .unwrap()["points"]
                .is_array()
        );

        let repo_path = baseline["repo_path"].as_str().unwrap().to_owned();
        assert!(
            store
                .snapshot_for_commit(&repo_path, Some("main"), "unit", &commit)
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .snapshot_for_commit(&repo_path, Some("other"), "unit", &commit)
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .snapshot_for_commit(&repo_path, None, "unit", "missing-commit")
                .unwrap()
                .is_none()
        );
        store.inject_query_fault_after(1);
        assert!(
            store
                .snapshot_for_commit(&repo_path, Some("other"), "unit", "missing-commit")
                .is_err()
        );
        store.clear_query_fault();
        assert_eq!(
            store
                .source_resolution(baseline["id"].as_str().unwrap(), "src/a.py")
                .unwrap(),
            "snapshot_commit"
        );
        let invalid_commit = store
            .ingest_report(
                &fallback_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some("missing-commit"),
                None,
                "invalid-source",
            )
            .unwrap();
        assert_eq!(
            store
                .source_resolution(invalid_commit["id"].as_str().unwrap(), "src/a.py")
                .unwrap(),
            "current_checkout_fallback"
        );
        assert!(
            !store
                .source_lines(invalid_commit["id"].as_str().unwrap(), "src/a.py", 1, 2)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .source_resolution(invalid_commit["id"].as_str().unwrap(), "missing.py")
                .unwrap(),
            "unavailable"
        );
        let checkout_source = store
            .ingest_report(
                &fallback_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "checkout-source",
            )
            .unwrap();
        store
            .with_connection(|connection| {
                connection
                    .execute(
                        "UPDATE snapshots SET commit_sha = NULL WHERE id = ?",
                        params![checkout_source["id"].as_str().unwrap()],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(
            store
                .source_resolution(checkout_source["id"].as_str().unwrap(), "src/a.py")
                .unwrap(),
            "current_checkout"
        );
        assert_eq!(
            store
                .source_resolution(checkout_source["id"].as_str().unwrap(), "missing.py")
                .unwrap(),
            "unavailable"
        );

        let artifact_path = directory.path().join("artifact.txt");
        std::fs::write(&artifact_path, "before").unwrap();
        let artifact_command = json!({
            "name":"artifact-edge",
            "repo_path":repo_path,
            "artifact_specs":[{"kind":"coverage","path":"artifact.txt","required":false,"coverage_format":"invalid-format","suite":"unit"}]
        });
        let stale = store
            .collect_artifacts("stale-artifact", &artifact_command, &repo_path, true)
            .unwrap();
        assert_eq!(stale[0]["ingest_status"], "skipped_stale");
        let fingerprint = artifact_fingerprint(&artifact_path, true).unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO run_artifact_baselines (run_id, kind, path, exists, size_bytes, modified_ns, sha256) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![
                        "failed-artifact",
                        "coverage",
                        artifact_path.to_string_lossy(),
                        fingerprint.exists,
                        fingerprint.size_bytes,
                        fingerprint.modified_ns,
                        fingerprint.sha256,
                    ],
                ).unwrap();
                Ok(())
            })
            .unwrap();
        std::fs::write(&artifact_path, "after").unwrap();
        let failed = store
            .collect_artifacts("failed-artifact", &artifact_command, &repo_path, true)
            .unwrap();
        assert_eq!(failed[0]["ingest_status"], "failed");

        let git_directory = tempfile::tempdir().unwrap();
        init_test_git(git_directory.path());
        let no_commit = tempfile::tempdir().unwrap();
        assert!(
            store
                .reusable_run_id(
                    &json!({"cwd":no_commit.path().to_string_lossy()}),
                    "missing-command"
                )
                .unwrap()
                .is_none()
        );
        let reuse_command = store
            .register_command(
                "reuse-pass",
                "true",
                Some(git_directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "reuse edge",
                true,
            )
            .unwrap();
        assert!(
            store
                .reusable_run_id(&reuse_command, reuse_command["id"].as_str().unwrap())
                .unwrap()
                .is_none()
        );
        let passed = store
            .submit_command_with_options(
                reuse_command["id"].as_str().unwrap(),
                None,
                None,
                20,
                true,
            )
            .unwrap();
        let polling_command = store
            .register_command(
                "polling-run",
                "sleep 1",
                Some(git_directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "polling edge",
                true,
            )
            .unwrap();
        assert!(
            store
                .run_command(polling_command["id"].as_str().unwrap(), None, None, 20,)
                .unwrap()["terminal"]
                == true
        );
        assert_eq!(
            store
                .reusable_run_id(&reuse_command, reuse_command["id"].as_str().unwrap())
                .unwrap(),
            Some(passed["id"].as_str().unwrap().to_owned())
        );
        store.inject_query_fault_after(1);
        assert!(
            store
                .submit_command_with_options(
                    reuse_command["id"].as_str().unwrap(),
                    None,
                    None,
                    20,
                    true,
                )
                .is_err()
        );
        store.clear_query_fault();
        let reused = store
            .submit_command_with_options(
                reuse_command["id"].as_str().unwrap(),
                None,
                None,
                20,
                true,
            )
            .unwrap();
        assert_eq!(reused["submission_reused"], true);
        assert_eq!(reused["reuse_reason"], "unchanged_checkout");
        FORCE_REUSED_RESULT_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            store
                .submit_command_with_options(
                    reuse_command["id"].as_str().unwrap(),
                    None,
                    None,
                    20,
                    true,
                )
                .is_err()
        );
        store.inject_query_fault_after(2);
        assert!(
            store
                .submit_command_with_options(
                    reuse_command["id"].as_str().unwrap(),
                    None,
                    None,
                    20,
                    true,
                )
                .is_err()
        );
        store.clear_query_fault();
        std::fs::write(git_directory.path().join("dirty"), "dirty").unwrap();
        assert!(
            store
                .reusable_run_id(&reuse_command, reuse_command["id"].as_str().unwrap())
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(git_directory.path().join("dirty")).unwrap();
        store
            .with_connection(|connection| {
                connection
                    .execute(
                        "UPDATE runs SET repo_key = 'mismatch' WHERE id = ?",
                        params![passed["id"].as_str().unwrap()],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        assert!(
            store
                .reusable_run_id(&reuse_command, reuse_command["id"].as_str().unwrap())
                .unwrap()
                .is_none()
        );
        let fail_command = store
            .register_command(
                "reuse-fail",
                "false",
                Some(git_directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "reuse failure edge",
                true,
            )
            .unwrap();
        store
            .submit_command_with_options(
                fail_command["id"].as_str().unwrap(),
                None,
                None,
                20,
                false,
            )
            .unwrap();
        assert!(
            store
                .reusable_run_id(&fail_command, fail_command["id"].as_str().unwrap())
                .unwrap()
                .is_none()
        );
        assert!(store.registered_command("definitely-missing").is_err());

        let zero_report = directory.path().join("zero-coverage.lcov");
        let mut zero_lines = String::new();
        for line in 1..=20 {
            writeln!(zero_lines, "DA:{line},0").unwrap();
        }
        std::fs::write(
            &zero_report,
            format!("TN:\nSF:src/zero.py\n{zero_lines}end_of_record\n"),
        )
        .unwrap();
        let zero_snapshot = store
            .ingest_report(
                &zero_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&commit),
                None,
                "zero",
            )
            .unwrap();
        assert!(
            store
                .insights(zero_snapshot["id"].as_str().unwrap(), None, 10)
                .unwrap()["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["severity"] == "high")
        );

        macro_rules! sweep_valid_query_boundaries {
            ($expression:expr) => {{
                for skip in 0..=24 {
                    store.inject_query_fault_after(skip);
                    let _ = $expression;
                }
                store.inject_query_fault();
                let _ = store.project_settings();
            }};
        }
        sweep_valid_query_boundaries!(store.list_worktrees(10));
        sweep_valid_query_boundaries!(store.worktree(worktree_id));
        sweep_valid_query_boundaries!(store.worktree_baseline_snapshot(worktree_id, "fallback"));
        sweep_valid_query_boundaries!(store.worktree_progress(worktree_id, "unit", None, 10));
        sweep_valid_query_boundaries!(store.worktree_progress(
            worktree_id,
            "unit",
            Some("src/a.py"),
            10
        ));
        sweep_valid_query_boundaries!(store.trend(
            None,
            Some("main"),
            Some("unit"),
            None,
            None,
            10
        ));
        sweep_valid_query_boundaries!(store.trend(
            None,
            Some("main"),
            Some("unit"),
            Some("src/a.py"),
            None,
            10
        ));
        sweep_valid_query_boundaries!(store.trend(None, None, None, None, Some(worktree_id), 10));
        sweep_valid_query_boundaries!(store.compare_worktree(
            worktree_id,
            Some(regressed["id"].as_str().unwrap()),
            10,
            10
        ));
        sweep_valid_query_boundaries!(store.compare_worktree_regions(
            worktree_id,
            Some(regressed["id"].as_str().unwrap()),
            Some("src/a.py"),
            false,
            10
        ));
        sweep_valid_query_boundaries!(store.compare_worktree(worktree_id, None, 10, 10));
        sweep_valid_query_boundaries!(store.compare_worktree_regions(
            worktree_id,
            None,
            None,
            false,
            10
        ));
        sweep_valid_query_boundaries!(store.changed_lines(
            regressed["id"].as_str().unwrap(),
            baseline["id"].as_str().unwrap(),
            None,
            false,
            10
        ));
        sweep_valid_query_boundaries!(store.changed_regions(
            regressed["id"].as_str().unwrap(),
            baseline["id"].as_str().unwrap(),
            Some("src/a.py"),
            false,
            10
        ));
        sweep_valid_query_boundaries!(store.snapshot(baseline["id"].as_str().unwrap()));
        sweep_valid_query_boundaries!(store.list_snapshots(None, Some("main"), Some("unit"), 10));
        sweep_valid_query_boundaries!(store.latest_snapshot(None, Some("main"), Some("unit")));
        sweep_valid_query_boundaries!(store.files(baseline["id"].as_str().unwrap(), 10));
        sweep_valid_query_boundaries!(
            store.file_coverage(baseline["id"].as_str().unwrap(), "src/a.py")
        );
        sweep_valid_query_boundaries!(store.lines(
            baseline["id"].as_str().unwrap(),
            "src/a.py",
            10
        ));
        sweep_valid_query_boundaries!(store.lines_in_ranges(
            baseline["id"].as_str().unwrap(),
            "src/a.py",
            &[(1, 1)]
        ));
        sweep_valid_query_boundaries!(store.file_gaps(
            baseline["id"].as_str().unwrap(),
            "src/a.py",
            10
        ));
        sweep_valid_query_boundaries!(store.line_history(
            "src/a.py",
            1,
            Some("main"),
            Some("unit"),
            10
        ));
        sweep_valid_query_boundaries!(store.source_lines(
            baseline["id"].as_str().unwrap(),
            "src/a.py",
            1,
            1
        ));
        sweep_valid_query_boundaries!(
            store.source_resolution(baseline["id"].as_str().unwrap(), "src/a.py")
        );
        sweep_valid_query_boundaries!(store.targets(
            regressed["id"].as_str().unwrap(),
            "priority",
            10
        ));
        sweep_valid_query_boundaries!(store.insights(
            regressed["id"].as_str().unwrap(),
            Some(baseline["id"].as_str().unwrap()),
            10
        ));
        sweep_valid_query_boundaries!(store.list_registered_commands(10));
        sweep_valid_query_boundaries!(store.registered_command("reuse-pass"));
        sweep_valid_query_boundaries!(
            store.registered_command(reuse_command["id"].as_str().unwrap())
        );
        sweep_valid_query_boundaries!(
            store.latest_run(Some(reuse_command["id"].as_str().unwrap()))
        );
        sweep_valid_query_boundaries!(store.run_result(passed["id"].as_str().unwrap(), 20));
        sweep_valid_query_boundaries!(store.list_run_queue(10));
        sweep_valid_query_boundaries!(store.search_run_logs(
            passed["id"].as_str().unwrap(),
            &["output".to_owned()],
            "both",
            1,
            5,
            false,
            20
        ));
        sweep_valid_query_boundaries!(
            store.reusable_run_id(&reuse_command, reuse_command["id"].as_str().unwrap())
        );
        store.close().unwrap();
    }

    fn test_settings() -> ProjectSettings {
        ProjectSettings {
            repo_key: "repo".to_owned(),
            repo_path: "/repo".to_owned(),
            created_at: String::new(),
            updated_at: String::new(),
            compaction_enabled: true,
            compaction_after_days: 30,
            compaction_interval_seconds: 3600,
            compaction_batch_size: 100,
            compaction_last_run_at: None,
            compaction_last_status: "never_run".to_owned(),
            compaction_last_snapshot_count: 0,
            compaction_last_bytes_before: 0,
            compaction_last_bytes_after: 0,
        }
    }

    fn test_config() -> ServerConfig {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 59_471,
            default_repository_path: None,
            common_db_path: PathBuf::from("common.duckdb"),
            run_retention: 100,
            run_concurrency: 1,
            mcp_http_concurrency: 16,
            db_pool_size: 4,
            db_acquire_timeout_ms: 5_000,
            db_query_timeout_ms: 30_000,
            http_request_timeout_seconds: 60,
            http_max_body_bytes: 1_048_576,
            run_log_max_bytes: 10 * 1024 * 1024,
            default_compaction_after_days: 30,
            default_compaction_interval_seconds: 3_600,
            default_compaction_batch_size: 100,
        }
    }

    fn stop_compaction_worker(store: &CoverageStore) {
        store.inner.closing.store(true, Ordering::SeqCst);
        let thread = store
            .inner
            .compaction_thread
            .lock()
            .unwrap()
            .take()
            .unwrap();
        thread.thread().unpark();
        thread.join().unwrap();
        store.inner.closing.store(false, Ordering::SeqCst);
    }

    fn make_readonly_view(store: &CoverageStore, table: &str) {
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP INDEX IF EXISTS idx_project_settings_updated; DROP INDEX IF EXISTS idx_run_jobs_status_time; DROP INDEX IF EXISTS idx_run_artifacts_kind; DROP INDEX IF EXISTS idx_run_artifact_baselines_run; DROP INDEX IF EXISTS idx_registered_commands_name; DROP INDEX IF EXISTS idx_worktrees_repo; DROP INDEX IF EXISTS idx_runs_command_time; DROP INDEX IF EXISTS idx_lines_lookup; DROP INDEX IF EXISTS idx_files_snapshot; DROP INDEX IF EXISTS idx_snapshots_repo_time; DROP INDEX IF EXISTS idx_snapshots_commit; DROP INDEX IF EXISTS idx_compacted_repo; ALTER TABLE {table} RENAME TO {table}_base; CREATE VIEW {table} AS SELECT * FROM {table}_base;"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_broken_view(store: &CoverageStore, table: &str) {
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP TABLE {table}; CREATE VIEW {table} AS SELECT 1 AS broken;"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_query_error_view(store: &CoverageStore, table: &str) {
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP TABLE {table}; CREATE VIEW {table} AS SELECT 'a.py' AS file_path, CAST(error('query failure') AS BIGINT) AS line_number, 1 AS hits, true AS covered, 1 AS count_line, 0 AS total_branches, 0 AS covered_branches, 0 AS total_functions, 0 AS covered_functions, '' AS details;"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_query_execution_view(store: &CoverageStore, table: &str, index: &str) {
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP INDEX IF EXISTS {index};
                         DROP INDEX IF EXISTS idx_snapshots_repo_time;
                         DROP INDEX IF EXISTS idx_snapshots_commit;
                         DROP INDEX IF EXISTS idx_files_snapshot;
                         DROP INDEX IF EXISTS idx_lines_lookup;
                         DROP INDEX IF EXISTS idx_worktrees_repo;
                         DROP INDEX IF EXISTS idx_registered_commands_name;
                         DROP INDEX IF EXISTS idx_runs_command_time;
                         DROP INDEX IF EXISTS idx_run_artifacts_kind;
                         DROP INDEX IF EXISTS idx_run_artifact_baselines_run;
                         DROP INDEX IF EXISTS idx_run_jobs_status_time;
                         DROP INDEX IF EXISTS idx_project_settings_updated;
                         DROP INDEX IF EXISTS idx_compacted_repo;
                         ALTER TABLE {table} RENAME TO {table}_base;
                         CREATE VIEW {table} AS SELECT * FROM {table}_base WHERE error('query failure');"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_malformed_projection_view(
        store: &CoverageStore,
        table: &str,
        index: &str,
        columns: &[&str],
        bad_column: &str,
        expression: &str,
    ) {
        let projection = columns
            .iter()
            .map(|column| {
                if *column == bad_column {
                    format!("{expression} AS {column}")
                } else {
                    (*column).to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP INDEX IF EXISTS {index};
                         DROP INDEX IF EXISTS idx_snapshots_repo_time;
                         DROP INDEX IF EXISTS idx_snapshots_commit;
                         DROP INDEX IF EXISTS idx_files_snapshot;
                         DROP INDEX IF EXISTS idx_lines_lookup;
                         DROP INDEX IF EXISTS idx_worktrees_repo;
                         DROP INDEX IF EXISTS idx_registered_commands_name;
                         DROP INDEX IF EXISTS idx_runs_command_time;
                         DROP INDEX IF EXISTS idx_run_artifacts_kind;
                         DROP INDEX IF EXISTS idx_run_artifact_baselines_run;
                         DROP INDEX IF EXISTS idx_run_jobs_status_time;
                         DROP INDEX IF EXISTS idx_project_settings_updated;
                         DROP INDEX IF EXISTS idx_compacted_repo;
                         ALTER TABLE {table} RENAME TO {table}_base;
                         CREATE VIEW {table} AS SELECT {projection} FROM {table}_base;"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_query_error_worktree_view(store: &CoverageStore) {
        let repo_key = store.project().unwrap().repo_key.replace('\'', "''");
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP TABLE worktrees;
                         CREATE VIEW worktrees AS
                         SELECT CAST(NULL AS VARCHAR) AS id,
                                current_timestamp AS created_at,
                                CAST(NULL AS VARCHAR) AS name,
                                '/tmp/worktree' AS path,
                                '{repo_key}' AS repo_path,
                                '{repo_key}' AS repo_key,
                                CAST(NULL AS VARCHAR) AS branch,
                                CAST(NULL AS VARCHAR) AS head_sha,
                                'main' AS base_ref,
                                CAST(NULL AS VARCHAR) AS base_sha,
                                CAST(NULL AS VARCHAR) AS baseline_snapshot_id"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn make_query_error_command_view(store: &CoverageStore) {
        let repo_key = store.project().unwrap().repo_key.replace('\'', "''");
        let repo_path = store.project().unwrap().repo_path.replace('\'', "''");
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP TABLE registered_commands;
                         CREATE VIEW registered_commands AS
                         SELECT CAST(NULL AS VARCHAR) AS id,
                                current_timestamp AS created_at,
                                'missing' AS name,
                                'true' AS command,
                                '{repo_path}' AS cwd,
                                '{repo_path}' AS repo_path,
                                '{repo_key}' AS repo_key,
                                CAST(NULL AS VARCHAR) AS branch,
                                CAST(NULL AS VARCHAR) AS commit_sha,
                                '/bin/sh' AS shell,
                                'tester' AS approved_by,
                                'approved' AS approval_note,
                                '{{}}' AS artifact_specs,
                                true AS enabled,
                                CAST(NULL AS INTEGER) AS duration_estimate_ms,
                                CAST(NULL AS INTEGER) AS duration_p90_ms,
                                0 AS duration_sample_count"
                    ))
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn init_test_git(path: &Path) {
        std::fs::create_dir_all(path.join("src")).unwrap();
        std::fs::write(path.join("src/a.py"), "one\ntwo\n").unwrap();
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.email", "rust@example.com"],
            vec!["config", "user.name", "Rust Tests"],
            vec!["add", "."],
            vec!["commit", "-m", "base"],
        ] {
            let output = Command::new("git")
                .arg("-C")
                .arg(path)
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success());
        }
    }
}
