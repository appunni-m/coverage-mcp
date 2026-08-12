use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use duckdb::{Connection, OptionalExt, params, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::compaction::{CompactionPolicy, CompactionResult};
use crate::config::ServerConfig;
use crate::error::{AppError, AppResult};
use crate::git::{GitInfo, inspect_git, merge_base};
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

const UPSERT_PROJECT_SETTINGS_SQL: &str = "INSERT INTO project_settings (repo_key, repo_path, created_at, updated_at, compaction_enabled, compaction_after_days, compaction_interval_seconds, compaction_batch_size) VALUES (?, ?, ?, ?, true, ?, ?, ?) ON CONFLICT (repo_key) DO UPDATE SET repo_path = excluded.repo_path, updated_at = excluded.updated_at";
const UPSERT_REPOSITORY_SQL: &str = "INSERT INTO repositories (id, repo_key, last_seen) VALUES (?, ?, ?) ON CONFLICT (repo_key) DO UPDATE SET last_seen = excluded.last_seen";
const UPDATE_PROJECT_SETTINGS_SQL: &str = "UPDATE project_settings SET updated_at = ?, compaction_enabled = ?, compaction_after_days = ?, compaction_interval_seconds = ?, compaction_batch_size = ? WHERE repo_key = ?";
const UPDATE_COMPACTION_STATUS_SQL: &str = "UPDATE project_settings SET compaction_last_run_at = ?, compaction_last_status = ?, compaction_last_snapshot_count = ?, compaction_last_bytes_before = ?, compaction_last_bytes_after = ?, updated_at = ? WHERE repo_key = ?";
const INSERT_RUN_ARTIFACT_SQL: &str = "INSERT INTO run_artifacts (run_id, kind, path, exists, size_bytes, coverage_format, suite, modified_by_run, ingest_status, snapshot_id, ingest_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
        CompactionPolicy {
            enabled: self.compaction_enabled,
            older_than_days: self.compaction_after_days,
            interval_seconds: self.compaction_interval_seconds,
            batch_size: self.compaction_batch_size,
        }
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
    compaction_wakeup: Condvar,
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

#[allow(clippy::single_match)]
fn retain_compaction_thread(
    slot: &Mutex<Option<JoinHandle<()>>>,
    handle: std::io::Result<JoinHandle<()>>,
) -> AppResult<()> {
    match handle {
        Ok(handle) => {
            *slot.lock().map_err(lock_error)? = Some(handle);
        }
        Err(_) => {}
    }
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
        Ok(value) => {
            connection.execute_batch("COMMIT")?;
            Ok(value)
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

impl CoverageStore {
    /// Opens or creates a repository database and starts maintenance workers.
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
            compaction_wakeup: Condvar::new(),
        });
        let store = Self { inner };
        store.init_schema()?;
        store.start_compaction_worker();
        Ok(store)
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
        if !self.inner.closing.swap(true, Ordering::SeqCst) {
            self.inner.compaction_wakeup.notify_all();
            if let Some(thread) = self
                .inner
                .compaction_thread
                .lock()
                .map_err(lock_error)?
                .take()
            {
                let _ = thread.join();
            }
            let current = thread::current().id();
            let mut threads = self.inner.run_threads.lock().map_err(lock_error)?;
            for thread in threads.drain(..) {
                if thread.thread().id() != current {
                    let _ = thread.join();
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
        Ok(())
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        let _write_gate = self.inner.write_gate.lock().map_err(lock_error)?;
        self.with_pooled_connection(operation)
    }

    fn with_connection_mut<T>(
        &self,
        operation: impl FnOnce(&Connection) -> AppResult<T>,
    ) -> AppResult<T> {
        let _write_gate = self.inner.write_gate.lock().map_err(lock_error)?;
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
        self.ensure_store_open()?;
        let connection = self.checkout_connection()?;
        let query_guard = self
            .inner
            .query_tracker
            .begin(connection.interrupt_handle())?;
        let result = run_with_timeout(
            &connection,
            Duration::from_millis(self.inner.config.db_query_timeout_ms),
            "DuckDB operation",
            operation,
        );
        drop(query_guard);
        result
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
                CREATE INDEX IF NOT EXISTS idx_run_jobs_status_time ON run_jobs(status, queued_at);
                CREATE INDEX IF NOT EXISTS idx_project_settings_updated ON project_settings(updated_at);
                CREATE INDEX IF NOT EXISTS idx_compacted_repo ON coverage_compacted_payloads(repo_key, compacted_at);
                "#;
        self.with_connection(|connection| {
            connection.execute_batch(schema)?;
            migrate_schema(connection)?;
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
            statement.query_row(params![project.repo_key], |row| {
                Ok(ProjectSettings {
                    repo_key: row.get(0)?, repo_path: row.get(1)?, created_at: timestamp_string(row.get_ref(2)?), updated_at: timestamp_string(row.get_ref(3)?),
                    compaction_enabled: row.get(4)?, compaction_after_days: row.get::<_, i64>(5)? as u32, compaction_interval_seconds: row.get::<_, i64>(6)? as u64,
                    compaction_batch_size: row.get::<_, i64>(7)? as u32, compaction_last_run_at: optional_timestamp(row.get_ref(8)?), compaction_last_status: row.get(9)?,
                    compaction_last_snapshot_count: row.get::<_, i64>(10)? as u64, compaction_last_bytes_before: row.get::<_, i64>(11)? as u64, compaction_last_bytes_after: row.get::<_, i64>(12)? as u64,
                })
            }).map_err(AppError::from)
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
        self.inner.compaction_wakeup.notify_all();
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
            let latest = connection.query_row(&format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE repo_key = ? ORDER BY created_at DESC LIMIT 1"), params![project.repo_key], |row| snapshot_from_row(row)).optional()?;
            let mut result = Map::new();
            result.insert("id".to_owned(), json!(stable_project_id(&project.repo_key)));
            result.insert("repo_key".to_owned(), json!(project.repo_key));
            result.insert("repo_path".to_owned(), json!(project.repo_path));
            result.insert("snapshot_count".to_owned(), json!(snapshot_count));
            result.insert("branch_count".to_owned(), json!(connection.query_row("SELECT count(DISTINCT branch) FROM snapshots WHERE repo_key = ?", params![project.repo_key], |row| row.get::<_, i64>(0))?));
            result.insert("command_count".to_owned(), json!(command_count));
            result.insert("run_count".to_owned(), json!(run_count));
            result.insert("latest_snapshot_id".to_owned(), latest.as_ref().and_then(|value| value.get("id")).cloned().unwrap_or(Value::Null));
            if let Some(latest) = latest { for key in ["created_at", "branch", "commit_sha", "suite", "format", "total_lines", "covered_lines", "line_rate", "total_branches", "covered_branches", "branch_rate", "total_functions", "covered_functions", "function_rate", "total_regions", "covered_regions", "region_rate"] { if let Some(value) = latest.get(key) { result.insert(format!("latest_{key}"), value.clone()); } } }
            result.insert("compaction".to_owned(), serde_json::to_value(settings)?);
            Ok(Value::Object(result))
        })
    }

    fn start_compaction_worker(&self) {
        let store = self.clone();
        let handle = thread::Builder::new()
            .name("coverage-mcp-compactor".to_owned())
            .spawn(move || {
                while !store.inner.closing.load(Ordering::SeqCst) {
                    let settings = store.project_settings().ok();
                    let wait_seconds = settings
                        .as_ref()
                        .map(|settings| settings.compaction_interval_seconds.clamp(1, 86_400))
                        .unwrap_or(1);
                    if let Some(settings) = settings {
                        if settings.compaction_enabled && maintenance_due(&settings) {
                            let _ = store.compact_now();
                        }
                    }
                    for _ in 0..wait_seconds.min(5) {
                        if store.inner.closing.load(Ordering::SeqCst) {
                            break;
                        }
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            });
        let _ = retain_compaction_thread(&self.inner.compaction_thread, handle);
    }

    /// Compacts eligible older snapshots immediately, regardless of the enable flag.
    pub fn compact_now(&self) -> AppResult<Value> {
        let project = self.project()?;
        let settings = self.project_settings()?;
        let result = self.compact_project(&project, &settings.policy())?;
        self.with_connection(|connection| {
            let values = params![
                Utc::now(),
                result.status,
                result.compacted_snapshots as i64,
                result.bytes_before as i64,
                result.bytes_after as i64,
                Utc::now(),
                project.repo_key
            ];
            connection.execute(UPDATE_COMPACTION_STATUS_SQL, values)?;
            Ok(())
        })?;
        Ok(serde_json::to_value(result)?)
    }

    fn compact_project(
        &self,
        project: &GitInfo,
        policy: &CompactionPolicy,
    ) -> AppResult<CompactionResult> {
        let cutoff = Utc::now() - ChronoDuration::days(i64::from(policy.older_than_days));
        let ids = self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT s.id FROM snapshots s LEFT JOIN coverage_compacted_payloads p ON p.snapshot_id = s.id WHERE s.repo_key = ? AND s.created_at < ? AND p.snapshot_id IS NULL ORDER BY s.created_at ASC LIMIT ?")?;
            let rows = statement.query_map(params![project.repo_key, cutoff, policy.batch_size as i64], |row| row.get::<_, String>(0))?;
            let mut ids = Vec::new(); for row in rows { ids.push(row?); } Ok(ids)
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
            result.compacted_snapshots += inserted;
            result.bytes_before += bytes_before * inserted;
            result.bytes_after += bytes_after * inserted;
        }
        if result.compacted_snapshots > 0 {
            let checkpointed = self
                .with_connection(|connection| {
                    connection
                        .execute_batch("CHECKPOINT")
                        .map(|_| true)
                        .map_err(AppError::from)
                })
                .unwrap_or(false);
            result.checkpointed = checkpointed;
        }
        Ok(result)
    }

    fn compact_snapshot_detail(
        &self,
        repo_key: &str,
        snapshot_id: &str,
    ) -> AppResult<(bool, u64, u64)> {
        let payload = self.detail_payload(snapshot_id)?;
        let encoded = serde_json::to_vec(&payload)?;
        let mut encoded_reader = encoded.as_slice();
        let compressed = compress_coverage_payload(&mut encoded_reader)?;
        let inserted = self.with_connection_mut(|connection| {
            connection.execute_batch("BEGIN TRANSACTION")?;
            let outcome = (|| {
                let changed = connection.execute("INSERT INTO coverage_compacted_payloads (snapshot_id, repo_key, compacted_at, original_bytes, compressed_bytes, payload) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (snapshot_id) DO NOTHING", params![snapshot_id, repo_key, Utc::now(), encoded.len() as i64, compressed.len() as i64, compressed])?;
                remove_compacted_detail(connection, snapshot_id, changed == 1)?;
                Ok::<bool, AppError>(changed == 1)
            })();
            match outcome {
                Ok(changed) => {
                    connection.execute_batch("COMMIT")?;
                    Ok(changed)
                }
                Err(error) => {
                    let _ = connection.execute_batch("ROLLBACK");
                    Err(error)
                }
            }
        })?;
        Ok((inserted, encoded.len() as u64, compressed.len() as u64))
    }

    fn detail_payload(&self, snapshot_id: &str) -> AppResult<Value> {
        let files = self.with_read_connection(|connection| {
            let mut statement = connection.prepare("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? ORDER BY file_path")?;
            let rows = statement.query_map(params![snapshot_id], file_from_row)?;
            let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
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
    pub fn register_worktree(
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
            let rows = statement.query_map(params![project.repo_key, collection_limit(limit) as i64], worktree_from_row)?;
            let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
        })
    }

    /// Returns one worktree.
    pub fn worktree(&self, worktree_id: &str) -> AppResult<Value> {
        self.with_read_connection(|connection| connection.query_row("SELECT id, created_at, name, path, repo_path, repo_key, branch, head_sha, base_ref, base_sha, baseline_snapshot_id FROM worktrees WHERE id = ?", params![worktree_id], worktree_from_row).optional()?.ok_or_else(|| AppError::NotFound(format!("worktree not found: {worktree_id}"))))
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
        let path = worktree
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut points = self.list_snapshots(Some(path), None, Some(suite), limit)?;
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
            let path = worktree
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default();
            snapshots
                .retain(|snapshot| snapshot.get("repo_path").and_then(Value::as_str) == Some(path));
        }
        if let Some(file_path) = file_path {
            let mut values = Vec::new();
            for snapshot in snapshots {
                let id = snapshot
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(file) = self
                    .files(id, MAX_COLLECTION_RECORDS)?
                    .into_iter()
                    .find(|file| file.get("file_path").and_then(Value::as_str) == Some(file_path))
                {
                    let mut point = file;
                    #[allow(clippy::option_map_unit_fn)]
                    point.as_object_mut().map(|object| {
                        object.insert("id".to_owned(), json!(id));
                        object.insert(
                            "created_at".to_owned(),
                            snapshot.get("created_at").cloned().unwrap_or(Value::Null),
                        );
                        object.insert(
                            "branch".to_owned(),
                            snapshot.get("branch").cloned().unwrap_or(Value::Null),
                        );
                        object.insert(
                            "commit_sha".to_owned(),
                            snapshot.get("commit_sha").cloned().unwrap_or(Value::Null),
                        );
                        object.insert(
                            "suite".to_owned(),
                            snapshot.get("suite").cloned().unwrap_or(Value::Null),
                        );
                    });
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
        let files = self.compare_files(snapshot_id, baseline_snapshot_id, file_limit)?;
        let changed_lines =
            self.changed_lines(snapshot_id, baseline_snapshot_id, None, false, line_limit)?;
        Ok(
            json!({"baseline": baseline, "current": current, "overall": overall_delta(&current, &baseline), "files": files, "changed_lines": changed_lines}),
        )
    }

    fn compare_files(
        &self,
        snapshot_id: &str,
        baseline_snapshot_id: &str,
        limit: usize,
    ) -> AppResult<Vec<Value>> {
        let baseline: HashMap<String, Value> = self
            .files(baseline_snapshot_id, MAX_COLLECTION_RECORDS)?
            .into_iter()
            .filter_map(|value| {
                let key = value.get("file_path").and_then(Value::as_str)?.to_owned();
                Some((key, value))
            })
            .collect();
        let current: HashMap<String, Value> = self
            .files(snapshot_id, MAX_COLLECTION_RECORDS)?
            .into_iter()
            .filter_map(|value| {
                let key = value.get("file_path").and_then(Value::as_str)?.to_owned();
                Some((key, value))
            })
            .collect();
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
            let left_covered = left
                .and_then(|value| value.get("covered"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let right_covered = right
                .and_then(|value| value.get("covered"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let left_hits = left
                .and_then(|value| value.get("hits"))
                .and_then(Value::as_i64);
            let right_hits = right
                .and_then(|value| value.get("hits"))
                .and_then(Value::as_i64);
            let left_branches = left
                .and_then(|value| value.get("covered_branches"))
                .and_then(Value::as_i64);
            let right_branches = right
                .and_then(|value| value.get("covered_branches"))
                .and_then(Value::as_i64);
            if left_covered == right_covered
                && left_hits == right_hits
                && left_branches == right_branches
            {
                continue;
            }
            let status = if left.is_none() {
                "new"
            } else if right.is_none() {
                "removed"
            } else if left_covered && !right_covered {
                "regressed"
            } else if !left_covered && right_covered {
                "improved"
            } else {
                "changed"
            };
            if only_regressions && status != "regressed" {
                continue;
            }
            values.push(json!({"file_path": path, "line_number": number, "baseline_covered": left.map(|value| value.get("covered").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "current_covered": right.map(|value| value.get("covered").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "baseline_hits": left.map(|value| value.get("hits").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "current_hits": right.map(|value| value.get("hits").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "baseline_total_branches": left.map(|value| value.get("total_branches").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "current_total_branches": right.map(|value| value.get("total_branches").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "baseline_covered_branches": left.map(|value| value.get("covered_branches").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "current_covered_branches": right.map(|value| value.get("covered_branches").cloned().unwrap_or(Value::Null)).unwrap_or(Value::Null), "status": status}));
        }
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

    fn lines_all(&self, snapshot_id: &str) -> AppResult<HashMap<(String, i64), Value>> {
        let mut values = HashMap::new();
        let files = self.files(snapshot_id, MAX_COLLECTION_RECORDS)?;
        for path in files.into_iter().filter_map(|file| {
            file.get("file_path")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }) {
            for line in self.lines(snapshot_id, &path, MAX_COLLECTION_RECORDS)? {
                append_line_with_path(&mut values, &path, line);
            }
        }
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
            let total = file
                .get("total_lines")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let covered = file
                .get("covered_lines")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let rate = file.get("line_rate").and_then(Value::as_f64);
            if total > 0 && covered == 0 {
                items.push(json!({"severity": if total >= 20 {"high"} else {"medium"},"category":"zero-coverage-file","title":"File has no covered lines","detail":format!("{} has 0/{total} covered lines.", file.get("file_path").and_then(Value::as_str).unwrap_or_default()),"file_path":file.get("file_path"),"uncovered_lines":total-covered,"line_rate":rate}));
            }
            if total >= 5 && covered > 0 && rate.is_some_and(|value| value < 0.6) {
                items.push(json!({"severity":"medium","category":"low-line-coverage","title":"File has low line coverage","detail":format!("{} is {:.1}% covered with {} uncovered lines.", file.get("file_path").and_then(Value::as_str).unwrap_or_default(), rate.unwrap_or_default()*100.0, total-covered),"file_path":file.get("file_path"),"uncovered_lines":total-covered,"line_rate":rate}));
            }
            let total_branches = file
                .get("total_branches")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let covered_branches = file
                .get("covered_branches")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let branch_rate = file.get("branch_rate").and_then(Value::as_f64);
            if total_branches >= 2 && branch_rate.is_none_or(|value| value < 0.7) {
                items.push(json!({"severity":"medium","category":"low-branch-coverage","title":"Branch coverage needs attention","detail":format!("{} covers {covered_branches}/{total_branches} branches.", file.get("file_path").and_then(Value::as_str).unwrap_or_default()),"file_path":file.get("file_path"),"uncovered_branches":total_branches-covered_branches,"branch_rate":branch_rate}));
            }
        }
        let baseline = if let Some(baseline) = baseline_snapshot_id {
            let comparison =
                self.compare(snapshot_id, baseline, limit, limit.saturating_mul(20))?;
            let overall = comparison.get("overall").cloned().unwrap_or(Value::Null);
            if overall
                .get("line_rate_delta")
                .and_then(Value::as_f64)
                .is_some_and(|value| value < 0.0)
            {
                items.push(json!({"severity":"high","category":"overall-regression","title":"Overall line coverage regressed","detail":"Overall line coverage decreased.","line_rate_delta":overall.get("line_rate_delta"),"covered_lines_delta":overall.get("covered_lines_delta")}));
            }
            for file in comparison
                .get("files")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .take(limit)
            {
                if file
                    .get("line_rate_delta")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value < 0.0)
                {
                    items.push(json!({"severity":"high","category":"file-regression","title":"File coverage regressed","detail":format!("{} changed coverage.", file.get("file_path").and_then(Value::as_str).unwrap_or_default()),"file_path":file.get("file_path"),"line_rate_delta":file.get("line_rate_delta")}));
                }
            }
            for line in comparison
                .get("changed_lines")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|line| line.get("status").and_then(Value::as_str) == Some("regressed"))
                .take(limit)
            {
                items.push(json!({"severity":"high","category":"line-regression","title":"Line became uncovered","detail":format!("{}:{} was covered and is now missed.", line.get("file_path").and_then(Value::as_str).unwrap_or_default(), line.get("line_number").and_then(Value::as_i64).unwrap_or_default()),"file_path":line.get("file_path"),"line_number":line.get("line_number")}));
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
        let worktree = self.worktree(worktree_id)?;
        let current_id = if let Some(snapshot_id) = snapshot_id {
            snapshot_id.to_owned()
        } else {
            let path = worktree.get("path").and_then(Value::as_str);
            let snapshots = self.trend(path, None, None, None, Some(worktree_id), 1)?;
            snapshots
                .into_iter()
                .next()
                .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
                .ok_or_else(|| {
                    AppError::NotFound(format!(
                        "no current snapshot found for worktree: {worktree_id}"
                    ))
                })?
        };
        let current = self.snapshot(&current_id)?;
        if current.get("repo_key") != worktree.get("repo_key")
            || current.get("repo_path") != worktree.get("path")
        {
            return Err(AppError::Validation(
                "current snapshot does not belong to the selected worktree".to_owned(),
            ));
        }
        let baseline = match worktree.get("baseline_snapshot_id").and_then(Value::as_str) {
            Some(value) => value,
            None => {
                return Err(AppError::NotFound(
                    "worktree has no baseline snapshot".to_owned(),
                ));
            }
        };
        let mut result = self.compare(&current_id, baseline, file_limit, line_limit)?;
        #[allow(clippy::option_map_unit_fn)]
        result.as_object_mut().map(|object| {
            object.insert("worktree".to_owned(), worktree);
        });
        Ok(result)
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
            connection.execute("INSERT INTO registered_commands (id, created_at, name, command, cwd, repo_path, repo_key, branch, commit_sha, shell, approved_by, approval_note, artifact_specs, enabled, duration_estimate_ms, duration_p90_ms, duration_sample_count, duration_stats_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL)", params![id, Utc::now(), name, command, cwd.to_string_lossy(), git.repo_path, git.repo_key, git.branch, git.commit_sha, shell, approved_by, approval_note, serde_json::to_string(&artifacts)?, enabled])?;
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
            let rows = statement.query_map(params![project.repo_key, collection_limit(limit) as i64], command_from_row)?;
            let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
        })
    }

    #[allow(clippy::single_match)]
    fn retain_run_thread(&self, handle: std::io::Result<JoinHandle<()>>) -> AppResult<()> {
        match handle {
            Ok(handle) => {
                self.inner
                    .run_threads
                    .lock()
                    .map_err(lock_error)?
                    .push(handle);
            }
            Err(_) => {}
        }
        Ok(())
    }

    /// Submits one approved command to the background runner.
    pub fn submit_command(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
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
        if !command
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false)
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
        let command_id = command
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Some(existing) = self.idempotent_run_id(command_id, key.as_deref())? {
            let mut value = self.run_result(&existing, max_summary_lines)?;
            #[allow(clippy::option_map_unit_fn)]
            value.as_object_mut().map(|object| {
                object.insert("submission_reused".to_owned(), json!(true));
            });
            return Ok(value);
        }
        let id = Uuid::new_v4().to_string();
        let run_path = self.inner.run_dir.join(&id);
        fs::create_dir_all(&run_path)?;
        let stdout = run_path.join("stdout.log");
        let stderr = run_path.join("stderr.log");
        File::create(&stdout)?;
        File::create(&stderr)?;
        let git = inspect_git(Path::new(
            command.get("cwd").and_then(Value::as_str).unwrap_or("."),
        ))?;
        self.with_connection(|connection| {
            connection.execute("INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 'queued', ?, ?, '', NULL)", params![id, command.get("id").and_then(Value::as_str), command.get("name").and_then(Value::as_str), command.get("command").and_then(Value::as_str), key, command.get("cwd").and_then(Value::as_str), git.repo_path, git.repo_key, git.branch, git.commit_sha, Utc::now(), timeout_seconds.map(|value| value as i64), max_summary_lines as i64, stdout.to_string_lossy(), stderr.to_string_lossy()])?;
            Ok(())
        })?;
        let store = self.clone();
        let run_id = id.clone();
        let handle = thread::Builder::new()
            .name(format!("coverage-mcp-run-{id}"))
            .spawn(move || {
                let _ = store.execute_run(&run_id);
            });
        self.retain_run_thread(handle)?;
        let mut result = self.run_result(&id, max_summary_lines)?;
        #[allow(clippy::option_map_unit_fn)]
        result.as_object_mut().map(|object| {
            object.insert("submission_reused".to_owned(), json!(false));
        });
        Ok(result)
    }

    /// Submits and waits for one command to reach a terminal state.
    pub fn run_command(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        max_summary_lines: usize,
    ) -> AppResult<Value> {
        let submission = (
            command_ref,
            timeout_seconds,
            idempotency_key,
            max_summary_lines,
        );
        let submitted =
            self.submit_command(submission.0, submission.1, submission.2, submission.3)?;
        let id = submitted
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        loop {
            let result = self.run_result(&id, max_summary_lines)?;
            if result
                .get("terminal")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(result);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn idempotent_run_id(&self, command_id: &str, key: Option<&str>) -> AppResult<Option<String>> {
        let Some(key) = key else {
            return Ok(None);
        };
        self.with_connection(|connection| Ok(connection.query_row("SELECT id FROM run_jobs WHERE command_id = ? AND idempotency_key = ? UNION ALL SELECT id FROM runs WHERE command_id = ? AND idempotency_key = ? LIMIT 1", params![command_id, key, command_id, key], |row| row.get::<_, String>(0)).optional()?))
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
            return Ok(());
        }
        *active += 1;
        drop(active);
        let result = self.execute_run_with_slot(run_id);
        let mut active = slot_lock.lock().map_err(lock_error)?;
        *active = active.saturating_sub(1);
        slot_cv.notify_one();
        drop(active);
        result
    }

    fn execute_run_with_slot(&self, run_id: &str) -> AppResult<()> {
        let job = match self.job(run_id)? {
            Some(job) => job,
            None => return Err(AppError::NotFound(format!("run not found: {run_id}"))),
        };
        if job.get("status").and_then(Value::as_str) != Some("queued") {
            return Ok(());
        }
        let started = Utc::now();
        self.with_connection(|connection| { connection.execute("UPDATE run_jobs SET status = 'running', started_at = ?, error = '' WHERE id = ? AND status = 'queued'", params![started, run_id])?; Ok(()) })?;
        let command = job
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let cwd = job.get("cwd").and_then(Value::as_str).unwrap_or_default();
        let shell = self
            .registered_command(
                job.get("command_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )?
            .get("shell")
            .and_then(Value::as_str)
            .unwrap_or("/bin/bash")
            .to_owned();
        let stdout_path = PathBuf::from(
            job.get("stdout_path")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let stderr_path = PathBuf::from(
            job.get("stderr_path")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let stdout_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&stdout_path)?;
        let stderr_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&stderr_path)?;
        let child = Command::new(&shell)
            .arg("-c")
            .arg(command)
            .current_dir(cwd)
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .spawn()?;
        let control = Arc::new(Mutex::new(Some(child)));
        self.inner
            .active_processes
            .lock()
            .map_err(lock_error)?
            .insert(run_id.to_owned(), control.clone());
        let timeout = job
            .get("timeout_seconds")
            .and_then(Value::as_i64)
            .map(|value| Duration::from_secs(value as u64));
        let started_instant = Instant::now();
        let exit_code;
        let mut status = "failed".to_owned();
        loop {
            let mut guard = control.lock().map_err(lock_error)?;
            let child = required_managed_child(&mut guard)?;
            if let Some(result) = child.try_wait()? {
                exit_code = result.code();
                status = if result.success() { "passed" } else { "failed" }.to_owned();
                break;
            }
            let cancelled = self.with_connection(|connection| {
                Ok(connection
                    .query_row(
                        "SELECT cancellation_requested_at IS NOT NULL FROM run_jobs WHERE id = ?",
                        params![run_id],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap_or(false))
            })?;
            if cancelled {
                let _ = child.kill();
                status = "cancelled".to_owned();
            }
            if timeout.is_some_and(|value| started_instant.elapsed() >= value) {
                let _ = child.kill();
                status = "timeout".to_owned();
            }
            drop(guard);
            if status == "cancelled" || status == "timeout" {
                let mut guard = control.lock().map_err(lock_error)?;
                reap_child(&mut guard);
                exit_code = None;
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        self.inner
            .active_processes
            .lock()
            .map_err(lock_error)?
            .remove(run_id);
        let ended = Utc::now();
        let duration_ms = ended
            .signed_duration_since(started)
            .num_milliseconds()
            .max(0);
        let exit_code = exit_code.map(i64::from);
        let command_id = job
            .get("command_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let command_row = self.registered_command(command_id)?;
        let artifacts = self.collect_artifacts(&command_row, cwd, status == "passed")?;
        let summary = summarize_logs(
            &stdout_path,
            &stderr_path,
            &status,
            exit_code,
            duration_ms,
            job.get("max_summary_lines")
                .and_then(Value::as_i64)
                .unwrap_or(20) as usize,
        );
        self.with_connection(|connection| {
            connection.execute("INSERT INTO runs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at) SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, started_at, ?, ?, ?, ?, stdout_path, stderr_path, ?, ?, queued_at, date_diff('millisecond', queued_at, started_at), cancellation_requested_at FROM run_jobs WHERE id = ?", params![ended, duration_ms, exit_code, status, serde_json::to_string(&summary)?, serde_json::to_string(&artifacts)?, run_id])?;
            for artifact in &artifacts {
                let values = params![
                    run_id,
                    artifact.get("kind").and_then(Value::as_str).unwrap_or_default(),
                    artifact.get("path").and_then(Value::as_str).unwrap_or_default(),
                    artifact.get("exists").and_then(Value::as_bool).unwrap_or(false),
                    artifact.get("size_bytes").and_then(Value::as_i64),
                    artifact.get("coverage_format").and_then(Value::as_str),
                    artifact.get("suite").and_then(Value::as_str),
                    artifact.get("modified_by_run").and_then(Value::as_bool).unwrap_or(false),
                    artifact.get("ingest_status").and_then(Value::as_str),
                    artifact.get("snapshot_id").and_then(Value::as_str),
                    artifact.get("ingest_error").and_then(Value::as_str),
                ];
                connection.execute(INSERT_RUN_ARTIFACT_SQL, values)?;
            }
            connection.execute("DELETE FROM run_jobs WHERE id = ?", params![run_id])?;
            Ok(())
        })?;
        let command_id = command_row
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.prune_runs(command_id)?;
        Ok(())
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
            if let Some(mut value) = terminal {
                #[allow(clippy::option_map_unit_fn)]
                value.as_object_mut().map(|object| {
                    object.insert("terminal".to_owned(), json!(true));
                    object.insert("poll_after_ms".to_owned(), Value::Null);
                    object.insert("queue_position".to_owned(), Value::Null);
                });
                return Ok(value);
            }
            return Err(AppError::NotFound(format!("run not found: {run_id}")));
        };
        let status = job
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("queued")
            .to_owned();
        let terminal = !matches!(status.as_str(), "queued" | "running");
        let queue_position = if status == "queued" {
            Some(self.with_connection(|connection| Ok(connection.query_row("SELECT count(*) FROM run_jobs WHERE status = 'queued' AND (queued_at < (SELECT queued_at FROM run_jobs WHERE id = ?) OR (queued_at = (SELECT queued_at FROM run_jobs WHERE id = ?) AND id <= ?))", params![run_id, run_id, run_id], |row| row.get::<_, i64>(0))?))?)
        } else {
            None
        };
        let mut result = job;
        #[allow(clippy::option_map_unit_fn)]
        result.as_object_mut().map(|object| {
            let duration_ms = run_duration_ms(object.get("started_at"), object.get("queued_at"));
            let stdout_path = object.get("stdout_path").cloned().unwrap_or(Value::Null);
            let stderr_path = object.get("stderr_path").cloned().unwrap_or(Value::Null);
            let cancellation_requested = object
                .get("cancellation_requested_at")
                .is_some_and(|value| !value.is_null());
            object.insert("terminal".to_owned(), json!(terminal));
            object.insert("exit_code".to_owned(), Value::Null);
            object.insert("duration_ms".to_owned(), json!(duration_ms));
            object.insert("artifact_paths".to_owned(), json!([]));
            object.insert("coverage_ingest".to_owned(), json!({"status":"pending","snapshot_ids":[],"configured":0,"ingested":0,"failed":0}));
            object.insert(
                "queue_position".to_owned(),
                queue_position
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
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
        });
        let _ = max_summary_lines;
        Ok(result)
    }

    /// Lists queued and running jobs.
    pub fn list_run_queue(&self, limit: usize) -> AppResult<Vec<Value>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at FROM run_jobs WHERE status IN ('queued', 'running') ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, queued_at LIMIT ?")?;
            let rows = statement.query_map(params![collection_limit(limit) as i64], job_from_row)?; let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
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
            Some(required_command_id(&command, reference)?)
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
            let path = result
                .get(path_key)
                .and_then(Value::as_str)
                .unwrap_or_default();
            let lines: Vec<String> = fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect();
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
            self.registered_command(reference)?
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            None
        };
        self.with_connection(|connection| {
            let row = if let Some(command_id) = command_id { connection.query_row("SELECT a.run_id, a.kind, a.path, a.exists, a.size_bytes, a.coverage_format, a.suite, a.modified_by_run, a.ingest_status, a.snapshot_id, a.ingest_error, r.command_id, r.command_name, r.repo_key, r.repo_path, r.started_at, r.ended_at, r.status, r.exit_code FROM run_artifacts a JOIN runs r ON r.id = a.run_id WHERE a.kind = ? AND r.command_id = ? ORDER BY r.started_at DESC LIMIT 1", params![kind, command_id], artifact_from_row).optional()? } else { connection.query_row("SELECT a.run_id, a.kind, a.path, a.exists, a.size_bytes, a.coverage_format, a.suite, a.modified_by_run, a.ingest_status, a.snapshot_id, a.ingest_error, r.command_id, r.command_name, r.repo_key, r.repo_path, r.started_at, r.ended_at, r.status, r.exit_code FROM run_artifacts a JOIN runs r ON r.id = a.run_id WHERE a.kind = ? ORDER BY r.started_at DESC LIMIT 1", params![kind], artifact_from_row).optional()? }; Ok(row)
        })
    }

    fn collect_artifacts(
        &self,
        command: &Value,
        cwd: &str,
        eligible: bool,
    ) -> AppResult<Vec<Value>> {
        let specs = command
            .get("artifact_specs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut artifacts = Vec::new();
        for spec in specs {
            let kind = spec.get("kind").and_then(Value::as_str).unwrap_or_default();
            let raw_path = spec.get("path").and_then(Value::as_str).unwrap_or_default();
            let path = PathBuf::from(raw_path);
            let path = if path.is_absolute() {
                path
            } else {
                PathBuf::from(cwd).join(path)
            };
            let metadata = fs::metadata(&path).ok();
            let coverage_format = spec.get("coverage_format").and_then(Value::as_str);
            let mut artifact = json!({"kind":kind,"path":path.to_string_lossy(),"exists":metadata.is_some(),"size_bytes":metadata.as_ref().map(|value| value.len()),"required":spec.get("required").and_then(Value::as_bool).unwrap_or(false),"coverage_format":coverage_format,"suite":spec.get("suite").and_then(Value::as_str),"modified_by_run":metadata.is_some(),"ingest_status":Value::Null,"snapshot_id":Value::Null,"ingest_error":Value::Null});
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
                } else {
                    match self.ingest_report(
                        &path,
                        format,
                        Some(Path::new(
                            command
                                .get("repo_path")
                                .and_then(Value::as_str)
                                .unwrap_or(cwd),
                        )),
                        command.get("branch").and_then(Value::as_str),
                        command.get("commit_sha").and_then(Value::as_str),
                        None,
                        spec.get("suite").and_then(Value::as_str).unwrap_or(
                            command
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("default"),
                        ),
                    ) {
                        Ok(snapshot) => {
                            #[allow(clippy::option_map_unit_fn)]
                            artifact.as_object_mut().map(|object| {
                                object.insert(
                                    "snapshot_id".to_owned(),
                                    snapshot.get("id").cloned().unwrap_or(Value::Null),
                                );
                            });
                            ("ingested".to_owned(), String::new())
                        }
                        Err(error) => ("failed".to_owned(), error.to_string()),
                    }
                };
                #[allow(clippy::option_map_unit_fn)]
                artifact.as_object_mut().map(|object| {
                    object.insert("ingest_status".to_owned(), json!(status.0));
                    if !status.1.is_empty() {
                        object.insert("ingest_error".to_owned(), json!(status.1));
                    }
                });
            }
            artifacts.push(artifact);
        }
        Ok(artifacts)
    }

    fn prune_runs(&self, command_id: &str) -> AppResult<()> {
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
            let _ = fs::remove_dir_all(self.inner.run_dir.join(id));
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
            connection.execute_batch("BEGIN TRANSACTION")?;
            let result = (|| {
                let warnings = serde_json::to_string(&report.warnings)?;
                let metadata = serde_json::to_string(&report.metadata)?;
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
                    let raw_metrics = serde_json::to_string(&file.raw_metrics)?;
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
                    let details = serde_json::to_string(&line.details)?;
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

    /// Returns file summaries, transparently restoring compacted detail payloads.
    pub fn files(&self, snapshot_id: &str, limit: usize) -> AppResult<Vec<Value>> {
        self.snapshot(snapshot_id)?;
        let rows = self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? ORDER BY file_path LIMIT ?")?;
            let rows = statement.query_map(params![snapshot_id, collection_limit(limit) as i64], file_from_row)?;
            let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
        })?;
        if !rows.is_empty() {
            return Ok(rows);
        }
        Ok(self
            .compacted_detail(snapshot_id)?
            .map(|payload| {
                payload
                    .get("files")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            })
            .unwrap_or_default())
    }

    /// Returns one file summary.
    pub fn file_coverage(&self, snapshot_id: &str, file_path: &str) -> AppResult<Value> {
        let file = self.with_connection(|connection| Ok(connection.query_row("SELECT file_path, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate, raw_metrics FROM files WHERE snapshot_id = ? AND file_path = ?", params![snapshot_id, file_path], file_from_row).optional()?))?;
        if let Some(file) = file {
            return Ok(file);
        }
        if let Some(payload) = self.compacted_detail(snapshot_id)? {
            if let Some(file) = payload
                .get("files")
                .and_then(Value::as_array)
                .and_then(|files| {
                    files.iter().find(|value| {
                        value.get("file_path").and_then(Value::as_str) == Some(file_path)
                    })
                })
            {
                return Ok(file.clone());
            }
        }
        Err(AppError::NotFound(format!("file not found: {file_path}")))
    }

    /// Returns exact line records for one file.
    pub fn lines(&self, snapshot_id: &str, file_path: &str, limit: usize) -> AppResult<Vec<Value>> {
        let rows = self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT line_number, hits, covered, count_line, total_branches, covered_branches, total_functions, covered_functions, details FROM lines WHERE snapshot_id = ? AND file_path = ? ORDER BY line_number LIMIT ?")?;
            let rows = statement.query_map(params![snapshot_id, file_path, collection_limit(limit) as i64], line_from_row)?;
            let mut values = Vec::new(); for row in rows { values.push(row?); } Ok(values)
        })?;
        if !rows.is_empty() {
            return Ok(rows);
        }
        Ok(self
            .compacted_detail(snapshot_id)?
            .map(|payload| {
                payload
                    .get("lines")
                    .and_then(Value::as_array)
                    .map(|lines| {
                        lines
                            .iter()
                            .filter(|line| {
                                line.get("file_path").and_then(Value::as_str) == Some(file_path)
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default())
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
        let selected: Vec<Value> = all
            .into_iter()
            .filter(|line| {
                let number = line
                    .get("line_number")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                ranges
                    .iter()
                    .any(|(start, end)| number >= *start && number <= *end)
            })
            .collect();
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
        for line in lines.iter().filter(|line| {
            line.get("count_line")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        }) {
            let number = line
                .get("line_number")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let uncovered = !line
                .get("covered")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if uncovered {
                if let Some((start, end)) = current.as_mut() {
                    if number <= *end + 1 {
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
            json!({"file_path": file_path, "uncovered_line_count": lines.iter().filter(|line| line.get("count_line").and_then(Value::as_bool).unwrap_or(false) && !line.get("covered").and_then(Value::as_bool).unwrap_or(false)).count(), "ranges": gaps, "returned_range_count": returned_range_count, "truncated": truncated}),
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
            let id = snapshot
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(line) = self
                .lines(id, file_path, MAX_COLLECTION_RECORDS)?
                .into_iter()
                .find(|line| line.get("line_number").and_then(Value::as_i64) == Some(line_number))
            {
                let mut point = Map::new();
                point.insert("snapshot_id".to_owned(), json!(id));
                point.insert(
                    "created_at".to_owned(),
                    snapshot.get("created_at").cloned().unwrap_or(Value::Null),
                );
                point.insert(
                    "branch".to_owned(),
                    snapshot.get("branch").cloned().unwrap_or(Value::Null),
                );
                point.insert(
                    "commit_sha".to_owned(),
                    snapshot.get("commit_sha").cloned().unwrap_or(Value::Null),
                );
                let suite_value = match suite {
                    Some(value) => value,
                    None => snapshot
                        .get("suite")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                };
                point.insert("suite".to_owned(), json!(suite_value));
                point.insert("file_path".to_owned(), json!(file_path));
                point.insert("line_number".to_owned(), json!(line_number));
                for key in ["hits", "covered", "total_branches", "covered_branches"] {
                    point.insert(
                        key.to_owned(),
                        line.get(key).cloned().unwrap_or(Value::Null),
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
        if end < start {
            return Err(AppError::Validation(
                "end must be greater than or equal to start".to_owned(),
            ));
        }
        let snapshot = self.snapshot(snapshot_id)?;
        let root = PathBuf::from(
            snapshot
                .get("repo_path")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )
        .canonicalize()?;
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
        let start = start.max(1);
        let end = end.min(start + 199);
        let mut result = Vec::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let number = index as i64 + 1;
            if number < start {
                continue;
            }
            if number > end {
                break;
            }
            result.push(
                json!({"line_number": number, "text": line?.trim_end_matches('\n').to_owned()}),
            );
        }
        Ok(result)
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

/// Inclusive line range used by coverage projections.
pub type LineRange = (i64, i64);

const SNAPSHOT_COLUMNS: &str = "id, created_at, repo_path, repo_key, branch, commit_sha, base_ref, suite, format, report_path, warnings, metadata, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate";

fn snapshot_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let mut value = Map::new();
    value.insert("id".to_owned(), json!(row.get::<_, String>(0)?));
    value.insert(
        "created_at".to_owned(),
        json!(timestamp_string(row.get_ref(1)?)),
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

fn migrate_schema(connection: &Connection) -> AppResult<()> {
    for (table, additions) in [
        (
            "snapshots",
            vec![
                ("total_regions", "INTEGER DEFAULT 0"),
                ("covered_regions", "INTEGER DEFAULT 0"),
                ("region_rate", "DOUBLE"),
            ],
        ),
        (
            "files",
            vec![
                ("total_regions", "INTEGER DEFAULT 0"),
                ("covered_regions", "INTEGER DEFAULT 0"),
                ("region_rate", "DOUBLE"),
            ],
        ),
        (
            "registered_commands",
            vec![
                ("duration_estimate_ms", "INTEGER"),
                ("duration_p90_ms", "INTEGER"),
                ("duration_sample_count", "INTEGER DEFAULT 0"),
                ("duration_stats_updated_at", "TIMESTAMP"),
            ],
        ),
        (
            "runs",
            vec![
                ("idempotency_key", "VARCHAR"),
                ("queued_at", "TIMESTAMP"),
                ("queue_duration_ms", "INTEGER"),
                ("cancellation_requested_at", "TIMESTAMP"),
            ],
        ),
        (
            "run_jobs",
            vec![
                ("idempotency_key", "VARCHAR"),
                ("cancellation_requested_at", "TIMESTAMP"),
            ],
        ),
        (
            "run_artifacts",
            vec![
                ("coverage_format", "VARCHAR"),
                ("suite", "VARCHAR"),
                ("modified_by_run", "BOOLEAN DEFAULT false"),
                ("ingest_status", "VARCHAR"),
                ("snapshot_id", "VARCHAR"),
                ("ingest_error", "VARCHAR"),
            ],
        ),
    ] {
        let mut statement =
            connection.prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<HashSet<_>, _>>()?;
        for (name, definition) in additions {
            if !columns.contains(name) {
                let sql = format!("ALTER TABLE {table} ADD COLUMN {name} {definition}");
                connection.execute(&sql, [])?;
            }
        }
    }
    let _ = connection.execute(
        "ALTER TABLE lines ALTER COLUMN hits SET DATA TYPE BIGINT",
        [],
    );
    Ok(())
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> AppError {
    AppError::Runtime("coverage store lock was poisoned".to_owned())
}

fn reap_child(child: &mut Option<std::process::Child>) {
    if let Some(child) = child {
        let _ = child.wait();
    }
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
            if start <= *previous_end + 1 {
                *previous_end = (*previous_end).max(end);
                continue;
            }
        }
        normalized.push((start, end));
    }
    let count: i64 = normalized.iter().map(|(start, end)| end - start + 1).sum();
    if count > 200 {
        return Err(AppError::Validation(
            "line_ranges combined unique span may contain at most 200 lines".to_owned(),
        ));
    }
    Ok(normalized)
}

fn worktree_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"created_at":timestamp_string(row.get_ref(1)?),"name":row.get::<_, Option<String>>(2)?,"path":row.get::<_, String>(3)?,"repo_path":row.get::<_, String>(4)?,"repo_key":row.get::<_, String>(5)?,"branch":row.get::<_, Option<String>>(6)?,"head_sha":row.get::<_, Option<String>>(7)?,"base_ref":row.get::<_, String>(8)?,"base_sha":row.get::<_, Option<String>>(9)?,"baseline_snapshot_id":row.get::<_, Option<String>>(10)?}),
    )
}

fn command_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"created_at":timestamp_string(row.get_ref(1)?),"name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"cwd":row.get::<_, String>(4)?,"repo_path":row.get::<_, String>(5)?,"repo_key":row.get::<_, String>(6)?,"branch":row.get::<_, Option<String>>(7)?,"commit_sha":row.get::<_, Option<String>>(8)?,"shell":row.get::<_, String>(9)?,"approved_by":row.get::<_, String>(10)?,"approval_note":row.get::<_, String>(11)?,"artifact_specs":json_string(row.get::<_, String>(12)?),"enabled":row.get::<_, bool>(13)?,"duration_estimate_ms":row.get::<_, Option<i64>>(14)?,"duration_p90_ms":row.get::<_, Option<i64>>(15)?,"duration_sample_count":row.get::<_, i64>(16)?}),
    )
}

fn job_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"id":row.get::<_, String>(0)?,"command_id":row.get::<_, String>(1)?,"command_name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"idempotency_key":row.get::<_, Option<String>>(4)?,"cwd":row.get::<_, String>(5)?,"repo_path":row.get::<_, String>(6)?,"repo_key":row.get::<_, String>(7)?,"branch":row.get::<_, Option<String>>(8)?,"commit_sha":row.get::<_, Option<String>>(9)?,"queued_at":timestamp_string(row.get_ref(10)?),"started_at":optional_timestamp(row.get_ref(11)?),"ended_at":optional_timestamp(row.get_ref(12)?),"timeout_seconds":row.get::<_, Option<i64>>(13)?,"max_summary_lines":row.get::<_, i64>(14)?,"status":row.get::<_, String>(15)?,"stdout_path":row.get::<_, String>(16)?,"stderr_path":row.get::<_, String>(17)?,"error":row.get::<_, String>(18)?,"cancellation_requested_at":optional_timestamp(row.get_ref(19)?)}),
    )
}

fn run_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    let parsed_summary = json_string(row.get::<_, String>(17)?);
    let artifact_paths = json_string(row.get::<_, String>(18)?);
    Ok(
        json!({"id":row.get::<_, String>(0)?,"command_id":row.get::<_, String>(1)?,"command_name":row.get::<_, String>(2)?,"command":row.get::<_, String>(3)?,"idempotency_key":row.get::<_, Option<String>>(4)?,"cwd":row.get::<_, String>(5)?,"repo_path":row.get::<_, String>(6)?,"repo_key":row.get::<_, String>(7)?,"branch":row.get::<_, Option<String>>(8)?,"commit_sha":row.get::<_, Option<String>>(9)?,"started_at":timestamp_string(row.get_ref(10)?),"ended_at":timestamp_string(row.get_ref(11)?),"duration_ms":row.get::<_, i64>(12)?,"exit_code":row.get::<_, Option<i64>>(13)?,"status":row.get::<_, String>(14)?,"stdout_path":row.get::<_, String>(15)?,"stderr_path":row.get::<_, String>(16)?,"parsed_summary":parsed_summary,"artifact_paths":artifact_paths,"queued_at":optional_timestamp(row.get_ref(19)?),"queue_duration_ms":row.get::<_, Option<i64>>(20)?,"cancellation_requested_at":optional_timestamp(row.get_ref(21)?),"terminal":true,"poll_after_ms":Value::Null,"queue_position":Value::Null,"execution_mode":"background","cancellation_requested":!matches!(row.get_ref(21)?, ValueRef::Null),"coverage_ingest":coverage_ingest(&artifact_paths)}),
    )
}

fn artifact_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"run_id":row.get::<_, String>(0)?,"kind":row.get::<_, String>(1)?,"path":row.get::<_, String>(2)?,"exists":row.get::<_, bool>(3)?,"size_bytes":row.get::<_, Option<i64>>(4)?,"coverage_format":row.get::<_, Option<String>>(5)?,"suite":row.get::<_, Option<String>>(6)?,"modified_by_run":row.get::<_, bool>(7)?,"ingest_status":row.get::<_, Option<String>>(8)?,"snapshot_id":row.get::<_, Option<String>>(9)?,"ingest_error":row.get::<_, Option<String>>(10)?,"command_id":row.get::<_, String>(11)?,"command_name":row.get::<_, String>(12)?,"repo_key":row.get::<_, String>(13)?,"repo_path":row.get::<_, String>(14)?,"started_at":timestamp_string(row.get_ref(15)?),"ended_at":timestamp_string(row.get_ref(16)?),"status":row.get::<_, String>(17)?,"exit_code":row.get::<_, Option<i64>>(18)?}),
    )
}

fn line_from_row_with_file(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    #[allow(clippy::question_mark)]
    let mut value = match line_from_row_offset(row) {
        Ok(value) => value,
        Err(error) => return Err(error),
    };
    #[allow(clippy::option_map_unit_fn)]
    value.as_object_mut().map(|object| {
        object.insert(
            "file_path".to_owned(),
            json!(row.get::<_, String>(0).unwrap_or_default()),
        );
    });
    Ok(value)
}

fn required_managed_child(guard: &mut Option<Child>) -> AppResult<&mut Child> {
    guard.as_mut().ok_or_else(|| {
        AppError::Runtime("managed run process control was removed unexpectedly".to_owned())
    })
}

fn append_line_with_path(values: &mut HashMap<(String, i64), Value>, path: &str, mut line: Value) {
    let Some(object) = line.as_object_mut() else {
        return;
    };
    let Some(number) = object.get("line_number").and_then(Value::as_i64) else {
        return;
    };
    object.insert("file_path".to_owned(), json!(path));
    values.insert((path.to_owned(), number), line);
}

fn line_from_row_offset(row: &duckdb::Row<'_>) -> duckdb::Result<Value> {
    Ok(
        json!({"line_number":row.get::<_, i64>(1)?,"hits":row.get::<_, i64>(2)?,"covered":row.get::<_, bool>(3)?,"count_line":row.get::<_, bool>(4)?,"total_branches":row.get::<_, i64>(5)?,"covered_branches":row.get::<_, i64>(6)?,"total_functions":row.get::<_, i64>(7)?,"covered_functions":row.get::<_, i64>(8)?,"details":json_string(row.get::<_, String>(9)?)}),
    )
}

fn normalize_artifact_specs(value: Value) -> AppResult<Vec<Value>> {
    let Some(object) = value.as_object() else {
        return Ok(Vec::new());
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
    } else {
        "partial"
    };
    json!({"status":status,"configured":configured,"ingested":ingested,"failed":failed,"snapshot_ids":snapshot_ids})
}

fn summarize_logs(
    stdout_path: &Path,
    stderr_path: &Path,
    status: &str,
    exit_code: Option<i64>,
    duration_ms: i64,
    max_lines: usize,
) -> Value {
    let stdout = fs::read_to_string(stdout_path).unwrap_or_default();
    let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
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
    json!({"status":status,"exit_code":exit_code,"duration_ms":duration_ms,"stdout_line_count":stdout.lines().count(),"stderr_line_count":stderr.lines().count(),"counters":counters,"excerpts":excerpts,"truncated":false,"stdout_path":stdout_path,"stderr_path":stderr_path})
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

fn run_duration_ms(started: Option<&Value>, queued: Option<&Value>) -> i64 {
    let selected = started
        .filter(|value| !value.is_null())
        .or(queued.filter(|value| !value.is_null()));
    selected
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| {
            Utc::now()
                .signed_duration_since(value.with_timezone(&Utc))
                .num_milliseconds()
                .max(0)
        })
        .unwrap_or(0)
}

fn maintenance_due(settings: &ProjectSettings) -> bool {
    settings
        .compaction_last_run_at
        .as_ref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|last| {
            Utc::now()
                .signed_duration_since(last.with_timezone(&Utc))
                .num_seconds()
                >= settings.compaction_interval_seconds as i64
        })
        .unwrap_or(true)
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

fn overall_delta(current: &Value, baseline: &Value) -> Value {
    json!({"line_rate_delta":delta(current.get("line_rate"), baseline.get("line_rate")),"covered_lines_delta":current.get("covered_lines").and_then(Value::as_i64).unwrap_or_default()-baseline.get("covered_lines").and_then(Value::as_i64).unwrap_or_default(),"total_lines_delta":current.get("total_lines").and_then(Value::as_i64).unwrap_or_default()-baseline.get("total_lines").and_then(Value::as_i64).unwrap_or_default(),"branch_rate_delta":delta(current.get("branch_rate"), baseline.get("branch_rate")),"covered_branches_delta":current.get("covered_branches").and_then(Value::as_i64).unwrap_or_default()-baseline.get("covered_branches").and_then(Value::as_i64).unwrap_or_default(),"total_branches_delta":current.get("total_branches").and_then(Value::as_i64).unwrap_or_default()-baseline.get("total_branches").and_then(Value::as_i64).unwrap_or_default(),"function_rate_delta":delta(current.get("function_rate"), baseline.get("function_rate")),"covered_functions_delta":current.get("covered_functions").and_then(Value::as_i64).unwrap_or_default()-baseline.get("covered_functions").and_then(Value::as_i64).unwrap_or_default(),"total_functions_delta":current.get("total_functions").and_then(Value::as_i64).unwrap_or_default()-baseline.get("total_functions").and_then(Value::as_i64).unwrap_or_default(),"region_rate_delta":delta(current.get("region_rate"), baseline.get("region_rate")),"covered_regions_delta":current.get("covered_regions").and_then(Value::as_i64).unwrap_or_default()-baseline.get("covered_regions").and_then(Value::as_i64).unwrap_or_default(),"total_regions_delta":current.get("total_regions").and_then(Value::as_i64).unwrap_or_default()-baseline.get("total_regions").and_then(Value::as_i64).unwrap_or_default()})
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
    use crate::pool::checkout;
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

        let directory = tempfile::tempdir().unwrap();
        let stdout_path = directory.path().join("stdout");
        let stderr_path = directory.path().join("stderr");
        std::fs::write(&stdout_path, "passed\nwarning\n").unwrap();
        std::fs::write(&stderr_path, "failed\nerror\n").unwrap();
        let summary = summarize_logs(&stdout_path, &stderr_path, "failed", Some(1), 10, 1);
        assert_eq!(summary["counters"]["passed"], 1);
        assert_eq!(summary["counters"]["failed"], 1);
        assert_eq!(summary["stdout_line_count"], 2);
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
            empty.path(),
            &directory.path().join("missing"),
            "passed",
            None,
            0,
            0,
        );
        assert_eq!(missing["stderr_line_count"], 0);
    }

    #[test]
    fn maintenance_helpers_cover_empty_and_error_arms() {
        assert!(ensure_db_parent(Path::new("coverage.duckdb")).is_ok());
        assert!(ensure_db_parent(Path::new("")).is_ok());

        let mut values = HashMap::new();
        append_line_with_path(&mut values, "a.py", json!({"line_number": 1}));
        append_line_with_path(&mut values, "a.py", json!({"covered": true}));
        append_line_with_path(&mut values, "a.py", json!("scalar"));
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
            retain_compaction_thread(
                &Mutex::new(None),
                Err(std::io::Error::other("spawn failure")),
            )
            .is_ok()
        );

        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("helpers.duckdb"), test_config()).unwrap();
        assert!(
            store
                .retain_run_thread(Err(std::io::Error::other("spawn failure")))
                .is_ok()
        );
        let mut no_child = None;
        reap_child(&mut no_child);
        assert!(required_managed_child(&mut no_child).is_err());
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
        let snapshot = store
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
        let first = store
            .compact_snapshot_detail(&project.repo_key, snapshot["id"].as_str().unwrap())
            .unwrap();
        assert!(first.0);
        let second = store
            .compact_snapshot_detail(&project.repo_key, snapshot["id"].as_str().unwrap())
            .unwrap();
        assert!(!second.0);
        store
            .with_connection(|connection| {
                connection.execute_batch("DROP TABLE coverage_compacted_payloads")?;
                Ok(())
            })
            .unwrap();
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
    }

    #[test]
    fn time_and_delta_helpers_handle_missing_and_valid_values() {
        let old = (Utc::now() - ChronoDuration::seconds(5)).to_rfc3339();
        assert!(run_duration_ms(Some(&json!(old)), None) >= 0);
        assert!(run_duration_ms(Some(&Value::Null), Some(&json!(old))) >= 0);
        assert_eq!(run_duration_ms(Some(&json!("bad")), None), 0);
        assert!(maintenance_due(&ProjectSettings {
            compaction_last_run_at: None,
            ..test_settings()
        }));
        assert!(maintenance_due(&ProjectSettings {
            compaction_last_run_at: Some(old.clone()),
            compaction_interval_seconds: 1,
            ..test_settings()
        }));
        let future = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
        assert!(!maintenance_due(&ProjectSettings {
            compaction_last_run_at: Some(future),
            compaction_interval_seconds: 3600,
            ..test_settings()
        }));
        assert_eq!(delta(Some(&json!(3.0)), Some(&json!(1.0))), json!(2.0));
        assert!(delta(Some(&Value::Null), Some(&json!(1.0))).is_null());
        let current =
            json!({"line_rate":0.75,"covered_lines":3,"total_lines":4,"branch_rate":null});
        let baseline = json!({"line_rate":0.5,"covered_lines":2,"total_lines":4,"branch_rate":0.2});
        assert_eq!(overall_delta(&current, &baseline)["covered_lines_delta"], 1);
        assert_eq!(status_order(&json!({"status":"regressed"})), 0);
        assert_eq!(status_order(&json!({"status":"improved"})), 1);
        assert_eq!(status_order(&json!({"status":"same"})), 2);
        assert_eq!(insight_order(&json!({"severity":"high"})), 0);
        assert_eq!(insight_order(&json!({"severity":"medium"})), 1);
        assert_eq!(insight_order(&json!({"severity":"info"})), 2);
        assert_eq!(json_string("{\"value\":1}".to_owned())["value"], 1);
        assert_eq!(json_string("not-json".to_owned()), json!("not-json"));
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
        assert!(matches!(
            store.with_read_connection(no_op_connection),
            Err(AppError::Busy { .. })
        ));
        drop(held);
        assert!(store.with_read_connection(no_op_connection).is_ok());
        store.close().unwrap();

        let closed_pool_store =
            CoverageStore::open(directory.path().join("closed-pool.duckdb"), test_config())
                .unwrap();
        closed_pool_store.inner.pool.lock().unwrap().take();
        assert!(matches!(
            closed_pool_store.with_read_connection(no_op_connection),
            Err(AppError::Runtime(message)) if message == "DuckDB pool is closed"
        ));
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
        assert!(matches!(error, AppError::Timeout { .. }));
        drop(guard);
        timeout_store.close().unwrap();
    }

    #[test]
    fn legacy_schema_rows_and_lock_errors_are_exercised() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE snapshots (id VARCHAR); CREATE TABLE files (id VARCHAR); CREATE TABLE registered_commands (id VARCHAR); CREATE TABLE runs (id VARCHAR); CREATE TABLE run_jobs (id VARCHAR); CREATE TABLE run_artifacts (id VARCHAR); CREATE TABLE lines (hits INTEGER);",
            )
            .unwrap();
        migrate_schema(&connection).unwrap();
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
        let mut invalid = connection.prepare("SELECT 1").unwrap();
        let mut invalid_rows = invalid.query([]).unwrap();
        assert!(line_from_row_with_file(invalid_rows.next().unwrap().unwrap()).is_err());
        assert!(
            lock_error(std::sync::PoisonError::new(()))
                .to_string()
                .contains("poisoned")
        );

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
    fn run_scheduler_and_terminal_job_edges_are_exercised() {
        let directory = tempfile::tempdir().unwrap();
        let config = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 59_471,
            db_path: None,
            common_db_path: directory.path().join("common.duckdb"),
            run_retention: 100,
            run_concurrency: 1,
            mcp_http_concurrency: 16,
            db_pool_size: 4,
            db_acquire_timeout_ms: 5_000,
            db_query_timeout_ms: 30_000,
            http_request_timeout_seconds: 60,
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
        assert!(waiting.join().unwrap().is_ok());
        store.inner.closing.store(false, Ordering::SeqCst);

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
                connection.execute(INSERT_EDGE_JOB_SQL, values)?;
                Ok(())
            })
        };

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
        store.close().unwrap();
    }

    #[test]
    fn database_leases_reject_duplicate_owners_and_release_on_close() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("leased.duckdb");
        let first = CoverageStore::open(db_path.clone(), test_config()).unwrap();
        let second = CoverageStore::open(db_path.clone(), test_config());
        assert!(matches!(second, Err(AppError::Busy { .. })));
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
        store.inner.closing.store(true, Ordering::SeqCst);
        store.inner.compaction_wakeup.notify_all();
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
        make_readonly_view(&cancel_store, "run_jobs");
        assert!(cancel_store.cancel_run(&cancel_id, 20).is_err());
        cancel_store.close().unwrap();

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
        submit_store.close().unwrap();

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
        make_broken_view(&worktree_query_store, "snapshots");
        assert!(
            worktree_query_store
                .register_worktree(git_directory.path(), "main", None)
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
                .register_worktree(git_directory.path(), "main", None)
                .is_err()
        );
        worktree_insert_store.close().unwrap();

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
        make_readonly_view(&execute_store, "runs");
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
            assert!(Instant::now() < deadline, "managed process did not start");
            std::thread::sleep(Duration::from_millis(5));
        }
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.inner.write_gate.lock().unwrap();
            panic!("injected polling write gate poison");
        }));
        assert!(poisoned.is_err());
        assert!(runner.join().unwrap().is_err());
        store.inner.write_gate.clear_poison();
        let control = store
            .inner
            .active_processes
            .lock()
            .unwrap()
            .remove(&run_id)
            .unwrap();
        let mut child = control.lock().unwrap().take().unwrap();
        let _ = child.kill();
        let _ = child.wait();
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
            db_path: None,
            common_db_path: PathBuf::from("common.duckdb"),
            run_retention: 100,
            run_concurrency: 1,
            mcp_http_concurrency: 16,
            db_pool_size: 4,
            db_acquire_timeout_ms: 5_000,
            db_query_timeout_ms: 30_000,
            http_request_timeout_seconds: 60,
            default_compaction_after_days: 30,
            default_compaction_interval_seconds: 3_600,
            default_compaction_batch_size: 100,
        }
    }

    fn stop_compaction_worker(store: &CoverageStore) {
        store.inner.closing.store(true, Ordering::SeqCst);
        store.inner.compaction_wakeup.notify_all();
        store
            .inner
            .compaction_thread
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();
        store.inner.closing.store(false, Ordering::SeqCst);
    }

    fn make_readonly_view(store: &CoverageStore, table: &str) {
        store
            .with_connection(|connection| {
                connection
                    .execute_batch(&format!(
                        "DROP INDEX IF EXISTS idx_project_settings_updated; DROP INDEX IF EXISTS idx_run_jobs_status_time; DROP INDEX IF EXISTS idx_run_artifacts_kind; DROP INDEX IF EXISTS idx_registered_commands_name; DROP INDEX IF EXISTS idx_worktrees_repo; DROP INDEX IF EXISTS idx_runs_command_time; ALTER TABLE {table} RENAME TO {table}_base; CREATE VIEW {table} AS SELECT * FROM {table}_base;"
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
