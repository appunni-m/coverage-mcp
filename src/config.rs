use std::env;
use std::path::{Path, PathBuf};

use crate::error::{AppError, AppResult};
use crate::git::inspect_git;

/// Default loopback port used by the shared daemon.
pub const DEFAULT_PORT: u16 = 59_471;
/// Default number of retained terminal runs per registered command.
pub const DEFAULT_RUN_RETENTION: usize = 100;
/// Default number of command workers.
pub const DEFAULT_RUN_CONCURRENCY: usize = 4;
/// Default concurrent MCP request limit.
pub const DEFAULT_MCP_HTTP_CONCURRENCY: usize = 16;
/// Default maximum number of DuckDB connections held by each project store.
pub const DEFAULT_DB_POOL_SIZE: usize = 4;
/// Default maximum wait for a connection from a project pool.
pub const DEFAULT_DB_ACQUIRE_TIMEOUT_MS: u64 = 5_000;
/// Default maximum duration of one DuckDB operation.
pub const DEFAULT_DB_QUERY_TIMEOUT_MS: u64 = 30_000;
/// Default maximum duration of one HTTP request, including body reads.
pub const DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS: u64 = 60;
/// Default age at which coverage detail becomes eligible for compression.
pub const DEFAULT_COMPACTION_AFTER_DAYS: u32 = 30;
/// Default background maintenance cadence.
pub const DEFAULT_COMPACTION_INTERVAL_SECONDS: u64 = 3_600;
/// Maximum number of snapshots handled in one maintenance pass.
pub const DEFAULT_COMPACTION_BATCH_SIZE: u32 = 100;

/// Runtime configuration shared by the HTTP, MCP, storage, and maintenance layers.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Bind host.
    pub host: String,
    /// Bind port.
    pub port: u16,
    /// Optional standalone repository database.
    pub db_path: Option<PathBuf>,
    /// Daemon-wide project registry database.
    pub common_db_path: PathBuf,
    /// Terminal run retention per command.
    pub run_retention: usize,
    /// Managed command worker count.
    pub run_concurrency: usize,
    /// Concurrent MCP request limit.
    pub mcp_http_concurrency: usize,
    /// Maximum number of pooled DuckDB connections per project.
    pub db_pool_size: usize,
    /// Maximum time to wait for a pooled DuckDB connection.
    pub db_acquire_timeout_ms: u64,
    /// Maximum time allowed for one DuckDB operation.
    pub db_query_timeout_ms: u64,
    /// Maximum time allowed for one HTTP request.
    pub http_request_timeout_seconds: u64,
    /// Default policy for newly seen projects.
    pub default_compaction_after_days: u32,
    /// Default maintenance interval for newly seen projects.
    pub default_compaction_interval_seconds: u64,
    /// Default maintenance batch size for newly seen projects.
    pub default_compaction_batch_size: u32,
}

impl ServerConfig {
    /// Builds configuration from CLI overrides and environment variables.
    pub fn from_environment(
        host: Option<String>,
        port: Option<u16>,
        db_path: Option<PathBuf>,
        common_db_path: Option<PathBuf>,
    ) -> AppResult<Self> {
        Self::from_environment_with_lookup(host, port, db_path, common_db_path, &|name| {
            env::var(name).ok()
        })
    }

    fn from_environment_with_lookup(
        host: Option<String>,
        port: Option<u16>,
        db_path: Option<PathBuf>,
        common_db_path: Option<PathBuf>,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> AppResult<Self> {
        let host = host
            .or_else(|| lookup("COVERAGE_MCP_HOST"))
            .unwrap_or_else(|| "127.0.0.1".to_owned());
        let port = port
            .or_else(|| lookup("COVERAGE_MCP_PORT").and_then(|value| value.parse().ok()))
            .unwrap_or(DEFAULT_PORT);
        let common_db_path = common_db_path
            .or_else(|| lookup("COVERAGE_MCP_COMMON_DB").map(PathBuf::from))
            .unwrap_or_else(|| default_common_db_path_with_lookup(lookup));
        let run_retention = env_usize(
            "COVERAGE_MCP_RUN_RETENTION",
            DEFAULT_RUN_RETENTION,
            1,
            10_000,
            lookup,
        )?;
        let run_concurrency = env_usize(
            "COVERAGE_MCP_RUN_CONCURRENCY",
            DEFAULT_RUN_CONCURRENCY,
            1,
            32,
            lookup,
        )?;
        let mcp_http_concurrency = env_usize(
            "COVERAGE_MCP_HTTP_CONCURRENCY",
            DEFAULT_MCP_HTTP_CONCURRENCY,
            1,
            128,
            lookup,
        )?;
        let db_pool_size = env_usize(
            "COVERAGE_MCP_DB_POOL_SIZE",
            DEFAULT_DB_POOL_SIZE,
            1,
            16,
            lookup,
        )?;
        let db_acquire_timeout_ms = env_u64(
            "COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS",
            DEFAULT_DB_ACQUIRE_TIMEOUT_MS,
            50,
            120_000,
            lookup,
        )?;
        let db_query_timeout_ms = env_u64(
            "COVERAGE_MCP_DB_QUERY_TIMEOUT_MS",
            DEFAULT_DB_QUERY_TIMEOUT_MS,
            100,
            3_600_000,
            lookup,
        )?;
        let http_request_timeout_seconds = env_u64(
            "COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS,
            1,
            3_600,
            lookup,
        )?;
        if db_query_timeout_ms >= http_request_timeout_seconds.saturating_mul(1_000) {
            return Err(AppError::Validation(
                "COVERAGE_MCP_DB_QUERY_TIMEOUT_MS must be shorter than the HTTP request timeout"
                    .to_owned(),
            ));
        }
        let default_compaction_after_days = env_u32(
            "COVERAGE_MCP_COMPACTION_AFTER_DAYS",
            DEFAULT_COMPACTION_AFTER_DAYS,
            1,
            36_500,
            lookup,
        )?;
        let default_compaction_interval_seconds = env_u64(
            "COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS",
            DEFAULT_COMPACTION_INTERVAL_SECONDS,
            60,
            86_400,
            lookup,
        )?;
        let default_compaction_batch_size = env_u32(
            "COVERAGE_MCP_COMPACTION_BATCH_SIZE",
            DEFAULT_COMPACTION_BATCH_SIZE,
            1,
            10_000,
            lookup,
        )?;
        Ok(Self {
            host,
            port,
            db_path,
            common_db_path,
            run_retention,
            run_concurrency,
            mcp_http_concurrency,
            db_pool_size,
            db_acquire_timeout_ms,
            db_query_timeout_ms,
            http_request_timeout_seconds,
            default_compaction_after_days,
            default_compaction_interval_seconds,
            default_compaction_batch_size,
        })
    }

    /// Builds standalone configuration rooted at a repository checkout.
    pub fn for_repository(path: PathBuf) -> AppResult<Self> {
        Self::for_repository_with_lookup(path, &|name| env::var(name).ok())
    }

    fn for_repository_with_lookup(
        path: PathBuf,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> AppResult<Self> {
        let git = inspect_git(&path)?;
        let mut config = Self::from_environment_with_lookup(None, None, None, None, lookup)?;
        config.db_path = Some(default_db_path(Path::new(&git.repo_key)));
        Ok(config)
    }
}

/// Returns the repository-local database path used by standalone mode.
pub fn default_db_path(path: &Path) -> PathBuf {
    path.join(".coverage-mcp").join("coverage.duckdb")
}

/// Returns the daemon-wide registry path.
pub fn default_common_db_path() -> PathBuf {
    default_common_db_path_with_lookup(&|name| env::var(name).ok())
}

fn default_common_db_path_with_lookup(lookup: &dyn Fn(&str) -> Option<String>) -> PathBuf {
    lookup("COVERAGE_MCP_COMMON_DB")
        .or_else(|| lookup("COVERAGE_MCP_DB"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(lookup("HOME").unwrap_or_else(|| ".".to_owned()))
                .join(".coverage-mcp")
                .join("common.duckdb")
        })
}

fn env_usize(
    name: &str,
    default: usize,
    min: usize,
    max: usize,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> AppResult<usize> {
    let value = lookup(name)
        .map(|raw| {
            raw.parse::<usize>()
                .map_err(|_| AppError::Validation(format!("{name} must be an integer")))
        })
        .transpose()?
        .unwrap_or(default);
    if !(min..=max).contains(&value) {
        return Err(AppError::Validation(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn env_u32(
    name: &str,
    default: u32,
    min: u32,
    max: u32,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> AppResult<u32> {
    let value = lookup(name)
        .map(|raw| {
            raw.parse::<u32>()
                .map_err(|_| AppError::Validation(format!("{name} must be an integer")))
        })
        .transpose()?
        .unwrap_or(default);
    if !(min..=max).contains(&value) {
        return Err(AppError::Validation(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn env_u64(
    name: &str,
    default: u64,
    min: u64,
    max: u64,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> AppResult<u64> {
    let value = lookup(name)
        .map(|raw| {
            raw.parse::<u64>()
                .map_err(|_| AppError::Validation(format!("{name} must be an integer")))
        })
        .transpose()?
        .unwrap_or(default);
    if !(min..=max).contains(&value) {
        return Err(AppError::Validation(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_explicit_overrides_are_stable() {
        let config = ServerConfig::from_environment(
            Some("localhost".to_owned()),
            Some(1234),
            Some(PathBuf::from("db.duckdb")),
            Some(PathBuf::from("common.duckdb")),
        )
        .expect("configuration");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1234);
        assert_eq!(config.db_path, Some(PathBuf::from("db.duckdb")));
        assert_eq!(config.common_db_path, PathBuf::from("common.duckdb"));
        assert_eq!(
            config.default_compaction_after_days,
            DEFAULT_COMPACTION_AFTER_DAYS
        );

        let defaults =
            ServerConfig::from_environment(None, None, None, Some(PathBuf::from("common")))
                .expect("default configuration");
        assert_eq!(defaults.host, "127.0.0.1");
        assert_eq!(defaults.port, DEFAULT_PORT);
        assert_eq!(defaults.run_retention, DEFAULT_RUN_RETENTION);
        assert_eq!(defaults.run_concurrency, DEFAULT_RUN_CONCURRENCY);
        assert_eq!(defaults.mcp_http_concurrency, DEFAULT_MCP_HTTP_CONCURRENCY);
        assert_eq!(defaults.db_pool_size, DEFAULT_DB_POOL_SIZE);
        assert_eq!(
            defaults.db_acquire_timeout_ms,
            DEFAULT_DB_ACQUIRE_TIMEOUT_MS
        );
        assert_eq!(defaults.db_query_timeout_ms, DEFAULT_DB_QUERY_TIMEOUT_MS);
        assert_eq!(
            defaults.http_request_timeout_seconds,
            DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS
        );
        assert_eq!(
            defaults.default_compaction_interval_seconds,
            DEFAULT_COMPACTION_INTERVAL_SECONDS
        );
        assert_eq!(
            defaults.default_compaction_batch_size,
            DEFAULT_COMPACTION_BATCH_SIZE
        );
    }

    #[test]
    fn repository_and_path_defaults_are_usable() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = ServerConfig::for_repository(directory.path().to_path_buf()).expect("config");
        assert!(
            config
                .db_path
                .expect("db path")
                .ends_with("coverage.duckdb")
        );
        assert_eq!(
            default_db_path(directory.path()),
            directory.path().join(".coverage-mcp/coverage.duckdb")
        );
        assert!(default_common_db_path().ends_with("common.duckdb"));

        let invalid = |name: &str| {
            (name == "COVERAGE_MCP_RUN_RETENTION").then(|| "not-an-integer".to_owned())
        };
        assert!(
            ServerConfig::for_repository_with_lookup(directory.path().to_path_buf(), &invalid)
                .is_err()
        );
        let no_environment = |_: &str| None;
        assert!(
            ServerConfig::for_repository_with_lookup(PathBuf::from("\0"), &no_environment).is_err()
        );
    }

    #[test]
    fn numeric_helpers_validate_defaults_and_bad_environment_values() {
        let real_env = |name: &str| env::var(name).ok();
        assert_eq!(
            env_usize("COVERAGE_MCP_TEST_MISSING", 5, 1, 10, &real_env).unwrap(),
            5
        );
        assert_eq!(
            env_u32("COVERAGE_MCP_TEST_MISSING", 5, 1, 10, &real_env).unwrap(),
            5
        );
        assert_eq!(
            env_u64("COVERAGE_MCP_TEST_MISSING", 5, 1, 10, &real_env).unwrap(),
            5
        );
        assert!(env_usize("PATH", 5, 1, 10, &real_env).is_err());
        assert!(env_u32("PATH", 5, 1, 10, &real_env).is_err());
        assert!(env_u64("PATH", 5, 1, 10, &real_env).is_err());
        let missing = |_: &str| None;
        assert!(env_usize("COVERAGE_MCP_TEST_MISSING", 0, 1, 10, &missing).is_err());
        assert!(env_u32("COVERAGE_MCP_TEST_MISSING", 0, 1, 10, &missing).is_err());
        assert!(env_u64("COVERAGE_MCP_TEST_MISSING", 0, 1, 10, &missing).is_err());

        let overrides = |name: &str| {
            Some(
                match name {
                    "COVERAGE_MCP_HOST" => "localhost",
                    "COVERAGE_MCP_PORT" => "1234",
                    "COVERAGE_MCP_COMMON_DB" => "common.duckdb",
                    "COVERAGE_MCP_RUN_RETENTION" => "7",
                    "COVERAGE_MCP_RUN_CONCURRENCY" => "2",
                    "COVERAGE_MCP_HTTP_CONCURRENCY" => "3",
                    "COVERAGE_MCP_DB_POOL_SIZE" => "5",
                    "COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS" => "250",
                    "COVERAGE_MCP_DB_QUERY_TIMEOUT_MS" => "2000",
                    "COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS" => "90",
                    "COVERAGE_MCP_COMPACTION_AFTER_DAYS" => "9",
                    "COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS" => "120",
                    "COVERAGE_MCP_COMPACTION_BATCH_SIZE" => "11",
                    _ => return None,
                }
                .to_owned(),
            )
        };
        assert!(overrides("UNKNOWN").is_none());
        let configured =
            ServerConfig::from_environment_with_lookup(None, None, None, None, &overrides).unwrap();
        assert_eq!(configured.host, "localhost");
        assert_eq!(configured.port, 1234);
        assert_eq!(configured.run_retention, 7);
        assert_eq!(configured.run_concurrency, 2);
        assert_eq!(configured.mcp_http_concurrency, 3);
        assert_eq!(configured.db_pool_size, 5);
        assert_eq!(configured.db_acquire_timeout_ms, 250);
        assert_eq!(configured.db_query_timeout_ms, 2000);
        assert_eq!(configured.http_request_timeout_seconds, 90);
        assert_eq!(configured.default_compaction_after_days, 9);
        assert_eq!(configured.default_compaction_interval_seconds, 120);
        assert_eq!(configured.default_compaction_batch_size, 11);

        let invalid_deadline = |name: &str| match name {
            "COVERAGE_MCP_DB_QUERY_TIMEOUT_MS" => Some("30000".to_owned()),
            "COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS" => Some("30".to_owned()),
            _ => None,
        };
        assert!(
            ServerConfig::from_environment_with_lookup(
                None,
                None,
                None,
                Some(PathBuf::from("common")),
                &invalid_deadline,
            )
            .is_err()
        );

        for invalid_name in [
            "COVERAGE_MCP_RUN_RETENTION",
            "COVERAGE_MCP_RUN_CONCURRENCY",
            "COVERAGE_MCP_HTTP_CONCURRENCY",
            "COVERAGE_MCP_DB_POOL_SIZE",
            "COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS",
            "COVERAGE_MCP_DB_QUERY_TIMEOUT_MS",
            "COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS",
            "COVERAGE_MCP_COMPACTION_AFTER_DAYS",
            "COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS",
            "COVERAGE_MCP_COMPACTION_BATCH_SIZE",
        ] {
            let invalid = |name: &str| (name == invalid_name).then(|| "not-an-integer".to_owned());
            assert!(
                ServerConfig::from_environment_with_lookup(None, None, None, None, &invalid)
                    .is_err(),
                "{invalid_name} should reject malformed values"
            );
        }

        let invalid_port =
            |name: &str| (name == "COVERAGE_MCP_PORT").then(|| "not-an-integer".to_owned());
        let fallback_port = ServerConfig::from_environment_with_lookup(
            None,
            None,
            None,
            Some(PathBuf::from("common")),
            &invalid_port,
        )
        .unwrap();
        assert_eq!(fallback_port.port, DEFAULT_PORT);

        let legacy_db =
            |name: &str| (name == "COVERAGE_MCP_DB").then(|| "legacy.duckdb".to_owned());
        assert_eq!(
            default_common_db_path_with_lookup(&legacy_db),
            PathBuf::from("legacy.duckdb")
        );
        let home = |name: &str| (name == "HOME").then(|| "/tmp/test-home".to_owned());
        assert_eq!(
            default_common_db_path_with_lookup(&home),
            PathBuf::from("/tmp/test-home/.coverage-mcp/common.duckdb")
        );
        let no_environment = |_: &str| None;
        assert_eq!(
            default_common_db_path_with_lookup(&no_environment),
            PathBuf::from("./.coverage-mcp/common.duckdb")
        );

        for invalid in [
            "COVERAGE_MCP_RUN_RETENTION",
            "COVERAGE_MCP_RUN_CONCURRENCY",
            "COVERAGE_MCP_HTTP_CONCURRENCY",
            "COVERAGE_MCP_COMPACTION_AFTER_DAYS",
            "COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS",
            "COVERAGE_MCP_COMPACTION_BATCH_SIZE",
        ] {
            let lookup = |name: &str| (name == invalid).then(|| "not-an-integer".to_owned());
            assert!(
                ServerConfig::from_environment_with_lookup(
                    None,
                    None,
                    None,
                    Some(PathBuf::from("common")),
                    &lookup,
                )
                .is_err(),
                "invalid environment value should fail: {invalid}"
            );
        }

        for (name, value) in [
            ("COVERAGE_MCP_RUN_RETENTION", "10001"),
            ("COVERAGE_MCP_RUN_CONCURRENCY", "33"),
            ("COVERAGE_MCP_HTTP_CONCURRENCY", "129"),
            ("COVERAGE_MCP_DB_POOL_SIZE", "17"),
            ("COVERAGE_MCP_DB_ACQUIRE_TIMEOUT_MS", "49"),
            ("COVERAGE_MCP_DB_QUERY_TIMEOUT_MS", "99"),
            ("COVERAGE_MCP_HTTP_REQUEST_TIMEOUT_SECONDS", "0"),
            ("COVERAGE_MCP_COMPACTION_AFTER_DAYS", "36501"),
            ("COVERAGE_MCP_COMPACTION_INTERVAL_SECONDS", "86401"),
            ("COVERAGE_MCP_COMPACTION_BATCH_SIZE", "10001"),
        ] {
            let lookup = |candidate: &str| (candidate == name).then(|| value.to_owned());
            assert!(
                ServerConfig::from_environment_with_lookup(
                    None,
                    None,
                    None,
                    Some(PathBuf::from("common")),
                    &lookup,
                )
                .is_err(),
                "out-of-range environment value should fail: {name}"
            );
        }
    }
}
