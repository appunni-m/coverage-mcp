//! Coverage MCP command-line entry point.

#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use coverage_mcp::config::default_db_path;
use coverage_mcp::error::{AppError, AppResult};
use coverage_mcp::git::inspect_git;
use coverage_mcp::http::REPOSITORY_HEADER;
use coverage_mcp::mcp;
use coverage_mcp::service::{CoverageService, RequestContext};
use coverage_mcp::{CoverageServer, CoverageStore, SCHEMA_REVISION, ServerConfig, VERSION};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_HEADER_LIMIT: usize = 64 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "coverage-mcp",
    version,
    about = "Local-first coverage MCP server"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the shared HTTP daemon.
    Serve {
        /// Bind host, defaulting to COVERAGE_MCP_HOST or 127.0.0.1.
        #[arg(long, env = "COVERAGE_MCP_HOST")]
        host: Option<String>,
        /// Bind port, defaulting to COVERAGE_MCP_PORT or 59471.
        #[arg(long, env = "COVERAGE_MCP_PORT")]
        port: Option<u16>,
        /// Use one standalone repository database instead of lazy project routing.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Override the daemon-wide common registry database.
        #[arg(long, env = "COVERAGE_MCP_COMMON_DB")]
        common_db: Option<PathBuf>,
    },
    /// Bridge MCP JSON-RPC stdio to the shared HTTP daemon.
    #[command(alias = "stdio")]
    Connect {
        /// Repository checkout used for project selection, defaulting to `.`.
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        /// Run standalone against this database instead of using the shared daemon.
        #[arg(long, env = "COVERAGE_MCP_DB")]
        db: Option<PathBuf>,
    },
    /// Run one compaction pass for a project and exit.
    Compact {
        /// Repository checkout or project path.
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        /// Override the number of days before a coverage event is compacted.
        #[arg(long)]
        older_than_days: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve {
        host: None,
        port: None,
        db: None,
        common_db: None,
    }) {
        Command::Serve {
            host,
            port,
            db,
            common_db,
        } => {
            let config = ServerConfig::from_environment(host, port, db, common_db)?;
            CoverageServer::new(config)?.run().await?;
        }
        Command::Connect { repo, db } => run_stdio(repo, db).await?,
        Command::Compact {
            repo,
            older_than_days,
        } => {
            let config = ServerConfig::for_repository(repo.clone())?;
            let db_path = standalone_db_path(&config)?;
            let store = coverage_mcp::CoverageStore::open(db_path, config.clone())?;
            store.ensure_project(&repo)?;
            if let Some(days) = older_than_days {
                store.update_project_settings(coverage_mcp::storage::ProjectSettingsPatch {
                    compaction_after_days: Some(days),
                    ..Default::default()
                })?;
            }
            let result = store.compact_now()?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            store.close()?;
        }
    }
    Ok(())
}

async fn run_stdio(repo: PathBuf, db: Option<PathBuf>) -> AppResult<()> {
    if let Some(db) = db {
        return run_standalone_stdio(repo, db).await;
    }
    run_shared_stdio(repo).await
}

async fn run_standalone_stdio(repo: PathBuf, db: PathBuf) -> AppResult<()> {
    let mut config = ServerConfig::for_repository(repo.clone())?;
    config.db_path = Some(db);
    let repo_path = config
        .db_path
        .clone()
        .unwrap_or_else(|| default_db_path(&repo));
    let store = CoverageStore::open(repo_path, config)?;
    let git = store.ensure_project(&repo)?;
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
    );

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let stdout = BufWriter::new(tokio::io::stdout());
    let mut stdout = stdout;
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => mcp::dispatch_json_rpc(Some(&service), &request),
            Err(error) => Some(json_rpc_error(None, coverage_mcp::AppError::Json(error))),
        };
        if let Some(response) = response {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    store.close()?;
    Ok(())
}

async fn run_shared_stdio(repo: PathBuf) -> AppResult<()> {
    let config = ServerConfig::from_environment(None, None, None, None)?;
    validate_loopback_host(&config.host)?;
    let git = inspect_git(&repo)?;
    ensure_daemon(&config).await?;

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = BufWriter::new(tokio::io::stdout());
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => {
                let id = request.get("id").cloned();
                match forward_mcp_request(&config, &git.repo_path, &request).await {
                    Ok(response) => response,
                    Err(error) if id.is_some() => Some(json_rpc_error(id, error)),
                    Err(_) => None,
                }
            }
            Err(error) => Some(json_rpc_error(None, AppError::Json(error))),
        };
        if let Some(response) = response {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum DaemonHealth {
    Compatible,
    Unavailable,
    Incompatible(String),
}

async fn ensure_daemon(config: &ServerConfig) -> AppResult<()> {
    match daemon_health(config).await? {
        DaemonHealth::Compatible => return Ok(()),
        DaemonHealth::Incompatible(message) => return Err(AppError::Runtime(message)),
        DaemonHealth::Unavailable => {}
    }

    spawn_daemon(config)?;
    let started = Instant::now();
    while started.elapsed() < DAEMON_START_TIMEOUT {
        match daemon_health(config).await? {
            DaemonHealth::Compatible => return Ok(()),
            DaemonHealth::Incompatible(message) => return Err(AppError::Runtime(message)),
            DaemonHealth::Unavailable => sleep(DAEMON_START_POLL_INTERVAL).await,
        }
    }
    Err(AppError::Runtime(format!(
        "Coverage MCP daemon did not become healthy at http://{}; inspect {}",
        host_authority(&config.host, config.port),
        daemon_log_path(&config.common_db_path).display()
    )))
}

async fn daemon_health(config: &ServerConfig) -> AppResult<DaemonHealth> {
    let response = match daemon_request(config, "GET", "/health", None, &[]).await {
        Ok(response) => response,
        Err(AppError::Io(_) | AppError::Timeout { .. }) => {
            return Ok(DaemonHealth::Unavailable);
        }
        Err(error) => return Err(error),
    };
    if response.status != 200 {
        return Ok(DaemonHealth::Unavailable);
    }
    let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
        AppError::Runtime(format!(
            "Coverage MCP daemon returned invalid health JSON: {error}"
        ))
    })?;
    let healthy = value.get("status").and_then(Value::as_str) == Some("ok")
        || value.get("ok").and_then(Value::as_bool) == Some(true);
    let version = value
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let schema = value
        .get("schema_revision")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let common_db = value
        .get("common_db_path")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if healthy
        && version == VERSION
        && schema == u64::from(SCHEMA_REVISION)
        && same_path(Path::new(common_db), &config.common_db_path)
    {
        return Ok(DaemonHealth::Compatible);
    }
    Ok(DaemonHealth::Incompatible(format!(
        "Coverage MCP daemon at http://{} reports version {version}, schema {schema}, common database {common_db}; connector requires version {VERSION}, schema {SCHEMA_REVISION}, common database {}",
        host_authority(&config.host, config.port),
        config.common_db_path.display()
    )))
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn spawn_daemon(config: &ServerConfig) -> AppResult<()> {
    let log_path = daemon_log_path(&config.common_db_path);
    std::fs::create_dir_all(log_path.parent().unwrap_or(Path::new(".")))?;
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let stderr = stdout.try_clone()?;
    let mut command = ProcessCommand::new(std::env::current_exe()?);
    command
        .arg("serve")
        .arg("--host")
        .arg(&config.host)
        .arg("--port")
        .arg(config.port.to_string())
        .arg("--common-db")
        .arg(&config.common_db_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn()?;
    let _ = child.try_wait()?;
    Ok(())
}

fn daemon_log_path(common_db_path: &Path) -> PathBuf {
    common_db_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("daemon.log")
}

async fn forward_mcp_request(
    config: &ServerConfig,
    repo_path: &str,
    request: &Value,
) -> AppResult<Option<Value>> {
    let body = request.to_string();
    let response =
        daemon_request(config, "POST", "/mcp/", Some(repo_path), body.as_bytes()).await?;
    match response.status {
        200 => Ok(Some(serde_json::from_slice(&response.body)?)),
        202 => Ok(None),
        status => Err(AppError::Runtime(format!(
            "Coverage MCP daemon returned HTTP {status}: {}",
            String::from_utf8_lossy(&response.body)
        ))),
    }
}

struct WireResponse {
    status: u16,
    body: Vec<u8>,
}

async fn daemon_request(
    config: &ServerConfig,
    method: &str,
    path: &str,
    repo_path: Option<&str>,
    body: &[u8],
) -> AppResult<WireResponse> {
    validate_loopback_host(&config.host)?;
    if repo_path.is_some_and(|value| value.contains(['\r', '\n'])) {
        return Err(AppError::Validation(
            "repository path cannot contain an HTTP line break".to_owned(),
        ));
    }
    let authority = host_authority(&config.host, config.port);
    let request_timeout = Duration::from_secs(config.http_request_timeout_seconds);
    let operation = async {
        let mut stream = TcpStream::connect((config.host.as_str(), config.port)).await?;
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        )
        .into_bytes();
        if let Some(repo_path) = repo_path {
            request.extend_from_slice(format!("{REPOSITORY_HEADER}: {repo_path}\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        stream.write_all(&request).await?;
        stream.flush().await?;

        let response_limit = config.http_max_body_bytes + HTTP_HEADER_LIMIT;
        let mut bytes = Vec::new();
        stream
            .take((response_limit + 1) as u64)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > response_limit {
            return Err(AppError::Validation(
                "Coverage MCP daemon response is too large".to_owned(),
            ));
        }
        parse_http_response(bytes)
    };
    timeout(request_timeout, operation)
        .await
        .map_err(|_| AppError::Timeout {
            operation: format!("{method} {path}"),
            timeout_ms: config.http_request_timeout_seconds * 1_000,
        })?
}

fn parse_http_response(bytes: Vec<u8>) -> AppResult<WireResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            AppError::Runtime("Coverage MCP daemon returned malformed HTTP".to_owned())
        })?;
    if header_end > HTTP_HEADER_LIMIT {
        return Err(AppError::Validation(
            "Coverage MCP daemon response headers are too large".to_owned(),
        ));
    }
    let headers = std::str::from_utf8(&bytes[..header_end]).map_err(|_| {
        AppError::Runtime("Coverage MCP daemon returned non-UTF-8 HTTP headers".to_owned())
    })?;
    let mut lines = headers.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| {
            AppError::Runtime("Coverage MCP daemon returned an invalid HTTP status".to_owned())
        })?;
    let content_length = lines.find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    let body = bytes[(header_end + 4)..].to_vec();
    if content_length.is_some_and(|length| length != body.len()) {
        return Err(AppError::Runtime(
            "Coverage MCP daemon returned an incomplete HTTP body".to_owned(),
        ));
    }
    Ok(WireResponse { status, body })
}

fn validate_loopback_host(host: &str) -> AppResult<()> {
    if matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Ok(());
    }
    Err(AppError::Validation(
        "daemon host must be loopback".to_owned(),
    ))
}

fn host_authority(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn json_rpc_error(id: Option<Value>, error: coverage_mcp::AppError) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id.unwrap_or(Value::Null),
        "error":{"code":-32000,"message":error.to_string()}
    })
}

fn standalone_db_path(config: &ServerConfig) -> AppResult<PathBuf> {
    config.db_path.clone().ok_or_else(|| {
        coverage_mcp::AppError::Validation(
            "standalone compaction requires a repository database path".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_db_path_enforces_repository_mode() {
        let mut config = ServerConfig::from_environment(
            None,
            None,
            Some(PathBuf::from("coverage.duckdb")),
            None,
        )
        .expect("config");
        assert_eq!(
            standalone_db_path(&config).unwrap(),
            PathBuf::from("coverage.duckdb")
        );
        config.db_path = None;
        assert!(standalone_db_path(&config).is_err());
    }

    #[test]
    fn connector_http_response_parser_is_strict() {
        let response =
            parse_http_response(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec())
                .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"{}");
        assert!(parse_http_response(b"not-http".to_vec()).is_err());
        assert!(
            parse_http_response(b"HTTP/1.1 invalid OK\r\nContent-Length: 2\r\n\r\n{}".to_vec())
                .is_err()
        );
        assert!(
            parse_http_response(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}".to_vec())
                .is_err()
        );
    }

    #[test]
    fn connector_restricts_loopback_and_compares_resolved_paths() {
        assert!(validate_loopback_host("127.0.0.1").is_ok());
        assert!(validate_loopback_host("localhost").is_ok());
        assert!(validate_loopback_host("::1").is_ok());
        assert!(validate_loopback_host("0.0.0.0").is_err());
        assert_eq!(host_authority("127.0.0.1", 59471), "127.0.0.1:59471");
        assert_eq!(host_authority("::1", 59471), "[::1]:59471");

        let directory = tempfile::tempdir().unwrap();
        assert!(same_path(directory.path(), directory.path()));
        assert!(!same_path(directory.path(), Path::new("missing")));
    }
}
