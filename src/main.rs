//! Coverage MCP command-line entry point.

#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use coverage_mcp::error::{AppError, AppResult};
use coverage_mcp::git::inspect_git;
use coverage_mcp::http::{DAEMON_HANDOFF_PATH, REPOSITORY_HEADER};
use coverage_mcp::lock::{DaemonLeaseOwner, daemon_lock_path, held_daemon_owner};
use coverage_mcp::{CoverageServer, SCHEMA_REVISION, ServerConfig, VERSION};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DAEMON_HANDOFF_TIMEOUT: Duration = Duration::from_secs(10);
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
        common_db: None,
    }) {
        Command::Serve {
            host,
            port,
            common_db,
        } => {
            let config = ServerConfig::from_environment(host, port, common_db)?;
            CoverageServer::new(config)?.run().await?;
        }
        Command::Connect { repo } => run_shared_stdio(repo).await?,
        Command::Compact {
            repo,
            older_than_days,
        } => {
            run_compact(repo, older_than_days).await?;
        }
    }
    Ok(())
}

async fn run_shared_stdio(repo: PathBuf) -> AppResult<()> {
    let config = ServerConfig::from_environment(None, None, None)?;
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
                match forward_mcp_request_with_recovery(&config, &git.repo_path, &request).await {
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

async fn run_compact(repo: PathBuf, older_than_days: Option<u32>) -> AppResult<()> {
    let config = ServerConfig::from_environment(None, None, None)?;
    validate_loopback_host(&config.host)?;
    let git = inspect_git(&repo)?;
    ensure_daemon(&config).await?;

    if let Some(days) = older_than_days {
        let body = json!({"compaction_after_days":days}).to_string();
        let response = daemon_request_with_recovery(
            &config,
            "PATCH",
            "/api/projects/project",
            Some(&git.repo_path),
            body.as_bytes(),
        )
        .await?;
        successful_daemon_json(response, "update compaction settings")?;
    }

    let response = daemon_request_with_recovery(
        &config,
        "POST",
        "/api/projects/project/compact",
        Some(&git.repo_path),
        b"{}",
    )
    .await?;
    let envelope = successful_daemon_json(response, "compact project")?;
    let result = envelope.get("data").unwrap_or(&envelope);
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

async fn forward_mcp_request_with_recovery(
    config: &ServerConfig,
    repo_path: &str,
    request: &Value,
) -> AppResult<Option<Value>> {
    let body = request.to_string();
    let response =
        daemon_request_with_recovery(config, "POST", "/mcp/", Some(repo_path), body.as_bytes())
            .await?;
    match response.status {
        200 => Ok(Some(serde_json::from_slice(&response.body)?)),
        202 => Ok(None),
        status => Err(AppError::Runtime(format!(
            "Coverage MCP daemon returned HTTP {status}: {}",
            String::from_utf8_lossy(&response.body)
        ))),
    }
}

fn is_transport_interruption(error: &AppError) -> bool {
    matches!(error, AppError::Io(_) | AppError::Timeout { .. })
}

fn is_connection_refused(error: &AppError) -> bool {
    matches!(error, AppError::Io(source) if source.kind() == std::io::ErrorKind::ConnectionRefused)
}

#[derive(Debug, PartialEq, Eq)]
enum DaemonHealth {
    Compatible,
    Unavailable,
    Incompatible(DaemonObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DaemonObservation {
    healthy: bool,
    version: String,
    schema: u64,
    common_db: String,
    daemon_path: Option<PathBuf>,
    pid: Option<u32>,
    instance_id: Option<String>,
    handoff_supported: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReleaseVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl ReleaseVersion {
    fn parse(value: &str) -> Option<Self> {
        let mut components = value.split('.');
        let major = components.next()?.parse().ok()?;
        let minor = components.next()?.parse().ok()?;
        let patch = components.next()?.parse().ok()?;
        if components.next().is_some() {
            return None;
        }
        Some(Self {
            major,
            minor,
            patch,
        })
    }
}

async fn ensure_daemon(config: &ServerConfig) -> AppResult<()> {
    match daemon_health(config).await? {
        DaemonHealth::Compatible => return Ok(()),
        DaemonHealth::Incompatible(observation) => {
            if recover_incompatible_daemon(config, &observation).await? {
                return Ok(());
            }
        }
        DaemonHealth::Unavailable => {}
    }

    spawn_daemon(config)?;
    let started = Instant::now();
    while started.elapsed() < DAEMON_START_TIMEOUT {
        match daemon_health(config).await? {
            DaemonHealth::Compatible => return Ok(()),
            DaemonHealth::Incompatible(observation) => {
                if recover_incompatible_daemon(config, &observation).await? {
                    return Ok(());
                }
                // The verified older owner has exited. A previous spawn may
                // have lost its lease race, so start the current release now.
                spawn_daemon(config)?;
            }
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
        .unwrap_or("unknown")
        .to_owned();
    let schema = value
        .get("schema_revision")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let common_db = value
        .get("common_db_path")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let daemon_path = value
        .get("daemon_path")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    let pid = value
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok());
    let instance_id = value
        .get("instance_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let handoff_supported = value
        .get("handoff_supported")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if healthy
        && version == VERSION
        && schema == u64::from(SCHEMA_REVISION)
        && same_path(Path::new(&common_db), &config.common_db_path)
    {
        return Ok(DaemonHealth::Compatible);
    }
    Ok(DaemonHealth::Incompatible(DaemonObservation {
        healthy,
        version,
        schema,
        common_db,
        daemon_path,
        pid,
        instance_id,
        handoff_supported,
    }))
}

async fn recover_incompatible_daemon(
    config: &ServerConfig,
    observation: &DaemonObservation,
) -> AppResult<bool> {
    validate_recoverable_daemon(config, observation)?;
    let lock_path = daemon_lock_path(&config.common_db_path);
    let Some(owner) = held_daemon_owner(&lock_path)? else {
        return state_after_missing_owner(config, observation).await;
    };
    validate_daemon_owner(config, observation, &owner)?;

    if observation.handoff_supported {
        let token = owner.handoff_token.as_deref().ok_or_else(|| {
            recovery_refused(
                config,
                observation,
                "the held daemon lease has no handoff capability",
            )
        })?;
        let body = serde_json::to_vec(&json!({"token":token}))?;
        match daemon_request(config, "POST", DAEMON_HANDOFF_PATH, None, &body).await {
            Ok(response) if response.status == 202 => {}
            Ok(response) => {
                return Err(recovery_refused(
                    config,
                    observation,
                    &format!(
                        "the daemon rejected graceful handoff with HTTP {}",
                        response.status
                    ),
                ));
            }
            Err(AppError::Io(_) | AppError::Timeout { .. }) => {
                // Another racing connector may already have stopped the exact
                // owner. The bounded state/lease loop below decides safely.
            }
            Err(error) => return Err(error),
        }
    } else {
        if owner.instance_id.is_some() || owner.handoff_token.is_some() {
            return Err(recovery_refused(
                config,
                observation,
                "health and lease metadata disagree about handoff support",
            ));
        }
        match daemon_health(config).await? {
            DaemonHealth::Incompatible(current) if current == *observation => {
                terminate_pre_handoff_daemon(owner.pid)?;
            }
            DaemonHealth::Compatible => return Ok(true),
            DaemonHealth::Unavailable => return Ok(false),
            DaemonHealth::Incompatible(_) => {
                return Err(recovery_refused(
                    config,
                    observation,
                    "the daemon identity changed before pre-handoff recovery",
                ));
            }
        }
    }

    wait_for_daemon_handoff(config, observation, &owner).await
}

fn validate_recoverable_daemon(
    config: &ServerConfig,
    observation: &DaemonObservation,
) -> AppResult<()> {
    if !observation.healthy {
        return Err(recovery_refused(
            config,
            observation,
            "the listener did not report a healthy Coverage MCP daemon",
        ));
    }
    if !same_path(Path::new(&observation.common_db), &config.common_db_path) {
        return Err(recovery_refused(
            config,
            observation,
            "the listener belongs to a different common database",
        ));
    }
    let running = ReleaseVersion::parse(&observation.version).ok_or_else(|| {
        recovery_refused(
            config,
            observation,
            "the daemon did not report a stable x.y.z release",
        )
    })?;
    let connector = ReleaseVersion::parse(VERSION).ok_or_else(|| {
        AppError::Runtime(format!(
            "Coverage MCP connector version {VERSION} is not a stable x.y.z release"
        ))
    })?;
    if running >= connector {
        let reason = if running > connector {
            "the running daemon is newer; an older connector must not downgrade it"
        } else {
            "the equal-version daemon has an incompatible schema or configuration"
        };
        return Err(recovery_refused(config, observation, reason));
    }
    Ok(())
}

fn validate_daemon_owner(
    config: &ServerConfig,
    observation: &DaemonObservation,
    owner: &DaemonLeaseOwner,
) -> AppResult<()> {
    let expected_resource = format!("Coverage MCP daemon on port {}", config.port);
    if owner.resource != expected_resource {
        return Err(recovery_refused(
            config,
            observation,
            "the held lease describes a different daemon resource",
        ));
    }
    let daemon_path = observation.daemon_path.as_deref().ok_or_else(|| {
        recovery_refused(
            config,
            observation,
            "health did not expose the daemon executable",
        )
    })?;
    if !same_path(daemon_path, &owner.executable) {
        return Err(recovery_refused(
            config,
            observation,
            "health and lease metadata identify different executables",
        ));
    }
    if observation.pid.is_some_and(|pid| pid != owner.pid) {
        return Err(recovery_refused(
            config,
            observation,
            "health and lease metadata identify different processes",
        ));
    }
    match (&observation.instance_id, &owner.instance_id) {
        (Some(health), Some(lease)) if health == lease => {}
        (None, None) => {}
        _ => {
            return Err(recovery_refused(
                config,
                observation,
                "health and lease metadata identify different daemon instances",
            ));
        }
    }
    Ok(())
}

async fn state_after_missing_owner(
    config: &ServerConfig,
    observation: &DaemonObservation,
) -> AppResult<bool> {
    match daemon_health(config).await? {
        DaemonHealth::Compatible => Ok(true),
        DaemonHealth::Unavailable => Ok(false),
        DaemonHealth::Incompatible(current) if current != *observation => Err(recovery_refused(
            config,
            observation,
            "the daemon identity changed while its ownership lease was inspected",
        )),
        DaemonHealth::Incompatible(_) => Err(recovery_refused(
            config,
            observation,
            "the reported daemon does not hold the expected ownership lease",
        )),
    }
}

async fn wait_for_daemon_handoff(
    config: &ServerConfig,
    observation: &DaemonObservation,
    owner: &DaemonLeaseOwner,
) -> AppResult<bool> {
    let started = Instant::now();
    while started.elapsed() < DAEMON_HANDOFF_TIMEOUT {
        match daemon_health(config).await? {
            DaemonHealth::Compatible => return Ok(true),
            DaemonHealth::Unavailable => {
                if held_daemon_owner(&daemon_lock_path(&config.common_db_path))?.is_none() {
                    return Ok(false);
                }
            }
            DaemonHealth::Incompatible(current) if current == *observation => {
                if let Some(current_owner) =
                    held_daemon_owner(&daemon_lock_path(&config.common_db_path))?
                {
                    if current_owner != *owner {
                        return Err(recovery_refused(
                            config,
                            observation,
                            "the ownership lease changed during handoff",
                        ));
                    }
                }
            }
            DaemonHealth::Incompatible(_) => {
                return Err(recovery_refused(
                    config,
                    observation,
                    "a different incompatible daemon appeared during handoff",
                ));
            }
        }
        sleep(DAEMON_START_POLL_INTERVAL).await;
    }
    Err(recovery_refused(
        config,
        observation,
        "the owned daemon did not release its listener and lease within 10 seconds",
    ))
}

fn daemon_incompatibility(config: &ServerConfig, observation: &DaemonObservation) -> String {
    format!(
        "Coverage MCP daemon at http://{} reports version {}, schema {}, common database {}; connector requires version {VERSION}, schema {SCHEMA_REVISION}, common database {}",
        host_authority(&config.host, config.port),
        observation.version,
        observation.schema,
        observation.common_db,
        config.common_db_path.display()
    )
}

fn recovery_refused(
    config: &ServerConfig,
    observation: &DaemonObservation,
    reason: &str,
) -> AppError {
    AppError::Runtime(format!(
        "{}; automatic recovery refused because {reason}",
        daemon_incompatibility(config, observation)
    ))
}

#[cfg(unix)]
fn terminate_pre_handoff_daemon(pid: u32) -> AppResult<()> {
    let status = ProcessCommand::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(AppError::Runtime(format!(
        "could not request graceful shutdown from pre-handoff Coverage MCP daemon pid {pid}"
    )))
}

#[cfg(windows)]
fn terminate_pre_handoff_daemon(pid: u32) -> AppResult<()> {
    let status = ProcessCommand::new("taskkill")
        .args(["/PID", &pid.to_string()])
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(AppError::Runtime(format!(
        "could not request shutdown from pre-handoff Coverage MCP daemon pid {pid}"
    )))
}

#[cfg(not(any(unix, windows)))]
fn terminate_pre_handoff_daemon(pid: u32) -> AppResult<()> {
    Err(AppError::Runtime(format!(
        "automatic recovery from pre-handoff Coverage MCP daemon pid {pid} is unsupported on this platform"
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

async fn daemon_request_with_recovery(
    config: &ServerConfig,
    method: &str,
    path: &str,
    repo_path: Option<&str>,
    body: &[u8],
) -> AppResult<WireResponse> {
    match daemon_request(config, method, path, repo_path, body).await {
        Ok(response) => Ok(response),
        Err(error) if is_transport_interruption(&error) => {
            // Re-establish the verified single owner after any transport
            // interruption. Replay only connection refusal, which proves the
            // request never reached the previous process.
            let replay = is_connection_refused(&error);
            ensure_daemon(config).await?;
            if replay {
                daemon_request(config, method, path, repo_path, body).await
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn successful_daemon_json(response: WireResponse, operation: &str) -> AppResult<Value> {
    if response.status != 200 {
        return Err(AppError::Runtime(format!(
            "Coverage MCP daemon could not {operation}; HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        )));
    }
    Ok(serde_json::from_slice(&response.body)?)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn recovery_config(common_db: PathBuf) -> ServerConfig {
        ServerConfig::from_environment(Some("127.0.0.1".to_owned()), Some(59_471), Some(common_db))
            .expect("config")
    }

    fn older_observation(config: &ServerConfig) -> DaemonObservation {
        DaemonObservation {
            healthy: true,
            version: "0.8.5".to_owned(),
            schema: u64::from(SCHEMA_REVISION),
            common_db: config.common_db_path.to_string_lossy().into_owned(),
            daemon_path: Some(std::env::current_exe().expect("executable")),
            pid: Some(std::process::id()),
            instance_id: Some("instance-1".to_owned()),
            handoff_supported: true,
        }
    }

    fn matching_owner() -> DaemonLeaseOwner {
        DaemonLeaseOwner {
            pid: std::process::id(),
            resource: "Coverage MCP daemon on port 59471".to_owned(),
            executable: std::env::current_exe().expect("executable"),
            instance_id: Some("instance-1".to_owned()),
            handoff_token: Some("secret".to_owned()),
        }
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

    #[test]
    fn daemon_recovery_only_replaces_verified_older_releases() {
        let directory = tempfile::tempdir().unwrap();
        let config = recovery_config(directory.path().join("common.duckdb"));
        let observation = older_observation(&config);
        assert!(validate_recoverable_daemon(&config, &observation).is_ok());
        assert!(validate_daemon_owner(&config, &observation, &matching_owner()).is_ok());

        assert_eq!(
            ReleaseVersion::parse("1.2.3"),
            Some(ReleaseVersion {
                major: 1,
                minor: 2,
                patch: 3
            })
        );
        assert!(ReleaseVersion::parse("1.2").is_none());
        assert!(ReleaseVersion::parse("1.2.3.4").is_none());
        assert!(ReleaseVersion::parse("1.2.beta").is_none());

        let mut invalid = observation.clone();
        invalid.version = VERSION.to_owned();
        assert!(validate_recoverable_daemon(&config, &invalid).is_err());
        invalid.version = "99.0.0".to_owned();
        assert!(validate_recoverable_daemon(&config, &invalid).is_err());
        invalid.version = "not-a-version".to_owned();
        assert!(validate_recoverable_daemon(&config, &invalid).is_err());
        invalid = observation.clone();
        invalid.healthy = false;
        assert!(validate_recoverable_daemon(&config, &invalid).is_err());
        invalid = observation.clone();
        invalid.common_db = directory
            .path()
            .join("different.duckdb")
            .to_string_lossy()
            .into_owned();
        assert!(validate_recoverable_daemon(&config, &invalid).is_err());

        let mut owner = matching_owner();
        owner.resource = "another daemon".to_owned();
        assert!(validate_daemon_owner(&config, &observation, &owner).is_err());
        owner = matching_owner();
        owner.executable = directory.path().join("another-binary");
        assert!(validate_daemon_owner(&config, &observation, &owner).is_err());
        owner = matching_owner();
        owner.pid = owner.pid.saturating_add(1);
        assert!(validate_daemon_owner(&config, &observation, &owner).is_err());
        owner = matching_owner();
        owner.instance_id = Some("another-instance".to_owned());
        assert!(validate_daemon_owner(&config, &observation, &owner).is_err());

        let mut pre_handoff_observation = observation;
        pre_handoff_observation.pid = None;
        pre_handoff_observation.instance_id = None;
        pre_handoff_observation.handoff_supported = false;
        let mut pre_handoff_owner = matching_owner();
        pre_handoff_owner.instance_id = None;
        pre_handoff_owner.handoff_token = None;
        assert!(
            validate_daemon_owner(&config, &pre_handoff_observation, &pre_handoff_owner).is_ok()
        );
        assert!(
            daemon_incompatibility(&config, &pre_handoff_observation)
                .contains("connector requires version")
        );
    }

    #[cfg(unix)]
    #[test]
    fn pre_handoff_daemon_termination_requests_sigterm() {
        let mut child = ProcessCommand::new("sleep")
            .arg("30")
            .spawn()
            .expect("sleep process");
        terminate_pre_handoff_daemon(child.id()).expect("terminate request");
        let status = child.wait().expect("terminated process");
        assert!(!status.success());
    }
}
