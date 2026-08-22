//! Hyper-based REST, dashboard, and stateless MCP transport.

// Responses assembled from validated registry and schema rows have invariant
// shapes. Keep those assertions local to this transport module; fallible I/O
// and request validation still use typed errors.
#![allow(clippy::expect_used, clippy::unwrap_in_result)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use duckdb::Connection;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{
    CACHE_CONTROL, CONTENT_LENGTH, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, HeaderMap,
    HeaderValue,
};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri, http::uri::Authority};
use hyper_util::rt::TokioIo;
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Duration as TokioDuration, timeout};
use uuid::Uuid;

use crate::config::{
    MAX_HTTP_MAX_BODY_BYTES, MIN_HTTP_MAX_BODY_BYTES, ServerConfig, default_db_path,
};
use crate::error::{AppError, AppResult};
use crate::git::inspect_git;
use crate::lock::{FileLease, daemon_lock_path};
use crate::mcp;
use crate::service::{CoverageService, DEFAULT_MAX_WORDS, RequestContext};
use crate::storage::{COLLECTION_FETCH_LIMIT, CoverageStore, ProjectSettingsPatch};
use crate::{SCHEMA_REVISION, VERSION, stable_project_id};

/// Header selecting a repository in daemon-wide mode.
pub const REPOSITORY_HEADER: &str = "x-coverage-mcp-repo";
/// Private loopback route used by a newer connector for graceful daemon handoff.
pub const DAEMON_HANDOFF_PATH: &str = "/_coverage-mcp/handoff";

type HttpResponse = Response<Full<Bytes>>;

#[cfg(test)]
static FORCE_BOUNDED_ACCEPT_FAILURE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static FORCE_SHUTDOWN_ACCEPT_FAILURE: AtomicBool = AtomicBool::new(false);

/// HTTP server state. Stores are opened lazily per selected repository.
#[derive(Clone)]
pub struct CoverageServer {
    config: ServerConfig,
    stores: Arc<Mutex<HashMap<String, CoverageStore>>>,
    store_open_gate: Arc<Mutex<()>>,
    mcp_limiter: Arc<Semaphore>,
    instance_id: Arc<str>,
    handoff_token: Arc<str>,
    shutdown: Arc<Notify>,
}

impl std::fmt::Debug for CoverageServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoverageServer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CoverageServer {
    /// Creates a server without opening a repository until the first request.
    pub fn new(config: ServerConfig) -> AppResult<Self> {
        if !(MIN_HTTP_MAX_BODY_BYTES..=MAX_HTTP_MAX_BODY_BYTES)
            .contains(&config.http_max_body_bytes)
        {
            return Err(AppError::Validation(format!(
                "http_max_body_bytes must be between {MIN_HTTP_MAX_BODY_BYTES} and {MAX_HTTP_MAX_BODY_BYTES}"
            )));
        }
        Ok(Self {
            mcp_limiter: Arc::new(Semaphore::new(config.mcp_http_concurrency)),
            config,
            stores: Arc::new(Mutex::new(HashMap::new())),
            store_open_gate: Arc::new(Mutex::new(())),
            instance_id: Uuid::new_v4().to_string().into(),
            handoff_token: Uuid::new_v4().to_string().into(),
            shutdown: Arc::new(Notify::new()),
        })
    }

    /// Runs the loopback HTTP server until the process receives a shutdown signal.
    pub async fn run(self) -> AppResult<()> {
        if self.config.host != "127.0.0.1"
            && self.config.host != "localhost"
            && self.config.host != "::1"
        {
            return Err(AppError::Validation(
                "daemon host must be loopback".to_owned(),
            ));
        }
        let _daemon_lease = FileLease::acquire_daemon(
            daemon_lock_path(&self.config.common_db_path),
            &format!("Coverage MCP daemon on port {}", self.config.port),
            self.instance_id.as_ref(),
            self.handoff_token.as_ref(),
        )?;
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        self.serve_until_shutdown(listener).await
    }

    /// Serves an already-bound listener; useful for embedders and integration tests.
    #[rustfmt::skip]
    pub async fn serve_listener(self, listener: TcpListener) -> AppResult<()> { self.serve_until_shutdown(listener).await }

    #[cfg(test)]
    async fn serve_listener_until(
        self,
        listener: TcpListener,
        max_connections: Option<usize>,
    ) -> AppResult<()> {
        self.serve_bounded_until(|| listener.accept(), max_connections)
            .await
    }

    #[cfg(test)]
    async fn serve_bounded_until<A, Fut>(
        self,
        mut acceptor: A,
        max_connections: Option<usize>,
    ) -> AppResult<()>
    where
        A: FnMut() -> Fut,
        Fut: Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>,
    {
        let mut accepted = 0usize;
        loop {
            if max_connections.is_some_and(|limit| accepted >= limit) {
                return Ok(());
            }
            #[cfg(test)]
            let accept_result = if FORCE_BOUNDED_ACCEPT_FAILURE.swap(false, Ordering::SeqCst) {
                Err(std::io::Error::other("injected bounded accept failure"))
            } else {
                acceptor().await
            };
            #[cfg(not(test))]
            let accept_result = acceptor().await;
            let (stream, _) = accept_result?;
            accepted += 1;
            self.spawn_connection(stream);
        }
    }

    async fn serve_until_shutdown(self, listener: TcpListener) -> AppResult<()> {
        self.serve_until_shutdown_with(listener, shutdown_signal())
            .await
    }

    async fn serve_until_shutdown_with<F>(self, listener: TcpListener, shutdown: F) -> AppResult<()>
    where
        F: Future<Output = AppResult<()>>,
    {
        let internal_shutdown = self.shutdown.clone();
        self.serve_until_shutdown_with_acceptor(|| listener.accept(), async move {
            tokio::select! {
                result = shutdown => result,
                () = internal_shutdown.notified() => Ok(()),
            }
        })
        .await
    }

    async fn serve_until_shutdown_with_acceptor<F, A, Fut>(
        self,
        mut acceptor: A,
        shutdown: F,
    ) -> AppResult<()>
    where
        F: Future<Output = AppResult<()>>,
        A: FnMut() -> Fut,
        Fut: Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>,
    {
        let mut shutdown = Box::pin(shutdown);
        let result = loop {
            #[cfg(test)]
            let accept_result = async {
                if FORCE_SHUTDOWN_ACCEPT_FAILURE.swap(false, Ordering::SeqCst) {
                    Err(std::io::Error::other("injected shutdown accept failure"))
                } else {
                    acceptor().await
                }
            };
            #[cfg(not(test))]
            let accept_result = acceptor();
            tokio::select! {
                shutdown_result = &mut shutdown => break shutdown_result,
                result = accept_result => {
                    if let Err(error) = self.process_listener_result(result) { break Err(error); }
                }
            }
        };
        let close_result = self.close_stores();
        combine_shutdown_results(result, close_result)
    }

    fn spawn_connection(&self, stream: tokio::net::TcpStream) {
        let server = self.clone();
        let request_timeout = TokioDuration::from_secs(server.config.http_request_timeout_seconds);
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |request| {
                let server = server.clone();
                async move {
                    match timeout(request_timeout, server.handle(request)).await {
                        Ok(response) => response,
                        Err(_) => Ok(error_response(AppError::Timeout {
                            operation: "HTTP request".to_owned(),
                            timeout_ms: request_timeout.as_millis() as u64,
                        })),
                    }
                }
            });
            if let Err(error) = hyper::server::conn::http1::Builder::new()
                .keep_alive(false)
                .serve_connection(io, service)
                .await
            {
                eprintln!("coverage-mcp HTTP connection error: {error}");
            }
        });
    }

    /// Returns the shared health payload.
    pub fn health(&self) -> AppResult<Value> {
        let repository_count = self
            .stores
            .lock()
            .map_err(|_| AppError::Runtime("store lock poisoned".to_owned()))?
            .len();
        let daemon_path = std::env::current_exe()
            .expect("a running daemon has an executable path")
            .to_string_lossy()
            .into_owned();
        Ok(json!({
            "status":"ok",
            "version":VERSION,
            "schema_revision":SCHEMA_REVISION,
            "run_retention":self.config.run_retention,
            "run_concurrency":self.config.run_concurrency,
            "mcp_http_concurrency":self.config.mcp_http_concurrency,
            "db_pool_size":self.config.db_pool_size,
            "db_acquire_timeout_ms":self.config.db_acquire_timeout_ms,
            "db_query_timeout_ms":self.config.db_query_timeout_ms,
            "http_request_timeout_seconds":self.config.http_request_timeout_seconds,
            "http_max_body_bytes":self.config.http_max_body_bytes,
            "run_log_max_bytes":self.config.run_log_max_bytes,
            "common_db_path":self.config.common_db_path,
            "repository_count":repository_count,
            "daemon_path":daemon_path,
            "pid":std::process::id(),
            "instance_id":self.instance_id.as_ref(),
            "handoff_supported":true
        }))
    }

    async fn handle(self, request: Request<Incoming>) -> Result<HttpResponse, Infallible> {
        let _mcp_permit = if request.uri().path() == "/mcp" || request.uri().path() == "/mcp/" {
            Some(
                self.mcp_limiter
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| AppError::Runtime("MCP request limiter is closed".to_owned())),
            )
        } else {
            None
        };
        let result = match _mcp_permit {
            Some(Ok(_permit)) => self.dispatch(request).await,
            Some(Err(error)) => Err(error),
            None => self.dispatch(request).await,
        };
        Ok(match result {
            Ok(response) => response,
            Err(error) => error_response(error),
        })
    }

    async fn dispatch(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        if !trusted_host(request.headers())? {
            return Err(AppError::Validation("untrusted host".to_owned()));
        }
        let path = request.uri().path().to_owned();
        if path == "/health" && request.method() == Method::GET {
            return Ok(json_response(StatusCode::OK, self.health()?));
        }
        if path == DAEMON_HANDOFF_PATH {
            return self.dispatch_daemon_handoff(request).await;
        }
        if path == "/favicon.ico" {
            return Ok(empty_response(StatusCode::NO_CONTENT));
        }
        if path == "/" && request.method() == Method::GET {
            return Ok(dashboard_response());
        }
        if path == "/mcp" || path == "/mcp/" {
            return self.dispatch_mcp(request).await;
        }
        self.dispatch_rest(request).await
    }

    async fn dispatch_daemon_handoff(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        if request.method() != Method::POST {
            return Ok(empty_response(StatusCode::METHOD_NOT_ALLOWED));
        }
        let body = json_body(request, self.config.http_max_body_bytes).await?;
        if body.get("token").and_then(Value::as_str) != Some(self.handoff_token.as_ref()) {
            return Ok(empty_response(StatusCode::FORBIDDEN));
        }
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // Let Hyper flush the accepted response before the binary's main
            // future returns and starts shutting down the Tokio runtime.
            tokio::time::sleep(TokioDuration::from_millis(25)).await;
            shutdown.notify_one();
        });
        Ok(json_response(
            StatusCode::ACCEPTED,
            json!({
                "status":"shutting_down",
                "instance_id":self.instance_id.as_ref()
            }),
        ))
    }

    async fn dispatch_mcp(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        if request.method() != Method::POST {
            return Ok(empty_response(StatusCode::METHOD_NOT_ALLOWED));
        }
        let repository = repository_header(request.headers())?;
        let body = json_body(request, self.config.http_max_body_bytes).await?;
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Validation("method is required".to_owned()))?;
        let service = matches!(method, "resources/read" | "tools/call")
            .then(|| {
                self.service_for_repository_path(
                    &repository.clone().unwrap_or(self.default_repository_path()),
                )
            })
            .transpose()?;
        match mcp::dispatch_json_rpc(service.as_ref(), &body) {
            Some(response) => Ok(json_response(StatusCode::OK, response)),
            None => Ok(empty_response(StatusCode::ACCEPTED)),
        }
    }

    async fn dispatch_rest(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        let method = request.method().clone();
        let uri = request.uri().clone();
        let repository_header = repository_header(request.headers())?;
        let query = query_params(&uri);
        let path = uri.path().trim_matches('/').split('/').collect::<Vec<_>>();
        let body = if matches!(method, Method::POST | Method::PATCH | Method::PUT) {
            json_body(request, self.config.http_max_body_bytes).await?
        } else {
            Value::Null
        };
        if path.first().copied() != Some("api") {
            return Err(AppError::NotFound("route not found".to_owned()));
        }
        if matches!(method, Method::GET)
            && path.as_slice() == ["api", "projects"]
            && repository_header.is_none()
            && query_value(&query, "repo_path").is_none()
            && self.config.default_repository_path.is_none()
        {
            return Ok(json_response(StatusCode::OK, self.unscoped_projects()?));
        }
        let creating_project =
            matches!(method, Method::POST) && path.as_slice() == ["api", "projects"];
        let project_reference = match path.as_slice() {
            ["api", "projects", project_id] | ["api", "projects", project_id, "compact"] => {
                Some(*project_id)
            }
            _ => None,
        };
        let repository = if creating_project {
            optional_body_string(&body, "repo_path")?.map(str::to_owned)
        } else if let Some(project_id) = project_reference {
            if project_id == "project" {
                repository_header
                    .or_else(|| query_value(&query, "repo_path").map(str::to_owned))
                    .or_else(|| self.config.default_repository_path_string())
            } else {
                Some(self.repository_for_project_id(project_id)?)
            }
        } else {
            repository_header
                .or_else(|| query_value(&query, "repo_path").map(str::to_owned))
                .or_else(|| self.config.default_repository_path_string())
        };
        let service = self.service_for_repository_path(&repository.ok_or_else(|| {
            AppError::Validation(format!(
                "{REPOSITORY_HEADER} header is required in common database mode"
            ))
        })?)?;
        let store = service.store().clone();
        let max_words = query_usize(&query, "max_words", DEFAULT_MAX_WORDS)?;
        let detailed = query_bool(&query, "detailed")?;
        if method == Method::GET
            && path.len() >= 5
            && path[0] == "api"
            && path[1] == "snapshots"
            && path[3] == "files"
        {
            let file_path = path[4..].join("/");
            let response = service.file_detail(
                path[2],
                &file_path,
                query
                    .get("cursor")
                    .and_then(|values| values.first())
                    .map(String::as_str),
                max_words,
                detailed,
            )?;
            return Ok(json_response(StatusCode::OK, response));
        }
        let response = match (method, path.as_slice()) {
            (Method::POST, ["api", "ingest"]) => {
                if let Some(repo_path) = optional_body_string(&body, "repo_path")? { service.validate_repository_path(Some(repo_path))?; }
                service.ingest(required_body(&body, "report_path")?, optional_body_string(&body, "format")?.unwrap_or("auto"), optional_body_string(&body, "suite")?.unwrap_or("default"), optional_body_string(&body, "branch")?, optional_body_string(&body, "commit_sha")?, optional_body_string(&body, "base_ref")?, detailed)?
            }
            (Method::GET, ["api", "projects"]) => self.project_list(service.clone(), query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words)?,
            (Method::POST, ["api", "projects"]) => {
                service.update_project_settings(project_patch(&body)?)?
            }
            (Method::PATCH, ["api", "projects", _]) => service.update_project_settings(project_patch(&body)?)?,
            (Method::POST, ["api", "projects", _, "compact"]) => service.compact_now()?,
            (Method::GET, ["api", "projects", _]) => service.envelope(store.project_summary()?, None, None),
            (Method::GET, ["api", "snapshots"]) => {
                let values = store.list_snapshots(query_value(&query, "repo_path"), query_value(&query, "branch"), query_value(&query, "suite"), COLLECTION_FETCH_LIMIT)?;
                let (values, page) = service.page(&values, query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words, "rest-snapshots", None)?;
                service.envelope(Value::Array(values), None, Some(page))
            }
            (Method::GET, ["api", "snapshots", "latest"]) => {
                let value = store.latest_snapshot(query_value(&query, "repo_path"), query_value(&query, "branch"), query_value(&query, "suite"))?.ok_or_else(|| AppError::NotFound("no snapshots found".to_owned()))?;
                service.envelope(value, None, None)
            }
            (Method::GET, ["api", "snapshots", snapshot_id]) => service.envelope(store.snapshot(snapshot_id)?, None, None),
            (Method::GET, ["api", "snapshots", snapshot_id, "files"]) => {
                if let Some(file_path) = query_value(&query, "file_path") {
                    service.file_detail(snapshot_id, file_path, query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words, detailed)?
                } else {
                    let values = store.files(snapshot_id, COLLECTION_FETCH_LIMIT)?;
                    let (values, page) = service.page(&values, query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words, &format!("rest-files:{snapshot_id}"), None)?;
                    service.envelope(Value::Array(values), None, Some(page))
                }
            }
            (Method::GET, ["api", "snapshots", snapshot_id, "insights"]) => service.envelope(store.insights(snapshot_id, query_value(&query, "baseline_snapshot_id"), COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "trend"]) => service.envelope(Value::Array(store.trend(query_value(&query, "repo_path"), query_value(&query, "branch"), query_value(&query, "suite"), query_value(&query, "file_path"), query_value(&query, "worktree_id"), query_usize(&query, "limit", 100)? )?), None, None),
            (Method::GET, ["api", "compare"]) => service.envelope(store.compare(required_query(&query, "snapshot_id")?, required_query(&query, "baseline_snapshot_id")?, COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::POST, ["api", "compare"]) => service.envelope(store.compare(required_body(&body, "snapshot_id")?, required_body(&body, "baseline_snapshot_id")?, COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "changed-lines"]) => service.envelope(json!({"lines":store.changed_lines(required_query(&query, "snapshot_id")?, required_query(&query, "baseline_snapshot_id")?, query_value(&query, "file_path"), query_bool(&query, "only_regressions")?, COLLECTION_FETCH_LIMIT)?}), None, None),
            (Method::GET, ["api", "line-history"]) => service.envelope(Value::Array(store.line_history(required_query(&query, "file_path")?, required_query(&query, "line_number")?.parse().map_err(|_| AppError::Validation("line_number must be an integer".to_owned()))?, query_value(&query, "branch"), query_value(&query, "suite"), COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::GET, ["api", "source-lines"]) => service.source(required_query(&query, "snapshot_id")?, required_query(&query, "file_path")?, required_query(&query, "start")?.parse().map_err(|_| AppError::Validation("start must be an integer".to_owned()))?, required_query(&query, "end")?.parse().map_err(|_| AppError::Validation("end must be an integer".to_owned()))?, query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words)?,
            (Method::GET, ["api", "worktrees"]) => service.envelope(Value::Array(store.list_worktrees(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::POST, ["api", "worktrees", "register"]) => service.ensure_lineage_baseline(required_body(&body, "path")?, required_body(&body, "base_ref")?, optional_body_string(&body, "name")?)?,
            (Method::GET, ["api", "worktrees", worktree_id, "progress"]) => service.envelope(store.worktree_progress(worktree_id, query_value(&query, "suite").ok_or_else(|| AppError::Validation("suite is required".to_owned()))?, query_value(&query, "file_path"), COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "worktrees", worktree_id, "compare"]) => service.envelope(store.compare_worktree(worktree_id, query_value(&query, "snapshot_id"), COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "commands"]) => service.envelope(Value::Array(store.list_registered_commands(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::POST, ["api", "commands", "register"]) => service.command_registration(required_body(&body, "name")?, required_body(&body, "command")?, optional_body_bool(&body, "human_approved")?.unwrap_or(false), optional_body_string(&body, "approved_by")?.unwrap_or_default(), optional_body_string(&body, "approval_note")?.unwrap_or_default(), optional_body_string(&body, "cwd")?, optional_body_string(&body, "shell")?.unwrap_or("/bin/bash"), body.get("artifact_paths").cloned(), detailed)?,
            (Method::GET, ["api", "commands", reference]) => service.envelope(store.registered_command(reference)?, None, None),
            (Method::POST, ["api", "runs", "profiled"]) => service.run_submission(required_body(&body, "command_ref")?, optional_body_u64(&body, "timeout_seconds")?, optional_body_string(&body, "idempotency_key")?, optional_body_bool(&body, "wait")?.unwrap_or(false), detailed)?,
            (Method::GET, ["api", "runs", "queue"]) => service.envelope(Value::Array(store.list_run_queue(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::GET, ["api", "runs", "latest"]) => service.envelope(store.latest_run(query_value(&query, "command_ref"))?.ok_or_else(|| AppError::NotFound("no runs found".to_owned()))?, None, None),
            (Method::GET, ["api", "runs", run_id]) => service.run_state(run_id, "status", detailed)?,
            (Method::POST, ["api", "runs", run_id, "cancel"]) => service.run_state(run_id, "cancel", detailed)?,
            (Method::GET, ["api", "runs", run_id, "logs", "search"]) => service.search_logs(run_id, query_values(&query, "query"), query_value(&query, "stream").unwrap_or("both"), query_usize(&query, "context_lines", 3)?, query_usize(&query, "max_matches", 5)?, max_words, query_bool(&query, "case_sensitive")?)?,
            (Method::GET, ["api", "artifacts", "latest"]) => service.envelope(store.latest_artifact(required_query(&query, "kind")?, query_value(&query, "command_ref"))?.ok_or_else(|| AppError::NotFound("artifact not found".to_owned()))?, None, None),
            (Method::GET, ["api", "topology", kind, reference]) => service.envelope(topology(&store, kind, reference)?, None, None),
            _ => return Err(AppError::NotFound("route not found".to_owned())),
        };
        Ok(json_response(StatusCode::OK, response))
    }

    fn default_repository_path(&self) -> String {
        self.config
            .default_repository_path_string()
            .unwrap_or_else(|| ".".to_owned())
    }

    fn repository_for_project_id(&self, project_id: &str) -> AppResult<String> {
        let repository = self
            .registry_repositories()?
            .into_iter()
            .find(|repo_key| key_hash(repo_key) == project_id);
        repository.ok_or_else(|| AppError::NotFound(format!("project not found: {project_id}")))
    }

    fn service_for_repository_path(&self, repo_path: &str) -> AppResult<CoverageService> {
        let git = inspect_git(Path::new(repo_path))?;
        let key = git.repo_key.clone();
        self.register_repository(&key)?;
        let _open_gate = self
            .store_open_gate
            .lock()
            .map_err(|_| AppError::Runtime("store-open lock poisoned".to_owned()))?;
        let mut stores = self.stores()?;
        if let Some(store) = stores.get(&key).cloned() {
            store.ensure_project(Path::new(&git.repo_path))?;
            return Ok(CoverageService::new(
                store,
                RequestContext {
                    repo_key: key,
                    checkout_path: git.repo_path,
                    suite: None,
                },
            ));
        }
        let db_path = project_database_path(&self.config.common_db_path, &key);
        let store = CoverageStore::open(db_path, self.config.clone())?;
        self.ensure_new_store_project(&store, Path::new(&git.repo_path))?;
        stores.insert(key.clone(), store.clone());
        Ok(CoverageService::new(
            store,
            RequestContext {
                repo_key: key,
                checkout_path: git.repo_path,
                suite: None,
            },
        ))
    }

    fn ensure_new_store_project(&self, store: &CoverageStore, path: &Path) -> AppResult<()> {
        store.ensure_project(path)?;
        Ok(())
    }

    fn stores(&self) -> AppResult<std::sync::MutexGuard<'_, HashMap<String, CoverageStore>>> {
        self.stores
            .lock()
            .map_err(|_| AppError::Runtime("store lock poisoned".to_owned()))
    }

    fn close_stores(&self) -> AppResult<()> {
        let stores = {
            let mut stores = self.stores()?;
            stores.drain().map(|(_, store)| store).collect::<Vec<_>>()
        };
        let first_error = stores.into_iter().fold(None, |first_error, store| {
            first_error.or(store.close().err())
        });
        first_error.map_or(Ok(()), Err)
    }

    fn process_listener_result(
        &self,
        result: std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
    ) -> AppResult<()> {
        match result {
            Ok((stream, _)) => {
                self.spawn_connection(stream);
                Ok(())
            }
            Err(error) => Err(listener_error(error)),
        }
    }

    fn project_list(
        &self,
        selected: CoverageService,
        cursor: Option<&str>,
        max_words: usize,
    ) -> AppResult<Value> {
        let mut values = Vec::new();
        let stores = self.stores()?;
        for store in stores.values() {
            values.push(store.project_summary()?);
        }
        if values.is_empty() {
            values.push(selected.store().project_summary()?);
        }
        let (values, page) = selected.page(&values, cursor, max_words, "rest-projects", None)?;
        Ok(selected.envelope(Value::Array(values), None, Some(page)))
    }

    fn unscoped_projects(&self) -> AppResult<Value> {
        let stores = self.stores()?.values().cloned().collect::<Vec<_>>();
        let registered = self.registry_repositories()?;
        if !registered.is_empty() {
            let mut values = Vec::new();
            for repo_key in registered {
                values.push(registry_project_summary(
                    &repo_key,
                    self.service_for_repository_path(&repo_key),
                ));
            }
            let context = json!({
                "repo_key":"",
                "checkout_path":"",
                "suite":null,
                "schema_revision":SCHEMA_REVISION
            });
            return Ok(json!({
                "context":context,
                "data":values,
                "page":{"returned":values.len(),"total":values.len(),"word_count":0,"max_words":5000,"truncated":false,"next_cursor":null}
            }));
        }
        if let Some(store) = stores.into_iter().next() {
            let project = store.project()?;
            let service = CoverageService::new(
                store,
                RequestContext {
                    repo_key: project.repo_key.clone(),
                    checkout_path: project.repo_path.clone(),
                    suite: None,
                },
            );
            return self.project_list(service, None, 5_000);
        }
        Ok(json!({
            "context":{"repo_key":"","checkout_path":"","suite":null,"schema_revision":SCHEMA_REVISION},
            "data":[],
            "page":{"returned":0,"total":0,"word_count":0,"max_words":5000,"truncated":false,"next_cursor":null}
        }))
    }

    fn register_repository(&self, repo_key: &str) -> AppResult<()> {
        let connection = registry_connection(&self.config.common_db_path)?;
        connection.execute(
            "INSERT INTO repositories (id, repo_key, last_seen) VALUES (?, ?, current_timestamp) ON CONFLICT (repo_key) DO UPDATE SET last_seen = excluded.last_seen",
            duckdb::params![key_hash(repo_key), repo_key],
        )?;
        Ok(())
    }

    fn registry_repositories(&self) -> AppResult<Vec<String>> {
        self.registry_repositories_with_limit(COLLECTION_FETCH_LIMIT as i64)
    }

    fn registry_repositories_with_limit(&self, limit: i64) -> AppResult<Vec<String>> {
        if !self.config.common_db_path.exists() {
            return Ok(Vec::new());
        }
        let connection = Connection::open(&self.config.common_db_path)?;
        let mut statement = connection
            .prepare("SELECT repo_key FROM repositories ORDER BY last_seen DESC LIMIT ?")?;
        let rows = statement.query_map(duckdb::params![limit], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
    }
}

fn registry_project_summary(repo_key: &str, result: AppResult<CoverageService>) -> Value {
    match result.and_then(|service| service.store().project_summary()) {
        Ok(summary) => summary,
        Err(error) => registry_project_unavailable(repo_key, &error),
    }
}

fn listener_error(error: std::io::Error) -> AppError {
    AppError::Io(error)
}

fn combine_shutdown_results(result: AppResult<()>, close_result: AppResult<()>) -> AppResult<()> {
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn json_body<B>(request: Request<B>, max_bytes: usize) -> AppResult<Value>
where
    B: hyper::body::Body<Data = Bytes> + Send + Unpin,
    B::Error: Into<AppError>,
{
    validate_body_content_length(request.headers(), max_bytes)?;
    let mut body = request.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(Into::into)?;
        append_body_data(&mut bytes, frame.data_ref(), max_bytes)?;
    }
    parse_json_body(&bytes)
}

fn validate_body_content_length(headers: &HeaderMap, max_bytes: usize) -> AppResult<()> {
    if let Some(content_length) = headers.get(CONTENT_LENGTH) {
        let declared = parse_content_length(content_length)?;
        if declared > max_bytes {
            return Err(AppError::Validation(format!(
                "request body exceeds {max_bytes} bytes"
            )));
        }
    }
    Ok(())
}
fn append_body_chunk(bytes: &mut Vec<u8>, data: &Bytes, max_bytes: usize) -> AppResult<()> {
    let next_len = bytes.len().saturating_add(data.len());
    if next_len > max_bytes {
        return Err(AppError::Validation(format!(
            "request body exceeds {max_bytes} bytes"
        )));
    }
    bytes.extend_from_slice(data);
    Ok(())
}

fn append_body_data(bytes: &mut Vec<u8>, data: Option<&Bytes>, max_bytes: usize) -> AppResult<()> {
    if let Some(data) = data {
        append_body_chunk(bytes, data, max_bytes)?;
    }
    Ok(())
}

fn parse_content_length(value: &HeaderValue) -> AppResult<usize> {
    header_to_str(value, CONTENT_LENGTH.as_str())?
        .parse::<usize>()
        .map_err(|_| AppError::Validation("content-length must be an integer".to_owned()))
}

fn parse_json_body(bytes: &[u8]) -> AppResult<Value> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn repository_header(headers: &HeaderMap) -> AppResult<Option<String>> {
    headers
        .get(REPOSITORY_HEADER)
        .map(|value| header_to_str(value, REPOSITORY_HEADER).map(str::to_owned))
        .transpose()
}

fn header_to_str<'a>(value: &'a HeaderValue, name: &str) -> AppResult<&'a str> {
    value
        .to_str()
        .map_err(|_| AppError::Validation(format!("{name} must be valid UTF-8")))
}

#[cfg(test)]
fn error_string(error: AppError) -> String {
    error.to_string()
}

fn trusted_host(headers: &HeaderMap) -> AppResult<bool> {
    let Some(host) = headers.get(HOST) else {
        return Ok(false);
    };
    let host = header_to_str(host, HOST.as_str())?;
    let authority = parse_authority(host)?;
    Ok(matches!(
        authority.host(),
        "127.0.0.1" | "localhost" | "::1" | "[::1]" | "testserver"
    ))
}

fn parse_authority(host: &str) -> AppResult<Authority> {
    host.parse::<Authority>()
        .map_err(|_| AppError::Validation("host must be a valid authority".to_owned()))
}

#[cfg(unix)]
type ShutdownFuture = Pin<Box<dyn Future<Output = AppResult<()>> + Send>>;

#[cfg(unix)]
fn terminate_signal_future(mut signal: tokio::signal::unix::Signal) -> ShutdownFuture {
    Box::pin(async move {
        signal.recv().await;
        Ok(())
    })
}

#[cfg(unix)]
fn terminate_signal_future_with(
    register: fn() -> std::io::Result<tokio::signal::unix::Signal>,
) -> AppResult<ShutdownFuture> {
    let signal = match register() {
        Ok(signal) => signal,
        Err(error) => return Err(error.into()),
    };
    Ok(terminate_signal_future(signal))
}

#[cfg(unix)]
async fn shutdown_signal_with_registration(
    terminate: AppResult<ShutdownFuture>,
    ctrl_c: ShutdownFuture,
) -> AppResult<()> {
    wait_for_shutdown(Some(terminate?), ctrl_c).await
}

async fn shutdown_signal() -> AppResult<()> {
    #[cfg(unix)]
    {
        let terminate = terminate_signal_future_with(|| {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        });
        let ctrl_c = Box::pin(async { tokio::signal::ctrl_c().await.map_err(AppError::from) })
            as ShutdownFuture;
        shutdown_signal_with_registration(terminate, ctrl_c).await
    }
    #[cfg(not(unix))]
    {
        let ctrl_c = Box::pin(async { tokio::signal::ctrl_c().await.map_err(AppError::from) });
        ctrl_c.await
    }
}

async fn wait_for_shutdown(
    terminate: Option<Pin<Box<dyn Future<Output = AppResult<()>> + Send>>>,
    ctrl_c: Pin<Box<dyn Future<Output = AppResult<()>> + Send>>,
) -> AppResult<()> {
    if let Some(terminate) = terminate {
        tokio::select! {
            result = ctrl_c => result,
            result = terminate => result,
        }
    } else {
        ctrl_c.await
    }
}

fn registry_connection(path: &Path) -> AppResult<Connection> {
    registry_connection_with_schema(
        path,
        "CREATE TABLE IF NOT EXISTS repositories (id VARCHAR PRIMARY KEY, repo_key VARCHAR UNIQUE NOT NULL, last_seen TIMESTAMP NOT NULL)",
    )
}

fn registry_connection_with_schema(path: &Path, schema: &str) -> AppResult<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.execute_batch(schema)?;
    Ok(connection)
}

fn registry_project(repo_key: &str) -> Value {
    json!({
        "id":key_hash(repo_key),
        "repo_key":repo_key,
        "repo_path":repo_key,
        "snapshot_count":0,
        "branch_count":0,
        "command_count":0,
        "run_count":0,
        "latest_snapshot_id":null,
        "latest_snapshot_age":null,
        "latest_run_age":null,
        "latest_suite":null,
        "line_rate":null
    })
}

fn registry_project_unavailable(repo_key: &str, error: &AppError) -> Value {
    let mut value = registry_project(repo_key);
    let object = value
        .as_object_mut()
        .expect("registry_project always returns an object");
    object.insert("status".to_owned(), json!("unavailable"));
    object.insert("error".to_owned(), json!(error.to_string()));
    value
}

fn project_database_path(common_db_path: &Path, repo_key: &str) -> PathBuf {
    let repository_local = default_db_path(Path::new(repo_key));
    let centralized = common_db_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("projects")
        .join(format!("{}.duckdb", key_hash(repo_key)));
    if repository_local.exists() || !centralized.exists() {
        repository_local
    } else {
        centralized
    }
}

fn key_hash(value: &str) -> String {
    stable_project_id(value)
}
fn query_params(uri: &Uri) -> HashMap<String, Vec<String>> {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .into_owned()
        .fold(HashMap::new(), |mut output, (key, value)| {
            output.entry(key).or_default().push(value);
            output
        })
}
fn query_value<'a>(query: &'a HashMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    query
        .get(key)
        .and_then(|values| values.first())
        .map(String::as_str)
}
fn query_values(query: &HashMap<String, Vec<String>>, key: &str) -> Vec<String> {
    query.get(key).cloned().unwrap_or_default()
}
fn query_usize(
    query: &HashMap<String, Vec<String>>,
    key: &str,
    default: usize,
) -> AppResult<usize> {
    query_value(query, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| AppError::Validation(format!("{key} must be an integer")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}
fn required_query<'a>(query: &'a HashMap<String, Vec<String>>, key: &str) -> AppResult<&'a str> {
    query_value(query, key).ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn required_body<'a>(body: &'a Value, key: &str) -> AppResult<&'a str> {
    optional_body_string(body, key)?
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn optional_body_string<'a>(body: &'a Value, key: &str) -> AppResult<Option<&'a str>> {
    body.get(key)
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| AppError::Validation(format!("{key} must be a string")))
        })
        .transpose()
}
fn optional_body_bool(body: &Value, key: &str) -> AppResult<Option<bool>> {
    body.get(key)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| AppError::Validation(format!("{key} must be a boolean")))
        })
        .transpose()
}
fn optional_body_u64(body: &Value, key: &str) -> AppResult<Option<u64>> {
    body.get(key)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| AppError::Validation(format!("{key} must be an unsigned integer")))
        })
        .transpose()
}
fn query_bool(query: &HashMap<String, Vec<String>>, key: &str) -> AppResult<bool> {
    match query_value(query, key) {
        None => Ok(false),
        Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        Some(_) => Err(AppError::Validation(format!(
            "{key} must be true, false, 1, or 0"
        ))),
    }
}
fn project_patch(body: &Value) -> AppResult<ProjectSettingsPatch> {
    let source = body
        .get("compaction")
        .unwrap_or(body)
        .as_object()
        .ok_or_else(|| AppError::Validation("project settings must be an object".to_owned()))?;
    Ok(ProjectSettingsPatch {
        compaction_enabled: optional_json_bool(source, "compaction_enabled")?,
        compaction_after_days: optional_json_u32(source, "compaction_after_days")?,
        compaction_interval_seconds: optional_json_u64(source, "compaction_interval_seconds")?,
        compaction_batch_size: optional_json_u32(source, "compaction_batch_size")?,
    })
}
fn optional_json_bool(object: &Map<String, Value>, key: &str) -> AppResult<Option<bool>> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be a boolean"))),
    }
}
fn optional_json_u32(object: &Map<String, Value>, key: &str) -> AppResult<Option<u32>> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| {
                AppError::Validation(format!("{key} must be a 32-bit unsigned integer"))
            }),
    }
}
fn optional_json_u64(object: &Map<String, Value>, key: &str) -> AppResult<Option<u64>> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be an unsigned integer"))),
    }
}
fn topology(store: &CoverageStore, kind: &str, reference: &str) -> AppResult<Value> {
    match kind {
        "run" => Ok(json!({"topology":{"kind":"run","run":store.run_result(reference, 20)?}})),
        "snapshot" => {
            Ok(json!({"topology":{"kind":"snapshot","snapshot":store.snapshot(reference)?}}))
        }
        _ => Err(AppError::Validation(
            "topology kind must be run or snapshot".to_owned(),
        )),
    }
}

fn json_response(status: StatusCode, value: Value) -> HttpResponse {
    let mut response = Response::new(Full::new(Bytes::from(value.to_string())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    harden(response)
}
fn error_response(error: AppError) -> HttpResponse {
    json_response(
        StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        json!({"detail":error.to_string(),"error":error.to_string()}),
    )
}
fn empty_response(status: StatusCode) -> HttpResponse {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    harden(response)
}
fn harden(mut response: HttpResponse) -> HttpResponse {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
        .headers_mut()
        .insert("x-frame-options", HeaderValue::from_static("DENY"));
    response.headers_mut().insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'self'; frame-ancestors 'none'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"));
    response
}
fn dashboard_response() -> HttpResponse {
    let html = include_str!("dashboard.html");
    let mut response = Response::new(Full::new(Bytes::from(html)));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    harden(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn listener_or_skip(
        result: std::io::Result<TcpListener>,
    ) -> Result<Option<TcpListener>, String> {
        match result {
            Ok(listener) => Ok(Some(listener)),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    async fn exercise_bounded_listener(
        server: CoverageServer,
        result: std::io::Result<TcpListener>,
    ) -> Result<(), String> {
        let listener = match listener_or_skip(result) {
            Ok(listener) => listener,
            Err(error) => return Err(error),
        };
        match listener {
            Some(listener) => server
                .serve_listener_until(listener, Some(0))
                .await
                .map_err(error_string),
            None => Ok(()),
        }
    }

    fn config() -> ServerConfig {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 59_471,
            default_repository_path: None,
            common_db_path: std::env::temp_dir().join(format!(
                "coverage-mcp-http-test-{}.duckdb",
                std::process::id()
            )),
            run_retention: 100,
            run_concurrency: 4,
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

    #[tokio::test]
    async fn private_http_helpers_cover_routing_and_response_policy() {
        let request = Request::builder()
            .header(HOST, "127.0.0.1:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(request.headers()).unwrap());
        let mut selected_headers = HeaderMap::new();
        selected_headers.insert(REPOSITORY_HEADER, HeaderValue::from_static("repo"));
        assert_eq!(
            repository_header(&selected_headers).unwrap().as_deref(),
            Some("repo")
        );
        let mut invalid_header = HeaderMap::new();
        invalid_header.insert(
            REPOSITORY_HEADER,
            HeaderValue::from_bytes(&[0xff]).expect("invalid header bytes are accepted"),
        );
        assert!(repository_header(&invalid_header).is_err());
        let request = Request::builder()
            .header(HOST, "localhost:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, "[::1]:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, "testserver")
            .body(())
            .unwrap();
        assert!(trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, "evil.example")
            .body(())
            .unwrap();
        assert!(!trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, "localhost.evil")
            .body(())
            .unwrap();
        assert!(!trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, "127.0.0.1.evil:59471")
            .body(())
            .unwrap();
        assert!(!trusted_host(request.headers()).unwrap());
        let request = Request::new(());
        assert!(!trusted_host(request.headers()).unwrap());
        let request = Request::builder()
            .header(HOST, HeaderValue::from_bytes(&[0xff]).unwrap())
            .body(())
            .unwrap();
        assert!(trusted_host(request.headers()).is_err());
        let request = Request::builder().header(HOST, "[").body(()).unwrap();
        assert!(trusted_host(request.headers()).is_err());
        #[cfg(unix)]
        assert!(
            terminate_signal_future_with(|| {
                Err(std::io::Error::other("terminate registration failed"))
            })
            .is_err()
        );
        #[cfg(unix)]
        {
            let terminate = terminate_signal_future_with(|| {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
            })
            .unwrap();
            let terminate_task = tokio::spawn(terminate);
            tokio::time::sleep(TokioDuration::from_millis(10)).await;
            std::process::Command::new("kill")
                .args(["-USR1", &std::process::id().to_string()])
                .status()
                .unwrap();
            assert!(terminate_task.await.unwrap().is_ok());
        }

        let uri: Uri = "/api/items?query=one&query=two&flag=1".parse().unwrap();
        let mut query = query_params(&uri);
        assert_eq!(query_values(&query, "query"), vec!["one", "two"]);
        assert_eq!(query_value(&query, "flag"), Some("1"));
        assert!(query_value(&query, "missing").is_none());
        assert!(query_bool(&query, "flag").unwrap());
        assert!(!query_bool(&query, "missing").unwrap());
        query.insert("invalid".to_owned(), vec!["maybe".to_owned()]);
        assert!(query_bool(&query, "invalid").is_err());
        assert_eq!(query_usize(&query, "missing", 17).unwrap(), 17);
        query.insert("limit".to_owned(), vec!["4".to_owned()]);
        assert_eq!(query_usize(&query, "limit", 17).unwrap(), 4);
        query.insert("invalid_limit".to_owned(), vec!["many".to_owned()]);
        assert!(query_usize(&query, "invalid_limit", 17).is_err());
        assert_eq!(required_query(&query, "query").unwrap(), "one");
        assert!(required_query(&query, "missing").is_err());
        assert_eq!(
            required_body(&json!({"name":"value"}), "name").unwrap(),
            "value"
        );
        assert!(required_body(&json!({}), "name").is_err());
        assert!(optional_body_string(&json!({"name":9}), "name").is_err());
        assert!(optional_body_bool(&json!({"enabled":"yes"}), "enabled").is_err());
        assert!(optional_body_u64(&json!({"timeout":"60"}), "timeout").is_err());

        let patch = project_patch(
            &json!({"compaction":{"compaction_enabled":true,"compaction_after_days":7,"compaction_interval_seconds":60,"compaction_batch_size":5}}),
        )
        .unwrap();
        assert_eq!(patch.compaction_enabled, Some(true));
        assert_eq!(patch.compaction_after_days, Some(7));
        assert_eq!(patch.compaction_interval_seconds, Some(60));
        assert_eq!(patch.compaction_batch_size, Some(5));
        assert_eq!(
            project_patch(&json!({"compaction_after_days":9}))
                .unwrap()
                .compaction_after_days,
            Some(9)
        );
        assert!(project_patch(&json!({"compaction_after_days":"9"})).is_err());
        assert!(project_patch(&json!({"compaction": []})).is_err());
        assert!(project_patch(&json!({"compaction_enabled":"true"})).is_err());
        assert!(project_patch(&json!({"compaction_interval_seconds":"60"})).is_err());
        assert!(project_patch(&json!({"compaction_batch_size":"5"})).is_err());

        assert_eq!(key_hash("repo").len(), 24);
        let database_directory = tempfile::tempdir().unwrap();
        let repository = database_directory.path().join("repo");
        std::fs::create_dir_all(&repository).unwrap();
        let common = database_directory.path().join("common.duckdb");
        let repository_local = default_db_path(&repository);
        let centralized = common
            .parent()
            .unwrap()
            .join("projects")
            .join(format!("{}.duckdb", key_hash(repository.to_str().unwrap())));
        assert_eq!(
            project_database_path(&common, repository.to_str().unwrap()),
            repository_local
        );
        std::fs::create_dir_all(centralized.parent().unwrap()).unwrap();
        std::fs::write(&centralized, []).unwrap();
        assert_eq!(
            project_database_path(&common, repository.to_str().unwrap()),
            centralized
        );
        std::fs::create_dir_all(repository_local.parent().unwrap()).unwrap();
        std::fs::write(&repository_local, []).unwrap();
        assert_eq!(
            project_database_path(&common, repository.to_str().unwrap()),
            repository_local
        );
        assert!(error_string(AppError::Validation("test".to_owned())).contains("test"));
        let _ = listener_error(std::io::Error::other("listener"));
        let server = CoverageServer::new(config()).unwrap();
        let mut invalid_body_config = config();
        invalid_body_config.http_max_body_bytes = MIN_HTTP_MAX_BODY_BYTES - 1;
        assert!(CoverageServer::new(invalid_body_config).is_err());
        let mut body = Vec::new();
        append_body_chunk(&mut body, &Bytes::from_static(b"{}"), 2).unwrap();
        assert_eq!(body, b"{}".to_vec());
        assert!(append_body_chunk(&mut body, &Bytes::from_static(b"!"), 2).is_err());
        append_body_data(&mut body, None, 2).unwrap();
        assert_eq!(
            parse_content_length(&HeaderValue::from_static("2")).unwrap(),
            2
        );
        assert!(parse_content_length(&HeaderValue::from_static("bad")).is_err());
        assert!(parse_authority("[").is_err());
        assert_eq!(parse_json_body(&[]).unwrap(), json!({}));
        assert!(
            server
                .process_listener_result(Err(std::io::Error::other("accept")))
                .is_err()
        );
        assert!(
            combine_shutdown_results(Err(AppError::Runtime("serve".to_owned())), Ok(())).is_err()
        );
        assert!(
            combine_shutdown_results(Ok(()), Err(AppError::Runtime("close".to_owned()))).is_err()
        );
        assert!(combine_shutdown_results(Ok(()), Ok(())).is_ok());

        let response = json_response(StatusCode::CREATED, json!({"ok":true}));
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            error_response(AppError::NotFound("missing".to_owned())).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            empty_response(StatusCode::ACCEPTED).status(),
            StatusCode::ACCEPTED
        );
        let dashboard = dashboard_response();
        assert_eq!(dashboard.status(), StatusCode::OK);
        let dashboard_body = String::from_utf8(
            dashboard
                .into_body()
                .into_inner()
                .expect("dashboard body")
                .to_vec(),
        )
        .unwrap();
        assert!(dashboard_body.contains("getAllJSON('/api/projects?max_words=5000')"));
        assert!(dashboard_body.contains("coverageViewer"));
        assert!(dashboard_body.contains("compactionAfterDays"));
        assert!(format!("{:?}", CoverageServer::new(config()).unwrap()).contains("CoverageServer"));
    }

    #[tokio::test]
    async fn body_and_shutdown_registration_errors_are_typed() {
        struct TestBody {
            fail: bool,
            yielded: bool,
        }

        impl hyper::body::Body for TestBody {
            type Data = Bytes;
            type Error = std::io::Error;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
                let body = self.as_mut().get_mut();
                if body.yielded {
                    return Poll::Ready(None);
                }
                body.yielded = true;
                if body.fail {
                    Poll::Ready(Some(Err(std::io::Error::other("body frame failed"))))
                } else {
                    Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                        b"{}",
                    )))))
                }
            }

            fn is_end_stream(&self) -> bool {
                self.yielded
            }

            fn size_hint(&self) -> hyper::body::SizeHint {
                hyper::body::SizeHint::default()
            }
        }

        let invalid_length = Request::builder()
            .header(CONTENT_LENGTH, "bad")
            .body(TestBody {
                fail: true,
                yielded: false,
            })
            .unwrap();
        assert!(json_body(invalid_length, 100).await.is_err());
        let failing_body = TestBody {
            fail: true,
            yielded: false,
        };
        assert!(!hyper::body::Body::is_end_stream(&failing_body));
        let _ = hyper::body::Body::size_hint(&failing_body);
        assert!(json_body(Request::new(failing_body), 100).await.is_err());
        let valid_body = TestBody {
            fail: false,
            yielded: false,
        };
        assert_eq!(
            json_body(Request::new(valid_body), 100).await.unwrap(),
            json!({})
        );
        let oversized_body = TestBody {
            fail: false,
            yielded: false,
        };
        assert!(json_body(Request::new(oversized_body), 1).await.is_err());
        let invalid_utf8_length = Request::builder()
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_bytes(&[0xff]).expect("header bytes are accepted"),
            )
            .body(TestBody {
                fail: true,
                yielded: false,
            })
            .unwrap();
        assert!(json_body(invalid_utf8_length, 100).await.is_err());
        #[cfg(unix)]
        {
            assert!(
                shutdown_signal_with_registration(
                    Err(AppError::Runtime(
                        "terminate registration failed".to_owned()
                    )),
                    Box::pin(std::future::ready(Ok(()))),
                )
                .await
                .is_err()
            );
            assert!(
                shutdown_signal_with_registration(
                    Ok(Box::pin(std::future::ready(Ok(())))),
                    Box::pin(std::future::ready(Ok(()))),
                )
                .await
                .is_ok()
            );
        }
    }

    #[test]
    fn server_health_and_unscoped_projects_are_safe_before_selection() {
        let server = CoverageServer::new(config()).unwrap();
        let health = server.health().unwrap();
        assert_eq!(health["status"], "ok");
        assert_eq!(health["schema_revision"], SCHEMA_REVISION);
        assert_eq!(health["pid"], std::process::id());
        assert_eq!(health["instance_id"], server.instance_id.as_ref());
        assert_eq!(health["handoff_supported"], true);
        assert!(!health.to_string().contains(server.handoff_token.as_ref()));
        let fallback_directory = tempfile::tempdir().unwrap();
        let fallback_store =
            CoverageStore::open(fallback_directory.path().join("fallback.duckdb"), config())
                .unwrap();
        let fallback_project = fallback_store
            .ensure_project(fallback_directory.path())
            .unwrap();
        let fallback_service = CoverageService::new(
            fallback_store.clone(),
            RequestContext {
                repo_key: fallback_project.repo_key,
                checkout_path: fallback_project.repo_path,
                suite: None,
            },
        );
        let fallback_projects = server.project_list(fallback_service, None, 600).unwrap();
        assert_eq!(fallback_projects["data"].as_array().unwrap().len(), 1);
        fallback_store.close().unwrap();
        let poisoned_server = CoverageServer::new(config()).unwrap();
        let stores = poisoned_server.stores.clone();
        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = stores.lock().unwrap();
            panic!("intentional store-lock poison for health error coverage");
        }));
        assert!(poison_result.is_err());
        assert!(poisoned_server.health().is_err());
        assert!(poisoned_server.close_stores().is_err());
        assert!(server.unscoped_projects().unwrap()["data"].is_array());
        let isolated_directory = tempfile::tempdir().unwrap();
        let mut isolated_config = config();
        isolated_config.common_db_path = isolated_directory.path().join("isolated-common.duckdb");
        let isolated_store = CoverageStore::open(
            isolated_directory.path().join("isolated.duckdb"),
            isolated_config,
        )
        .unwrap();
        let isolated_project = isolated_store
            .ensure_project(isolated_directory.path())
            .unwrap();
        let isolated_repo_key = isolated_project.repo_key.clone();
        server
            .stores
            .lock()
            .unwrap()
            .insert(isolated_repo_key.clone(), isolated_store.clone());
        assert_eq!(
            server.unscoped_projects().unwrap()["data"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        server.stores.lock().unwrap().remove(&isolated_repo_key);
        isolated_store.close().unwrap();
        assert_eq!(server.default_repository_path(), ".");
        let directory = tempfile::tempdir().unwrap();
        let store =
            CoverageStore::open(directory.path().join("selected.duckdb"), config()).unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
        server
            .stores
            .lock()
            .unwrap()
            .insert(project.repo_key.clone(), store.clone());
        let routed = server
            .service_for_repository_path(directory.path().to_str().unwrap())
            .unwrap();
        assert_eq!(routed.context(None).repo_key, project.repo_key);
        let service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );
        assert!(server.project_list(service.clone(), None, 600).is_ok());
        let closed_directory = tempfile::tempdir().unwrap();
        let closed_store = CoverageStore::open(
            closed_directory.path().join("closed-project.duckdb"),
            config(),
        )
        .unwrap();
        let closed_service = CoverageService::new(
            closed_store.clone(),
            RequestContext {
                repo_key: "closed-project".to_owned(),
                checkout_path: closed_directory.path().display().to_string(),
                suite: None,
            },
        );
        closed_store.close().unwrap();
        let empty_server = CoverageServer::new(config()).unwrap();
        assert!(
            empty_server
                .project_list(closed_service, None, 600)
                .is_err()
        );
        let stale_directory = tempfile::tempdir().unwrap();
        let stale =
            CoverageStore::open(stale_directory.path().join("stale.duckdb"), config()).unwrap();
        let stale_project = stale.ensure_project(stale_directory.path()).unwrap();
        stale.close().unwrap();
        server
            .stores
            .lock()
            .unwrap()
            .insert(stale_project.repo_key, stale);
        let selected = server
            .service_for_repository_path(directory.path().to_str().unwrap())
            .unwrap();
        assert!(server.project_list(selected, None, 600).is_err());
        let helper_server = CoverageServer::new(config()).unwrap();
        let helper_store =
            CoverageStore::open(directory.path().join("helper.duckdb"), config()).unwrap();
        assert!(
            helper_server
                .ensure_new_store_project(&helper_store, Path::new("\0"))
                .is_err()
        );
        let repository_directory = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(repository_directory.path())
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.email", "coverage@example.com"])
            .current_dir(repository_directory.path())
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.name", "Coverage Tests"])
            .current_dir(repository_directory.path())
            .status()
            .unwrap();
        std::fs::write(repository_directory.path().join("README.md"), "coverage\n").unwrap();
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(repository_directory.path())
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "base"])
            .current_dir(repository_directory.path())
            .status()
            .unwrap();
        let repository = repository_directory.path();
        let mut broken_store_config = config();
        broken_store_config.common_db_path = directory.path().join("broken-common.duckdb");
        let repository_key = inspect_git(repository).unwrap().repo_key;
        let centralized_broken_path = broken_store_config
            .common_db_path
            .parent()
            .unwrap()
            .join("projects")
            .join(format!("{}.duckdb", key_hash(&repository_key)));
        let initial_broken_store =
            CoverageStore::open(centralized_broken_path.clone(), broken_store_config.clone())
                .unwrap();
        initial_broken_store.close().unwrap();
        let broken_db_path =
            project_database_path(&broken_store_config.common_db_path, &repository_key);
        assert_eq!(broken_db_path, centralized_broken_path);
        let broken_store =
            CoverageStore::open(broken_db_path, broken_store_config.clone()).unwrap();
        broken_store.ensure_project(repository).unwrap();
        broken_store
            .execute_sql_for_test(
                "DROP INDEX IF EXISTS idx_project_settings_updated;
                 DROP TABLE project_settings;
                 CREATE TABLE project_settings (repo_key VARCHAR, updated_at TIMESTAMP)",
            )
            .unwrap();
        broken_store.close().unwrap();
        let broken_server = CoverageServer::new(broken_store_config).unwrap();
        assert!(
            broken_server
                .service_for_repository_path(repository.to_str().unwrap())
                .is_err()
        );
        helper_store.close().unwrap();
        store.close().unwrap();
        assert!(
            topology(
                &CoverageStore::open(directory.path().join("topology.duckdb"), config()).unwrap(),
                "unknown",
                "id"
            )
            .is_err()
        );
        let poisoned_server = CoverageServer::new(config()).unwrap();
        let poisoned_stores = poisoned_server.stores.clone();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_stores.lock().unwrap();
            panic!("injected HTTP store lock poison");
        }));
        assert!(poison.is_err());
        assert!(poisoned_server.stores().is_err());
        assert!(
            poisoned_server
                .service_for_repository_path(directory.path().to_str().unwrap())
                .is_err()
        );
        assert!(poisoned_server.project_list(service, None, 600).is_err());
        assert!(poisoned_server.unscoped_projects().is_err());

        let unselected_directory = tempfile::tempdir().unwrap();
        let mut unselected_config = config();
        unselected_config.common_db_path = unselected_directory.path().join("registry.duckdb");
        let unselected_server = CoverageServer::new(unselected_config.clone()).unwrap();
        let unselected_store = CoverageStore::open(
            unselected_directory.path().join("unselected.duckdb"),
            unselected_config,
        )
        .unwrap();
        unselected_server
            .stores
            .lock()
            .unwrap()
            .insert("unselected".to_owned(), unselected_store.clone());
        assert!(unselected_server.unscoped_projects().is_err());
        unselected_store.close().unwrap();

        let poisoned_gate_server = CoverageServer::new(config()).unwrap();
        let poisoned_gate = poisoned_gate_server.store_open_gate.clone();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_gate.lock().unwrap();
            panic!("injected HTTP store-open lock poison");
        }));
        assert!(poison.is_err());
        assert!(
            poisoned_gate_server
                .service_for_repository_path(directory.path().to_str().unwrap())
                .is_err()
        );
    }

    #[test]
    fn registry_project_listing_handles_stale_and_invalid_registries() {
        let directory = tempfile::tempdir().unwrap();
        let mut registry_config = config();
        registry_config.common_db_path = directory.path().join("common.duckdb");
        let registry_server = CoverageServer::new(registry_config.clone()).unwrap();
        registry_server.register_repository("\0").unwrap();
        let projects = registry_server.unscoped_projects().unwrap();
        assert_eq!(projects["data"][0]["snapshot_count"], 0);
        assert_eq!(registry_project("repo")["repo_path"], "repo");
        assert!(
            registry_server
                .registry_repositories_with_limit(-1)
                .is_err()
        );
        assert!(
            registry_server
                .repository_for_project_id("missing")
                .is_err()
        );
        let registry_directory = directory.path().join("registry-directory");
        std::fs::create_dir_all(&registry_directory).unwrap();
        let mut directory_config = config();
        directory_config.common_db_path = registry_directory;
        assert!(
            CoverageServer::new(directory_config)
                .unwrap()
                .registry_repositories_with_limit(10)
                .is_err()
        );

        let missing_table = directory.path().join("missing-table.duckdb");
        drop(Connection::open(&missing_table).unwrap());
        let mut missing_config = config();
        missing_config.common_db_path = missing_table;
        assert!(
            CoverageServer::new(missing_config)
                .unwrap()
                .unscoped_projects()
                .is_err()
        );

        let query_error = directory.path().join("query-error.duckdb");
        let connection = Connection::open(&query_error).unwrap();
        connection
            .execute_batch(
                "CREATE VIEW repositories AS SELECT CAST(error('query failure') AS VARCHAR) AS repo_key",
            )
            .unwrap();
        drop(connection);
        let mut query_error_config = config();
        query_error_config.common_db_path = query_error;
        assert!(
            CoverageServer::new(query_error_config)
                .unwrap()
                .registry_repositories()
                .is_err()
        );

        let wrong_schema = directory.path().join("wrong-schema.duckdb");
        let connection = Connection::open(&wrong_schema).unwrap();
        connection
            .execute_batch("CREATE TABLE repositories (id VARCHAR)")
            .unwrap();
        drop(connection);
        let mut wrong_config = config();
        wrong_config.common_db_path = wrong_schema;
        let wrong_server = CoverageServer::new(wrong_config).unwrap();
        assert!(wrong_server.unscoped_projects().is_err());
        assert!(wrong_server.repository_for_project_id("missing").is_err());

        let closed_summary_store =
            CoverageStore::open(directory.path().join("closed-summary.duckdb"), config()).unwrap();
        closed_summary_store.close().unwrap();
        let closed_summary_service = CoverageService::new(
            closed_summary_store,
            RequestContext {
                repo_key: "closed-summary".to_owned(),
                checkout_path: directory.path().display().to_string(),
                suite: None,
            },
        );
        let unavailable = registry_project_summary("closed-summary", Ok(closed_summary_service));
        assert_eq!(unavailable["status"], "unavailable");

        let execute_error = directory.path().join("execute-error.duckdb");
        let connection = Connection::open(&execute_error).unwrap();
        connection
            .execute_batch("CREATE TABLE repositories (id VARCHAR)")
            .unwrap();
        drop(connection);
        let mut execute_config = config();
        execute_config.common_db_path = execute_error;
        assert!(
            CoverageServer::new(execute_config)
                .unwrap()
                .register_repository("repo")
                .is_err()
        );
        assert!(registry_connection(Path::new("")).is_ok());
        let view_path = directory.path().join("view.duckdb");
        drop(Connection::open(&view_path).unwrap());
        std::fs::set_permissions(
            &view_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o444),
        )
        .unwrap();
        assert!(registry_connection(&view_path).is_err());
        let invalid_schema_path = directory.path().join("invalid-schema.duckdb");
        assert!(registry_connection_with_schema(&invalid_schema_path, "not valid sql").is_err());
        let parent_file = directory.path().join("not-a-directory");
        std::fs::write(&parent_file, "file").unwrap();
        assert!(
            registry_connection_with_schema(&parent_file.join("child.duckdb"), "SELECT 1").is_err()
        );

        let shared_server = CoverageServer::new(config()).unwrap();
        let store =
            CoverageStore::open(directory.path().join("selected.duckdb"), config()).unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
        shared_server
            .stores
            .lock()
            .unwrap()
            .insert(project.repo_key, store.clone());
        assert!(shared_server.unscoped_projects().is_ok());
        store.close().unwrap();
    }

    #[tokio::test]
    async fn daemon_handoff_requires_the_lease_capability_and_stops_cleanly() {
        let server = CoverageServer::new(config()).unwrap();
        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(
                server
                    .clone()
                    .serve_until_shutdown_with(listener, std::future::pending::<AppResult<()>>()),
            );

            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "GET {DAEMON_HANDOFF_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .contains("405 Method Not Allowed")
            );

            let wrong_body = br#"{"token":"wrong"}"#;
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST {DAEMON_HANDOFF_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        wrong_body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(wrong_body).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .contains("403 Forbidden")
            );

            let body = json!({"token":server.handoff_token.as_ref()}).to_string();
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST {DAEMON_HANDOFF_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("202 Accepted"));
            assert!(response.contains(server.instance_id.as_ref()));
            tokio::time::timeout(TokioDuration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn bounded_listener_loop_can_finish_cleanly() {
        let server = CoverageServer::new(config()).unwrap();
        assert!(
            listener_or_skip(Err(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            )))
            .unwrap()
            .is_none()
        );
        assert!(listener_or_skip(Err(std::io::Error::other("bind failure"))).is_err());
        assert!(
            exercise_bounded_listener(
                CoverageServer::new(config()).unwrap(),
                Err(std::io::Error::other("listener failure")),
            )
            .await
            .is_err()
        );
        assert!(
            exercise_bounded_listener(
                CoverageServer::new(config()).unwrap(),
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied,)),
            )
            .await
            .is_ok()
        );
        assert!(
            exercise_bounded_listener(server, TcpListener::bind("127.0.0.1:0").await)
                .await
                .is_ok()
        );

        let server = CoverageServer::new(config()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        FORCE_BOUNDED_ACCEPT_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            server
                .clone()
                .serve_listener_until(listener, Some(1))
                .await
                .is_err()
        );

        let server = CoverageServer::new(config()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_listener = tokio::time::timeout(
            TokioDuration::from_millis(10),
            server.serve_listener(listener),
        )
        .await;
        assert!(public_listener.is_err());

        let server = CoverageServer::new(config()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let task = tokio::spawn(server.clone().serve_listener(listener));
        server.shutdown.notify_one();
        assert!(task.await.unwrap().is_ok());

        let server = CoverageServer::new(config()).unwrap();
        let accept_error = server
            .serve_bounded_until(
                || {
                    std::future::ready(Err::<
                        (tokio::net::TcpStream, std::net::SocketAddr),
                        std::io::Error,
                    >(std::io::Error::other(
                        "bounded accept failure",
                    )))
                },
                Some(1),
            )
            .await;
        assert!(accept_error.is_err());

        let server = CoverageServer::new(config()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        assert!(
            server
                .process_listener_result(Ok((stream, address)))
                .is_ok()
        );
        drop(client);
    }

    #[tokio::test]
    async fn wire_dispatch_error_branches_are_explicit() {
        async fn raw_once(server: CoverageServer, request: Vec<u8>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(&request).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            task.await.unwrap().unwrap();
            String::from_utf8(response).unwrap()
        }

        fn isolated_config(directory: &tempfile::TempDir) -> ServerConfig {
            let mut value = config();
            value.common_db_path = directory.path().join("common.duckdb");
            value
        }

        let directory = tempfile::tempdir().unwrap();
        let response = raw_once(
            CoverageServer::new(isolated_config(&directory)).unwrap(),
            b"GET /health HTTP/1.1\r\nHost: [\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(response.contains("400 Bad Request"));

        let mut artifact_config = isolated_config(&directory);
        artifact_config.default_repository_path = Some(directory.path().to_path_buf());
        let response = raw_once(
            CoverageServer::new(artifact_config).unwrap(),
            b"GET /api/artifacts/latest?kind=coverage HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(response.contains("404 Not Found"));

        let success_directory = tempfile::tempdir().unwrap();
        let mut artifact_success_config = isolated_config(&success_directory);
        artifact_success_config.default_repository_path =
            Some(success_directory.path().to_path_buf());
        let artifact_success_server = CoverageServer::new(artifact_success_config).unwrap();
        let artifact_service = artifact_success_server
            .service_for_repository_path(success_directory.path().to_str().unwrap())
            .unwrap();
        let artifact_project = artifact_service.store().project().unwrap();
        let repo_path = artifact_project.repo_path.replace('\'', "''");
        let repo_key = artifact_project.repo_key.replace('\'', "''");
        artifact_service
            .store()
            .execute_sql_for_test(&format!(
                "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at) VALUES ('artifact-run', 'artifact-command', 'artifact-command', 'true', '{repo_path}', '{repo_path}', '{repo_key}', NULL, NULL, current_timestamp, current_timestamp, 1, 0, 'passed', 'stdout.log', 'stderr.log', '{{}}', '[]', current_timestamp, 0, NULL); INSERT INTO run_artifacts (run_id, kind, path, exists, size_bytes, coverage_format, suite, modified_by_run, ingest_status, snapshot_id, ingest_error) VALUES ('artifact-run', 'coverage', 'coverage.json', true, 1, NULL, NULL, true, NULL, NULL, NULL);"
            ))
            .unwrap();
        let response = raw_once(
            artifact_success_server,
            b"GET /api/artifacts/latest?kind=coverage HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(response.contains("200 OK"));

        let server = CoverageServer::new(isolated_config(&directory)).unwrap();
        let stores = server.stores.clone();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = stores.lock().unwrap();
            panic!("intentional health store-lock poison");
        }));
        assert!(poison.is_err());
        let response = raw_once(
            server,
            b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(response.contains("500 Internal Server Error"));

        let directory = tempfile::tempdir().unwrap();
        let response = raw_once(
            CoverageServer::new(isolated_config(&directory)).unwrap(),
            format!(
                "POST {DAEMON_HANDOFF_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\n{{"
            )
            .into_bytes(),
        )
        .await;
        assert!(
            response.contains("400 Bad Request") || response.contains("500 Internal Server Error")
        );

        let directory = tempfile::tempdir().unwrap();
        let mut invalid_mcp_repository = b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 2\r\nx-coverage-mcp-repo: ".to_vec();
        invalid_mcp_repository.push(0xff);
        invalid_mcp_repository.extend_from_slice(b"\r\n\r\n{}");
        let response = raw_once(
            CoverageServer::new(isolated_config(&directory)).unwrap(),
            invalid_mcp_repository,
        )
        .await;
        assert!(response.contains("400 Bad Request"));

        let directory = tempfile::tempdir().unwrap();
        let mut invalid_rest_repository = b"GET /api/projects HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nx-coverage-mcp-repo: ".to_vec();
        invalid_rest_repository.push(0xff);
        invalid_rest_repository.extend_from_slice(b"\r\n\r\n");
        let response = raw_once(
            CoverageServer::new(isolated_config(&directory)).unwrap(),
            invalid_rest_repository,
        )
        .await;
        assert!(response.contains("400 Bad Request"));

        let directory = tempfile::tempdir().unwrap();
        let common_db = directory.path().join("registry.duckdb");
        Connection::open(&common_db)
            .unwrap()
            .execute_batch("CREATE TABLE repositories (id VARCHAR)")
            .unwrap();
        let mut invalid_registry = config();
        invalid_registry.common_db_path = common_db;
        let response = raw_once(
            CoverageServer::new(invalid_registry).unwrap(),
            b"GET /api/projects HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(response.contains("500 Internal Server Error"));

        let directory = tempfile::tempdir().unwrap();
        let response = raw_once(
            CoverageServer::new(isolated_config(&directory)).unwrap(),
            b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: bad\r\n\r\n{}".to_vec(),
        )
        .await;
        assert!(response.contains("400 Bad Request"));
    }

    #[tokio::test]
    async fn closed_mcp_limiter_returns_an_explicit_error() {
        let server = CoverageServer::new(config()).unwrap();
        server.mcp_limiter.close();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(server.serve_listener_until(listener, Some(1)));
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .contains("500 Internal Server Error")
        );
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_startup_shutdown_and_signal_wait_paths_are_testable() {
        let directory = tempfile::tempdir().unwrap();
        let mut startup_config = config();
        startup_config.common_db_path = directory.path().join("startup-common.duckdb");
        startup_config.port = 0;
        let daemon_holder = FileLease::acquire(
            daemon_lock_path(&startup_config.common_db_path),
            "test daemon holder",
        )
        .unwrap();
        let startup_error = CoverageServer::new(startup_config.clone())
            .unwrap()
            .run()
            .await
            .expect_err("held daemon lease must reject startup");
        let _ = startup_error;
        drop(daemon_holder);
        let bind_holder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut bind_config = startup_config.clone();
        bind_config.port = bind_holder.local_addr().unwrap().port();
        assert!(
            CoverageServer::new(bind_config)
                .unwrap()
                .run()
                .await
                .is_err()
        );
        drop(bind_holder);
        let startup = CoverageServer::new(startup_config).unwrap();
        let task = tokio::spawn(startup.run());
        tokio::time::sleep(TokioDuration::from_millis(50)).await;
        task.abort();
        let _ = task.await;

        #[cfg(unix)]
        for probe in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            drop(probe);
            let mut graceful_config = config();
            graceful_config.common_db_path = directory.path().join("graceful-common.duckdb");
            graceful_config.port = 0;
            let graceful = tokio::spawn(CoverageServer::new(graceful_config).unwrap().run());
            tokio::time::sleep(TokioDuration::from_millis(50)).await;
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &std::process::id().to_string()])
                .status();
            tokio::time::timeout(TokioDuration::from_secs(2), graceful)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }

        let mut shutdown_config = config();
        shutdown_config.common_db_path = directory.path().join("shutdown-common.duckdb");
        let shutdown_server = CoverageServer::new(shutdown_config.clone()).unwrap();
        let store = CoverageStore::open(
            directory.path().join("shutdown-project.duckdb"),
            shutdown_config,
        )
        .unwrap();
        shutdown_server
            .stores
            .lock()
            .unwrap()
            .insert("shutdown-project".to_owned(), store.clone());
        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            FORCE_SHUTDOWN_ACCEPT_FAILURE.store(true, Ordering::SeqCst);
            assert!(
                shutdown_server
                    .clone()
                    .serve_until_shutdown_with(listener, std::future::pending::<AppResult<()>>())
                    .await
                    .is_err()
            );
        }
        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            shutdown_server
                .clone()
                .serve_until_shutdown_with(listener, async { Ok(()) })
                .await
                .unwrap();
            assert!(shutdown_server.stores.lock().unwrap().is_empty());
        }
        store.close().unwrap();

        let immediate_ctrl_c: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(async { Ok(()) });
        wait_for_shutdown(None, immediate_ctrl_c).await.unwrap();
        let immediate_terminate: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(async { Ok(()) });
        let pending_ctrl_c: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(std::future::pending());
        wait_for_shutdown(Some(immediate_terminate), pending_ctrl_c)
            .await
            .unwrap();
        let failed_ctrl_c: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(async { Err(AppError::Runtime("ctrl-c registration failed".to_owned())) });
        assert!(wait_for_shutdown(None, failed_ctrl_c).await.is_err());
        let failed_terminate: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(async {
                Err(AppError::Runtime(
                    "terminate registration failed".to_owned(),
                ))
            });
        let pending_ctrl_c: Pin<Box<dyn Future<Output = AppResult<()>> + Send>> =
            Box::pin(std::future::pending());
        assert!(
            wait_for_shutdown(Some(failed_terminate), pending_ctrl_c)
                .await
                .is_err()
        );

        #[cfg(unix)]
        {
            let signal_task = tokio::spawn(shutdown_signal());
            tokio::time::sleep(TokioDuration::from_millis(50)).await;
            std::process::Command::new("kill")
                .args(["-INT", &std::process::id().to_string()])
                .status()
                .unwrap();
            let signal_result = tokio::time::timeout(TokioDuration::from_secs(2), signal_task)
                .await
                .unwrap()
                .unwrap();
            signal_result.unwrap();
        }
    }

    #[tokio::test]
    async fn shutdown_accept_errors_are_reported_and_stores_are_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        drop(client);
        let mut first_stream = Some(stream);
        let server = CoverageServer::new(config()).unwrap();
        let result = server
            .serve_until_shutdown_with_acceptor(
                move || {
                    let result = first_stream
                        .take()
                        .map(|stream| Ok((stream, address)))
                        .unwrap_or_else(|| {
                            Err::<(tokio::net::TcpStream, std::net::SocketAddr), std::io::Error>(
                                std::io::Error::other("accept failure"),
                            )
                        });
                    std::future::ready(result)
                },
                std::future::pending(),
            )
            .await;
        let error = result.expect_err("accept failure must be reported");
        let _ = error;
    }

    #[tokio::test]
    async fn hanging_request_body_returns_a_gateway_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let mut server_config = config();
        server_config.common_db_path = directory.path().join("timeout-common.duckdb");
        server_config.http_request_timeout_seconds = 1;
        let server = CoverageServer::new(server_config).unwrap();
        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("504 Gateway Timeout"));
            task.await.unwrap().unwrap();
        }

        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("400 Bad Request"));
            task.await.unwrap().unwrap();
        }

        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .contains("400 Bad Request")
            );
            task.await.unwrap().unwrap();
        }

        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{}",
                )
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("500 Internal Server Error"));
            assert!(response.contains("request body could not be read"));
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn oversized_request_body_is_rejected_before_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let mut server_config = config();
        server_config.common_db_path = directory.path().join("body-limit-common.duckdb");
        server_config.http_max_body_bytes = 1_024;
        let server = CoverageServer::new(server_config).unwrap();
        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 1025\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("400 Bad Request"));
            assert!(response.contains("request body exceeds 1024 bytes"));
            task.await.unwrap().unwrap();
        }

        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            let oversized = vec![b'a'; 1_025];
            let mut request = "POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n401\r\n"
                .to_owned()
                .into_bytes();
            request.extend_from_slice(&oversized);
            request.extend_from_slice(b"\r\n0\r\n\r\n");
            stream.write_all(&request).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(response.contains("400 Bad Request"));
            assert!(response.contains("request body exceeds 1024 bytes"));
            task.await.unwrap().unwrap();
        }

        for listener in std::iter::once(
            listener_or_skip(TcpListener::bind("127.0.0.1:0").await).unwrap_or(None),
        )
        .flatten()
        {
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener_until(listener, Some(1)));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    b"POST /mcp/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .contains("400 Bad Request")
            );
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn rest_error_routes_preserve_storage_failures() {
        async fn request(
            address: std::net::SocketAddr,
            method: &str,
            path: &str,
            payload: &str,
        ) -> String {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
                        payload.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            String::from_utf8(response).unwrap()
        }

        async fn broken_routes(table: &str, routes: &[(&str, &str, &str)]) {
            let directory = tempfile::tempdir().unwrap();
            let mut server_config = config();
            server_config.default_repository_path = Some(directory.path().to_path_buf());
            server_config.common_db_path = directory.path().join("common.duckdb");
            let server = CoverageServer::new(server_config).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(server.clone().serve_listener(listener));
            let initial = request(address, "GET", "/api/projects", "").await;
            assert!(initial.contains("200 OK"));
            let store = server
                .stores
                .lock()
                .unwrap()
                .values()
                .next()
                .cloned()
                .expect("initial request opens the repository store");
            if table == "lines" {
                let project = store.project().unwrap();
                let repo_key = project.repo_key.replace('\'', "''");
                store
                    .execute_sql_for_test(&format!(
                        "INSERT INTO snapshots (id, created_at, minute_bucket, repo_path, repo_key, branch, commit_sha, base_ref, suite, format, report_path, warnings, metadata, total_lines, covered_lines, total_branches, covered_branches, total_functions, covered_functions, total_regions, covered_regions, line_rate, branch_rate, function_rate, region_rate) VALUES ('fault-snapshot', current_timestamp, current_timestamp, '{repo_key}', '{repo_key}', 'main', 'fault-commit', NULL, 'unit', 'lcov', 'fault', '[]', '{{}}', 1, 0, 0, 0, 0, 0, 1, 0, 0.0, NULL, NULL, 0.0)"
                    ))
                    .unwrap();
            }
            store
                .execute_sql_for_test(&format!(
                    "DROP TABLE {table}; CREATE VIEW {table} AS SELECT 1 AS broken;"
                ))
                .unwrap();
            for (method, path, payload) in routes {
                let response = request(address, method, path, payload).await;
                assert!(
                    response.contains("400 Bad Request")
                        || response.contains("404 Not Found")
                        || response.contains("500 Internal Server Error")
                );
            }
            task.abort();
            let _ = task.await;
            server.close_stores().unwrap();
        }

        broken_routes(
            "project_settings",
            &[
                ("POST", "/api/projects", "{}"),
                ("PATCH", "/api/projects/project", "{}"),
                ("POST", "/api/projects/project/compact", "{}"),
                ("GET", "/api/projects/project", ""),
            ],
        )
        .await;
        broken_routes(
            "snapshots",
            &[
                ("GET", "/api/snapshots", ""),
                ("GET", "/api/snapshots/latest?suite=unit", ""),
                ("GET", "/api/projects/project", ""),
                ("GET", "/api/snapshots/missing", ""),
                ("GET", "/api/snapshots/missing/insights", ""),
                ("GET", "/api/trend", ""),
                (
                    "GET",
                    "/api/compare?snapshot_id=missing&baseline_snapshot_id=missing",
                    "",
                ),
                (
                    "POST",
                    "/api/compare",
                    r#"{"snapshot_id":"missing","baseline_snapshot_id":"missing"}"#,
                ),
                (
                    "GET",
                    "/api/changed-lines?snapshot_id=missing&baseline_snapshot_id=missing",
                    "",
                ),
                (
                    "GET",
                    "/api/source-lines?snapshot_id=missing&file_path=a.py&start=1&end=2",
                    "",
                ),
                ("GET", "/api/snapshots/missing/files", ""),
                ("POST", "/api/projects/project/compact", "{}"),
            ],
        )
        .await;
        broken_routes(
            "lines",
            &[("GET", "/api/line-history?file_path=a.py&line_number=1", "")],
        )
        .await;
        broken_routes(
            "worktrees",
            &[
                ("GET", "/api/worktrees", ""),
                (
                    "POST",
                    "/api/worktrees/register",
                    r#"{"path":".","base_ref":"main","name":7}"#,
                ),
                (
                    "POST",
                    "/api/worktrees/register",
                    r#"{"path":".","base_ref":"main"}"#,
                ),
            ],
        )
        .await;
        broken_routes(
            "run_jobs",
            &[
                ("GET", "/api/runs/queue", ""),
                ("GET", "/api/runs/missing", ""),
                ("POST", "/api/runs/missing/cancel", "{}"),
                ("GET", "/api/runs/missing/logs/search?query=term", ""),
            ],
        )
        .await;
        broken_routes(
            "run_artifacts",
            &[("GET", "/api/artifacts/latest?kind=coverage", "")],
        )
        .await;

        let directory = tempfile::tempdir().unwrap();
        let mut command_config = config();
        command_config.default_repository_path = Some(directory.path().to_path_buf());
        command_config.common_db_path = directory.path().join("command-common.duckdb");
        let command_server = CoverageServer::new(command_config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(command_server.clone().serve_listener(listener));
        let _ = request(address, "GET", "/api/projects", "").await;
        let command_payload = format!(
            "{{\"name\":\"http-fault-command\",\"command\":\"true\",\"cwd\":\"{}\",\"human_approved\":true,\"approved_by\":\"tester\",\"approval_note\":\"fault\"}}",
            directory.path().display()
        );
        let registered = request(address, "POST", "/api/commands/register", &command_payload).await;
        assert!(registered.contains("200 OK"));
        let command_store = command_server
            .stores
            .lock()
            .unwrap()
            .values()
            .next()
            .cloned()
            .expect("initial request opens the command store");
        command_store
            .execute_sql_for_test(
                "DROP TABLE registered_commands; CREATE VIEW registered_commands AS SELECT 1 AS broken;",
            )
            .unwrap();
        for (method, path, payload) in [
            ("GET", "/api/commands", ""),
            ("GET", "/api/commands/missing", ""),
            ("POST", "/api/commands/register", command_payload.as_str()),
            (
                "POST",
                "/api/runs/profiled",
                r#"{"command_ref":"http-fault-command"}"#,
            ),
        ] {
            let response = request(address, method, path, payload).await;
            assert!(
                response.contains("400 Bad Request")
                    || response.contains("500 Internal Server Error")
            );
        }
        task.abort();
        let _ = task.await;
        command_server.close_stores().unwrap();
    }

    #[test]
    fn close_stores_reports_success_and_debug_is_stable() {
        let directory = tempfile::tempdir().unwrap();
        let server = CoverageServer::new(config()).unwrap();
        let store = CoverageStore::open(directory.path().join("close.duckdb"), config()).unwrap();
        assert!(format!("{store:?}").contains("CoverageStore"));
        server
            .stores
            .lock()
            .unwrap()
            .insert("close-project".to_owned(), store);
        assert!(server.close_stores().is_ok());
        assert!(server.stores.lock().unwrap().is_empty());
    }
}
