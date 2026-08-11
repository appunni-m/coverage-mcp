//! Hyper-based REST, dashboard, and stateless MCP transport.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, HeaderMap, HeaderValue,
};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::{Duration as TokioDuration, timeout};

use crate::config::ServerConfig;
use crate::error::{AppError, AppResult};
use crate::git::inspect_git;
use crate::lock::{FileLease, daemon_lock_path};
use crate::mcp;
use crate::service::{CoverageService, DEFAULT_MAX_WORDS, RequestContext};
use crate::storage::{COLLECTION_FETCH_LIMIT, CoverageStore, ProjectSettingsPatch};
use crate::{SCHEMA_REVISION, VERSION};

/// Header selecting a repository in daemon-wide mode.
pub const REPOSITORY_HEADER: &str = "x-coverage-mcp-repo";

type HttpResponse = Response<Full<Bytes>>;

/// HTTP server state. Stores are opened lazily per selected repository.
#[derive(Clone)]
pub struct CoverageServer {
    config: ServerConfig,
    stores: Arc<Mutex<HashMap<String, CoverageStore>>>,
    mcp_limiter: Arc<Semaphore>,
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
        Ok(Self {
            mcp_limiter: Arc::new(Semaphore::new(config.mcp_http_concurrency)),
            config,
            stores: Arc::new(Mutex::new(HashMap::new())),
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
        let _daemon_lease = FileLease::acquire(
            daemon_lock_path(&self.config.common_db_path),
            &format!("Coverage MCP daemon on port {}", self.config.port),
        )?;
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        self.serve_until_shutdown(listener).await
    }

    /// Serves an already-bound listener; useful for embedders and integration tests.
    #[rustfmt::skip]
    pub async fn serve_listener(self, listener: TcpListener) -> AppResult<()> { self.serve_listener_until(listener, None).await }

    async fn serve_listener_until(
        self,
        listener: TcpListener,
        max_connections: Option<usize>,
    ) -> AppResult<()> {
        let mut accepted = 0usize;
        loop {
            if max_connections.is_some_and(|limit| accepted >= limit) {
                return Ok(());
            }
            let (stream, _) = listener.accept().await?;
            accepted += 1;
            self.spawn_connection(stream);
        }
    }

    async fn serve_until_shutdown(self, listener: TcpListener) -> AppResult<()> {
        self.serve_until_shutdown_with_acceptor(|| listener.accept(), shutdown_signal())
            .await
    }

    #[cfg(test)]
    async fn serve_until_shutdown_with<F>(self, listener: TcpListener, shutdown: F) -> AppResult<()>
    where
        F: Future<Output = ()>,
    {
        self.serve_until_shutdown_with_acceptor(|| listener.accept(), shutdown)
            .await
    }

    async fn serve_until_shutdown_with_acceptor<F, A, Fut>(
        self,
        mut acceptor: A,
        shutdown: F,
    ) -> AppResult<()>
    where
        F: Future<Output = ()>,
        A: FnMut() -> Fut,
        Fut: Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>,
    {
        let mut shutdown = Box::pin(shutdown);
        let result = loop {
            tokio::select! {
                _ = &mut shutdown => break Ok(()),
                result = acceptor() => {
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
    pub fn health(&self) -> Value {
        let repository_count = self
            .stores
            .lock()
            .map(|stores| stores.len())
            .unwrap_or_default();
        json!({
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
            "common_db_path":self.config.common_db_path,
            "repository_count":repository_count,
            "daemon_path":std::env::current_exe().ok().map(|path| path.to_string_lossy().into_owned())
        })
    }

    async fn handle(self, request: Request<Incoming>) -> Result<HttpResponse, Infallible> {
        let _mcp_permit = if request.uri().path() == "/mcp" || request.uri().path() == "/mcp/" {
            self.mcp_limiter.clone().acquire_owned().await.ok()
        } else {
            None
        };
        let result = self.dispatch(request).await;
        Ok(match result {
            Ok(response) => response,
            Err(error) => error_response(error),
        })
    }

    async fn dispatch(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        if !trusted_host(&request) {
            return Err(AppError::Validation("untrusted host".to_owned()));
        }
        let path = request.uri().path().to_owned();
        if path == "/health" && request.method() == Method::GET {
            return Ok(json_response(StatusCode::OK, self.health()));
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

    async fn dispatch_mcp(&self, request: Request<Incoming>) -> AppResult<HttpResponse> {
        if request.method() != Method::POST {
            return Ok(empty_response(StatusCode::METHOD_NOT_ALLOWED));
        }
        let repository = repository_header(request.headers());
        let body = json_body(request).await?;
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let service = matches!(method, "resources/read" | "tools/call")
            .then(|| {
                self.service_for_repository_path(
                    &repository
                        .clone()
                        .unwrap_or(self.default_repository_path()?),
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
        let repository_header = repository_header(request.headers());
        let query = query_params(&uri);
        let path = uri.path().trim_matches('/').split('/').collect::<Vec<_>>();
        let body = if matches!(method, Method::POST | Method::PATCH | Method::PUT) {
            json_body(request).await?
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
            && self.config.db_path.is_none()
        {
            return Ok(json_response(StatusCode::OK, self.unscoped_projects()?));
        }
        let creating_project =
            matches!(method, Method::POST) && path.as_slice() == ["api", "projects"];
        let repository = if creating_project {
            body.get("repo_path")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            repository_header
                .or_else(|| query_value(&query, "repo_path").map(str::to_owned))
                .or_else(|| {
                    self.config
                        .db_path
                        .as_ref()
                        .map(|path| database_repository(path))
                })
        };
        let service = self.service_for_repository_path(&repository.ok_or_else(|| {
            AppError::Validation(format!(
                "{REPOSITORY_HEADER} header is required in common database mode"
            ))
        })?)?;
        let store = service.store().clone();
        let max_words = query
            .get("max_words")
            .and_then(|values| values.first())
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_MAX_WORDS);
        let detailed = query
            .get("detailed")
            .and_then(|values| values.first())
            .is_some_and(|value| value == "true" || value == "1");
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
                if let Some(repo_path) = body.get("repo_path").and_then(Value::as_str) { service.validate_repository_path(Some(repo_path))?; }
                service.ingest(body.get("report_path").and_then(Value::as_str).ok_or_else(|| AppError::Validation("report_path is required".to_owned()))?, body.get("format").and_then(Value::as_str).unwrap_or("auto"), body.get("suite").and_then(Value::as_str).unwrap_or("default"), body.get("branch").and_then(Value::as_str), body.get("commit_sha").and_then(Value::as_str), body.get("base_ref").and_then(Value::as_str), detailed)?
            }
            (Method::GET, ["api", "projects"]) => self.project_list(service.clone(), query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words)?,
            (Method::POST, ["api", "projects"]) => {
                let repo_path = body
                    .get("repo_path")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.service_for_repository_path(repo_path)?.update_project_settings(project_patch(&body))?
            }
            (Method::PATCH, ["api", "projects", _]) => service.update_project_settings(project_patch(&body))?,
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
            (Method::GET, ["api", "trend"]) => service.envelope(Value::Array(store.trend(query_value(&query, "repo_path"), query_value(&query, "branch"), query_value(&query, "suite"), query_value(&query, "file_path"), query_value(&query, "worktree_id"), query.get("limit").and_then(|values| values.first()).and_then(|value| value.parse().ok()).unwrap_or(100))?), None, None),
            (Method::GET, ["api", "compare"]) => service.envelope(store.compare(required_query(&query, "snapshot_id")?, required_query(&query, "baseline_snapshot_id")?, COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::POST, ["api", "compare"]) => service.envelope(store.compare(required_body(&body, "snapshot_id")?, required_body(&body, "baseline_snapshot_id")?, COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "changed-lines"]) => service.envelope(json!({"lines":store.changed_lines(required_query(&query, "snapshot_id")?, required_query(&query, "baseline_snapshot_id")?, query_value(&query, "file_path"), query_bool(&query, "only_regressions"), COLLECTION_FETCH_LIMIT)?}), None, None),
            (Method::GET, ["api", "line-history"]) => service.envelope(Value::Array(store.line_history(required_query(&query, "file_path")?, required_query(&query, "line_number")?.parse().map_err(|_| AppError::Validation("line_number must be an integer".to_owned()))?, query_value(&query, "branch"), query_value(&query, "suite"), COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::GET, ["api", "source-lines"]) => service.source(required_query(&query, "snapshot_id")?, required_query(&query, "file_path")?, required_query(&query, "start")?.parse().map_err(|_| AppError::Validation("start must be an integer".to_owned()))?, required_query(&query, "end")?.parse().map_err(|_| AppError::Validation("end must be an integer".to_owned()))?, query.get("cursor").and_then(|values| values.first()).map(String::as_str), max_words)?,
            (Method::GET, ["api", "worktrees"]) => service.envelope(Value::Array(store.list_worktrees(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::POST, ["api", "worktrees", "register"]) => service.worktree_registration(required_body(&body, "path")?, required_body(&body, "base_ref")?, body.get("name").and_then(Value::as_str))?,
            (Method::GET, ["api", "worktrees", worktree_id, "progress"]) => service.envelope(store.worktree_progress(worktree_id, query_value(&query, "suite").ok_or_else(|| AppError::Validation("suite is required".to_owned()))?, query_value(&query, "file_path"), COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "worktrees", worktree_id, "compare"]) => service.envelope(store.compare_worktree(worktree_id, query_value(&query, "snapshot_id"), COLLECTION_FETCH_LIMIT, COLLECTION_FETCH_LIMIT)?, None, None),
            (Method::GET, ["api", "commands"]) => service.envelope(Value::Array(store.list_registered_commands(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::POST, ["api", "commands", "register"]) => service.command_registration(required_body(&body, "name")?, required_body(&body, "command")?, body.get("human_approved").and_then(Value::as_bool).unwrap_or(false), body.get("approved_by").and_then(Value::as_str).unwrap_or_default(), body.get("approval_note").and_then(Value::as_str).unwrap_or_default(), body.get("cwd").and_then(Value::as_str), body.get("shell").and_then(Value::as_str).unwrap_or("/bin/bash"), body.get("artifact_paths").cloned(), detailed)?,
            (Method::GET, ["api", "commands", reference]) => service.envelope(store.registered_command(reference)?, None, None),
            (Method::POST, ["api", "runs", "profiled"]) => service.run_submission(required_body(&body, "command_ref")?, body.get("timeout_seconds").and_then(Value::as_u64), body.get("idempotency_key").and_then(Value::as_str), body.get("wait").and_then(Value::as_bool).unwrap_or(false), detailed)?,
            (Method::GET, ["api", "runs", "queue"]) => service.envelope(Value::Array(store.list_run_queue(COLLECTION_FETCH_LIMIT)?), None, None),
            (Method::GET, ["api", "runs", "latest"]) => service.envelope(store.latest_run(query_value(&query, "command_ref"))?.ok_or_else(|| AppError::NotFound("no runs found".to_owned()))?, None, None),
            (Method::GET, ["api", "runs", run_id]) => service.run_state(run_id, "status", detailed)?,
            (Method::POST, ["api", "runs", run_id, "cancel"]) => service.run_state(run_id, "cancel", detailed)?,
            (Method::GET, ["api", "runs", run_id, "logs", "search"]) => service.search_logs(run_id, query_values(&query, "query"), query.get("stream").and_then(|values| values.first()).map(String::as_str).unwrap_or("both"), query.get("context_lines").and_then(|values| values.first()).and_then(|value| value.parse().ok()).unwrap_or(3), query.get("max_matches").and_then(|values| values.first()).and_then(|value| value.parse().ok()).unwrap_or(5), max_words, query_bool(&query, "case_sensitive"))?,
            (Method::GET, ["api", "artifacts", "latest"]) => service.envelope(store.latest_artifact(required_query(&query, "kind")?, query_value(&query, "command_ref"))?.ok_or_else(|| AppError::NotFound("artifact not found".to_owned()))?, None, None),
            (Method::GET, ["api", "topology", kind, reference]) => service.envelope(topology(&store, kind, reference)?, None, None),
            _ => return Err(AppError::NotFound("route not found".to_owned())),
        };
        Ok(json_response(StatusCode::OK, response))
    }

    fn default_repository_path(&self) -> AppResult<String> {
        if let Some(path) = &self.config.db_path {
            return Ok(database_repository(path));
        }
        std::env::current_dir()
            .map(|path| path.to_string_lossy().into_owned())
            .map_err(AppError::from)
    }

    fn service_for_repository_path(&self, repo_path: &str) -> AppResult<CoverageService> {
        let git = inspect_git(Path::new(repo_path))?;
        let key = git.repo_key.clone();
        self.register_repository(&key)?;
        if let Some(store) = self.stores()?.get(&key).cloned() {
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
        let db_path = match self.config.db_path.clone() {
            Some(path) => path,
            None => project_database_path(&self.config.common_db_path, &key),
        };
        let store = CoverageStore::open(db_path, self.config.clone())?;
        store.ensure_project(Path::new(&git.repo_path))?;
        self.stores()?.insert(key.clone(), store.clone());
        Ok(CoverageService::new(
            store,
            RequestContext {
                repo_key: key,
                checkout_path: git.repo_path,
                suite: None,
            },
        ))
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
            if let Ok(value) = store.project_summary() {
                values.push(value);
            }
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
                match self.service_for_repository_path(&repo_key) {
                    Ok(service) => values.push(service.store().project_summary()?),
                    Err(_) => values.push(registry_project(&repo_key)),
                }
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
        if self.config.db_path.is_some() {
            return Ok(());
        }
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
        if self.config.db_path.is_some() || !self.config.common_db_path.exists() {
            return Ok(Vec::new());
        }
        let connection = Connection::open(&self.config.common_db_path)?;
        let mut statement = match connection
            .prepare("SELECT repo_key FROM repositories ORDER BY last_seen DESC LIMIT ?")
        {
            Ok(statement) => statement,
            Err(error) if error.to_string().contains("does not exist") => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let rows = statement.query_map(duckdb::params![limit], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
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

async fn json_body(request: Request<Incoming>) -> AppResult<Value> {
    let bytes = request.into_body().collect().await?.to_bytes();
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn repository_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(REPOSITORY_HEADER)
        .and_then(header_to_str)
        .map(str::to_owned)
}

fn header_to_str(value: &HeaderValue) -> Option<&str> {
    value.to_str().ok()
}

#[cfg(test)]
fn error_string(error: AppError) -> String {
    error.to_string()
}

fn trusted_host<B>(request: &Request<B>) -> bool {
    request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(|host| {
            host.starts_with("127.0.0.1")
                || host.starts_with("localhost")
                || host.starts_with("[::1]")
                || host.starts_with("::1")
                || host.starts_with("testserver")
        })
        .unwrap_or(true)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        type ShutdownFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .ok()
            .map(|mut signal| {
                Box::pin(async move {
                    let _ = signal.recv().await;
                }) as ShutdownFuture
            });
        let ctrl_c = Box::pin(async {
            let _ = tokio::signal::ctrl_c().await;
        }) as ShutdownFuture;
        wait_for_shutdown(terminate, ctrl_c).await;
    }
    #[cfg(not(unix))]
    {
        let ctrl_c = Box::pin(async {
            let _ = tokio::signal::ctrl_c().await;
        });
        ctrl_c.await;
    }
}

async fn wait_for_shutdown(
    terminate: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ctrl_c: Pin<Box<dyn Future<Output = ()> + Send>>,
) {
    if let Some(terminate) = terminate {
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    } else {
        ctrl_c.await;
    }
}

fn database_repository(path: &Path) -> String {
    let parent = path.parent().unwrap_or(Path::new("."));
    if parent
        .file_name()
        .is_some_and(|name| name == ".coverage-mcp")
    {
        parent
            .parent()
            .unwrap_or(parent)
            .to_string_lossy()
            .into_owned()
    } else {
        parent.to_string_lossy().into_owned()
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

fn project_database_path(common_db_path: &Path, repo_key: &str) -> PathBuf {
    common_db_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("projects")
        .join(format!("{}.duckdb", key_hash(repo_key)))
}

fn key_hash(value: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(value.as_bytes())
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn query_params(uri: &Uri) -> HashMap<String, Vec<String>> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
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
fn required_query<'a>(query: &'a HashMap<String, Vec<String>>, key: &str) -> AppResult<&'a str> {
    query_value(query, key).ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn required_body<'a>(body: &'a Value, key: &str) -> AppResult<&'a str> {
    body.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn query_bool(query: &HashMap<String, Vec<String>>, key: &str) -> bool {
    query_value(query, key).is_some_and(|value| value == "true" || value == "1")
}
fn project_patch(body: &Value) -> ProjectSettingsPatch {
    let source = body.get("compaction").unwrap_or(body);
    ProjectSettingsPatch {
        compaction_enabled: source.get("compaction_enabled").and_then(Value::as_bool),
        compaction_after_days: source
            .get("compaction_after_days")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        compaction_interval_seconds: source
            .get("compaction_interval_seconds")
            .and_then(Value::as_u64),
        compaction_batch_size: source
            .get("compaction_batch_size")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
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
            db_path: None,
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
            default_compaction_after_days: 30,
            default_compaction_interval_seconds: 3_600,
            default_compaction_batch_size: 100,
        }
    }

    #[test]
    fn private_http_helpers_cover_routing_and_response_policy() {
        let request = Request::builder()
            .header(HOST, "127.0.0.1:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(&request));
        let mut selected_headers = HeaderMap::new();
        selected_headers.insert(REPOSITORY_HEADER, HeaderValue::from_static("repo"));
        assert_eq!(
            repository_header(&selected_headers).as_deref(),
            Some("repo")
        );
        let mut invalid_header = HeaderMap::new();
        invalid_header.insert(
            REPOSITORY_HEADER,
            HeaderValue::from_bytes(&[0xff]).expect("invalid header bytes are accepted"),
        );
        assert!(repository_header(&invalid_header).is_none());
        let request = Request::builder()
            .header(HOST, "localhost:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(&request));
        let request = Request::builder()
            .header(HOST, "[::1]:59471")
            .body(())
            .unwrap();
        assert!(trusted_host(&request));
        let request = Request::builder()
            .header(HOST, "testserver")
            .body(())
            .unwrap();
        assert!(trusted_host(&request));
        let request = Request::builder()
            .header(HOST, "evil.example")
            .body(())
            .unwrap();
        assert!(!trusted_host(&request));
        let request = Request::new(());
        assert!(trusted_host(&request));

        let uri: Uri = "/api/items?query=one&query=two&flag=1".parse().unwrap();
        let query = query_params(&uri);
        assert_eq!(query_values(&query, "query"), vec!["one", "two"]);
        assert_eq!(query_value(&query, "flag"), Some("1"));
        assert!(query_value(&query, "missing").is_none());
        assert!(query_bool(&query, "flag"));
        assert!(!query_bool(&query, "missing"));
        assert_eq!(required_query(&query, "query").unwrap(), "one");
        assert!(required_query(&query, "missing").is_err());
        assert_eq!(
            required_body(&json!({"name":"value"}), "name").unwrap(),
            "value"
        );
        assert!(required_body(&json!({}), "name").is_err());

        let patch = project_patch(
            &json!({"compaction":{"compaction_enabled":true,"compaction_after_days":7,"compaction_interval_seconds":60,"compaction_batch_size":5}}),
        );
        assert_eq!(patch.compaction_enabled, Some(true));
        assert_eq!(patch.compaction_after_days, Some(7));
        assert_eq!(patch.compaction_interval_seconds, Some(60));
        assert_eq!(patch.compaction_batch_size, Some(5));
        assert_eq!(
            project_patch(&json!({"compaction_after_days":9})).compaction_after_days,
            Some(9)
        );

        let normal_db = database_repository(Path::new("/tmp/data.duckdb"));
        assert!(normal_db.ends_with("/tmp"));
        let project_db =
            database_repository(Path::new("/tmp/project/.coverage-mcp/coverage.duckdb"));
        assert!(project_db.ends_with("/tmp/project"));
        assert_eq!(key_hash("repo").len(), 24);
        assert!(database_repository(Path::new("")).ends_with("."));
        assert!(
            project_database_path(Path::new(""), "repo")
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".duckdb"))
        );
        assert!(error_string(AppError::Validation("test".to_owned())).contains("test"));
        assert!(matches!(
            listener_error(std::io::Error::other("listener")),
            AppError::Io(_)
        ));
        let server = CoverageServer::new(config()).unwrap();
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

    #[test]
    fn server_health_and_unscoped_projects_are_safe_before_selection() {
        let server = CoverageServer::new(config()).unwrap();
        assert_eq!(server.health()["status"], "ok");
        assert_eq!(server.health()["schema_revision"], SCHEMA_REVISION);
        assert!(server.unscoped_projects().unwrap()["data"].is_array());
        assert!(
            server
                .default_repository_path()
                .unwrap()
                .contains("coverage-mcp")
        );
        let directory = tempfile::tempdir().unwrap();
        let mut standalone_config = config();
        standalone_config.db_path = Some(directory.path().join("coverage.duckdb"));
        let standalone = CoverageServer::new(standalone_config).unwrap();
        assert_eq!(
            standalone.default_repository_path().unwrap(),
            directory.path().to_string_lossy()
        );
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
        assert!(standalone.project_list(service.clone(), None, 600).is_ok());
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
        assert!(server.project_list(selected, None, 600).is_ok());
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

        let missing_table = directory.path().join("missing-table.duckdb");
        drop(Connection::open(&missing_table).unwrap());
        let mut missing_config = config();
        missing_config.common_db_path = missing_table;
        assert_eq!(
            CoverageServer::new(missing_config)
                .unwrap()
                .unscoped_projects()
                .unwrap()["data"],
            json!([])
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
        assert!(
            CoverageServer::new(wrong_config)
                .unwrap()
                .unscoped_projects()
                .is_err()
        );

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

        let mut standalone_config = config();
        standalone_config.db_path = Some(directory.path().join("standalone.duckdb"));
        let standalone_server = CoverageServer::new(standalone_config).unwrap();
        let store =
            CoverageStore::open(directory.path().join("selected.duckdb"), config()).unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
        standalone_server
            .stores
            .lock()
            .unwrap()
            .insert(project.repo_key, store.clone());
        assert!(standalone_server.unscoped_projects().is_ok());
        store.close().unwrap();
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
        assert!(matches!(
            CoverageServer::new(startup_config.clone())
                .unwrap()
                .run()
                .await,
            Err(AppError::Busy { .. })
        ));
        drop(daemon_holder);
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
            shutdown_server
                .clone()
                .serve_until_shutdown_with(listener, async {})
                .await
                .unwrap();
            assert!(shutdown_server.stores.lock().unwrap().is_empty());
        }
        store.close().unwrap();

        let immediate_ctrl_c: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async {});
        wait_for_shutdown(None, immediate_ctrl_c).await;
        let immediate_terminate: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async {});
        let pending_ctrl_c: Pin<Box<dyn Future<Output = ()> + Send>> =
            Box::pin(std::future::pending());
        wait_for_shutdown(Some(immediate_terminate), pending_ctrl_c).await;

        #[cfg(unix)]
        {
            let signal_task = tokio::spawn(shutdown_signal());
            tokio::time::sleep(TokioDuration::from_millis(50)).await;
            std::process::Command::new("kill")
                .args(["-INT", &std::process::id().to_string()])
                .status()
                .unwrap();
            tokio::time::timeout(TokioDuration::from_secs(2), signal_task)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn shutdown_accept_errors_are_reported_and_stores_are_closed() {
        let server = CoverageServer::new(config()).unwrap();
        let result = server
            .serve_until_shutdown_with_acceptor(
                || async {
                    Err::<(tokio::net::TcpStream, std::net::SocketAddr), _>(std::io::Error::other(
                        "accept failure",
                    ))
                },
                std::future::pending(),
            )
            .await;
        assert!(matches!(result, Err(AppError::Io(_))));
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
