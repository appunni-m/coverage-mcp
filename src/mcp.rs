//! Stateless JSON MCP contract and transport dispatcher for the Rust daemon.
//!
//! The public inventory is explicit so the wire contract is reviewable,
//! deterministic, and independent of a third-party MCP runtime. The same
//! dispatcher is used by the loopback HTTP endpoint and the native stdio
//! transport.

use serde_json::{Map, Value, json};

use crate::VERSION;
use crate::error::{AppError, AppResult};
use crate::service::{CoverageService, DEFAULT_MAX_WORDS};
use crate::storage::LineRange;

/// MCP stream endpoint instructions shown during initialization.
pub const MCP_INSTRUCTIONS: &str = r#"Coverage MCP 0.8.4 schema 7 exposes a compact, composable agent interface.

Start with project_context, then run only exact approved registrations returned there or created with register_test_command after human approval. Submit with run_test(wait=false), save the run id, then fetch status with get_run_data(detailed=false). get_run_data is read-only: it only returns durable run data and never starts, advances, reruns, or cancels work. For every non-terminal response, wait at least the returned poll_after_ms before the next get_run_data call; do not poll immediately. Use cancel_run only when the user no longer wants the run. Use search_test_logs for targeted retained stdout/stderr evidence; managed output is byte-capped and a terminal summary reports truncated=true when the cap was reached. Run setup, capture, polling, persistence, timeout, cancellation, and shutdown failures are terminalized as failed durable jobs, so never assume a non-terminal run is permanent.

Coverage queries are deliberately narrow and composable. Each coverage_query, coverage_compare, or source_context call answers one projection or one bounded source range; it is expected and supported to make multiple calls for one user task. Use coverage_query view=targets for the ranked next work, coverage_compare view=regions for grouped previous-session impact, coverage_query view=file for one file's red regions, and source_context for the exact source text of a selected region. Chain calls by carrying forward snapshot_id, file_path, and start/end ranges from earlier results. Run independent calls separately or in parallel; run source follow-ups only after their target ranges are known. Use coverage_query view=file with line_ranges or coverage_compare view=lines only when exact per-line audit data is needed.

Every successful response is {context,data,page}; max_words is the per-call response budget (50–5000, default 600) and collections continue with page.next_cursor. Omit detailed or keep it false for normal work; set it true only when a tool description names required audit or raw-provenance fields. detailed never returns logs. The daemon uses stateless JSON responses, bounded MCP concurrency, bounded HTTP/DuckDB deadlines, and bounded HTTP bodies; keep query fan-out bounded and retry an individual request with backoff after a transient 503/504 or interrupted connection. Coverage ingestion is capped at 64 MiB and malformed numeric report fields are validation errors, never silent zeroes."#;

/// Returns the MCP initialize result.
pub fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {"tools": {"listChanged": false}, "resources": {"subscribe": false, "listChanged": false}},
        "serverInfo": {"name": "coverage-mcp", "version": VERSION},
        "instructions": MCP_INSTRUCTIONS,
    })
}

/// Returns the complete public tool inventory.
pub fn tools_list() -> Value {
    Value::Array(vec![
        tool(
            "project_context",
            "Discover project state before work: stable project id, metrics/freshness, exact approved commands, latest_run, active runs, and page metadata. The returned latest_run.id is the run_id for get_run_data; get_run_data has no implicit latest-run selection. Use detailed only for approval audit fields and full project chronology.",
            read_only(),
            object_schema(
                &[
                    (
                        "cursor",
                        string("Opaque page cursor returned by a previous collection response."),
                    ),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &[],
            ),
        ),
        tool(
            "register_test_command",
            "Register one immutable command after human approval of the exact command, cwd, shell, and artifacts. The returned id/name can be passed to run_test; declare coverage artifacts with coverage_format and suite for automatic ingestion.",
            local_write(),
            object_schema(
                &[
                    ("name", string("Stable command name.")),
                    (
                        "command",
                        string("Exact shell command approved by the human."),
                    ),
                    (
                        "human_approved",
                        boolean("Must be true after human approval."),
                    ),
                    ("approved_by", string("Human approving the command.")),
                    ("approval_note", string("Reason and scope of approval.")),
                    (
                        "cwd",
                        nullable_string("Checkout directory for the command."),
                    ),
                    ("shell", string("Executable shell path.")),
                    (
                        "artifact_paths",
                        json_schema("Coverage and artifact specifications."),
                    ),
                    ("max_words", budget_schema()),
                ],
                &[
                    "name",
                    "command",
                    "human_approved",
                    "approved_by",
                    "approval_note",
                ],
            ),
        ),
        tool(
            "run_test",
            "Submit one approved command. Prefer wait=false with a stable idempotency_key; returns durable run id, queue/ETA, poll_after_ms, counters when known, and coverage_ingest. Managed output is byte-capped and setup, persistence, timeout, cancellation, or shutdown failures become terminal failed jobs. Use get_run_data only after waiting the returned poll_after_ms.",
            command_execution(),
            object_schema(
                &[
                    ("command_ref", string("Registered command UUID or name.")),
                    (
                        "timeout_seconds",
                        integer("Maximum execution time in seconds."),
                    ),
                    (
                        "idempotency_key",
                        nullable_string("Stable caller key for safe resubmission."),
                    ),
                    (
                        "wait",
                        boolean(
                            "Prefer false and poll with get_run_data; true waits for terminal completion.",
                        ),
                    ),
                    ("max_words", budget_schema()),
                ],
                &["command_ref"],
            ),
        ),
        tool(
            "get_run_data",
            "Fetch durable run data for one required run_id. There is no implicit latest-run selection: call project_context, read data.latest_run.id, then pass that id here. This tool is read-only: it only returns current state and never starts, advances, reruns, or cancels a run. terminal=false means wait at least poll_after_ms before the next get_run_data call; do not immediately call again. Use detailed only for artifact paths, exact timestamps, or execution audit.",
            read_only(),
            object_schema(
                &[
                    (
                        "run_id",
                        string(
                            "Required durable run UUID. To inspect the latest run, use data.latest_run.id from project_context; this tool does not infer it.",
                        ),
                    ),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &["run_id"],
            ),
        ),
        tool(
            "cancel_run",
            "Request process-group cancellation for a durable run that the user no longer wants. This is the mutating counterpart to read-only get_run_data. Use detailed only for artifact paths, exact timestamps, or execution audit.",
            command_execution(),
            object_schema(
                &[
                    ("run_id", string("Durable run UUID.")),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &["run_id"],
            ),
        ),
        tool(
            "search_test_logs",
            "Search retained stdout/stderr literally for one query string or a list of query strings matched with OR. Returns word-bounded merged context windows; no matches is a successful empty result and full logs are never embedded. Retained output is capped per stream, and run summaries report truncation.",
            read_only(),
            object_schema(
                &[
                    ("run_id", string("Durable run UUID.")),
                    ("query", query_schema()),
                    (
                        "stream",
                        enum_schema(&["stdout", "stderr", "both"], "Output stream to search."),
                    ),
                    ("context_lines", integer("Context lines around each match.")),
                    ("max_matches", integer("Maximum returned matches.")),
                    ("max_words", budget_schema()),
                    (
                        "case_sensitive",
                        boolean("Whether matching preserves case."),
                    ),
                ],
                &["run_id", "query"],
            ),
        ),
        tool(
            "ingest_coverage",
            "Ingest one external or historical coverage report. Relative report_path resolves inside the selected checkout; returns immutable snapshot summary, parser warnings, and provenance compactly. Reports larger than 64 MiB and malformed numeric fields are explicit validation errors.",
            local_write(),
            object_schema(
                &[
                    ("report_path", string("Coverage artifact path.")),
                    (
                        "format",
                        string(
                            "auto, lcov, coverage.py, cobertura, jacoco, istanbul, go, or llvm.",
                        ),
                    ),
                    ("suite", string("Logical test suite name.")),
                    ("branch", nullable_string("Branch selector.")),
                    ("commit_sha", nullable_string("Measured commit.")),
                    ("base_ref", nullable_string("Comparison base ref.")),
                    ("max_words", budget_schema()),
                ],
                &["report_path"],
            ),
        ),
        tool(
            "register_worktree",
            "Register one linked checkout and freeze the current baseline for base_ref when available. Returns worktree_id, checkout/head/base identity, and baseline_snapshot_id for coverage_compare worktree mode.",
            local_write(),
            object_schema(
                &[
                    ("path", string("Git worktree path.")),
                    ("base_ref", string("Base branch or ref.")),
                    ("name", nullable_string("Optional human label.")),
                    ("max_words", budget_schema()),
                ],
                &["path", "base_ref"],
            ),
        ),
        tool(
            "coverage_query",
            "Read exactly one compact coverage projection per call. Compose multiple narrow calls when a task needs more than one answer. Use targets for ranked next work (red uncovered regions; order_by=priority, uncovered_lines, line_rate, or file_path), file for one file's red regions, summary/files/insights for summaries, or line_history for one line over time. Omit snapshot_id to use the latest snapshot for the selected checkout; continue collections with the returned cursor. Use detailed only for report/parser provenance, raw file metrics, or unabridged line-history records. Use coverage_compare view=regions for grouped previous-session impact and source_context for the follow-up source range.",
            read_only(),
            object_schema(
                &[
                    (
                        "view",
                        enum_schema(
                            &[
                                "summary",
                                "files",
                                "targets",
                                "file",
                                "insights",
                                "line_history",
                            ],
                            "Exactly one projection per call: targets, file, summary, files, insights, or line_history.",
                        ),
                    ),
                    (
                        "snapshot_id",
                        nullable_string(
                            "Snapshot UUID. Optional except line_history; omitted selects the latest snapshot for the selected checkout.",
                        ),
                    ),
                    (
                        "baseline_snapshot_id",
                        nullable_string(
                            "Optional baseline UUID for insights; use coverage_compare for snapshot comparisons.",
                        ),
                    ),
                    (
                        "suite",
                        nullable_string("Suite selector; omitted uses the request context."),
                    ),
                    (
                        "branch",
                        nullable_string("Branch selector for snapshot selection or line_history."),
                    ),
                    (
                        "file_path",
                        nullable_string(
                            "Repository-relative file path. Required for file and line_history; optional filter for targets.",
                        ),
                    ),
                    (
                        "line_number",
                        integer(
                            "One-based line number; required with file_path and suite for line_history.",
                        ),
                    ),
                    (
                        "line_ranges",
                        json_schema(
                            "Optional array of inclusive {start,end} ranges for exact selected lines in file view; multiple disjoint ranges are allowed.",
                        ),
                    ),
                    (
                        "order_by",
                        enum_schema(
                            &["priority", "uncovered_lines", "line_rate", "file_path"],
                            "Ordering for targets only; priority is the default, followed by uncovered_lines, line_rate, or file_path.",
                        ),
                    ),
                    (
                        "cursor",
                        nullable_string(
                            "Opaque cursor from the same view, filters, and order; pass page.next_cursor unchanged.",
                        ),
                    ),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &["view"],
            ),
        ),
        tool(
            "coverage_compare",
            "Read exactly one comparison projection per call. Compose overview, regions, and source_context calls when you need both a summary and code context. regions returns grouped changed ranges and is the token-efficient default for previous-session impact; when direct ids are omitted it compares the latest snapshot in the selected checkout with its previous matching snapshot. Direct mode accepts snapshot_id plus baseline_snapshot_id; worktree mode uses worktree_id. Views are overview, files, lines, regions, and progress. Use detailed only for raw baseline/current snapshot provenance.",
            read_only(),
            object_schema(
                &[
                    (
                        "view",
                        enum_schema(
                            &["overview", "files", "lines", "regions", "progress"],
                            "Exactly one comparison projection per call: overview, files, lines, regions, or progress.",
                        ),
                    ),
                    (
                        "snapshot_id",
                        nullable_string(
                            "Current snapshot UUID; required with baseline_snapshot_id for direct overview/files/lines mode, optional for regions auto-selection.",
                        ),
                    ),
                    (
                        "baseline_snapshot_id",
                        nullable_string(
                            "Baseline snapshot UUID; direct comparison partner for snapshot_id.",
                        ),
                    ),
                    (
                        "worktree_id",
                        nullable_string("Registered worktree UUID; uses its frozen baseline."),
                    ),
                    (
                        "suite",
                        nullable_string(
                            "Suite selector for automatic regions selection or progress.",
                        ),
                    ),
                    (
                        "file_path",
                        nullable_string(
                            "Optional repository-relative file filter for files/lines/regions.",
                        ),
                    ),
                    (
                        "only_regressions",
                        boolean("For lines or regions, keep only status=regressed changes."),
                    ),
                    (
                        "cursor",
                        nullable_string("Opaque cursor from the same comparison view and filters."),
                    ),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &[],
            ),
        ),
        tool(
            "source_context",
            "Read exactly one bounded, contiguous source range per call. Make separate calls for disjoint ranges. Each line includes a compact coverage status and red/green/yellow/gray marker; red_regions groups missed executable lines. Request one-based start/end ranges after coverage_query or coverage_compare identifies the file and lines; ranges are capped at 200 lines.",
            read_only(),
            object_schema(
                &[
                    (
                        "snapshot_id",
                        string("Snapshot UUID returned by a coverage projection."),
                    ),
                    (
                        "file_path",
                        string(
                            "Repository-relative source file returned by a coverage projection.",
                        ),
                    ),
                    (
                        "start",
                        integer("One-based inclusive start line from a red or changed region."),
                    ),
                    (
                        "end",
                        integer("One-based inclusive end line; maximum 200 lines per call."),
                    ),
                    (
                        "cursor",
                        nullable_string("Opaque cursor for the same source range and budget."),
                    ),
                    ("max_words", budget_schema()),
                ],
                &["snapshot_id", "file_path", "start", "end"],
            ),
        ),
    ])
}

/// Returns the static resource inventory.
pub fn resources_list() -> Value {
    json!([{"uri":"coverage://context","name":"Coverage project context","mimeType":"application/json","description":"Current project summary, compaction policy, commands, and active runs."}])
}

/// Returns the static resource template inventory.
pub fn resource_templates_list() -> Value {
    json!([{"uriTemplate":"coverage://snapshot/{snapshot_id}/summary","name":"Coverage snapshot summary","mimeType":"application/json","description":"Compact summary for one immutable coverage snapshot."}])
}

/// Dispatches one JSON-RPC MCP request.
///
/// `None` is returned for notifications, which must not receive a response.
/// Requests that need project data require a selected service; inventory
/// methods such as `initialize` and `tools/list` work without one.
pub fn dispatch_json_rpc(service: Option<&CoverageService>, request: &Value) -> Option<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Some(json_rpc_error(
            id,
            AppError::Validation("MCP request method is required".to_owned()),
        ));
    };
    if method.starts_with("notifications/") {
        return None;
    }

    let response = match method {
        "initialize" => json_rpc_result(id.clone(), initialize_result()),
        "tools/list" => json_rpc_result(id.clone(), json!({"tools":tools_list()})),
        "resources/list" => json_rpc_result(id.clone(), json!({"resources":resources_list()})),
        "resources/templates/list" => json_rpc_result(
            id.clone(),
            json!({"resourceTemplates":resource_templates_list()}),
        ),
        "resources/read" => {
            let uri = request
                .get("params")
                .and_then(|value| value.get("uri"))
                .and_then(Value::as_str);
            let Some(uri) = uri else {
                return Some(json_rpc_error(
                    id,
                    AppError::Validation("resources/read requires uri".to_owned()),
                ));
            };
            let Some(service) = service else {
                return Some(json_rpc_error(
                    id,
                    AppError::Validation("resources/read requires a selected project".to_owned()),
                ));
            };
            match read_resource(service, uri) {
                Ok(value) => json_rpc_result(
                    id,
                    json!({"contents":[{"uri":uri,"mimeType":"application/json","text":value.to_string()}]}),
                ),
                Err(error) => json_rpc_error(id, error),
            }
        }
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = params.get("name").and_then(Value::as_str);
            let Some(name) = name else {
                return Some(json_rpc_error(
                    id,
                    AppError::Validation("tools/call requires name".to_owned()),
                ));
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let Some(service) = service else {
                return Some(json_rpc_error(
                    id,
                    AppError::Validation("tools/call requires a selected project".to_owned()),
                ));
            };
            match call_tool(service, name, &arguments) {
                Ok(value) => json_rpc_result(
                    id,
                    json!({"structuredContent":value,"content":[{"type":"text","text":value.to_string()}],"isError":false}),
                ),
                Err(error) => json_rpc_result(
                    id,
                    json!({"content":[{"type":"text","text":error.to_string()}],"isError":true}),
                ),
            }
        }
        _ => json_rpc_error(
            id,
            AppError::NotFound(format!("unknown MCP method: {method}")),
        ),
    };
    Some(response)
}

/// Dispatches one MCP tool call into the shared service.
pub fn call_tool(service: &CoverageService, name: &str, args: &Value) -> AppResult<Value> {
    let args = args
        .as_object()
        .ok_or_else(|| AppError::Validation("tool arguments must be an object".to_owned()))?;
    let get = |key: &str| args.get(key);
    let max_words = optional_usize(args, "max_words")?.unwrap_or(DEFAULT_MAX_WORDS);
    let detailed = optional_bool(args, "detailed")?.unwrap_or(false);
    match name {
        "project_context" => {
            service.project_context(get("cursor").and_then(Value::as_str), max_words, detailed)
        }
        "register_test_command" => service
            .command_registration(
                required_string(args, "name")?,
                required_string(args, "command")?,
                optional_bool(args, "human_approved")?.unwrap_or(false),
                required_string(args, "approved_by")?,
                required_string(args, "approval_note")?,
                optional_string(args, "cwd")?,
                optional_string(args, "shell")?.unwrap_or("/bin/bash"),
                get("artifact_paths").cloned(),
                detailed,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "run_test" => service
            .run_submission(
                required_string(args, "command_ref")?,
                optional_u64(args, "timeout_seconds")?,
                optional_string(args, "idempotency_key")?,
                optional_bool(args, "wait")?.unwrap_or(false),
                false,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "get_run_data" => service
            .run_state(required_string(args, "run_id")?, "status", detailed)
            .and_then(|value| service.apply_budget(value, max_words)),
        "cancel_run" => service
            .run_state(required_string(args, "run_id")?, "cancel", detailed)
            .and_then(|value| service.apply_budget(value, max_words)),
        "search_test_logs" => service.search_logs(
            required_string(args, "run_id")?,
            query_values(get("query"))?,
            optional_string(args, "stream")?.unwrap_or("both"),
            optional_usize(args, "context_lines")?.unwrap_or(3),
            optional_usize(args, "max_matches")?.unwrap_or(5),
            max_words,
            optional_bool(args, "case_sensitive")?.unwrap_or(false),
        ),
        "ingest_coverage" => service
            .ingest(
                required_string(args, "report_path")?,
                optional_string(args, "format")?.unwrap_or("auto"),
                optional_string(args, "suite")?.unwrap_or("default"),
                optional_string(args, "branch")?,
                optional_string(args, "commit_sha")?,
                optional_string(args, "base_ref")?,
                false,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "register_worktree" => service
            .worktree_registration(
                required_string(args, "path")?,
                required_string(args, "base_ref")?,
                optional_string(args, "name")?,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "coverage_query" => service.coverage_query_ordered(
            optional_string(args, "view")?.unwrap_or(""),
            optional_string(args, "snapshot_id")?,
            optional_string(args, "baseline_snapshot_id")?,
            optional_string(args, "suite")?,
            optional_string(args, "branch")?,
            optional_string(args, "file_path")?,
            optional_i64(args, "line_number")?,
            parse_ranges(get("line_ranges"))?,
            optional_string(args, "order_by")?,
            optional_string(args, "cursor")?,
            max_words,
            detailed,
        ),
        "coverage_compare" => service.coverage_comparison(
            optional_string(args, "view")?.unwrap_or("overview"),
            optional_string(args, "snapshot_id")?,
            optional_string(args, "baseline_snapshot_id")?,
            optional_string(args, "worktree_id")?,
            optional_string(args, "suite")?,
            optional_string(args, "file_path")?,
            optional_bool(args, "only_regressions")?.unwrap_or(false),
            optional_string(args, "cursor")?,
            max_words,
            detailed,
        ),
        "source_context" => service.source(
            required_string(args, "snapshot_id")?,
            required_string(args, "file_path")?,
            required_i64(args, "start")?,
            required_i64(args, "end")?,
            optional_string(args, "cursor")?,
            max_words,
        ),
        _ => Err(AppError::NotFound(format!("unknown MCP tool: {name}"))),
    }
}

/// Reads one MCP resource through the shared service.
pub fn read_resource(service: &CoverageService, uri: &str) -> AppResult<Value> {
    if uri == "coverage://context" {
        return service.project_context(None, DEFAULT_MAX_WORDS, false);
    }
    let prefix = "coverage://snapshot/";
    if let Some(snapshot_id) = uri
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix("/summary"))
    {
        return service.coverage_query(
            "summary",
            Some(snapshot_id),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            DEFAULT_MAX_WORDS,
            false,
        );
    }
    Err(AppError::NotFound(format!("unknown MCP resource: {uri}")))
}

fn required_string<'a>(args: &'a Map<String, Value>, key: &str) -> AppResult<&'a str> {
    optional_string(args, key)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn required_i64(args: &Map<String, Value>, key: &str) -> AppResult<i64> {
    optional_i64(args, key)?.ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn optional_string<'a>(args: &'a Map<String, Value>, key: &str) -> AppResult<Option<&'a str>> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be a string"))),
    }
}
fn optional_bool(args: &Map<String, Value>, key: &str) -> AppResult<Option<bool>> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be a boolean"))),
    }
}
fn optional_u64(args: &Map<String, Value>, key: &str) -> AppResult<Option<u64>> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be an unsigned integer"))),
    }
}
fn optional_usize(args: &Map<String, Value>, key: &str) -> AppResult<Option<usize>> {
    optional_u64(args, key)?
        .map(|value| checked_usize(value, key))
        .transpose()
}
#[cfg(target_pointer_width = "32")]
fn checked_usize(value: u64, key: &str) -> AppResult<usize> {
    if value > usize::MAX as u64 {
        return Err(AppError::Validation(format!(
            "{key} is too large for this platform"
        )));
    }
    Ok(value as usize)
}
#[cfg(target_pointer_width = "64")]
fn checked_usize(value: u64, _key: &str) -> AppResult<usize> {
    Ok(value as usize)
}
fn optional_i64(args: &Map<String, Value>, key: &str) -> AppResult<Option<i64>> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{key} must be an integer"))),
    }
}
fn query_values(value: Option<&Value>) -> AppResult<Vec<String>> {
    match value {
        Some(Value::String(value)) => Ok(vec![value.clone()]),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    AppError::Validation("query array items must be strings".to_owned())
                })
            })
            .collect(),
        _ => Err(AppError::Validation(
            "query must be a string or array of strings".to_owned(),
        )),
    }
}
fn parse_ranges(value: Option<&Value>) -> AppResult<Option<Vec<LineRange>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or_else(|| AppError::Validation("line_ranges must be an array".to_owned()))?;
    let mut result = Vec::new();
    for value in values {
        let object = value
            .as_object()
            .ok_or_else(|| AppError::Validation("line range must be an object".to_owned()))?;
        let start = object
            .get("start")
            .and_then(Value::as_i64)
            .ok_or_else(|| AppError::Validation("line range start is required".to_owned()))?;
        let end = object
            .get("end")
            .and_then(Value::as_i64)
            .ok_or_else(|| AppError::Validation("line range end is required".to_owned()))?;
        result.push((start, end));
    }
    Ok(Some(result))
}

fn tool(name: &str, description: &str, annotations: Value, input_schema: Value) -> Value {
    json!({"name":name,"description":description,"inputSchema":input_schema,"outputSchema":output_schema(),"annotations":annotations})
}
fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn json_rpc_error(id: Value, error: AppError) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":error.to_string()}})
}
fn output_schema() -> Value {
    json!({"type":"object","properties":{"context":{"type":"object","description":"Request repository, checkout, suite, and schema context."},"data":{"description":"Projection-specific tool data; request one narrow projection at a time and compose multiple responses by carrying ids and ranges forward."},"page":{"type":["object","null"],"description":"Per-call pagination and response-budget metadata, including returned, total, word_count, max_words, truncated, and next_cursor."}},"required":["context","data","page"]})
}
fn object_schema(properties: &[(&str, Value)], required: &[&str]) -> Value {
    let mut map = Map::new();
    for (key, value) in properties {
        map.insert((*key).to_owned(), value.clone());
    }
    json!({"type":"object","properties":map,"required":required})
}
fn string(description: &str) -> Value {
    json!({"type":"string","description":description})
}
fn nullable_string(description: &str) -> Value {
    json!({"type":["string","null"],"description":description})
}
fn integer(description: &str) -> Value {
    json!({"type":"integer","description":description})
}
fn boolean(description: &str) -> Value {
    json!({"type":"boolean","description":description})
}
fn json_schema(description: &str) -> Value {
    json!({"description":description})
}
fn enum_schema(values: &[&str], description: &str) -> Value {
    json!({"type":"string","enum":values,"description":description})
}
fn budget_schema() -> Value {
    json!({"type":"integer","minimum":50,"maximum":5000,"default":600,"description":"Per-call response word budget, 50–5000, default 600; use page.next_cursor for more collection items."})
}
fn detailed_schema() -> Value {
    json!({"type":"boolean","default":false,"description":"Keep false for normal work; true only for explicitly requested audit, raw-metric, or provenance fields."})
}
fn query_schema() -> Value {
    json!({"anyOf":[{"type":"string"},{"type":"array","items":{"type":"string"}}],"description":"One query string or an array of query strings; matches use OR semantics and return bounded context windows."})
}
fn annotations(read_only: bool, destructive: bool, idempotent: bool, open_world: bool) -> Value {
    json!({"readOnlyHint":read_only,"destructiveHint":destructive,"idempotentHint":idempotent,"openWorldHint":open_world})
}
fn read_only() -> Value {
    annotations(true, false, true, false)
}
fn local_write() -> Value {
    annotations(false, false, false, false)
}
fn command_execution() -> Value {
    annotations(false, true, false, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_helpers_validate_shapes_and_ranges() {
        let mut args = Map::new();
        args.insert("name".to_owned(), json!("value"));
        args.insert("number".to_owned(), json!(7));
        assert_eq!(required_string(&args, "name").unwrap(), "value");
        assert_eq!(required_i64(&args, "number").unwrap(), 7);
        assert!(required_string(&args, "missing").is_err());
        args.insert("name".to_owned(), json!(" "));
        assert!(required_string(&args, "name").is_err());
        args.insert("name".to_owned(), json!(7));
        assert!(optional_string(&args, "name").is_err());
        assert!(required_i64(&args, "missing").is_err());
        args.insert("number".to_owned(), json!("7"));
        assert!(required_i64(&args, "number").is_err());

        assert_eq!(query_values(Some(&json!("one"))).unwrap(), vec!["one"]);
        assert!(query_values(Some(&json!(["one", 2, "two"]))).is_err());
        assert!(query_values(None).is_err());
        assert!(query_values(Some(&json!(true))).is_err());

        assert_eq!(parse_ranges(None).unwrap(), None);
        assert_eq!(
            parse_ranges(Some(&json!([{ "start": 2, "end": 4 }]))).unwrap(),
            Some(vec![(2, 4)])
        );
        assert!(parse_ranges(Some(&json!([{ "end": 4 }]))).is_err());
        assert!(parse_ranges(Some(&json!([{ "start": 2 }]))).is_err());
        assert!(parse_ranges(Some(&json!([3]))).is_err());
        assert!(parse_ranges(Some(&json!("bad"))).is_err());
        assert!(optional_bool(&args, "number").is_err());
        assert!(optional_usize(&args, "number").is_err());
        args.insert("number".to_owned(), json!(7));
        assert_eq!(optional_usize(&args, "number").unwrap(), Some(7));
    }

    #[test]
    fn schemas_and_annotations_are_deterministic() {
        let schema = object_schema(&[("field", string("field"))], &["field"]);
        assert_eq!(schema["required"][0], "field");
        assert_eq!(nullable_string("nullable")["type"][1], "null");
        assert_eq!(integer("integer")["type"], "integer");
        assert_eq!(boolean("boolean")["type"], "boolean");
        assert_eq!(json_schema("json")["description"], "json");
        assert_eq!(enum_schema(&["a", "b"], "enum")["enum"][1], "b");
        assert_eq!(budget_schema()["default"], 600);
        assert_eq!(budget_schema()["minimum"], 50);
        assert!(!detailed_schema()["default"].as_bool().unwrap());
        assert!(query_schema()["anyOf"].is_array());
        assert_eq!(read_only()["readOnlyHint"], true);
        assert_eq!(local_write()["destructiveHint"], false);
        assert_eq!(command_execution()["openWorldHint"], true);
        assert!(output_schema()["required"].as_array().unwrap().len() == 3);
        assert!(tool("name", "description", read_only(), schema)["outputSchema"].is_object());

        let initialize = initialize_result();
        let instructions = initialize["instructions"].as_str().unwrap();
        assert!(instructions.contains("multiple calls"));
        assert!(instructions.contains("snapshot_id, file_path, and start/end ranges"));
        let tools = tools_list();
        let coverage_query = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "coverage_query")
            .unwrap();
        assert!(
            coverage_query["description"]
                .as_str()
                .unwrap()
                .contains("exactly one compact coverage projection per call")
        );
        let project_context = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "project_context")
            .unwrap();
        assert!(
            project_context["description"]
                .as_str()
                .unwrap()
                .contains("latest_run.id is the run_id")
        );
        let get_run_data = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "get_run_data")
            .unwrap();
        assert!(
            get_run_data["description"]
                .as_str()
                .unwrap()
                .contains("no implicit latest-run selection")
        );
        assert_eq!(
            coverage_query["inputSchema"]["properties"]["order_by"]["enum"][0],
            "priority"
        );
    }

    #[test]
    fn json_rpc_dispatch_keeps_inventory_and_notifications_transport_neutral() {
        let initialize =
            dispatch_json_rpc(None, &json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
                .unwrap();
        assert_eq!(initialize["result"]["serverInfo"]["name"], "coverage-mcp");

        let tools = dispatch_json_rpc(None, &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .unwrap();
        assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 11);

        assert!(
            dispatch_json_rpc(
                None,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            )
            .is_none()
        );
        assert!(
            dispatch_json_rpc(
                None,
                &json!({"jsonrpc":"2.0","id":3,"method":"resources/read"}),
            )
            .unwrap()["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("requires uri"))
        );
        assert!(
            dispatch_json_rpc(None, &json!({"id":4})).unwrap()["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("method is required"))
        );

        let resource_without_project = dispatch_json_rpc(
            None,
            &json!({"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"coverage://context"}}),
        )
        .unwrap();
        assert!(
            resource_without_project["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("selected project"))
        );

        let tool_without_project = dispatch_json_rpc(
            None,
            &json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"project_context"}}),
        )
        .unwrap();
        assert!(
            tool_without_project["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("selected project"))
        );
    }
}
