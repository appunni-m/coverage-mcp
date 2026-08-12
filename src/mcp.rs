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
pub const MCP_INSTRUCTIONS: &str = "Coverage MCP 0.8.0 schema 7 exposes a compact agent interface. Start with project_context, then run only exact approved registrations returned there or created with register_test_command after human approval. Submit with run_test(wait=false), save the run id, then fetch status with get_run_data(detailed=false). get_run_data is read-only: it only returns durable run data and never starts, advances, reruns, or cancels work. For every non-terminal response, wait at least the returned poll_after_ms before the next get_run_data call; do not poll immediately. Use cancel_run only when the user no longer wants the run. Use search_test_logs for targeted retained stdout/stderr evidence. Use coverage_query for snapshot reads, coverage_compare only for lineage-compatible snapshots or registered worktrees, and source_context only for bounded source ranges already identified by coverage data. Every response is {context,data,page}; max_words is the primary response budget and collections continue with page.next_cursor. Omit detailed or keep it false for normal work; set it true only when a tool description names required audit or raw-provenance fields. detailed never returns logs. The daemon uses stateless JSON responses, bounded MCP concurrency, and bounded HTTP/DuckDB deadlines; keep query fan-out bounded and retry an individual request with backoff after a transient 503/504 or interrupted connection.";

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
            "Discover project state before work: stable project id, metrics/freshness, exact approved commands, latest run, active runs, and page metadata. Use detailed only for approval audit fields and full project chronology.",
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
            "Submit one approved command. Prefer wait=false with a stable idempotency_key; returns durable run id, queue/ETA, poll_after_ms, counters when known, and coverage_ingest. Use get_run_data only after waiting the returned poll_after_ms.",
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
            "Fetch durable run data. This tool is read-only: it only returns current state and never starts, advances, reruns, or cancels a run. terminal=false means wait at least poll_after_ms before the next get_run_data call; do not immediately call again. Use detailed only for artifact paths, exact timestamps, or execution audit.",
            read_only(),
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
            "Search retained stdout/stderr literally for one query string or a list of query strings matched with OR. Returns word-bounded merged context windows; no matches is a successful empty result and full logs are never embedded.",
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
            "Ingest one external or historical coverage report. Relative report_path resolves inside the selected checkout; returns immutable snapshot summary, parser warnings, and provenance compactly.",
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
            "Read one coverage projection: summary, files, file gaps, insights, or line_history. Use snapshot_id/suite/branch/file_path/line_number/line_ranges as required by the view; continue collections with cursor. Use detailed only for report/parser provenance, raw file metrics, or unabridged line-history records.",
            read_only(),
            object_schema(
                &[
                    (
                        "view",
                        enum_schema(
                            &["summary", "files", "file", "insights", "line_history"],
                            "Coverage projection.",
                        ),
                    ),
                    ("snapshot_id", nullable_string("Snapshot UUID.")),
                    (
                        "baseline_snapshot_id",
                        nullable_string("Optional baseline UUID for insights."),
                    ),
                    ("suite", nullable_string("Suite selector.")),
                    ("branch", nullable_string("Branch selector.")),
                    (
                        "file_path",
                        nullable_string("Repository-relative file path."),
                    ),
                    ("line_number", integer("One-based line for line_history.")),
                    (
                        "line_ranges",
                        json_schema("Inclusive line ranges with start and end."),
                    ),
                    ("cursor", nullable_string("Opaque page cursor.")),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &["view"],
            ),
        ),
        tool(
            "coverage_compare",
            "Compare compatible coverage lineage. Direct mode uses snapshot_id plus baseline_snapshot_id; worktree mode uses worktree_id. Views are overview, files, lines, and progress. Use detailed only for raw baseline/current snapshot provenance.",
            read_only(),
            object_schema(
                &[
                    (
                        "view",
                        enum_schema(
                            &["overview", "files", "lines", "progress"],
                            "Comparison projection.",
                        ),
                    ),
                    ("snapshot_id", nullable_string("Current snapshot UUID.")),
                    (
                        "baseline_snapshot_id",
                        nullable_string("Baseline snapshot UUID."),
                    ),
                    ("worktree_id", nullable_string("Registered worktree UUID.")),
                    ("suite", nullable_string("Suite selector.")),
                    ("file_path", nullable_string("Optional file filter.")),
                    ("only_regressions", boolean("Keep only regressed lines.")),
                    ("cursor", nullable_string("Opaque page cursor.")),
                    ("max_words", budget_schema()),
                    ("detailed", detailed_schema()),
                ],
                &[],
            ),
        ),
        tool(
            "source_context",
            "Read numbered source lines for one snapshot and repository-relative file_path. Request bounded one-based start/end ranges, usually after coverage_query identifies gaps or lines of interest.",
            read_only(),
            object_schema(
                &[
                    ("snapshot_id", string("Snapshot UUID.")),
                    ("file_path", string("Repository-relative source file.")),
                    ("start", integer("One-based inclusive start line.")),
                    ("end", integer("One-based inclusive end line.")),
                    ("cursor", nullable_string("Opaque page cursor.")),
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
    let max_words = get("max_words")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_WORDS as u64) as usize;
    let detailed = get("detailed").and_then(Value::as_bool).unwrap_or(false);
    match name {
        "project_context" => {
            service.project_context(get("cursor").and_then(Value::as_str), max_words, detailed)
        }
        "register_test_command" => service
            .command_registration(
                required_string(args, "name")?,
                required_string(args, "command")?,
                get("human_approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                required_string(args, "approved_by")?,
                required_string(args, "approval_note")?,
                get("cwd").and_then(Value::as_str),
                get("shell").and_then(Value::as_str).unwrap_or("/bin/bash"),
                get("artifact_paths").cloned(),
                detailed,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "run_test" => service
            .run_submission(
                required_string(args, "command_ref")?,
                get("timeout_seconds").and_then(Value::as_u64),
                get("idempotency_key").and_then(Value::as_str),
                get("wait").and_then(Value::as_bool).unwrap_or(false),
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
            get("stream").and_then(Value::as_str).unwrap_or("both"),
            get("context_lines").and_then(Value::as_u64).unwrap_or(3) as usize,
            get("max_matches").and_then(Value::as_u64).unwrap_or(5) as usize,
            max_words,
            get("case_sensitive")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
        "ingest_coverage" => service
            .ingest(
                required_string(args, "report_path")?,
                get("format").and_then(Value::as_str).unwrap_or("auto"),
                get("suite").and_then(Value::as_str).unwrap_or("default"),
                get("branch").and_then(Value::as_str),
                get("commit_sha").and_then(Value::as_str),
                get("base_ref").and_then(Value::as_str),
                false,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "register_worktree" => service
            .worktree_registration(
                required_string(args, "path")?,
                required_string(args, "base_ref")?,
                get("name").and_then(Value::as_str),
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "coverage_query" => service.coverage_query(
            get("view").and_then(Value::as_str).unwrap_or(""),
            get("snapshot_id").and_then(Value::as_str),
            get("baseline_snapshot_id").and_then(Value::as_str),
            get("suite").and_then(Value::as_str),
            get("branch").and_then(Value::as_str),
            get("file_path").and_then(Value::as_str),
            get("line_number").and_then(Value::as_i64),
            parse_ranges(get("line_ranges"))?,
            get("cursor").and_then(Value::as_str),
            max_words,
            detailed,
        ),
        "coverage_compare" => service.coverage_comparison(
            get("view").and_then(Value::as_str).unwrap_or("overview"),
            get("snapshot_id").and_then(Value::as_str),
            get("baseline_snapshot_id").and_then(Value::as_str),
            get("worktree_id").and_then(Value::as_str),
            get("suite").and_then(Value::as_str),
            get("file_path").and_then(Value::as_str),
            get("only_regressions")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            get("cursor").and_then(Value::as_str),
            max_words,
            detailed,
        ),
        "source_context" => service.source(
            required_string(args, "snapshot_id")?,
            required_string(args, "file_path")?,
            required_i64(args, "start")?,
            required_i64(args, "end")?,
            get("cursor").and_then(Value::as_str),
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
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn required_i64(args: &Map<String, Value>, key: &str) -> AppResult<i64> {
    args.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
}
fn query_values(value: Option<&Value>) -> AppResult<Vec<String>> {
    match value {
        Some(Value::String(value)) => Ok(vec![value.clone()]),
        Some(Value::Array(values)) => Ok(values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()),
        _ => Err(AppError::Validation(
            "query must be a string or array of strings".to_owned(),
        )),
    }
}
fn parse_ranges(value: Option<&Value>) -> AppResult<Option<Vec<LineRange>>> {
    let Some(Value::Array(values)) = value else {
        return Ok(None);
    };
    let mut result = Vec::new();
    for value in values {
        let start = value
            .get("start")
            .and_then(Value::as_i64)
            .ok_or_else(|| AppError::Validation("line range start is required".to_owned()))?;
        let end = value
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
    json!({"type":"object","properties":{"context":{"type":"object","description":"Request repository and schema context."},"data":{"description":"Tool data payload."},"page":{"type":["object","null"],"description":"Pagination and response-budget metadata."}},"required":["context","data","page"]})
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
    json!({"type":"integer","minimum":20,"maximum":5000,"default":600,"description":"Primary response word budget; use pagination when truncated."})
}
fn detailed_schema() -> Value {
    json!({"type":"boolean","default":false,"description":"Keep false for normal work; true only for explicitly requested audit or provenance fields."})
}
fn query_schema() -> Value {
    json!({"anyOf":[{"type":"string"},{"type":"array","items":{"type":"string"}}],"description":"One query string or a list of query strings; any term is present uses OR matching."})
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
        assert!(required_i64(&args, "missing").is_err());
        args.insert("number".to_owned(), json!("7"));
        assert!(required_i64(&args, "number").is_err());

        assert_eq!(query_values(Some(&json!("one"))).unwrap(), vec!["one"]);
        assert_eq!(
            query_values(Some(&json!(["one", 2, "two"]))).unwrap(),
            vec!["one", "two"]
        );
        assert!(query_values(None).is_err());
        assert!(query_values(Some(&json!(true))).is_err());

        assert_eq!(parse_ranges(None).unwrap(), None);
        assert_eq!(
            parse_ranges(Some(&json!([{ "start": 2, "end": 4 }]))).unwrap(),
            Some(vec![(2, 4)])
        );
        assert!(parse_ranges(Some(&json!([{ "end": 4 }]))).is_err());
        assert!(parse_ranges(Some(&json!([{ "start": 2 }]))).is_err());
        assert!(parse_ranges(Some(&json!("bad"))).unwrap().is_none());
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
        assert!(!detailed_schema()["default"].as_bool().unwrap());
        assert!(query_schema()["anyOf"].is_array());
        assert_eq!(read_only()["readOnlyHint"], true);
        assert_eq!(local_write()["destructiveHint"], false);
        assert_eq!(command_execution()["openWorldHint"], true);
        assert!(output_schema()["required"].as_array().unwrap().len() == 3);
        assert!(tool("name", "description", read_only(), schema)["outputSchema"].is_object());
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
