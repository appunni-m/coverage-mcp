//! Stateless JSON MCP contract and transport dispatcher for the Rust daemon.
//!
//! The public inventory is explicit so the wire contract is reviewable,
//! deterministic, and independent of a third-party MCP runtime. The same
//! dispatcher is used by the loopback HTTP endpoint and the native stdio
//! transport.

// The public schema is validated and normalized before these projections run;
// these assertions document the resulting internal shape invariants.
#![allow(clippy::expect_used, clippy::unwrap_in_result)]

use std::path::{Component, Path};

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::VERSION;
use crate::error::{AppError, AppResult};
use crate::git::{merge_base, parent_commit};
use crate::service::{CoverageService, DEFAULT_MAX_WORDS};

/// MCP stream endpoint instructions shown during initialization.
pub const MCP_INSTRUCTIONS: &str = concat!(
    "Coverage MCP ",
    env!("CARGO_PKG_VERSION"),
    r#" schema 9 exposes a consolidated, composable agent interface.

Start with project_context. Run only an exact approved registration, or register a command after human approval of its command, cwd, shell, and artifacts. Prefer run_test(wait=false, reuse_if_unchanged=true, idempotency_key=...). Save the returned run_id. If submission_reused=true, use that terminal evidence instead of launching duplicate work. For every non-terminal result, wait at least poll_after_ms, then use run_review(view=status, run_id=...). Use run_review(view=logs) only for targeted, bounded literal evidence; cancel_run is only for work the user no longer wants. Setup, capture, timeout, cancellation, persistence, and shutdown failures become terminal failed jobs.

Use coverage_review as the analysis boundary: task=change for changed-code coverage and regressions, task=history for two detailed points plus an aggregate window, task=insight for ranked red regions, task=source for grouped ranges, task=audit for exact records, and task=all for a bounded combination. Carry snapshot/run/baseline selectors inside the structured request. Independent reviews may use multiple calls in parallel; dependent source requests wait for their ranges. Responses always use {context,data,page}, state claim_status and reasons, preserve measurement lineage, and reject requests that exceed max_words (50–5000, default 600) or max_bytes. Keep detailed=false and source ranges bounded. In compact output, changed_code uses + for covered added executable lines, ! for uncovered added executable lines, ~ for branch gaps, . for non-executable additions, and ? for unavailable coverage; compact regions use + for improved/new, ! for regressed, - for removed, and ~ for changed regions. A file-metric legend defines the l/b/f/r arrays once.

Raw LLVM JSON, LCOV, or another report is useful for a one-off point-in-time grep. Coverage MCP adds immutable snapshots, repository/branch/commit lineage, freshness and artifact provenance, changed-code and history analysis, source resolution, approval-aware execution, idempotent reuse, and bounded token-efficient responses. Use coverage_import for an external report and then coverage_review; never infer a historical or changed-code claim from an unregistered raw file. A stdio bridge safely recovers a refused daemon connection once; ambiguous writes are not replayed. Ingestion is capped at 64 MiB and malformed numeric fields are validation errors."#
);

/// Returns the MCP initialize result.
pub fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {"tools": {"listChanged": false}, "resources": {"subscribe": false, "listChanged": false}},
        "serverInfo": {"name": "coverage-mcp", "version": VERSION},
        "instructions": MCP_INSTRUCTIONS,
    })
}

/// Returns the public tool inventory.
pub fn tools_list() -> Value {
    let mut tools = public_tools_list();
    let items = tools
        .as_array_mut()
        .expect("public tool inventory must be an array");
    for tool in items {
        let schema = tool
            .get_mut("inputSchema")
            .and_then(Value::as_object_mut)
            .expect("public tool must declare an input schema");
        schema.insert("additionalProperties".to_owned(), Value::Bool(false));
    }
    tools
}

/// Returns the consolidated public tool inventory before strict wire decoration.
fn public_tools_list() -> Value {
    Value::Array(vec![
        tool(
            "project_context",
            "Discover project state before work: stable project id, metrics/freshness, exact approved commands, latest_run, active runs, and page metadata. The returned latest_run.id is the explicit run_id for run_review; run_review never infers the latest run: there is no implicit latest selection. Use detailed only for approval audit fields and full project chronology.",
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
            "Submit one approved command. Prefer wait=false with a stable idempotency_key; returns durable run id, queue/ETA, poll_after_ms, counters when known, and coverage_ingest. Managed output is byte-capped and setup, persistence, timeout, cancellation, or shutdown failures become terminal failed jobs. Use run_review(view=status, run_id=...) only after waiting the returned poll_after_ms.",
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
                            "Prefer false and poll with run_review(view=status); true waits for terminal completion.",
                        ),
                    ),
                    (
                        "reuse_if_unchanged",
                        json!({
                            "type":"boolean",
                            "default":true,
                            "description":"Reuse the latest compatible terminal run when the registered command, clean checkout, branch, and commit are unchanged; set false to force execution."
                        }),
                    ),
                    ("max_words", budget_schema()),
                ],
                &["command_ref"],
            ),
        ),
        tool_with_output(
            "run_review",
            "Read one durable run without starting or advancing work. Use view=status for current state and terminal coverage evidence, or view=logs for targeted literal stdout/stderr matches. An explicit run_id is always required; this tool has no implicit latest selection.",
            read_only(),
            object_schema(
                &[
                    ("run_id", string("Required durable run UUID.")),
                    (
                        "view",
                        enum_default_schema(&["status", "logs"], "status", "Run projection."),
                    ),
                    ("query", query_schema()),
                    (
                        "stream",
                        enum_schema(&["stdout", "stderr", "both"], "Output stream for logs."),
                    ),
                    (
                        "context_lines",
                        bounded_integer(0, 20, 3, "Log context lines."),
                    ),
                    (
                        "max_matches",
                        bounded_integer(1, 50, 5, "Maximum log matches."),
                    ),
                    (
                        "case_sensitive",
                        boolean("Whether log matching preserves case."),
                    ),
                    ("max_words", budget_schema()),
                    (
                        "max_bytes",
                        bounded_integer(1_000, 2_000_000, 12_000, "Complete response byte budget."),
                    ),
                ],
                &["run_id"],
            ),
            output_schema(),
        ),
        tool(
            "cancel_run",
            "Request process-group cancellation for a durable run that the user no longer wants. This is separate from read-only run_review. Use detailed only for artifact paths, exact timestamps, or execution audit.",
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
        tool_with_output(
            "coverage_import",
            "Import one external or historical coverage report with explicit provenance. This is never treated as an artifact produced by run_test; use coverage_review afterward to analyze the imported snapshot.",
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
                    (
                        "max_bytes",
                        bounded_integer(1_000, 2_000_000, 12_000, "Complete response byte budget."),
                    ),
                ],
                &["report_path"],
            ),
            output_schema(),
        ),
        tool_with_output(
            "coverage_review",
            "Return one bounded, task-oriented coverage review. Use task=change for changed-code coverage and regressions, task=history for detailed recent snapshots, line-over-time (line over time) history, plus an aggregate window, task=insight for ranked red uncovered regions, task=source for grouped source ranges, task=audit for exact records, or task=all for a compact combination. The server resolves compatible snapshots and groups regions.",
            read_only(),
            coverage_review_input_schema(),
            coverage_review_output_schema(),
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
        "tools/list" => json_rpc_result(
            id.clone(),
            json!({
                "tools": tools_list(),
                "contract": public_contract_metadata(),
            }),
        ),
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
    validate_public_tool_keys(name, args)?;
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
            .run_submission_with_options(
                required_string(args, "command_ref")?,
                optional_u64(args, "timeout_seconds")?,
                optional_string(args, "idempotency_key")?,
                optional_bool(args, "wait")?.unwrap_or(false),
                optional_bool(args, "reuse_if_unchanged")?.unwrap_or(true),
                false,
            )
            .and_then(|value| service.apply_budget(value, max_words)),
        "run_review" => service.run_review(
            required_string(args, "run_id")?,
            optional_string(args, "view")?.unwrap_or("status"),
            match get("query") {
                Some(value) => Some(query_values(Some(value))?),
                None => None,
            },
            optional_string(args, "stream")?.unwrap_or("both"),
            optional_usize(args, "context_lines")?.unwrap_or(3),
            optional_usize(args, "max_matches")?.unwrap_or(5),
            optional_bool(args, "case_sensitive")?.unwrap_or(false),
            max_words,
            optional_usize(args, "max_bytes")?.unwrap_or(12_000),
        ),
        "cancel_run" => {
            let value = service.run_state(required_string(args, "run_id")?, "cancel", detailed)?;
            service.apply_budget(value, max_words)
        }
        "coverage_import" => service.coverage_import(
            required_string(args, "report_path")?,
            optional_string(args, "format")?.unwrap_or("auto"),
            optional_string(args, "suite")?.unwrap_or("default"),
            optional_string(args, "branch")?,
            optional_string(args, "commit_sha")?,
            optional_string(args, "base_ref")?,
            max_words,
            optional_usize(args, "max_bytes")?.unwrap_or(12_000),
        ),
        "coverage_review" => coverage_review_call(service, args, max_words),
        _ => Err(AppError::NotFound(format!("unknown MCP tool: {name}"))),
    }
}

fn coverage_review_call(
    service: &CoverageService,
    args: &Map<String, Value>,
    max_words: usize,
) -> AppResult<Value> {
    let task = args
        .get("task")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or(AppError::Validation(
            "task must be a non-blank string".to_owned(),
        ))?;
    if !matches!(
        task.as_str(),
        "change" | "history" | "insight" | "source" | "audit" | "all"
    ) {
        return Err(AppError::Validation(
            "coverage_review task must be change, history, insight, source, audit, or all"
                .to_owned(),
        ));
    }
    let request = validate_review_request(args, &task)?;
    let mut snapshot_id = request.measurement_snapshot;
    let run_id = request.measurement_run;
    if snapshot_id.is_none() {
        if let Some(run_id) = run_id.as_deref() {
            let run = service.store().run_result(run_id, 20)?;
            snapshot_id = run
                .get("coverage_ingest")
                .and_then(|value| value.get("snapshot_ids"))
                .and_then(Value::as_array)
                .and_then(|values| values.iter().find_map(Value::as_str))
                .map(str::to_owned);
            let _ = snapshot_id.as_ref().ok_or_else(|| {
                AppError::Validation(
                    "measurement.run_id has no ingested coverage snapshot".to_owned(),
                )
            })?;
        }
    }
    let suite = request.suite;
    let branch = request.branch;
    let file_path = request.file_path;
    let baseline_kind = request.baseline_kind;
    let mut baseline_snapshot_id = request.baseline_snapshot;
    let worktree_id = request.worktree_id;
    let baseline_ref = request.baseline_ref;
    if baseline_kind.as_deref() == Some("none") {
        baseline_snapshot_id = Some("none".to_owned());
    } else if baseline_kind.as_deref() == Some("explicit") && baseline_snapshot_id.is_none() {
        return Err(AppError::Validation(
            "baseline.kind=explicit requires baseline.snapshot_id".to_owned(),
        ));
    } else if baseline_kind.as_deref() == Some("worktree_base") && worktree_id.is_none() {
        return Err(AppError::Validation(
            "baseline.kind=worktree_base requires baseline.worktree_id".to_owned(),
        ));
    } else if matches!(baseline_kind.as_deref(), Some("parent_commit" | "ref")) {
        baseline_snapshot_id = Some(resolve_review_baseline(
            service,
            snapshot_id.as_deref(),
            baseline_kind.as_deref().unwrap_or("parent_commit"),
            baseline_ref.as_deref(),
            suite.as_deref(),
            branch.as_deref(),
        )?);
    }
    let detail_snapshots = request.detail_snapshots.unwrap_or(2);
    let summary_window = request.summary_window.unwrap_or(10);
    let max_files = request.max_files.unwrap_or(10);
    let max_regions = request.max_regions.unwrap_or(20);
    let max_source_lines = request.max_source_lines.unwrap_or(120);
    let max_words = request.max_words.unwrap_or(max_words);
    let max_bytes = request.max_bytes.unwrap_or(12_000);
    let include_source = request.include_source.unwrap_or(false);
    let context_lines = request.context_lines.unwrap_or(3);
    let representation = request.representation.unwrap_or_else(|| {
        if task == "audit" {
            "audit".to_owned()
        } else {
            "review".to_owned()
        }
    });
    if task == "source" {
        let snapshot_id = snapshot_id.ok_or_else(|| {
            AppError::Validation(
                "source review requires measurement.snapshot_id or run_id".to_owned(),
            )
        })?;
        let ranges = request
            .source_ranges
            .expect("source ranges are validated for source tasks");
        return service.source_review(&snapshot_id, ranges, max_source_lines, max_words, max_bytes);
    }
    let focus = if task == "audit" {
        "change"
    } else {
        task.as_str()
    };
    let mut response = service.coverage_review(
        focus,
        snapshot_id.as_deref(),
        baseline_snapshot_id.as_deref(),
        worktree_id.as_deref(),
        suite.as_deref(),
        branch.as_deref(),
        file_path.as_deref(),
        detail_snapshots,
        summary_window,
        max_files,
        max_regions,
        include_source,
        context_lines,
        max_source_lines,
        max_words,
        max_bytes,
        &representation,
    )?;
    response
        .get_mut("data")
        .and_then(Value::as_object_mut)
        .expect("coverage review service responses must contain an object data field")
        .insert("task".to_owned(), json!(task));
    Ok(response)
}

fn selected_value<'a>(
    args: &'a Map<String, Value>,
    group: &str,
    key: &str,
    flat_key: &str,
) -> Option<&'a Value> {
    if group.is_empty() {
        return args.get(flat_key);
    }
    args.get(group)
        .and_then(Value::as_object)
        .and_then(|object| object.get(key))
}

struct ReviewRequest {
    measurement_snapshot: Option<String>,
    measurement_run: Option<String>,
    suite: Option<String>,
    branch: Option<String>,
    file_path: Option<String>,
    baseline_kind: Option<String>,
    baseline_snapshot: Option<String>,
    worktree_id: Option<String>,
    baseline_ref: Option<String>,
    detail_snapshots: Option<usize>,
    summary_window: Option<usize>,
    max_files: Option<usize>,
    max_regions: Option<usize>,
    max_source_lines: Option<usize>,
    max_words: Option<usize>,
    max_bytes: Option<usize>,
    include_source: Option<bool>,
    context_lines: Option<usize>,
    source_ranges: Option<Vec<(String, i64, i64)>>,
    representation: Option<String>,
}

fn validate_review_request(args: &Map<String, Value>, task: &str) -> AppResult<ReviewRequest> {
    for (group, allowed) in [
        (
            "measurement",
            &["snapshot_id", "run_id", "suite", "branch", "file_path"][..],
        ),
        (
            "baseline",
            &["kind", "snapshot_id", "worktree_id", "ref"][..],
        ),
        ("source", &["ranges", "include", "context_lines"][..]),
        ("history", &["detail_snapshots", "summary_window"][..]),
        (
            "limits",
            &[
                "max_files",
                "max_regions",
                "max_source_lines",
                "max_words",
                "max_bytes",
            ][..],
        ),
    ] {
        if let Some(value) = args.get(group) {
            let Some(object) = value.as_object() else {
                return Err(AppError::Validation(format!("{group} must be an object")));
            };
            reject_unknown_keys(object, allowed, group)?;
        }
    }

    let measurement_snapshot =
        selected_optional_string(args, "measurement", "snapshot_id", "snapshot_id")?;
    let measurement_run = selected_optional_string(args, "measurement", "run_id", "run_id")?;
    if measurement_snapshot.is_some() && measurement_run.is_some() {
        return Err(AppError::Validation(
            "measurement.snapshot_id and measurement.run_id are mutually exclusive".to_owned(),
        ));
    }

    let suite = selected_optional_string(args, "measurement", "suite", "suite")?;
    let branch = selected_optional_string(args, "measurement", "branch", "branch")?;
    let file_path = selected_optional_string(args, "measurement", "file_path", "file_path")?;
    if let Some(file_path) = file_path.as_deref() {
        validate_relative_file_path(file_path, "measurement.file_path")?;
    }
    let baseline_kind = selected_optional_string(args, "baseline", "kind", "baseline_kind")?;
    let baseline_snapshot =
        selected_optional_string(args, "baseline", "snapshot_id", "baseline_snapshot_id")?;
    let worktree_id = selected_optional_string(args, "baseline", "worktree_id", "worktree_id")?;
    let baseline_ref = selected_optional_string(args, "baseline", "ref", "ref")?;

    if let Some(kind) = baseline_kind.as_deref() {
        if !matches!(
            kind,
            "worktree_base" | "parent_commit" | "ref" | "previous_snapshot" | "explicit" | "none"
        ) {
            return Err(AppError::Validation(
                "baseline.kind must be worktree_base, parent_commit, ref, previous_snapshot, explicit, or none"
                    .to_owned(),
            ));
        }
        match kind {
            "none"
                if baseline_snapshot.is_some()
                    || worktree_id.is_some()
                    || baseline_ref.is_some() =>
            {
                return Err(AppError::Validation(
                    "baseline.kind=none cannot include a snapshot_id, worktree_id, or ref"
                        .to_owned(),
                ));
            }
            "explicit" if worktree_id.is_some() || baseline_ref.is_some() => {
                return Err(AppError::Validation(
                    "baseline.kind=explicit accepts only baseline.snapshot_id".to_owned(),
                ));
            }
            "worktree_base" if baseline_snapshot.is_some() || baseline_ref.is_some() => {
                return Err(AppError::Validation(
                    "baseline.kind=worktree_base accepts only baseline.worktree_id".to_owned(),
                ));
            }
            "parent_commit"
                if baseline_snapshot.is_some()
                    || worktree_id.is_some()
                    || baseline_ref.is_some() =>
            {
                return Err(AppError::Validation(
                    "baseline.kind=parent_commit does not accept an explicit baseline selector"
                        .to_owned(),
                ));
            }
            "ref" if baseline_snapshot.is_some() || worktree_id.is_some() => {
                return Err(AppError::Validation(
                    "baseline.kind=ref accepts only baseline.ref".to_owned(),
                ));
            }
            "previous_snapshot"
                if baseline_snapshot.is_some()
                    || worktree_id.is_some()
                    || baseline_ref.is_some() =>
            {
                return Err(AppError::Validation(
                    "baseline.kind=previous_snapshot does not accept an explicit baseline selector"
                        .to_owned(),
                ));
            }
            _ => {}
        }
    }

    let detail_snapshots =
        selected_optional_usize(args, "history", "detail_snapshots", "detail_snapshots")?;
    if detail_snapshots.is_some_and(|value| !(1..=5).contains(&value)) {
        return Err(AppError::Validation(
            "history.detail_snapshots must be between 1 and 5".to_owned(),
        ));
    }
    let summary_window =
        selected_optional_usize(args, "history", "summary_window", "summary_window")?;
    if summary_window.is_some_and(|value| !(2..=50).contains(&value)) {
        return Err(AppError::Validation(
            "history.summary_window must be between 2 and 50".to_owned(),
        ));
    }

    let max_files = selected_optional_usize(args, "limits", "max_files", "max_files")?;
    let max_regions = selected_optional_usize(args, "limits", "max_regions", "max_regions")?;
    let max_source_lines =
        selected_optional_usize(args, "limits", "max_source_lines", "max_source_lines")?;
    let max_words = selected_optional_usize(args, "limits", "max_words", "max_words")?;
    let max_bytes = selected_optional_usize(args, "limits", "max_bytes", "max_bytes")?;
    for (key, value, minimum, maximum) in [
        ("max_files", max_files, 1, 50),
        ("max_regions", max_regions, 1, 100),
        ("max_source_lines", max_source_lines, 10, 500),
        ("max_words", max_words, 50, 5000),
        ("max_bytes", max_bytes, 1000, 2_000_000),
    ] {
        if value.is_some_and(|value| value < minimum || value > maximum) {
            return Err(AppError::Validation(format!(
                "limits.{key} must be between {minimum} and {maximum}"
            )));
        }
    }
    let include_source = selected_optional_bool(args, "source", "include", "include_source")?;
    let context_lines = selected_optional_usize(args, "source", "context_lines", "context_lines")?;
    if context_lines.is_some_and(|value| value > 20) {
        return Err(AppError::Validation(
            "source.context_lines must be between 0 and 20".to_owned(),
        ));
    }

    let source_ranges = args
        .get("source")
        .and_then(Value::as_object)
        .and_then(|source| source.get("ranges"));
    let source_ranges = if source_ranges.is_some() {
        Some(parse_review_source_ranges(source_ranges)?)
    } else if task == "source" {
        return Err(AppError::Validation(
            "source task requires source.ranges".to_owned(),
        ));
    } else {
        None
    };

    let representation = selected_optional_string(args, "", "representation", "representation")?;
    if let Some(representation) = representation.as_deref() {
        if !matches!(representation, "review" | "compact" | "audit") {
            return Err(AppError::Validation(
                "representation must be review, compact, or audit".to_owned(),
            ));
        }
    }
    Ok(ReviewRequest {
        measurement_snapshot,
        measurement_run,
        suite,
        branch,
        file_path,
        baseline_kind,
        baseline_snapshot,
        worktree_id,
        baseline_ref,
        detail_snapshots,
        summary_window,
        max_files,
        max_regions,
        max_source_lines,
        max_words,
        max_bytes,
        include_source,
        context_lines,
        source_ranges,
        representation,
    })
}

fn validate_relative_file_path(value: &str, field: &str) -> AppResult<()> {
    let path = Path::new(value);
    if value.trim().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AppError::Validation(format!(
            "{field} must be a repository-relative path"
        )));
    }
    Ok(())
}

fn validate_public_tool_keys(name: &str, args: &Map<String, Value>) -> AppResult<()> {
    let allowed: &[&str] = match name {
        "project_context" => &["cursor", "max_words", "detailed"],
        "register_test_command" => &[
            "name",
            "command",
            "human_approved",
            "approved_by",
            "approval_note",
            "cwd",
            "shell",
            "artifact_paths",
            "max_words",
            "detailed",
        ],
        "run_test" => &[
            "command_ref",
            "timeout_seconds",
            "idempotency_key",
            "wait",
            "reuse_if_unchanged",
            "max_words",
        ],
        "run_review" => &[
            "run_id",
            "view",
            "query",
            "stream",
            "context_lines",
            "max_matches",
            "case_sensitive",
            "max_words",
            "max_bytes",
        ],
        "cancel_run" => &["run_id", "max_words", "detailed"],
        "coverage_import" => &[
            "report_path",
            "format",
            "suite",
            "branch",
            "commit_sha",
            "base_ref",
            "max_words",
            "max_bytes",
        ],
        "coverage_review" => &[
            "task",
            "measurement",
            "baseline",
            "source",
            "history",
            "limits",
            "representation",
            "max_words",
            "max_bytes",
        ],
        _ => return Ok(()),
    };
    reject_unknown_keys(args, allowed, name)
}

fn reject_unknown_keys(
    object: &Map<String, Value>,
    allowed: &[&str],
    object_name: &str,
) -> AppResult<()> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(AppError::Validation(format!(
            "{object_name} does not accept argument {key}"
        )));
    }
    Ok(())
}

fn resolve_review_baseline(
    service: &CoverageService,
    snapshot_id: Option<&str>,
    kind: &str,
    reference: Option<&str>,
    suite: Option<&str>,
    branch: Option<&str>,
) -> AppResult<String> {
    let snapshot_id = snapshot_id.ok_or_else(|| {
        AppError::Validation(
            "parent_commit/ref baseline requires measurement.snapshot_id or run_id".to_owned(),
        )
    })?;
    let current = service.store().snapshot(snapshot_id)?;
    let repo_path = current["repo_path"]
        .as_str()
        .expect("stored snapshots always contain a repository path");
    let current_commit =
        current
            .get("commit_sha")
            .and_then(Value::as_str)
            .ok_or(AppError::Validation(
                "current snapshot has no commit_sha".to_owned(),
            ))?;
    let suite = suite
        .or_else(|| current["suite"].as_str())
        .expect("stored snapshots always contain a suite");
    let branch = branch.or_else(|| current.get("branch").and_then(Value::as_str));
    let commit = if kind == "parent_commit" {
        parent_commit(repo_path, current_commit).ok_or_else(|| {
            AppError::NotFound("could not resolve the current snapshot parent commit".to_owned())
        })?
    } else {
        let reference = reference.ok_or_else(|| {
            AppError::Validation("baseline.kind=ref requires baseline.ref".to_owned())
        })?;
        merge_base(repo_path, reference, current_commit).ok_or_else(|| {
            AppError::NotFound(format!("could not resolve merge base for ref {reference}"))
        })?
    };
    service
        .store()
        .snapshot_for_commit(repo_path, branch, suite, &commit)?
        .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "no compatible coverage snapshot exists at baseline commit {commit}"
            ))
        })
}

fn selected_optional_string(
    args: &Map<String, Value>,
    group: &str,
    key: &str,
    flat_key: &str,
) -> AppResult<Option<String>> {
    match selected_value(args, group, key, flat_key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .map(Some)
            .ok_or_else(|| {
                AppError::Validation(format!("{group}.{key} must be a non-blank string"))
            }),
    }
}

fn selected_optional_usize(
    args: &Map<String, Value>,
    group: &str,
    key: &str,
    flat_key: &str,
) -> AppResult<Option<usize>> {
    match selected_value(args, group, key, flat_key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| {
                AppError::Validation(format!("{group}.{key} must be an unsigned integer"))
            }),
    }
}

fn selected_optional_bool(
    args: &Map<String, Value>,
    group: &str,
    key: &str,
    flat_key: &str,
) -> AppResult<Option<bool>> {
    match selected_value(args, group, key, flat_key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| AppError::Validation(format!("{group}.{key} must be boolean"))),
    }
}

fn parse_review_source_ranges(value: Option<&Value>) -> AppResult<Vec<(String, i64, i64)>> {
    let Some(value) = value else {
        return Err(AppError::Validation(
            "source task requires source.ranges".to_owned(),
        ));
    };
    let values = value
        .as_array()
        .ok_or_else(|| AppError::Validation("source.ranges must be an array".to_owned()))?;
    if values.is_empty() || values.len() > 10 {
        return Err(AppError::Validation(
            "source.ranges must contain 1 through 10 ranges".to_owned(),
        ));
    }
    values
        .iter()
        .map(|range| {
            let object = range
                .as_object()
                .ok_or_else(|| AppError::Validation("source range must be an object".to_owned()))?;
            reject_unknown_keys(object, &["file_path", "start", "end"], "source range")?;
            let file_path = object
                .get("file_path")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    AppError::Validation("source range file_path is required".to_owned())
                })?;
            validate_relative_file_path(file_path, "source range file_path")?;
            let start = object
                .get("start")
                .and_then(Value::as_i64)
                .ok_or_else(|| AppError::Validation("source range start is required".to_owned()))?;
            let end = object
                .get("end")
                .and_then(Value::as_i64)
                .ok_or_else(|| AppError::Validation("source range end is required".to_owned()))?;
            if start < 1 || end < start {
                return Err(AppError::Validation(
                    "source range must have positive bounds with end >= start".to_owned(),
                ));
            }
            Ok((file_path.to_owned(), start, end))
        })
        .collect()
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
        return service.snapshot_summary(snapshot_id, DEFAULT_MAX_WORDS, false);
    }
    Err(AppError::NotFound(format!("unknown MCP resource: {uri}")))
}

fn required_string<'a>(args: &'a Map<String, Value>, key: &str) -> AppResult<&'a str> {
    optional_string(args, key)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::Validation(format!("{key} is required")))
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
fn query_values(value: Option<&Value>) -> AppResult<Vec<String>> {
    let values = match value {
        Some(Value::String(value)) => vec![value.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    AppError::Validation("query array items must be strings".to_owned())
                })
            })
            .collect::<AppResult<Vec<_>>>()?,
        _ => Err(AppError::Validation(
            "query must be a string or array of strings".to_owned(),
        ))?,
    };
    if values.is_empty() || values.len() > 20 || values.iter().any(|value| value.trim().is_empty())
    {
        return Err(AppError::Validation(
            "query must contain between 1 and 20 non-blank terms".to_owned(),
        ));
    }
    Ok(values)
}
fn tool(name: &str, description: &str, annotations: Value, input_schema: Value) -> Value {
    tool_with_output(
        name,
        description,
        annotations,
        input_schema,
        output_schema(),
    )
}
fn tool_with_output(
    name: &str,
    description: &str,
    annotations: Value,
    input_schema: Value,
    output: Value,
) -> Value {
    json!({"name":name,"description":description,"inputSchema":input_schema,"outputSchema":output,"annotations":annotations})
}
fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn json_rpc_error(id: Value, error: AppError) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":error.to_string()}})
}
fn output_schema() -> Value {
    json!({"type":"object","properties":{"context":{"type":"object","description":"Request repository, checkout, suite, and schema context."},"data":{"description":"One bounded task-level projection with measurement, lineage, claim status, reasons, and a next action when applicable."},"page":{"type":["object","null"],"description":"Per-call pagination and response-budget metadata, including returned, total, word_count, max_words, truncated, and next_cursor."}},"required":["context","data","page"]})
}
fn coverage_review_input_schema() -> Value {
    object_schema(
        &[
            (
                "task",
                enum_default_schema(
                    &["change", "history", "insight", "source", "audit", "all"],
                    "change",
                    "Review task; source returns grouped source ranges and audit returns exact records.",
                ),
            ),
            (
                "measurement",
                json!({
                    "type":"object",
                    "description":"Optional current measurement selectors.",
                    "properties":{
                        "snapshot_id":{"type":["string","null"]},
                        "run_id":{"type":["string","null"]},
                        "suite":{"type":["string","null"]},
                        "branch":{"type":["string","null"]},
                        "file_path":{"type":["string","null"]}
                    },
                    "additionalProperties":false
                }),
            ),
            (
                "baseline",
                json!({
                    "type":"object",
                    "description":"Optional compatible baseline selector.",
                    "properties":{
                        "kind":{"type":"string","enum":["worktree_base","parent_commit","ref","previous_snapshot","explicit","none"]},
                        "snapshot_id":{"type":["string","null"]},
                        "worktree_id":{"type":["string","null"]},
                        "ref":{"type":["string","null"]}
                    },
                    "additionalProperties":false
                }),
            ),
            (
                "source",
                json!({
                    "type":"object",
                    "description":"Grouped source ranges for task=source or optional change context.",
                    "properties":{
                        "ranges":{
                            "type":"array",
                            "maxItems":10,
                            "items":{
                                "type":"object",
                                "properties":{
                                    "file_path":{"type":"string"},
                                    "start":{"type":"integer","minimum":1},
                                    "end":{"type":"integer","minimum":1}
                                },
                                "required":["file_path","start","end"],
                                "additionalProperties":false
                            }
                        },
                        "include":{"type":"boolean","default":false},
                        "context_lines":{"type":"integer","minimum":0,"maximum":20,"default":3}
                    },
                    "additionalProperties":false
                }),
            ),
            (
                "history",
                json!({
                    "type":"object",
                    "description":"History detail and summary limits.",
                    "properties":{
                        "detail_snapshots":{"type":"integer","minimum":1,"maximum":5,"default":2},
                        "summary_window":{"type":"integer","minimum":2,"maximum":50,"default":10}
                    },
                    "additionalProperties":false
                }),
            ),
            (
                "limits",
                json!({
                    "type":"object",
                    "description":"Complete response word, byte, file, region, and source limits.",
                    "properties":{
                        "max_files":{"type":"integer","minimum":1,"maximum":50,"default":10},
                        "max_regions":{"type":"integer","minimum":1,"maximum":100,"default":20},
                        "max_source_lines":{"type":"integer","minimum":10,"maximum":500,"default":120},
                        "max_words":{"type":"integer","minimum":50,"maximum":5000,"default":600},
                        "max_bytes":{"type":"integer","minimum":1000,"maximum":2000000,"default":12000}
                    },
                    "additionalProperties":false
                }),
            ),
            (
                "representation",
                enum_default_schema(
                    &["review", "compact", "audit"],
                    "review",
                    "Readable grouped review, token-compressed ranges, or exact audit records.",
                ),
            ),
        ],
        &["task"],
    )
}
fn coverage_review_output_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "context":{"type":"object","description":"Request repository, checkout, suite, and schema context."},
            "data":{
                "type":"object",
                "description":"One bounded task-level review. Change, history, and insight are present according to focus.",
                "properties":{
                    "focus":{"type":"string","enum":["change","history","insight","source","audit","all"]},
                    "task":{"type":"string","enum":["change","history","insight","source","audit","all"]},
                    "representation":{"type":"string","enum":["review","compact","audit"]},
                    "claim_status":{"type":"string","enum":["supported","limited","not_measured","stale","invalid"]},
                    "measurement":{"type":["object","null"]},
                    "baseline":{"type":["object","null"]},
                    "change":{"type":"object"},
                    "next_action":{"type":"object"},
                    "source":{"type":"array"},
                    "history":{"type":"object"},
                    "insight":{"type":"object"},
                    "reasons":{"type":"array","items":{"type":"string"}}
                },
                "required":["focus","representation","claim_status","reasons","measurement","baseline"]
            },
            "page":{"type":["object","null"]}
        },
        "required":["context","data","page"]
    })
}

/// Returns the digest and dimensions of the generated public tool contract.
///
/// The digest is calculated from the exact `tools/list` tool array, excluding
/// this metadata object. Consumers can compare it with a pinned compatibility
/// record without copying the full schemas into another manual.
pub fn public_contract_metadata() -> Value {
    let tools = tools_list();
    let bytes = serde_json::to_vec(&tools).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    json!({
        "schema_revision": crate::SCHEMA_REVISION,
        "tool_count": tools.as_array().map_or(0, Vec::len),
        "tools_sha256": crate::hex_prefix(&digest, digest.len()),
    })
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
fn bounded_integer(minimum: u64, maximum: u64, default: u64, description: &str) -> Value {
    json!({
        "type":"integer",
        "minimum":minimum,
        "maximum":maximum,
        "default":default,
        "description":description
    })
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
fn enum_default_schema(values: &[&str], default: &str, description: &str) -> Value {
    json!({"type":"string","enum":values,"default":default,"description":description})
}
fn budget_schema() -> Value {
    json!({"type":"integer","minimum":50,"maximum":5000,"default":600,"description":"Per-call response word budget, 50–5000, default 600; use page.next_cursor for more collection items."})
}
fn detailed_schema() -> Value {
    json!({"type":"boolean","default":false,"description":"Keep false for normal work; true only for explicitly requested audit, raw-metric, or provenance fields."})
}
fn query_schema() -> Value {
    json!({"anyOf":[{"type":"string","minLength":1},{"type":"array","minItems":1,"maxItems":20,"items":{"type":"string","minLength":1}}],"description":"One non-blank query string or up to 20 non-blank query strings; matches use OR semantics and return bounded context windows."})
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
    use crate::config::ServerConfig;
    use crate::service::{CoverageService, RequestContext};
    use crate::storage::CoverageStore;
    use std::process::Command;

    #[test]
    fn argument_helpers_validate_shapes_and_ranges() {
        let mut args = Map::new();
        args.insert("name".to_owned(), json!("value"));
        args.insert("number".to_owned(), json!(7));
        assert_eq!(required_string(&args, "name").unwrap(), "value");
        assert!(required_string(&args, "missing").is_err());
        args.insert("name".to_owned(), json!(" "));
        assert!(required_string(&args, "name").is_err());
        args.insert("name".to_owned(), json!(7));
        assert!(optional_string(&args, "name").is_err());
        args.insert("number".to_owned(), json!("7"));

        assert_eq!(query_values(Some(&json!("one"))).unwrap(), vec!["one"]);
        assert!(query_values(Some(&json!(["one", 2, "two"]))).is_err());
        assert!(query_values(None).is_err());
        assert!(query_values(Some(&json!(true))).is_err());

        assert!(optional_bool(&args, "number").is_err());
        assert!(optional_usize(&args, "number").is_err());
        args.insert("number".to_owned(), json!(7));
        assert_eq!(optional_usize(&args, "number").unwrap(), Some(7));
    }

    #[test]
    fn coverage_review_validation_rejects_ambiguous_nested_inputs() {
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({"measurement":"snapshot"})).unwrap(),
                "change"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "measurement":{"unexpected":true}
                }))
                .unwrap(),
                "change"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({"limits":{"max_words":"600"}})).unwrap(),
                "change"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "measurement":{"snapshot_id":"current","run_id":"run"}
                }))
                .unwrap(),
                "change"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "baseline":{"kind":"none","snapshot_id":"baseline"}
                }))
                .unwrap(),
                "change"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({"task":"source"})).unwrap(),
                "source"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "task":"source",
                    "source":{"ranges":[{"file_path":"src/lib.rs","start":1,"end":2,"unexpected":true}]}
                }))
                .unwrap(),
                "source"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "task":"source",
                    "source":{"ranges":[{"file_path":"../outside.rs","start":1,"end":2}]}
                }))
                .unwrap(),
                "source"
            )
            .is_err()
        );
        assert!(
            validate_review_request(
                &serde_json::from_value(json!({
                    "task":"source",
                    "source":{"ranges":[{"file_path":"src/lib.rs","start":4,"end":3}]}
                }))
                .unwrap(),
                "source"
            )
            .is_err()
        );
        assert_eq!(query_values(Some(&json!("term"))).unwrap(), vec!["term"]);
        assert!(query_values(Some(&json!(" "))).is_err());
        assert!(query_values(Some(&json!([]))).is_err());
        assert!(query_values(Some(&json!((0..21).map(|_| "term").collect::<Vec<_>>()))).is_err());

        for arguments in [
            json!({"measurement":{"snapshot_id":7}}),
            json!({"measurement":{"run_id":7}}),
            json!({"measurement":{"suite":7}}),
            json!({"measurement":{"branch":7}}),
            json!({"measurement":{"file_path":7}}),
            json!({"baseline":{"kind":7}}),
            json!({"baseline":{"snapshot_id":7}}),
            json!({"baseline":{"worktree_id":7}}),
            json!({"baseline":{"ref":7}}),
            json!({"history":{"detail_snapshots":0}}),
            json!({"history":{"detail_snapshots":6}}),
            json!({"history":{"summary_window":1}}),
            json!({"history":{"summary_window":51}}),
            json!({"limits":{"max_files":0}}),
            json!({"limits":{"max_files":51}}),
            json!({"limits":{"max_regions":"bad"}}),
            json!({"limits":{"max_source_lines":"bad"}}),
            json!({"limits":{"max_words":"bad"}}),
            json!({"limits":{"max_bytes":"bad"}}),
            json!({"limits":{"max_regions":0}}),
            json!({"limits":{"max_regions":101}}),
            json!({"limits":{"max_source_lines":9}}),
            json!({"limits":{"max_source_lines":501}}),
            json!({"limits":{"max_words":49}}),
            json!({"limits":{"max_words":5001}}),
            json!({"limits":{"max_bytes":999}}),
            json!({"limits":{"max_bytes":2_000_001}}),
            json!({"source":{"include":7}}),
            json!({"source":{"context_lines":21}}),
            json!({"representation":"invalid"}),
        ] {
            let arguments = serde_json::from_value(arguments).unwrap();
            assert!(validate_review_request(&arguments, "change").is_err());
        }
        for arguments in [
            json!({"baseline":{"kind":"invalid"}}),
            json!({"baseline":{"kind":"none","worktree_id":"w"}}),
            json!({"baseline":{"kind":"explicit","worktree_id":"w"}}),
            json!({"baseline":{"kind":"explicit","ref":"main"}}),
            json!({"baseline":{"kind":"worktree_base","snapshot_id":"s"}}),
            json!({"baseline":{"kind":"worktree_base","ref":"main"}}),
            json!({"baseline":{"kind":"parent_commit","snapshot_id":"s"}}),
            json!({"baseline":{"kind":"parent_commit","worktree_id":"w"}}),
            json!({"baseline":{"kind":"parent_commit","ref":"main"}}),
            json!({"baseline":{"kind":"ref","snapshot_id":"s"}}),
            json!({"baseline":{"kind":"ref","worktree_id":"w"}}),
            json!({"baseline":{"kind":"previous_snapshot","snapshot_id":"s"}}),
            json!({"baseline":{"kind":"previous_snapshot","worktree_id":"w"}}),
            json!({"baseline":{"kind":"previous_snapshot","ref":"main"}}),
        ] {
            let arguments = serde_json::from_value(arguments).unwrap();
            assert!(validate_review_request(&arguments, "change").is_err());
        }
        for value in [
            json!(null),
            json!("not-an-array"),
            json!([]),
            json!(
                (0..11)
                    .map(|_| json!({"file_path":"src/lib.rs","start":1,"end":1}))
                    .collect::<Vec<_>>()
            ),
            json!(["not-an-object"]),
            json!([{"file_path":""}]),
            json!([{"file_path":"src/lib.rs"}]),
            json!([{"file_path":"src/lib.rs","start":1}]),
            json!([{"file_path":"src/lib.rs","end":1}]),
            json!([{"file_path":"src/lib.rs","start":0,"end":1}]),
        ] {
            assert!(parse_review_source_ranges(Some(&value)).is_err());
        }
        assert!(parse_review_source_ranges(None).is_err());
        assert!(
            selected_optional_bool(
                &serde_json::from_value(json!({"source":{"include":"yes"}})).unwrap(),
                "source",
                "include",
                "include_source"
            )
            .is_err()
        );
    }

    #[test]
    fn coverage_review_call_dispatch_covers_consolidated_public_paths() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("src")).unwrap();
        std::fs::write(directory.path().join("src/a.py"), "one\ntwo\n").unwrap();
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.email", "rust@example.com"],
            vec!["config", "user.name", "Rust Tests"],
            vec!["add", "."],
            vec!["commit", "-m", "base"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let report = directory.path().join("coverage.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,0\nDA:2,1\nend_of_record\n").unwrap();
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
        let store = CoverageStore::open(directory.path().join("coverage.duckdb"), config).unwrap();
        let project = store.ensure_project(directory.path()).unwrap();
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
        let service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );

        assert!(call_tool(&service, "project_context", &Value::Null).is_err());
        assert!(call_tool(&service, "project_context", &json!({"detailed":"yes"})).is_err());
        assert!(call_tool(&service, "not-a-tool", &json!({})).is_err());
        assert!(
            call_tool(
                &service,
                "register_test_command",
                &json!({
                    "command":"true",
                    "approved_by":"test",
                    "approval_note":"missing name"
                }),
            )
            .is_err()
        );
        for arguments in [
            json!({"unexpected":true}),
            json!({"name":"name"}),
            json!({"name":"name","command":"true","human_approved":"yes","approved_by":"test","approval_note":"note"}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":"note","max_words":"600"}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":7,"approval_note":"note"}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":7}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":"note","cwd":7}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":"note","shell":7}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":"note","artifact_paths":7}),
            json!({"name":"name","command":"true","human_approved":true,"approved_by":"test","approval_note":"note","detailed":"yes"}),
        ] {
            assert!(call_tool(&service, "register_test_command", &arguments).is_err());
        }
        for arguments in [
            json!({}),
            json!({"command_ref":7}),
            json!({"command_ref":"missing","timeout_seconds":"1"}),
            json!({"command_ref":"missing","idempotency_key":7}),
            json!({"command_ref":"missing","wait":"yes"}),
            json!({"command_ref":"missing","reuse_if_unchanged":"yes"}),
        ] {
            assert!(call_tool(&service, "run_test", &arguments).is_err());
        }
        for arguments in [
            json!({}),
            json!({"run_id":7}),
            json!({"run_id":"missing","view":7}),
            json!({"run_id":"missing","query":7}),
            json!({"run_id":"missing","stream":7}),
            json!({"run_id":"missing","context_lines":"3"}),
            json!({"run_id":"missing","max_matches":"3"}),
            json!({"run_id":"missing","case_sensitive":"yes"}),
            json!({"run_id":"missing","max_words":"600"}),
            json!({"run_id":"missing","max_bytes":"12000"}),
        ] {
            assert!(call_tool(&service, "run_review", &arguments).is_err());
        }
        assert!(call_tool(&service, "cancel_run", &json!({})).is_err());
        assert!(call_tool(&service, "cancel_run", &json!({"run_id":7})).is_err());
        assert!(call_tool(&service, "cancel_run", &json!({"run_id":"missing"})).is_err());
        for arguments in [
            json!({}),
            json!({"report_path":7}),
            json!({"report_path":"missing","format":7}),
            json!({"report_path":"missing","suite":7}),
            json!({"report_path":"missing","branch":7}),
            json!({"report_path":"missing","commit_sha":7}),
            json!({"report_path":"missing","base_ref":7}),
            json!({"report_path":"missing","max_bytes":"12000"}),
        ] {
            assert!(call_tool(&service, "coverage_import", &arguments).is_err());
        }
        for arguments in [
            json!({"task":7}),
            json!({"task":""}),
            json!({"task":"source","source":{"ranges":[{"file_path":"src/a.py","start":1,"end":1}]}}),
            json!({"task":"change","measurement":{"snapshot_id":7}}),
            json!({"task":"change","measurement":{"run_id":7}}),
            json!({"task":"change","measurement":{"suite":7}}),
            json!({"task":"change","measurement":{"branch":7}}),
            json!({"task":"change","measurement":{"file_path":7}}),
            json!({"task":"change","baseline":{"kind":7}}),
            json!({"task":"change","history":{"detail_snapshots":"2"}}),
            json!({"task":"change","history":{"summary_window":"10"}}),
            json!({"task":"change","limits":{"max_files":"10"}}),
            json!({"task":"change","source":{"include":"yes"}}),
            json!({"task":"change","source":{"context_lines":"3"}}),
            json!({"task":"change","representation":7}),
        ] {
            assert!(call_tool(&service, "coverage_review", &arguments).is_err());
        }
        for arguments in [
            json!({"task":"history","measurement":{"snapshot_id":snapshot_id},"history":{"detail_snapshots":1,"summary_window":2},"limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"source","measurement":{"snapshot_id":snapshot_id},"source":{"include":true,"context_lines":0,"ranges":[{"file_path":"src/a.py","start":1,"end":1}]},"limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"},"representation":"compact","limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"},"representation":"audit","limits":{"max_words":600,"max_bytes":12000}}),
        ] {
            assert!(call_tool(&service, "coverage_review", &arguments).is_ok());
        }
        assert!(call_tool(&service, "project_context", &json!({"max_words":600})).is_ok());
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"}}),
            )
            .is_ok()
        );
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"}}),
        ).is_ok());
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"},"limits":{"max_words":50,"max_bytes":12000}}),
        )
        .is_err());
        assert!(call_tool(&service, "coverage_review", &json!({"task":"invalid"})).is_err());
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"change","measurement":{"file_path":"../outside.rs"}}),
            )
            .is_err()
        );
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"change","measurement":{"run_id":"missing"}}),
            )
            .is_err()
        );
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"source","measurement":{"snapshot_id":snapshot_id}}),
            )
            .is_err()
        );
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"source","source":{"ranges":[{"file_path":"src/a.py","start":1,"end":1}]}}),
        )
        .is_err());
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"worktree_base"}}),
        )
        .is_err());
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"ref"}}),
        )
        .is_err());
        for arguments in [
            json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"none"}}),
            json!({"task":"history","measurement":{"snapshot_id":snapshot_id},"limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"insight","measurement":{"snapshot_id":snapshot_id},"limits":{"max_regions":5,"max_words":600,"max_bytes":12000}}),
            json!({"task":"source","measurement":{"snapshot_id":snapshot_id},"source":{"ranges":[{"file_path":"src/a.py","start":1,"end":1}]},"limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"audit","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"explicit","snapshot_id":snapshot_id},"limits":{"max_words":600,"max_bytes":12000}}),
            json!({"task":"all","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"explicit","snapshot_id":snapshot_id},"limits":{"max_words":1200,"max_bytes":20000}}),
        ] {
            assert!(call_tool(&service, "coverage_review", &arguments).is_ok());
        }
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"change","measurement":{"run_id":"missing"}}),
            )
            .is_err()
        );
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"explicit"}}),
        )
        .is_err());
        assert!(call_tool(
            &service,
            "coverage_review",
            &json!({"task":"change","measurement":{"snapshot_id":snapshot_id},"baseline":{"kind":"parent_commit"}}),
        )
        .is_err());
        let command = call_tool(
            &service,
            "register_test_command",
            &json!({
                "name":"coverage-review-command",
                "command":"printf coverage-review",
                "human_approved":true,
                "approved_by":"test",
                "approval_note":"coverage review dispatcher test",
                "cwd":directory.path(),
                "shell":"/bin/sh"
            }),
        )
        .unwrap();
        assert!(
            call_tool(
                &service,
                "run_test",
                &json!({"command_ref":command["data"]["id"],"wait":false}),
            )
            .is_ok()
        );
        let run = call_tool(
            &service,
            "run_test",
            &json!({"command_ref":command["data"]["id"],"wait":true,"idempotency_key":"coverage-review-dispatch"}),
        )
        .unwrap();
        let run_id = run["data"]["id"].as_str().unwrap();
        service.store().inject_query_fault();
        assert!(call_tool(&service, "cancel_run", &json!({"run_id":run_id}),).is_err());
        assert!(
            call_tool(
                &service,
                "cancel_run",
                &json!({"run_id":run_id,"max_words":49}),
            )
            .is_err()
        );
        let cancellable_command = call_tool(
            &service,
            "register_test_command",
            &json!({
                "name":"current-cancellable-command",
                "command":"sleep 30",
                "human_approved":true,
                "approved_by":"test",
                "approval_note":"cancel path",
                "cwd":directory.path(),
                "shell":"/bin/sh"
            }),
        )
        .unwrap();
        let cancellable_run = call_tool(
            &service,
            "run_test",
            &json!({"command_ref":cancellable_command["data"]["id"],"wait":false}),
        )
        .unwrap();
        assert!(
            call_tool(
                &service,
                "cancel_run",
                &json!({"run_id":cancellable_run["data"]["id"]}),
            )
            .is_ok()
        );
        assert!(
            call_tool(
                &service,
                "coverage_review",
                &json!({"task":"change","measurement":{"run_id":run_id}}),
            )
            .is_err()
        );
        assert!(
            call_tool(
                &service,
                "run_review",
                &json!({"run_id":run_id,"view":"status"})
            )
            .is_ok()
        );
        assert!(
            call_tool(
                &service,
                "run_review",
                &json!({"run_id":run_id,"view":"logs","query":"coverage-review","max_bytes":12000}),
            )
            .is_ok()
        );
        assert!(call_tool(&service, "coverage_import", &json!({"report_path":"coverage.lcov","format":"lcov","suite":"imported","max_bytes":12000})).is_ok());
        assert!(call_tool(&service, "coverage_import", &json!({"report_path":"coverage.lcov","format":"lcov","suite":"imported","max_bytes":999})).is_err());
        assert!(call_tool(&service, "cancel_run", &json!({"run_id":run_id})).is_err());

        let git_commit = |reference: &str| {
            String::from_utf8(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(["rev-parse", reference])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap()
            .trim()
            .to_owned()
        };
        let base_commit = git_commit("HEAD");
        let baseline_with_commit = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&base_commit),
                None,
                "unit",
            )
            .unwrap();
        std::fs::write(directory.path().join("src/a.py"), "one\ntwo\nthree\n").unwrap();
        for args in [vec!["add", "."], vec!["commit", "-m", "change"]] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let current_commit = git_commit("HEAD");
        let current_with_commit = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&current_commit),
                None,
                "unit",
            )
            .unwrap();
        for baseline in [
            json!({"kind":"parent_commit"}),
            json!({"kind":"ref","ref":"HEAD~1"}),
        ] {
            let result = call_tool(
                &service,
                "coverage_review",
                &json!({
                    "task":"change",
                    "measurement":{"snapshot_id":current_with_commit["id"]},
                    "baseline":baseline,
                    "limits":{"max_words":600,"max_bytes":12000}
                }),
            );
            assert!(result.is_ok());
        }
        assert!(
            resolve_review_baseline(&service, None, "parent_commit", None, None, None).is_err()
        );
        assert!(
            resolve_review_baseline(
                &service,
                Some("missing-snapshot"),
                "parent_commit",
                None,
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        assert!(
            resolve_review_baseline(
                &service,
                Some(current_with_commit["id"].as_str().unwrap()),
                "ref",
                None,
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        service.store().inject_query_fault_after(1);
        assert!(
            resolve_review_baseline(
                &service,
                Some(current_with_commit["id"].as_str().unwrap()),
                "ref",
                Some("HEAD"),
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        assert!(
            resolve_review_baseline(
                &service,
                Some(current_with_commit["id"].as_str().unwrap()),
                "ref",
                Some("missing-ref"),
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        assert!(
            resolve_review_baseline(
                &service,
                Some(snapshot_id),
                "parent_commit",
                None,
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        assert!(
            resolve_review_baseline(
                &service,
                Some(current_with_commit["id"].as_str().unwrap()),
                "parent_commit",
                None,
                Some("missing-suite"),
                Some("main"),
            )
            .is_err()
        );
        store
            .clear_snapshot_commit_for_test(current_with_commit["id"].as_str().unwrap())
            .unwrap();
        assert!(
            resolve_review_baseline(
                &service,
                Some(current_with_commit["id"].as_str().unwrap()),
                "parent_commit",
                None,
                Some("unit"),
                Some("main"),
            )
            .is_err()
        );
        assert_eq!(baseline_with_commit["commit_sha"], base_commit);
        store.close().unwrap();
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
        assert_eq!(initialize["serverInfo"]["version"], VERSION);
        assert!(instructions.starts_with(&format!("Coverage MCP {VERSION} schema 9")));
        assert!(instructions.contains("multiple calls"));
        assert!(instructions.contains("task=source"));
        assert!(instructions.contains("changed_code uses + for covered"));
        assert!(instructions.contains("compact regions use + for improved/new"));
        let tools = tools_list();
        assert_eq!(tools.as_array().unwrap().len(), 7);
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
                .contains("latest_run.id is the explicit run_id")
        );
        let run_test = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "run_test")
            .unwrap();
        assert_eq!(
            run_test["inputSchema"]["properties"]["reuse_if_unchanged"]["default"],
            true
        );
        let coverage_review = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "coverage_review")
            .unwrap();
        assert!(
            tools
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "run_review")
        );
        assert!(
            tools
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "coverage_import")
        );
        assert_eq!(
            coverage_review["outputSchema"]["properties"]["data"]["type"],
            "object"
        );
        assert!(
            coverage_review["description"]
                .as_str()
                .unwrap()
                .contains("task=change")
        );
        let run_review = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "run_review")
            .unwrap();
        assert_eq!(
            run_review["inputSchema"]["properties"]["query"]["anyOf"][1]["maxItems"],
            20
        );
        let coverage_import = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "coverage_import")
            .unwrap();
        assert_eq!(
            coverage_import["inputSchema"]["required"],
            json!(["report_path"])
        );
        assert_eq!(
            coverage_review["inputSchema"]["additionalProperties"],
            false
        );
        assert_eq!(
            coverage_review["inputSchema"]["properties"]["measurement"]["additionalProperties"],
            false
        );
        assert_eq!(
            coverage_review["inputSchema"]["properties"]["source"]["properties"]["ranges"]["items"]
                ["additionalProperties"],
            false
        );
        assert_eq!(coverage_review["inputSchema"]["required"], json!(["task"]));
        assert_eq!(run_review["inputSchema"]["additionalProperties"], false);
    }

    #[test]
    fn json_rpc_dispatch_keeps_inventory_and_notifications_transport_neutral() {
        let initialize =
            dispatch_json_rpc(None, &json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
                .unwrap();
        assert_eq!(initialize["result"]["serverInfo"]["name"], "coverage-mcp");

        let tools = dispatch_json_rpc(None, &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .unwrap();
        assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 7);
        assert_eq!(
            tools["result"]["contract"]["tools_sha256"],
            public_contract_metadata()["tools_sha256"]
        );

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
