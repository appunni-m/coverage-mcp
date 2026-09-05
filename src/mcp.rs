//! Two source-oriented coverage queries, shared by HTTP and stdio.
use crate::error::{AppError, AppResult};
use crate::service::CoverageService;
use crate::{VERSION, query};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Self-contained agent workflow; code search and test execution stay local.
pub const MCP_INSTRUCTIONS: &str = "Coverage MCP finds gaps and compares measurements. Call coverage_gaps with a repository-relative report path or saved snapshot ID; omit measurement only when a unique suite has saved history. Filter by a literal path/function query, optionally line, then use normal rg and bounded file reads to investigate the returned locations. Run the repository's existing test command and call coverage_compare with current and baseline report paths. No command registration, source paging, or run polling is needed. Default metric is regions when supplied, otherwise lines; branch detail is opt-in. Five function groups are returned by default, with up to three locations each; filter before paging. Full report totals remain separate from grouped observations. Source/build receipts bind reports to source hashes; absent receipts produce limited coordinate comparisons, never verified regressions. For new tests against a saved baseline use scope incremental: run only the new cases with the normal runner, then compare their report. Optional previous reports accumulate accepted batches without double counting. Incremental results show marginal gain, overlap, combined coverage and remaining baseline gaps; they never establish full-suite regressions. Keep source, instrumentation and baseline inventory fixed; changed or incomplete evidence cannot be combined. Selected-test absences are not regressions. Test results are unknown unless reported by a receipt. A source mismatch requires matching source or remeasurement. Keep zero-gain cases with regression value; coverage alone cannot prove tests interchangeable. Successful results are in structuredContent, with explicit status, scope, limitations, and omitted counts. Exact errors are actionable; fix an invalid selector instead of repeatedly increasing budgets.";

/// MCP initialization response.
pub fn initialize_result() -> Value {
    json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"coverage-mcp","version":VERSION},"instructions":MCP_INSTRUCTIONS})
}

/// Public query inventory. Removed tools are not accepted as hidden aliases.
pub fn tools_list() -> Value {
    let measurement = json!({"type":"string","minLength":1,"description":"Repository-relative coverage report path or immutable snapshot ID. Reports are read without importing or executing tests."});
    let common = json!({
        "query":{"type":"string","minLength":1,"maxLength":256,"description":"Literal, case-insensitive substring of a path or function; use normal rg to inspect code and tests."},
        "metric":{"type":"string","enum":["auto","regions","branches","lines"],"default":"auto","description":"Primary metric. Auto uses reported regions, otherwise lines. Unavailable exact detail is reported explicitly."},
        "limit":{"type":"integer","minimum":1,"maximum":20,"default":5,"description":"Maximum returned function groups; each contains at most three locations."}
    });
    let mut find = common.clone();
    find["measurement"] = measurement.clone();
    find["suite"] = json!({"type":"string","description":"Select a saved suite when measurement is omitted; ambiguous latest selections are errors."});
    find["line"] = json!({"type":"integer","minimum":1,"description":"Restrict to observations spanning this one-based source line."});
    find["offset"] = json!({"type":"integer","minimum":0,"default":0,"description":"Continue at next_offset using the same immutable measurement and filters."});
    let mut compare = common;
    compare["measurement"] = measurement.clone();
    compare["baseline"] = measurement.clone();
    compare["previous"] = json!({"type":"array","items":measurement.clone(),"maxItems":16,"default":[],"description":"Earlier accepted batch reports or snapshot IDs; incremental scope only. Keep each report in a separate file. Repeated hits count once."});
    compare["scope"] = json!({"type":"string","enum":["selected_tests","full","incremental"],"default":"selected_tests","description":"incremental unions a new batch with the baseline and previous batches, preserving the baseline denominator; totals ignore display filters. full regression claims require matching full-scope receipts. selected_tests compares two selections without combining."});
    json!([
        tool(
            "coverage_gaps",
            "Find missing coverage grouped by function and source location. Use query and line to narrow a target, then inspect local source with rg. Defaults are five groups and the provider's primary metric. Full totals, source state, omitted counts, and next_offset prevent false completeness claims. Invalid paths, unknown snapshots, ambiguous suites, and malformed reports return errors; missing metric detail returns detail_unavailable.",
            find,
            json!([])
        ),
        tool(
            "coverage_compare",
            "Compare two explicit measurements, or use scope incremental to measure only new tests against a fixed baseline. Incremental returns newly covered and already covered counts, percentage-point gain, combined totals and remaining gaps after union; previous accepts up to 16 earlier batches. Filters and limit affect groups only in incremental mode. Unknown test status yields a limited estimate; failed tests or incompatible inventory prevent a union. Other scopes return newly observed, absent, added and removed observations. Selected-test absences are not regressions; unverified source/build identity yields limited coordinate evidence. Matching full receipts allow regression claims. Reports with incompatible formats or unavailable detail return incomparable. Run tests with the existing repository command before comparing; this query never executes or imports them.",
            compare,
            json!(["measurement", "baseline"])
        )
    ])
}

fn tool(name: &str, description: &str, properties: Value, required: Value) -> Value {
    json!({"name":name,"description":description,"inputSchema":{"type":"object","additionalProperties":false,"properties":properties,"required":required},"outputSchema":{"type":"object","properties":{"context":{"type":"object"},"data":{"type":"object","description":"Outcome-first evidence with measurement identity, source state, status, grouped locations and explicit limitations."},"page":{"type":["object","null"]}},"required":["context","data","page"]},"annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}})
}

/// No source-reading or administrative resources are exposed.
pub fn resources_list() -> Value {
    json!([])
}
/// No parameterized resources are required by the query workflow.
pub fn resource_templates_list() -> Value {
    json!([])
}
/// Executes one query with strict typed inputs.
pub fn call_tool(service: &CoverageService, name: &str, args: &Value) -> AppResult<Value> {
    query::call_tool(service, name, args)
}

const STRUCTURED_CONTENT_HINT: &str = "See structuredContent for coverage evidence.";
fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn json_rpc_error(id: Value, error: AppError) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":error.to_string()}})
}
/// Dispatches the identical JSON-RPC protocol for HTTP and stdio.
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
        "resources/read" => json_rpc_error(
            id,
            AppError::NotFound(
                "resources removed; use coverage_gaps or coverage_compare".to_owned(),
            ),
        ),
        "ping" => json_rpc_result(id, json!({})),
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
                    json!({"structuredContent":value,"content":[{"type":"text","text":STRUCTURED_CONTENT_HINT}],"isError":false}),
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

/// Returns the deterministic schema revision and tool-list digest.
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
