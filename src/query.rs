//! Small, source-oriented coverage answers. Code search and execution belong
//! to the repository's normal tools; this module only reads measurements.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::models::{CoverageReport, normalize_report_path};
use crate::parser::{parse_coverage_bytes, read_report_bytes};
use crate::service::{CoverageService, RequestContext};
use crate::storage::CoverageStore;

fn default_limit() -> usize {
    5
}

/// Search inputs shared by MCP, REST, and the command line.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FindRequest {
    /// Repository-relative report path or immutable snapshot ID; absent selects a suite's latest snapshot.
    pub measurement: Option<String>,
    /// Suite required when latest selection would be ambiguous.
    pub suite: Option<String>,
    /// Literal, case-insensitive path or function substring.
    pub query: Option<String>,
    /// Optional one-based source line to inspect.
    pub line: Option<i64>,
    /// Primary metric: auto, regions, branches, or lines.
    pub metric: Option<String>,
    /// Number of groups to return, 1–20.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Offset into the stable filtered ordering.
    #[serde(default)]
    pub offset: usize,
}

/// Comparison inputs. Scope defaults conservatively to selected tests.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompareRequest {
    /// Current report path or snapshot ID.
    pub measurement: String,
    /// Explicit baseline report path or snapshot ID.
    pub baseline: String,
    /// Literal path or function filter.
    pub query: Option<String>,
    /// Primary metric: auto, regions, branches, or lines.
    pub metric: Option<String>,
    /// selected_tests (default), full, or incremental (baseline union plus marginal batch gain).
    pub scope: Option<String>,
    /// Earlier accepted report paths or snapshot IDs, up to 16; incremental scope only.
    #[serde(default)]
    pub previous: Vec<String>,
    /// Number of changed groups to return, 1–20.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

struct Reader<'a> {
    context: RequestContext,
    store: Option<&'a CoverageStore>,
}

impl Reader<'_> {
    fn context(&self, suite: Option<&str>) -> RequestContext {
        let mut context = self.context.clone();
        if let Some(suite) = suite {
            context.suite = Some(suite.to_owned());
        }
        context
    }
    fn store(&self) -> AppResult<&CoverageStore> {
        self.store.ok_or_else(|| {
            invalid(
                "saved measurements require the server; use a report path for a standalone query",
            )
        })
    }
    fn envelope(&self, data: Value, suite: Option<&str>, _page: Option<Value>) -> Value {
        let context = self.context(suite);
        json!({"context":{"repo_key":context.repo_key,"checkout_path":context.checkout_path,"suite":context.suite,"schema_revision":crate::SCHEMA_REVISION},"data":data,"page":null})
    }
}

/// Reads report files without a server, database, import, or test execution.
///
/// `name` is `coverage_gaps` or `coverage_compare`; `arguments` is a JSON object
/// matching [`FindRequest`] or [`CompareRequest`]. Unknown fields are rejected.
/// Report paths resolve inside `repository`. Saved IDs and latest-history
/// selection require [`call_tool`] with a service instead.
///
/// This blocking call reads complete reports (at most 64 MiB each), optional
/// producer receipts, and repository identity. It returns a JSON envelope with
/// `context`, `data`, and `page`, capped at 12 KiB. Display limits apply after
/// computing evidence; they never change report totals. An evidence status such
/// as `limited` or `tests_failed` is an `Ok` result, not a query error.
///
/// # Errors
///
/// Returns an error for unknown tools, malformed arguments or reports, invalid
/// receipts, paths outside the repository, missing files, I/O failures, or an
/// envelope whose fixed metadata cannot fit the response cap.
///
/// # Example
///
/// ```
/// use coverage_mcp::query::read_reports;
/// use serde_json::json;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let directory = tempfile::tempdir()?;
/// std::fs::write(directory.path().join("report.lcov"),
///     "TN:\nSF:example.rs\nDA:1,1\nDA:2,0\nend_of_record\n")?;
/// let result = read_reports(directory.path(), "coverage_gaps",
///     &json!({"measurement":"report.lcov"}))?;
/// assert_eq!(result["data"]["groups"][0]["locations"][0]["span"][0], 2);
/// assert_eq!(result["data"]["measurement"]["tests"], "unknown");
/// # Ok(())
/// # }
/// ```
pub fn read_reports(repository: &Path, name: &str, arguments: &Value) -> AppResult<Value> {
    let git = crate::git::inspect_git(repository)?;
    let reader = Reader {
        context: RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
        store: None,
    };
    call(&reader, name, arguments)
}

/// Reads reports or saved history using the service's selected repository.
///
/// The tool names, arguments, bounded envelope, and blocking behavior match
/// [`read_reports`]. This entry point additionally accepts immutable ordinary or
/// composite snapshot IDs and unambiguous latest-suite selection. Compacted
/// ordinary detail is restored before display limits are applied.
///
/// # Errors
///
/// Returns the query errors described by [`read_reports`], or a storage error,
/// unknown/foreign snapshot, or ambiguous suite selection. It never runs tests
/// or imports a report.
pub fn call_tool(service: &CoverageService, name: &str, arguments: &Value) -> AppResult<Value> {
    call(
        &Reader {
            context: service.context(None),
            store: Some(service.store()),
        },
        name,
        arguments,
    )
}

fn call(reader: &Reader<'_>, name: &str, arguments: &Value) -> AppResult<Value> {
    let result = match name {
        "coverage_gaps" => find(
            reader,
            &serde_json::from_value(arguments.clone())
                .map_err(|error| invalid(error.to_string()))?,
        ),
        "coverage_compare" => compare(
            reader,
            &serde_json::from_value(arguments.clone())
                .map_err(|error| invalid(error.to_string()))?,
        ),
        _ => Err(AppError::NotFound(format!(
            "unknown coverage tool: {name}; use coverage_gaps or coverage_compare"
        ))),
    }?;
    bounded(result)
}

fn bounded(mut envelope: Value) -> AppResult<Value> {
    while envelope.to_string().len() > 12_288 {
        let data = &mut envelope["data"];
        let largest = ["groups", "gained", "remaining"]
            .into_iter()
            .filter(|key| {
                data[key]
                    .as_array()
                    .is_some_and(|groups| !groups.is_empty())
            })
            .max_by_key(|key| data[key].to_string().len());
        let Some(key) = largest else {
            return Err(invalid(
                "measurement metadata exceeds the 12 KiB response limit; use shorter report paths",
            ));
        };
        // The selector above admits only nonempty arrays; no mutation precedes this read.
        #[allow(clippy::expect_used)]
        data[key]
            .as_array_mut()
            .expect("selected nonempty group array")
            .pop();
        if key == "groups" {
            if data[key].as_array().is_some_and(Vec::is_empty) {
                return Err(invalid(
                    "a single coverage group exceeds the response limit; inspect the original artifact",
                ));
            }
            data["next_offset"] = json!(
                data["next_offset"]
                    .as_u64()
                    .unwrap_or_else(|| data["group_count"].as_u64().unwrap_or(0))
                    .saturating_sub(1)
            );
        }
    }
    Ok(envelope)
}

struct Measurement {
    reference: String,
    report: CoverageReport,
    context: Value,
    source_state: &'static str,
    native: Option<BTreeMap<String, Observation>>,
}

#[derive(Clone)]
struct Observation {
    path: String,
    symbol: String,
    span: [i64; 4],
    kind: &'static str,
    arm: &'static str,
    hits: i64,
    identity: Option<String>,
    cause: Option<String>,
}

impl Observation {
    fn key(&self) -> String {
        if let Some(identity) = &self.identity {
            return identity.clone();
        }
        format!(
            "{}:{}:{}:{:?}:{}",
            self.kind, self.path, self.symbol, self.span, self.arm
        )
    }

    fn matches(&self, query: Option<&str>, line: Option<i64>) -> bool {
        let text = format!("{} {}", self.path, self.symbol).to_lowercase();
        query.is_none_or(|query| text.contains(&query.to_lowercase()))
            && line.is_none_or(|line| self.span[0] <= line && line <= self.span[2])
    }
}

fn invalid(message: impl Into<String>) -> AppError {
    AppError::Validation(message.into())
}

fn validate(limit: usize, query: Option<&str>) -> AppResult<()> {
    if !(1..=20).contains(&limit) {
        return Err(invalid("limit must be between 1 and 20"));
    }
    if query.is_some_and(|query| query.is_empty() || query.len() > 256) {
        return Err(invalid("query must contain 1–256 bytes"));
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    crate::hex_prefix(&hash, hash.len())
}

fn report_path(root: &Path, path: &str) -> AppResult<PathBuf> {
    let root = root.canonicalize()?;
    let path = root.join(path).canonicalize()?;
    if !path.starts_with(&root) {
        return Err(invalid(
            "measurement must be inside the selected repository",
        ));
    }
    Ok(path)
}

fn load(
    service: &Reader<'_>,
    reference: Option<&str>,
    suite: Option<&str>,
) -> AppResult<Measurement> {
    let context = service.context(suite);
    let selected = match reference {
        Some(reference) if !reference.trim().is_empty() && reference.len() <= 4096 => {
            reference.to_owned()
        }
        Some(_) => return Err(invalid("measurement must contain 1–4096 bytes")),
        None => {
            let snapshots =
                service
                    .store()?
                    .list_snapshots(Some(&context.checkout_path), None, suite, 5001)?;
            if suite.is_none() && snapshots.len() >= 5001 {
                return Err(invalid(
                    "large history requires an explicit suite or measurement",
                ));
            }
            let suites: BTreeSet<_> = snapshots
                .iter()
                .filter_map(|row| row["suite"].as_str())
                .collect();
            if suites.len() > 1 {
                return Err(invalid(format!(
                    "latest measurement is ambiguous; provide measurement or suite ({} suites)",
                    suites.len()
                )));
            }
            snapshots
                .first()
                .and_then(|row| row["id"].as_str())
                .ok_or_else(|| {
                    AppError::NotFound("no measurement; pass a coverage report path".to_owned())
                })?
                .to_owned()
        }
    };
    if !Path::new(&context.checkout_path).join(&selected).is_file()
        && (uuid::Uuid::parse_str(&selected).is_ok()
            || (!selected.contains('/') && Path::new(&selected).extension().is_none()))
    {
        let (snapshot, report) = match service.store()?.measurement_report(&selected) {
            Ok(measurement) => measurement,
            Err(AppError::NotFound(_)) => return load_composite(service, &selected),
            Err(error) => return Err(error),
        };
        if snapshot["repo_key"] != context.repo_key {
            return Err(invalid("measurement belongs to another repository"));
        }
        if suite.is_some_and(|suite| snapshot["suite"] != suite) {
            return Err(invalid("suite does not match measurement"));
        }
        return Ok(Measurement {
            reference: selected,
            report,
            context: json!({"recorded_revision":snapshot["commit_sha"],"suite":snapshot["suite"]}),
            source_state: "unverified",
            native: None,
        });
    }
    let path = report_path(Path::new(&context.checkout_path), &selected)?;
    let bytes = read_report_bytes(&path)?;
    let report_hash = digest(&bytes);
    let report = parse_coverage_bytes(&path, &bytes, "auto", Some(&context.checkout_path))?;
    let receipt_path = PathBuf::from(format!("{}.context.json", path.display()));
    let receipt = match std::fs::metadata(&receipt_path) {
        Ok(metadata) => {
            if metadata.len() > 1024 * 1024 {
                return Err(invalid("measurement context exceeds 1 MiB"));
            }
            let receipt_path = report_path(
                Path::new(&context.checkout_path),
                &receipt_path.to_string_lossy(),
            )?;
            let receipt: Value = serde_json::from_slice(&std::fs::read(&receipt_path)?)?;
            if receipt["report_sha256"] != report_hash {
                return Err(invalid("measurement context does not match report content"));
            }
            if !receipt["source_hashes"].is_object()
                || receipt["build_id"]
                    .as_str()
                    .is_none_or(|s| s.is_empty() || s.len() > 256)
            {
                return Err(invalid(
                    "measurement context requires source_hashes and build_id",
                ));
            }
            if receipt.get("test_status").is_some_and(|value| {
                !matches!(value.as_str(), Some("passed" | "failed" | "unknown"))
            }) || receipt
                .get("scope")
                .is_some_and(|value| !matches!(value.as_str(), Some("full" | "selected_tests")))
            {
                return Err(invalid("context test_status or scope is invalid"));
            }
            receipt
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(error) => return Err(error.into()),
    };
    let source_state = source_state(&context.checkout_path, &report, &receipt)?;
    Ok(Measurement {
        reference: selected,
        report,
        context: receipt,
        source_state,
        native: None,
    })
}

fn source_state(root: &str, report: &CoverageReport, receipt: &Value) -> AppResult<&'static str> {
    let Some(hashes) = receipt["source_hashes"].as_object() else {
        return Ok("unverified");
    };
    if report.files.is_empty() {
        return Ok("unverified");
    }
    let mut state = "matches_receipt";
    for file in &report.files {
        let Some(expected) = hashes.get(&file.file_path).and_then(Value::as_str) else {
            state = "unverified";
            continue;
        };
        let path = match report_path(Path::new(root), &file.file_path) {
            Ok(path) => path,
            Err(_) => return Ok("changed_or_missing"),
        };
        if digest(&std::fs::read(path)?) != expected {
            return Ok("changed_or_missing");
        }
    }
    Ok(state)
}

fn number(values: &[Value], index: usize) -> AppResult<i64> {
    values
        .get(index)
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid("invalid stored LLVM region coordinate or count"))
}

fn insert(observations: &mut BTreeMap<String, Observation>, observation: Observation) {
    observations
        .entry(observation.key())
        .and_modify(|previous| previous.hits = previous.hits.max(observation.hits))
        .or_insert(observation);
}

fn observations(report: &CoverageReport, root: &str) -> AppResult<BTreeMap<String, Observation>> {
    let mut result = BTreeMap::new();
    for file in &report.files {
        if let Some(functions) = file.raw_metrics["llvm_functions"].as_array() {
            for function in functions {
                let symbol = function["name"]
                    .as_str()
                    .ok_or_else(|| invalid("LLVM function name unavailable"))?;
                let names = function["filenames"]
                    .as_array()
                    .ok_or_else(|| invalid("LLVM filenames unavailable"))?;
                for (field, kind) in [("regions", "regions"), ("branches", "branches")] {
                    let Some(rows) = function[field].as_array() else {
                        continue;
                    };
                    for row in rows {
                        let values = row
                            .as_array()
                            .ok_or_else(|| invalid("invalid LLVM region"))?;
                        let branch = kind == "branches";
                        let file_index = number(values, if branch { 6 } else { 5 })? as usize;
                        let name = names
                            .get(file_index)
                            .and_then(Value::as_str)
                            .ok_or_else(|| invalid("LLVM region references an unknown file"))?;
                        if normalize_report_path(name, Some(root)) != file.file_path {
                            continue;
                        }
                        // LLVM expansion, skipped, gap, and branch records are not code regions.
                        if !branch && number(values, 7)? != 0 {
                            continue;
                        }
                        let span = [
                            number(values, 0)?,
                            number(values, 1)?,
                            number(values, 2)?,
                            number(values, 3)?,
                        ];
                        if span[0] == 0 || (span[2], span[3]) < (span[0], span[1]) {
                            return Err(invalid("invalid LLVM source span"));
                        }
                        for (count, arm) in if branch {
                            vec![(4, "true"), (5, "false")]
                        } else {
                            vec![(4, "")]
                        } {
                            insert(
                                &mut result,
                                Observation {
                                    path: file.file_path.clone(),
                                    symbol: symbol.to_owned(),
                                    span,
                                    kind,
                                    arm,
                                    hits: number(values, count)?,
                                    identity: None,
                                    cause: None,
                                },
                            );
                        }
                    }
                }
            }
        }
    }
    for line in &report.lines {
        if line.count_line {
            insert(
                &mut result,
                Observation {
                    path: line.file_path.clone(),
                    symbol: String::new(),
                    span: [line.line_number, 0, line.line_number, 0],
                    kind: "lines",
                    arm: "",
                    hits: i64::from(line.covered),
                    identity: None,
                    cause: None,
                },
            );
        }
    }
    Ok(result)
}

fn metric<'a>(requested: Option<&'a str>, report: &CoverageReport) -> AppResult<&'a str> {
    match requested.unwrap_or("auto") {
        "auto" => Ok(if report.total_regions() > 0 {
            "regions"
        } else {
            "lines"
        }),
        metric @ ("regions" | "branches" | "lines") => Ok(metric),
        _ => Err(invalid("metric must be auto, regions, branches, or lines")),
    }
}

fn counts(report: &CoverageReport, metric: &str) -> Value {
    let (covered, total) = match metric {
        "regions" => (report.covered_regions(), report.total_regions()),
        "branches" => (report.covered_branches(), report.total_branches()),
        _ => (report.covered_lines(), report.total_lines()),
    };
    json!({"metric":metric,"covered":covered,"total":total,"missing":total-covered})
}

fn summary(measurement: &Measurement, metric: &str) -> Value {
    json!({"ref":measurement.reference,"format":measurement.report.format,"coverage":counts(&measurement.report,metric),"source":measurement.source_state,"recorded_revision":measurement.context["recorded_revision"],"build_id":measurement.context["build_id"],"tests":measurement.context["test_status"].as_str().unwrap_or("unknown")})
}

fn groups<'a>(
    rows: impl Iterator<Item = &'a Observation>,
    limit: usize,
    offset: usize,
) -> (Vec<Value>, usize) {
    group_rows(rows.collect(), limit, offset)
}

fn group_rows(rows: Vec<&Observation>, limit: usize, offset: usize) -> (Vec<Value>, usize) {
    let mut grouped: BTreeMap<(&str, &str, i64, Option<&str>), Vec<&Observation>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((
                &row.path,
                &row.symbol,
                if row.symbol.is_empty() {
                    row.span[0]
                } else {
                    0
                },
                row.cause.as_deref(),
            ))
            .or_default()
            .push(row);
    }
    let mut groups: Vec<_> = grouped.into_values().collect();
    for rows in &mut groups {
        rows.sort_by_key(|row| (row.span, row.arm));
    }
    groups.sort_by(|a, b| {
        b.len()
            .cmp(&a.len())
            .then_with(|| a[0].path.cmp(&b[0].path))
            .then_with(|| a[0].span.cmp(&b[0].span))
            .then_with(|| a[0].symbol.cmp(&b[0].symbol))
    });
    let count = groups.len();
    let rows = groups.into_iter().skip(offset).take(limit).map(|mut rows| {
        rows.sort_by_key(|row| (row.span, row.arm));
        let first = rows[0];
        let symbol = rustc_demangle::try_demangle(&first.symbol).map(|name| format!("{name:#}")).unwrap_or_else(|_| first.symbol.clone());
        let symbol = if symbol.chars().count() > 160 { format!("{}…",symbol.chars().take(159).collect::<String>()) } else { symbol };
        json!({"path":first.path,"function":if symbol.is_empty(){None}else{Some(symbol)},"count":rows.len(),"unmeasured_reason":first.cause,"locations":rows.iter().take(3).map(|row|json!({"span":row.span,"arm":if row.arm.is_empty(){None}else{Some(row.arm)}})).collect::<Vec<_>>(),"locations_omitted":rows.len().saturating_sub(3)})
    }).collect();
    (rows, count)
}

/// Finds missing observations grouped by function, without returning source files.
fn find(service: &Reader<'_>, request: &FindRequest) -> AppResult<Value> {
    validate(request.limit, request.query.as_deref())?;
    if request.line.is_some_and(|line| line < 1) {
        return Err(invalid("line must be positive"));
    }
    let measurement = load(
        service,
        request.measurement.as_deref(),
        request.suite.as_deref(),
    )?;
    let metric = metric(request.metric.as_deref(), &measurement.report)?;
    let observations =
        measurement_observations(&measurement, &service.context(None).checkout_path)?;
    let available = observations.values().any(|row| row.kind == metric);
    let (items, total) = groups(
        observations.values().filter(|row| {
            row.kind == metric
                && row.hits <= 0
                && row.matches(request.query.as_deref(), request.line)
        }),
        request.limit,
        request.offset,
    );
    let next = request.offset.saturating_add(items.len());
    Ok(service.envelope(json!({"status":if observations.values().any(|row|row.hits<0){"incomplete"}else if available{"measured"}else{"detail_unavailable"},"measurement":summary(&measurement,metric),"ranking":"missing observations, then source location","groups":items,"group_count":total,"next_offset":if next<total{Some(next)}else{None},"warnings":measurement.report.warnings.iter().take(3).map(|s|s.chars().take(240).collect::<String>()).collect::<Vec<_>>(),"warnings_omitted":measurement.report.warnings.len().saturating_sub(3),"next_action":if measurement.source_state == "changed_or_missing" {"Source differs from the measurement. Remeasure or inspect the recorded source before drawing conclusions."}else if available{"Use rg and bounded file reads at these locations; test references are not coverage attribution."}else{"This report lacks exact detail for the selected metric. Export detailed coverage or choose another metric."}}),request.suite.as_deref(),None))
}

fn compatible_context(current: &Measurement, baseline: &Measurement) -> bool {
    let Some(build) = current.context["build_id"].as_str() else {
        return false;
    };
    if baseline.context["build_id"] != build {
        return false;
    }
    let Some(current_hashes) = current.context["source_hashes"].as_object() else {
        return false;
    };
    let Some(baseline_hashes) = baseline.context["source_hashes"].as_object() else {
        return false;
    };
    current
        .report
        .files
        .iter()
        .chain(&baseline.report.files)
        .all(|file| {
            current_hashes.get(&file.file_path).is_some_and(|hash| {
                hash.is_string() && baseline_hashes.get(&file.file_path) == Some(hash)
            })
        })
}

/// Compares native observations; subset absences never become regressions.
fn compare(service: &Reader<'_>, request: &CompareRequest) -> AppResult<Value> {
    validate(request.limit, request.query.as_deref())?;
    let scope = request.scope.as_deref().unwrap_or("selected_tests");
    if !matches!(scope, "selected_tests" | "full" | "incremental") {
        return Err(invalid(
            "scope must be selected_tests, full, or incremental",
        ));
    }
    if request.previous.len() > 16 || (scope != "incremental" && !request.previous.is_empty()) {
        return Err(invalid(
            "previous accepts up to 16 reports with incremental scope only",
        ));
    }
    let current = load(service, Some(&request.measurement), None)?;
    let baseline = load(service, Some(&request.baseline), None)?;
    let metric = metric(
        request.metric.as_deref(),
        if scope == "incremental" {
            &baseline.report
        } else {
            &current.report
        },
    )?;
    if scope == "incremental" {
        return incremental::compare_incremental(service, request, &current, &baseline, metric);
    }
    let verified = compatible_context(&current, &baseline);
    let full = scope == "full"
        && current.context["scope"] == "full"
        && baseline.context["scope"] == "full"
        && verified;
    let root = service.context(None).checkout_path;
    let after = measurement_observations(&current, &root)?;
    let before = measurement_observations(&baseline, &root)?;
    let selected =
        |row: &&Observation| row.kind == metric && row.matches(request.query.as_deref(), None);
    let current_rows: Vec<_> = after.values().filter(selected).collect();
    let baseline_rows: Vec<_> = before.values().filter(selected).collect();
    let conflict = current.context["build_id"].is_string()
        && baseline.context["build_id"].is_string()
        && (current.context["build_id"] != baseline.context["build_id"]
            || current
                .report
                .files
                .iter()
                .chain(&baseline.report.files)
                .any(|file| {
                    let a = &current.context["source_hashes"][&file.file_path];
                    let b = &baseline.context["source_hashes"][&file.file_path];
                    a.is_string() && b.is_string() && a != b
                }));
    let comparable = !conflict
        && current.report.format == baseline.report.format
        && !current_rows.is_empty()
        && !baseline_rows.is_empty()
        && current_rows
            .iter()
            .chain(&baseline_rows)
            .all(|row| row.hits >= 0);
    let gained: Vec<_> = current_rows
        .iter()
        .copied()
        .filter(|row| row.hits > 0 && before.get(&row.key()).is_some_and(|base| base.hits == 0))
        .collect();
    let lost: Vec<_> = baseline_rows
        .iter()
        .copied()
        .filter(|row| {
            row.hits > 0
                && after
                    .get(&row.key())
                    .is_none_or(|current| current.hits == 0)
        })
        .collect();
    let added = current_rows
        .iter()
        .filter(|row| !before.contains_key(&row.key()))
        .count();
    let removed = baseline_rows
        .iter()
        .filter(|row| !after.contains_key(&row.key()))
        .count();
    let (gained_groups, gained_count) = groups(gained.iter().copied(), request.limit, 0);
    let (remaining, remaining_count) = groups(
        current_rows.iter().copied().filter(|row| row.hits == 0),
        request.limit,
        0,
    );
    let status = if current.context["test_status"] == "failed" {
        "tests_failed"
    } else if !comparable {
        "incomparable"
    } else if !verified {
        "limited"
    } else if full && !lost.is_empty() {
        "regression"
    } else if gained.is_empty() {
        "no_gain"
    } else {
        "improved"
    };
    let reason = if !comparable {
        "Reports have conflicting source/build identities, incompatible formats, missing metric detail, or unmeasured inventory entries. Remeasure compatible inputs."
    } else if !verified {
        "Source/build identity is unverified; counts describe report-coordinate changes, not a verified coverage gain or regression."
    } else if !full {
        "Selected-test absences are not regressions; full-suite regression status is unknown."
    } else {
        "Full measurements have matching source/build context."
    };
    Ok(service.envelope(json!({"status":status,"measurement":summary(&current,metric),"baseline":summary(&baseline,metric),"scope":if full{"full"}else{"selected_tests"},"reason":reason,"changes":if comparable{json!({"newly_observed":gained.len(),"not_observed":lost.len(),"added":added,"removed":removed,"regressed":if full{Some(lost.len())}else{None}})}else{Value::Null},"gained":if comparable{gained_groups}else{Vec::new()},"gained_group_count":if comparable{gained_count}else{0},"remaining":remaining,"remaining_group_count":remaining_count}),None,None))
}

fn load_composite(service: &Reader<'_>, reference: &str) -> AppResult<Measurement> {
    let store = service.store()?;
    let snapshot = store.composite_snapshot(reference)?;
    if snapshot["repo_key"] != service.context(None).repo_key {
        return Err(invalid("measurement belongs to another repository"));
    }
    let rows = store.composite_regions_all(reference)?;
    decode_composite(
        &service.context(None).checkout_path,
        reference,
        &snapshot,
        rows,
    )
}

fn decode_composite(
    root: &str,
    reference: &str,
    snapshot: &Value,
    rows: Vec<Value>,
) -> AppResult<Measurement> {
    let mut native = BTreeMap::new();
    let mut files = BTreeMap::<String, crate::models::FileCoverage>::new();
    let mut hashes = serde_json::Map::new();
    for row in rows {
        let path = row["path"]
            .as_str()
            .ok_or_else(|| invalid("composite region path unavailable"))?
            .to_owned();
        let start = row["start_line"]
            .as_i64()
            .ok_or_else(|| invalid("composite region start unavailable"))?;
        let end = row["end_line"]
            .as_i64()
            .ok_or_else(|| invalid("composite region end unavailable"))?;
        let state = row["state"].as_str().unwrap_or("unmeasured");
        let hits = match state {
            "covered" => 1,
            "uncovered" => 0,
            _ => -1,
        };
        let key = row["region_key"]
            .as_str()
            .ok_or_else(|| invalid("composite region identity unavailable"))?
            .to_owned();
        insert(
            &mut native,
            Observation {
                path: path.clone(),
                symbol: String::new(),
                span: [start, 0, end, 0],
                kind: "regions",
                arm: "",
                hits,
                identity: Some(key),
                cause: if hits < 0 {
                    Some(state.to_owned())
                } else {
                    None
                },
            },
        );
        let file = files
            .entry(path.clone())
            .or_insert(crate::models::FileCoverage {
                file_path: path.clone(),
                total_lines: 0,
                covered_lines: 0,
                total_branches: 0,
                covered_branches: 0,
                total_functions: 0,
                covered_functions: 0,
                total_regions: 0,
                covered_regions: 0,
                raw_metrics: json!({}),
            });
        file.total_regions += 1;
        file.covered_regions += i64::from(hits > 0);
        if hashes
            .get(&path)
            .is_some_and(|hash| hash != &row["source_hash"])
        {
            return Err(invalid(
                "composite has conflicting source hashes for one path",
            ));
        }
        hashes.insert(path, row["source_hash"].clone());
    }
    let report = CoverageReport {
        format: "composite".to_owned(),
        report_path: reference.to_owned(),
        files: files.into_values().collect(),
        lines: Vec::new(),
        test_names: Vec::new(),
        test_coverage: Vec::new(),
        warnings: snapshot["reasons"]
            .as_array()
            .into_iter()
            .flatten()
            .map(Value::to_string)
            .collect(),
        metadata: json!({}),
    };
    if report.total_regions() != snapshot["total_regions"].as_i64().unwrap_or(-1)
        || report.covered_regions() != snapshot["covered_regions"].as_i64().unwrap_or(-1)
    {
        return Err(invalid(
            "composite inventory does not match its recorded totals",
        ));
    }
    let context = json!({"source_hashes":hashes,"build_id":format!("{}:{}",snapshot["mapping_version"],snapshot["inventory_hash"])});
    let source_state = source_state(root, &report, &context)?;
    Ok(Measurement {
        reference: reference.to_owned(),
        report,
        context,
        source_state,
        native: Some(native),
    })
}

fn measurement_observations(
    measurement: &Measurement,
    root: &str,
) -> AppResult<BTreeMap<String, Observation>> {
    match &measurement.native {
        Some(native) => Ok(native.clone()),
        None => observations(&measurement.report, root),
    }
}

/// Formats the same evidence used by MCP and REST as a concise terminal result.
pub fn render(envelope: &Value) -> String {
    let data = &envelope["data"];
    let measurement = &data["measurement"];
    let coverage = &measurement["coverage"];
    let mut output = format!(
        "{}: {} / {} {} covered\nSource: {} · Tests: {} · Status: {}\n",
        measurement["ref"].as_str().unwrap_or("measurement"),
        coverage["covered"],
        coverage["total"],
        coverage["metric"].as_str().unwrap_or("observations"),
        measurement["source"].as_str().unwrap_or("unverified"),
        measurement["tests"].as_str().unwrap_or("unknown"),
        data["status"].as_str().unwrap_or("unknown")
    );
    if let Some(reason) = data["reason"].as_str() {
        output.push_str(reason);
        output.push('\n');
    }
    if let Some(increment) = data["incremental"].as_object() {
        output.push_str(&format!(
            "Baseline + accepted batches: {} / {} · New batch adds: {} · Combined: {} / {} {}\nFull-suite regression: not checked\n",
            increment["accepted"]["covered"], increment["accepted"]["total"],
            increment["newly_covered"], increment["combined"]["covered"],
            increment["combined"]["total"], coverage["metric"].as_str().unwrap_or("observations")
        ));
    } else if data["changes"].is_object() {
        output.push_str(&format!(
            "Newly observed: {} · Not observed: {} · Added: {} · Removed: {}\n",
            data["changes"]["newly_observed"],
            data["changes"]["not_observed"],
            data["changes"]["added"],
            data["changes"]["removed"]
        ));
    }
    for (field, label, count) in [
        ("groups", "Gaps", "group_count"),
        ("gained", "Gained", "gained_group_count"),
        ("remaining", "Remaining", "remaining_group_count"),
    ] {
        if let Some(groups) = data[field].as_array() {
            output.push_str(&format!(
                "{label}: {} / {} groups\n",
                groups.len(),
                data[count]
            ));
            for group in groups {
                output.push_str(&format!(
                    "{}:{}  {}  ({} observations)\n",
                    group["path"].as_str().unwrap_or("?"),
                    group["locations"][0]["span"][0],
                    group["function"].as_str().unwrap_or("source range"),
                    group["count"]
                ));
                if let Some(cause) = group["unmeasured_reason"].as_str() {
                    output.push_str(&format!("  Evidence missing: {cause}\n"));
                }
                for location in group["locations"].as_array().into_iter().flatten() {
                    output.push_str(&format!(
                        "  {}:{}–{}:{} {}\n",
                        location["span"][0],
                        location["span"][1],
                        location["span"][2],
                        location["span"][3],
                        location["arm"].as_str().unwrap_or("")
                    ));
                }
                if group["locations_omitted"].as_u64().unwrap_or(0) > 0 {
                    output.push_str(&format!(
                        "  {} more locations; narrow the query or line\n",
                        group["locations_omitted"]
                    ));
                }
            }
        }
    }
    if data["next_offset"].is_number() {
        output.push_str(&format!("More groups: --offset {}\n", data["next_offset"]));
    }
    output.trim_end().to_owned()
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;

#[path = "query_incremental.rs"]
mod incremental;
