use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use quick_xml::events::Event;
use quick_xml::{Reader, XmlVersion};
use serde_json::{Map, Value, json};

use crate::error::{AppError, AppResult};
use crate::models::{CoverageBuilder, CoverageReport};

/// Formats accepted by the public parser contract.
pub const SUPPORTED_FORMATS: &[&str] = &[
    "auto",
    "lcov",
    "coverage.py",
    "coveragepy",
    "cobertura",
    "jacoco",
    "istanbul",
    "nyc",
    "go",
    "go-cover",
    "go-coverprofile",
    "llvm",
    "llvm-json",
];
/// Maximum coverage report size accepted by the parser.
pub const MAX_COVERAGE_REPORT_BYTES: u64 = 64 * 1024 * 1024;

/// Parses one supported coverage artifact into normalized rows.
pub fn parse_coverage_report(
    path: &Path,
    format: &str,
    repo_path: Option<&str>,
) -> AppResult<CoverageReport> {
    if !path.exists() {
        return Err(parse_error(format!(
            "coverage report does not exist: {}",
            path.display()
        )));
    }
    let report_size = fs::metadata(path)?.len();
    if report_size > MAX_COVERAGE_REPORT_BYTES {
        return Err(parse_error(format!(
            "coverage report exceeds the {} byte limit",
            MAX_COVERAGE_REPORT_BYTES
        )));
    }
    let selected = if format.trim().eq_ignore_ascii_case("auto") {
        detect_format(path)?
    } else {
        normalize_format(format)?
    };
    parse_selected(&selected, path, repo_path)
}

fn parse_selected(
    selected: &str,
    path: &Path,
    repo_path: Option<&str>,
) -> AppResult<CoverageReport> {
    match selected {
        "lcov" => parse_lcov(path, repo_path),
        "coveragepy" => parse_coveragepy_json(path, repo_path),
        "cobertura" => parse_cobertura(path, repo_path),
        "jacoco" => parse_jacoco(path, repo_path),
        "istanbul" => parse_istanbul(path, repo_path),
        "go" => parse_go(path, repo_path),
        "llvm" => parse_llvm(path, repo_path),
        other => Err(parse_error(format!("unsupported coverage format: {other}"))),
    }
}

/// Canonicalizes compatibility aliases.
pub fn normalize_format(format: &str) -> AppResult<String> {
    let lowered = format.trim().to_lowercase();
    let normalized = match lowered.as_str() {
        "coverage.py" | "coverage-json" | "coveragepy-json" => "coveragepy",
        "nyc" => "istanbul",
        "go-cover" | "go-coverprofile" | "coverprofile" => "go",
        "llvm-json" => "llvm",
        value => value,
    };
    if SUPPORTED_FORMATS.contains(&normalized) && normalized != "auto" {
        Ok(normalized.to_owned())
    } else {
        Err(parse_error(format!(
            "unsupported coverage format: {format}"
        )))
    }
}

fn detect_format(path: &Path) -> AppResult<String> {
    let bytes = fs::read(path)?;
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]);
    let trimmed = head.trim_start();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_lowercase();
    if extension == "lcov"
        || extension == "info"
        || trimmed.starts_with("TN:")
        || head.contains("\nSF:")
    {
        return Ok("lcov".to_owned());
    }
    if trimmed.starts_with("mode:") {
        return Ok("go".to_owned());
    }
    if extension == "xml" || trimmed.starts_with('<') {
        let root = parse_xml(path)?;
        if root.name == "coverage" {
            return Ok("cobertura".to_owned());
        }
        if root.name == "report"
            && (find_nodes(&root, "sourcefile").next().is_some()
                || find_nodes(&root, "counter").next().is_some())
        {
            return Ok("jacoco".to_owned());
        }
        return Err(parse_error(format!(
            "could not detect XML coverage format for {}",
            path.display()
        )));
    }
    if extension == "json" || trimmed.starts_with('{') {
        let data: Value = serde_json::from_slice(&bytes)?;
        if let Value::Object(object) = &data {
            if object.contains_key("data") {
                return Ok("llvm".to_owned());
            }
            if object
                .get("files")
                .and_then(Value::as_object)
                .is_some_and(|files| {
                    files
                        .values()
                        .any(|value| value.get("executed_lines").is_some())
                })
            {
                return Ok("coveragepy".to_owned());
            }
            if looks_like_istanbul(object) {
                return Ok("istanbul".to_owned());
            }
        }
        return Err(parse_error(format!(
            "could not detect JSON coverage format for {}",
            path.display()
        )));
    }
    Err(parse_error(format!(
        "could not detect coverage format for {}",
        path.display()
    )))
}

fn parse_lcov(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let mut builder = CoverageBuilder::new(repo_path);
    let mut current_file: Option<String> = None;
    let mut function_lines: BTreeMap<String, i64> = BTreeMap::new();
    for raw in fs::read_to_string(path)?.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(file) = line.strip_prefix("SF:") {
            current_file = Some(file.to_owned());
            function_lines.clear();
            continue;
        }
        if line == "end_of_record" {
            current_file = None;
            function_lines.clear();
            continue;
        }
        let Some(file) = current_file.as_deref() else {
            continue;
        };
        if let Some(payload) = line.strip_prefix("DA:") {
            let mut parts = payload.split(',');
            let line_number = parts
                .next()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| parse_error("LCOV DA record is missing a line number".to_owned()))?;
            let hits = parts
                .next()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| parse_error("LCOV DA record is missing hit data".to_owned()))?;
            builder.add_line(
                file,
                safe_i64(Some(line_number))?,
                safe_i64(Some(hits))?,
                None,
                true,
                0,
                0,
                0,
                0,
                json!({}),
            );
        } else if let Some(payload) = line.strip_prefix("FN:") {
            let (line_number, name) = payload
                .split_once(',')
                .ok_or_else(|| parse_error("LCOV FN record is malformed".to_owned()))?;
            function_lines.insert(name.to_owned(), safe_i64(Some(line_number))?);
        } else if let Some(payload) = line.strip_prefix("FNDA:") {
            let (hits, name) = payload
                .split_once(',')
                .ok_or_else(|| parse_error("LCOV FNDA record is malformed".to_owned()))?;
            if let Some(line_number) = function_lines.get(name) {
                builder.add_line(
                    file,
                    *line_number,
                    0,
                    Some(false),
                    false,
                    0,
                    0,
                    1,
                    i64::from(safe_i64(Some(hits))? > 0),
                    json!({}),
                );
            }
        } else if let Some(payload) = line.strip_prefix("BRDA:") {
            add_lcov_branch(&mut builder, file, payload)?;
        }
    }
    let mut report = builder.build("lcov", &path.to_string_lossy(), Vec::new(), json!({}));
    if report.lines.is_empty() {
        report
            .warnings
            .push("LCOV report contained no DA/BRDA/FNDA records.".to_owned());
    }
    Ok(report)
}

fn add_lcov_branch(builder: &mut CoverageBuilder, file: &str, payload: &str) -> AppResult<()> {
    let parts: Vec<&str> = payload.split(',').collect();
    if parts.len() < 4 {
        return Err(parse_error("LCOV BRDA record is malformed".to_owned()));
    }
    let taken = parts[3];
    let covered = if taken == "-" {
        0
    } else {
        i64::from(safe_i64(Some(taken))? > 0)
    };
    builder.add_line(
        file,
        safe_i64(parts.first().copied())?,
        0,
        Some(false),
        false,
        1,
        covered,
        0,
        0,
        json!({}),
    );
    Ok(())
}

fn parse_coveragepy_json(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let data: Value = serde_json::from_slice(&fs::read(path)?)?;
    let files = data
        .get("files")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            parse_error("coverage.py JSON report must contain a 'files' object".to_owned())
        })?;
    let mut builder = CoverageBuilder::new(repo_path);
    for (file_path, payload) in files {
        let payload = payload.as_object().ok_or_else(|| {
            parse_error(format!(
                "coverage.py file entry must be an object: {file_path}"
            ))
        })?;
        let mut line_numbers = Vec::new();
        for key in ["executed_lines", "missing_lines"] {
            if let Some(lines) = coverage_array(payload, key)? {
                line_numbers.extend(numeric_line_numbers(lines)?);
            }
        }
        line_numbers.sort_unstable();
        line_numbers.dedup();
        let executed: std::collections::BTreeSet<i64> = coverage_array(payload, "executed_lines")?
            .map(|lines| numeric_line_numbers(lines))
            .transpose()?
            .unwrap_or_default();
        for line in line_numbers {
            let covered = executed.contains(&line);
            builder.add_line(
                file_path,
                line,
                i64::from(covered),
                Some(covered),
                true,
                0,
                0,
                0,
                0,
                json!({}),
            );
        }
        let mut branches: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
        for (key, covered) in [("executed_branches", true), ("missing_branches", false)] {
            if let Some(values) = coverage_array(payload, key)? {
                for value in values {
                    let items = value.as_array().ok_or_else(|| {
                        parse_error(format!("coverage.py {key} entry must be an array"))
                    })?;
                    let line = items.first().ok_or_else(|| {
                        parse_error(format!("coverage.py {key} entry is missing a line number"))
                    })?;
                    let line = value_i64(line)?;
                    let entry = branches.entry(line).or_default();
                    if covered {
                        entry.1 += 1;
                    } else {
                        entry.0 += 1;
                    }
                }
            }
        }
        for (line, (missing, covered)) in branches {
            builder.add_line(
                file_path,
                line,
                0,
                Some(false),
                false,
                missing + covered,
                covered,
                0,
                0,
                json!({}),
            );
        }
        if let Some(summary) = payload.get("summary") {
            let mut metrics = Map::new();
            metrics.insert("coveragepy_summary".to_owned(), summary.clone());
            builder.add_file_metrics(file_path, metrics);
        }
    }
    Ok(builder.build(
        "coveragepy",
        &path.to_string_lossy(),
        vec![
            "coverage.py JSON reports line coverage as covered/missing, not execution counts."
                .to_owned(),
        ],
        json!({"meta": data.get("meta").cloned().unwrap_or(Value::Null)}),
    ))
}

fn coverage_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> AppResult<Option<&'a Vec<Value>>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_array()
            .map(Some)
            .ok_or_else(|| parse_error(format!("coverage.py {key} must be an array or null"))),
    }
}

fn numeric_line_numbers(values: &[Value]) -> AppResult<std::collections::BTreeSet<i64>> {
    values.iter().map(value_i64).collect()
}

fn parse_cobertura(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let root = parse_xml(path)?;
    let mut builder = CoverageBuilder::new(repo_path);
    for class_node in find_nodes(&root, "class") {
        let file_path = class_node
            .attrs
            .get("filename")
            .or_else(|| class_node.attrs.get("name"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| parse_error("Cobertura class is missing a filename".to_owned()))?;
        for line_node in descendants(class_node, "line") {
            let line_number = safe_i64(line_node.attrs.get("number").map(String::as_str))?;
            let hits = safe_i64(line_node.attrs.get("hits").map(String::as_str))?;
            let (total_branches, covered_branches) = cobertura_branch_counts(line_node)?;
            builder.add_line(
                file_path,
                line_number,
                hits,
                None,
                true,
                total_branches,
                covered_branches,
                0,
                0,
                json!({}),
            );
        }
        let mut metrics = Map::new();
        if let Some(value) = class_node.attrs.get("line-rate") {
            let value = parse_f64(value)?;
            metrics.insert("line_rate".to_owned(), json!(value));
        }
        if let Some(value) = class_node.attrs.get("branch-rate") {
            let value = parse_f64(value)?;
            metrics.insert("branch_rate".to_owned(), json!(value));
        }
        builder.add_file_metrics(file_path, metrics);
    }
    Ok(builder.build("cobertura", &path.to_string_lossy(), Vec::new(), json!({})))
}

fn parse_jacoco(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let root = parse_xml(path)?;
    let mut builder = CoverageBuilder::new(repo_path);
    for package in find_nodes(&root, "package") {
        let prefix = package
            .attrs
            .get("name")
            .map(|value| value.trim_matches('/'))
            .unwrap_or_default();
        for source in descendants(package, "sourcefile") {
            let name = source
                .attrs
                .get("name")
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| parse_error("JaCoCo sourcefile is missing a name".to_owned()))?;
            let file_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            for line_node in descendants(source, "line") {
                let missed_instructions = safe_i64(line_node.attrs.get("mi").map(String::as_str))?;
                let covered_instructions = safe_i64(line_node.attrs.get("ci").map(String::as_str))?;
                let missed_branches = safe_i64(line_node.attrs.get("mb").map(String::as_str))?;
                let covered_branches = safe_i64(line_node.attrs.get("cb").map(String::as_str))?;
                builder.add_line(
                    &file_path,
                    safe_i64(line_node.attrs.get("nr").map(String::as_str))?,
                    covered_instructions,
                    Some(covered_instructions > 0),
                    true,
                    missed_branches + covered_branches,
                    covered_branches,
                    0,
                    0,
                    json!({"missed_instructions": missed_instructions}),
                );
            }
        }
    }
    Ok(builder.build("jacoco", &path.to_string_lossy(), Vec::new(), json!({})))
}

fn parse_istanbul(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let data: Value = serde_json::from_slice(&fs::read(path)?)?;
    let object = data
        .as_object()
        .ok_or_else(|| parse_error("Istanbul JSON must be an object".to_owned()))?;
    if !looks_like_istanbul(object) {
        return Err(parse_error(
            "Istanbul JSON must contain coverage objects with statementMap/s/f/branchMap/b"
                .to_owned(),
        ));
    }
    let mut builder = CoverageBuilder::new(repo_path);
    for (key, payload) in object {
        let payload = payload
            .as_object()
            .ok_or_else(|| parse_error(format!("Istanbul file entry must be an object: {key}")))?;
        let statement_map = payload
            .get("statementMap")
            .and_then(Value::as_object)
            .ok_or_else(|| parse_error(format!("Istanbul entry is missing statementMap: {key}")))?;
        let statement_hits = payload.get("s").and_then(Value::as_object).ok_or_else(|| {
            parse_error(format!("Istanbul entry is missing statement hits: {key}"))
        })?;
        let file_path = payload
            .get("path")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| parse_error(format!("Istanbul path must be a string: {key}")))
            })
            .transpose()?
            .unwrap_or(key);
        for (statement_id, location) in statement_map {
            let line = location_line(location)?;
            let hits = statement_hits
                .get(statement_id)
                .ok_or_else(|| {
                    parse_error(format!(
                        "Istanbul statement is missing hit data: {key}:{statement_id}"
                    ))
                })
                .and_then(value_i64)?;
            builder.add_line(file_path, line, hits, None, true, 0, 0, 0, 0, json!({}));
        }
        match (payload.get("fnMap"), payload.get("f")) {
            (None, None) => {}
            (Some(function_map), Some(function_hits)) => {
                let function_map = function_map.as_object().ok_or_else(|| {
                    parse_error(format!("Istanbul fnMap must be an object: {key}"))
                })?;
                let function_hits = function_hits.as_object().ok_or_else(|| {
                    parse_error(format!("Istanbul function hits must be an object: {key}"))
                })?;
                for (function_id, function) in function_map {
                    let line = if let Some(location) = function.get("loc") {
                        location_line(location)?
                    } else if let Some(declaration) = function.get("decl") {
                        location_line(declaration)?
                    } else {
                        location_line(function)?
                    };
                    let hits = function_hits
                        .get(function_id)
                        .ok_or_else(|| {
                            parse_error(format!(
                                "Istanbul function is missing hit data: {key}:{function_id}"
                            ))
                        })
                        .and_then(value_i64)?;
                    builder.add_line(
                        file_path,
                        line,
                        0,
                        Some(false),
                        false,
                        0,
                        0,
                        1,
                        i64::from(hits > 0),
                        json!({}),
                    );
                }
            }
            _ => {
                return Err(parse_error(format!(
                    "Istanbul fnMap and function hits must be provided together: {key}"
                )));
            }
        }
        match (payload.get("branchMap"), payload.get("b")) {
            (None, None) => {}
            (Some(branch_map), Some(branch_hits)) => {
                let branch_map = branch_map.as_object().ok_or_else(|| {
                    parse_error(format!("Istanbul branchMap must be an object: {key}"))
                })?;
                let branch_hits = branch_hits.as_object().ok_or_else(|| {
                    parse_error(format!("Istanbul branch hits must be an object: {key}"))
                })?;
                for (branch_id, branch) in branch_map {
                    let line = if let Some(location) = branch.get("loc") {
                        location_line(location)?
                    } else {
                        location_line(branch)?
                    };
                    let counts = branch_hits
                        .get(branch_id)
                        .ok_or_else(|| {
                            parse_error(format!(
                                "Istanbul branch is missing hit data: {key}:{branch_id}"
                            ))
                        })?
                        .as_array()
                        .ok_or_else(|| {
                            parse_error(format!(
                                "Istanbul branch hit data must be an array: {key}:{branch_id}"
                            ))
                        })?;
                    let total = checked_len_i64(counts.len(), "Istanbul branch count")?;
                    let mut covered = 0_i64;
                    for value in counts {
                        if value_i64(value)? > 0 {
                            covered += 1;
                        }
                    }
                    builder.add_line(
                        file_path,
                        line,
                        0,
                        Some(false),
                        false,
                        total,
                        covered,
                        0,
                        0,
                        json!({}),
                    );
                }
            }
            _ => {
                return Err(parse_error(format!(
                    "Istanbul branchMap and branch hits must be provided together: {key}"
                )));
            }
        }
    }
    Ok(builder.build(
        "istanbul",
        &path.to_string_lossy(),
        vec![
            "Istanbul statement coverage is normalized to the starting line of each statement."
                .to_owned(),
        ],
        json!({}),
    ))
}

fn parse_go(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let lines = fs::read_to_string(path)?;
    let mut iter = lines.lines();
    if !iter.next().is_some_and(|line| line.starts_with("mode:")) {
        return Err(parse_error(
            "Go coverprofile must start with 'mode:'".to_owned(),
        ));
    }
    let mut builder = CoverageBuilder::new(repo_path);
    for raw in iter {
        if raw.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = raw.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(parse_error(format!(
                "Go coverprofile record must contain exactly three fields: {raw}"
            )));
        }
        let (file, range) = parts[0].rsplit_once(':').ok_or_else(|| {
            parse_error(format!("Go coverprofile range is malformed: {}", parts[0]))
        })?;
        let (start, end) = range
            .split_once(',')
            .ok_or_else(|| parse_error(format!("Go coverprofile range is malformed: {range}")))?;
        let start_line = go_position_line(start)?;
        let end_line = go_position_line(end)?;
        let statements = safe_i64(Some(parts[1]))?;
        let hits = safe_i64(Some(parts[2]))?;
        if statements < 0 || hits < 0 || end_line < start_line {
            return Err(parse_error(format!(
                "Go coverprofile record has invalid counts or range: {raw}"
            )));
        }
        let line_count = end_line - start_line + 1;
        if line_count > 1_000_000 {
            return Err(parse_error(
                "Go coverprofile range exceeds one million lines".to_owned(),
            ));
        }
        for line in start_line..=end_line {
            builder.add_line(
                file,
                line,
                hits,
                Some(hits > 0),
                true,
                0,
                0,
                0,
                0,
                json!({}),
            );
        }
    }
    Ok(builder.build(
        "go",
        &path.to_string_lossy(),
        vec!["Go coverprofiles report statement blocks; this expands each block to all touched lines.".to_owned()],
        json!({}),
    ))
}

fn parse_llvm(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let data: Value = serde_json::from_slice(&fs::read(path)?)?;
    let mut builder = CoverageBuilder::new(repo_path);
    let units = data
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("LLVM JSON must contain a data array".to_owned()))?;
    for unit in units {
        let unit = unit
            .as_object()
            .ok_or_else(|| parse_error("LLVM data entry must be an object".to_owned()))?;
        let files = unit
            .get("files")
            .and_then(Value::as_array)
            .ok_or_else(|| parse_error("LLVM data entry must contain a files array".to_owned()))?;
        for file_payload in files {
            let file_payload = file_payload
                .as_object()
                .ok_or_else(|| parse_error("LLVM file entry must be an object".to_owned()))?;
            let file_path = file_payload
                .get("filename")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| parse_error("LLVM file entry is missing filename".to_owned()))?;
            if let Some(segments) = file_payload.get("segments") {
                let segments = segments
                    .as_array()
                    .ok_or_else(|| parse_error("LLVM segments must be an array".to_owned()))?;
                for segment in segments {
                    let values = segment
                        .as_array()
                        .ok_or_else(|| parse_error("LLVM segment must be an array".to_owned()))?;
                    if values.len() < 4 {
                        return Err(parse_error(
                            "LLVM segment must contain line, column, count, and flag".to_owned(),
                        ));
                    }
                    let line = value_i64(&values[0])?;
                    let hits = value_i64(&values[2])?;
                    let Some(has_count) = values[3].as_bool() else {
                        return Err(parse_error(
                            "LLVM segment count flag must be a boolean".to_owned(),
                        ));
                    };
                    if !has_count {
                        continue;
                    }
                    builder.add_line(
                        file_path,
                        line,
                        hits,
                        Some(hits > 0),
                        true,
                        0,
                        0,
                        0,
                        0,
                        json!({}),
                    );
                }
            }
            if let Some(branches) = file_payload.get("branches") {
                let branches = branches
                    .as_array()
                    .ok_or_else(|| parse_error("LLVM branches must be an array".to_owned()))?;
                for branch in branches {
                    let (line, true_count, false_count) = llvm_branch_counts(branch)?;
                    builder.add_line(
                        file_path,
                        line,
                        0,
                        Some(false),
                        false,
                        2,
                        i64::from(true_count > 0) + i64::from(false_count > 0),
                        0,
                        0,
                        json!({}),
                    );
                }
            }
            if let Some(summary) = file_payload.get("summary") {
                let summary = summary
                    .as_object()
                    .ok_or_else(|| parse_error("LLVM summary must be an object".to_owned()))?;
                builder.add_file_metrics(file_path, llvm_summary_metrics(summary)?);
            }
        }
    }
    Ok(builder.build(
        "llvm",
        &path.to_string_lossy(),
        vec!["LLVM JSON segments are normalized to segment start lines; aggregate region coverage is preserved from summaries.".to_owned()],
        json!({}),
    ))
}

#[derive(Clone, Debug)]
struct XmlNode {
    name: String,
    attrs: BTreeMap<String, String>,
    children: Vec<XmlNode>,
}

fn parse_xml(path: &Path) -> AppResult<XmlNode> {
    let mut reader = Reader::from_file(path).map_err(|error| parse_error(error.to_string()))?;
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut stack: Vec<XmlNode> = Vec::new();
    let mut root = None;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => stack.push(XmlNode {
                name: xml_name(event.name().as_ref()),
                attrs: xml_attrs(&reader, &event)?,
                children: Vec::new(),
            }),
            Ok(Event::Empty(event)) => {
                let node = XmlNode {
                    name: xml_name(event.name().as_ref()),
                    attrs: xml_attrs(&reader, &event)?,
                    children: Vec::new(),
                };
                attach_xml_node(&mut stack, &mut root, node);
            }
            Ok(Event::End(_)) => append_xml_end(&mut stack, &mut root),
            Ok(Event::Eof) => break,
            Err(error) => return Err(parse_error(error.to_string())),
            _ => {}
        }
        buffer.clear();
    }
    root.ok_or_else(|| parse_error(format!("XML report is empty: {}", path.display())))
}

fn attach_xml_node(stack: &mut [XmlNode], root: &mut Option<XmlNode>, node: XmlNode) {
    match stack.last_mut() {
        Some(parent) => parent.children.push(node),
        None => *root = Some(node),
    }
}

fn append_xml_end(stack: &mut Vec<XmlNode>, root: &mut Option<XmlNode>) {
    match stack.pop() {
        Some(node) => attach_xml_node(stack, root, node),
        None => {
            *root = None;
        }
    }
}

fn xml_name(value: &[u8]) -> String {
    String::from_utf8_lossy(value)
        .rsplit('}')
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn xml_attrs(
    reader: &Reader<impl std::io::BufRead>,
    event: &quick_xml::events::BytesStart<'_>,
) -> AppResult<BTreeMap<String, String>> {
    let mut attrs = BTreeMap::new();
    for attr in event.attributes().with_checks(false) {
        let attr = attr.map_err(|error| parse_error(error.to_string()))?;
        let key = xml_name(attr.key.as_ref());
        let value = attr
            .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
            .map_err(|error| parse_error(error.to_string()))?;
        attrs.insert(key, value.into_owned());
    }
    Ok(attrs)
}

fn find_nodes<'a>(node: &'a XmlNode, name: &str) -> Box<dyn Iterator<Item = &'a XmlNode> + 'a> {
    let mut matches = Vec::new();
    collect_nodes(node, name, &mut matches);
    Box::new(matches.into_iter())
}

fn descendants<'a>(node: &'a XmlNode, name: &str) -> Vec<&'a XmlNode> {
    let mut matches = Vec::new();
    for child in &node.children {
        if child.name == name {
            matches.push(child);
        }
        matches.extend(descendants(child, name));
    }
    matches
}

fn collect_nodes<'a>(node: &'a XmlNode, name: &str, matches: &mut Vec<&'a XmlNode>) {
    if node.name == name {
        matches.push(node);
    }
    for child in &node.children {
        collect_nodes(child, name, matches);
    }
}

fn cobertura_branch_counts(node: &XmlNode) -> AppResult<(i64, i64)> {
    if !node
        .attrs
        .get("branch")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
    {
        return Ok((0, 0));
    }
    let explicit_total = node.attrs.get("branches-valid");
    let explicit_covered = node.attrs.get("branches-covered");
    if explicit_total.is_some() || explicit_covered.is_some() {
        let (Some(total), Some(covered)) = (explicit_total, explicit_covered) else {
            return Err(parse_error(
                "Cobertura branch counts must include both total and covered values".to_owned(),
            ));
        };
        return Ok((safe_i64(Some(total))?, safe_i64(Some(covered))?));
    }
    let Some(value) = node.attrs.get("condition-coverage") else {
        return Ok((0, 0));
    };
    let value = value
        .trim_matches(|character| character != '(' && character != ')')
        .trim_matches(['(', ')']);
    if let Some((covered, total)) = value.split_once('/') {
        return Ok((safe_i64(Some(total))?, safe_i64(Some(covered))?));
    }
    Err(parse_error(format!(
        "invalid Cobertura condition-coverage value: {value}"
    )))
}

fn looks_like_istanbul(object: &Map<String, Value>) -> bool {
    object
        .values()
        .any(|value| value.get("statementMap").is_some() && value.get("s").is_some())
}

fn location_line(value: &Value) -> AppResult<i64> {
    let object = value
        .as_object()
        .ok_or_else(|| parse_error("Istanbul location must be an object".to_owned()))?;
    let line_value = if let Some(start) = object.get("start") {
        start
            .as_object()
            .and_then(|object| object.get("line"))
            .ok_or_else(|| parse_error("Istanbul location is missing start.line".to_owned()))?
    } else {
        object
            .get("line")
            .ok_or_else(|| parse_error("Istanbul location is missing line".to_owned()))?
    };
    let line = value_i64(line_value)?;
    if line < 1 {
        return Err(parse_error(format!(
            "Istanbul line must be positive: {line}"
        )));
    }
    Ok(line)
}

fn llvm_summary_metrics(summary: &Map<String, Value>) -> AppResult<Map<String, Value>> {
    let mut metrics = Map::new();
    for name in ["lines", "branches", "functions", "regions"] {
        if let Some(payload) = summary.get(name).and_then(Value::as_object) {
            if let Some(count) = payload.get("count") {
                metrics.insert(format!("total_{name}"), json!(value_i64(count)?));
            }
            if let Some(covered) = payload.get("covered") {
                metrics.insert(format!("covered_{name}"), json!(value_i64(covered)?));
            }
        }
    }
    metrics.insert("llvm_summary".to_owned(), Value::Object(summary.clone()));
    Ok(metrics)
}

fn llvm_branch_counts(value: &Value) -> AppResult<(i64, i64, i64)> {
    if let Some(values) = value.as_array() {
        if values.len() < 6 {
            return Err(parse_error(
                "LLVM branch array must contain at least six values".to_owned(),
            ));
        }
        let line = value_i64(&values[0])?;
        if line < 1 {
            return Err(parse_error(format!(
                "LLVM branch line must be positive: {line}"
            )));
        }
        return Ok((line, value_i64(&values[4])?, value_i64(&values[5])?));
    }
    if let Some(object) = value.as_object() {
        let line = ["line", "line_start"]
            .iter()
            .find_map(|key| object.get(*key))
            .ok_or_else(|| parse_error("LLVM branch object is missing line".to_owned()))
            .and_then(value_i64)?;
        if line < 1 {
            return Err(parse_error(format!(
                "LLVM branch line must be positive: {line}"
            )));
        }
        let true_count = ["true_count", "trueCount"]
            .iter()
            .find_map(|key| object.get(*key))
            .ok_or_else(|| parse_error("LLVM branch object is missing true count".to_owned()))
            .and_then(value_i64)?;
        let false_count = ["false_count", "falseCount"]
            .iter()
            .find_map(|key| object.get(*key))
            .ok_or_else(|| parse_error("LLVM branch object is missing false count".to_owned()))
            .and_then(value_i64)?;
        return Ok((line, true_count, false_count));
    }
    Err(parse_error(
        "LLVM branch entry must be an array or object".to_owned(),
    ))
}

fn value_i64(value: &Value) -> AppResult<i64> {
    match value {
        Value::Null => Err(parse_error("coverage number must not be null".to_owned())),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(value)
            } else {
                let not_integer = Err(parse_error("coverage number is not an integer".to_owned()));
                number.as_f64().map(numeric_i64).unwrap_or(not_integer)
            }
        }
        Value::String(value) => parse_f64(value).and_then(numeric_i64),
        _ => Err(parse_error("coverage number must be numeric".to_owned())),
    }
}

fn safe_i64(value: Option<&str>) -> AppResult<i64> {
    let value = value.ok_or_else(|| parse_error("coverage number is missing".to_owned()))?;
    parse_f64(value).and_then(numeric_i64)
}

fn parse_f64(value: &str) -> AppResult<f64> {
    let value = value
        .parse::<f64>()
        .map_err(|_| parse_error(format!("invalid coverage number: {value}")))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(parse_error(format!(
            "coverage number must be finite: {value}"
        )))
    }
}

fn numeric_i64(value: f64) -> AppResult<i64> {
    const I64_MIN_AS_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_EXCLUSIVE_AS_F64: f64 = 9_223_372_036_854_775_808.0;
    if !(I64_MIN_AS_F64..I64_MAX_EXCLUSIVE_AS_F64).contains(&value) {
        return Err(parse_error(format!(
            "coverage number is outside the supported range: {value}"
        )));
    }
    if value.fract() != 0.0 {
        return Err(parse_error(format!(
            "coverage number is not an integer: {value}"
        )));
    }
    Ok(value as i64)
}

fn go_position_line(position: &str) -> AppResult<i64> {
    let (line, column) = position
        .split_once('.')
        .ok_or_else(|| parse_error(format!("Go coverprofile position is malformed: {position}")))?;
    let line = safe_i64(Some(line))?;
    let _column = safe_i64(Some(column))?;
    if line < 1 {
        return Err(parse_error(format!(
            "Go coverprofile line must be positive: {line}"
        )));
    }
    Ok(line)
}

fn checked_len_i64(value: usize, field: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| parse_error(format!("{field} exceeds the supported range")))
}

fn parse_error(message: String) -> AppError {
    AppError::Validation(format!("coverage parse error: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn lcov_parser_preserves_lines_and_branch_counts() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        writeln!(
            file,
            "TN:\nSF:src/a.py\nDA:1,2\nDA:2,0\nBRDA:2,0,0,1\nend_of_record"
        )
        .expect("write report");
        let report = parse_coverage_report(file.path(), "lcov", None).expect("parse report");
        assert_eq!(report.total_lines(), 2);
        assert_eq!(report.covered_lines(), 1);
        assert_eq!(report.total_branches(), 1);
        assert_eq!(report.covered_branches(), 1);
    }

    #[test]
    fn format_aliases_are_canonical() {
        assert_eq!(
            normalize_format("coverage.py").expect("format"),
            "coveragepy"
        );
        assert_eq!(normalize_format("nyc").expect("format"), "istanbul");
    }

    #[test]
    fn parser_strict_record_shapes_are_rejected_explicitly() {
        let directory = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, content: &str| {
            let path = directory.path().join(name);
            std::fs::write(&path, content).expect("write fixture");
            path
        };
        let bad = |name: &str, format: &str, content: &str| {
            assert!(
                parse_coverage_report(&write(name, content), format, None).is_err(),
                "{name} should be rejected"
            );
        };

        bad("missing-da-line.info", "lcov", "SF:a.py\nDA:\n");
        bad("missing-da-hits.info", "lcov", "SF:a.py\nDA:1,\n");
        bad("malformed-fn.info", "lcov", "SF:a.py\nFN:1\n");
        bad("malformed-fnda.info", "lcov", "SF:a.py\nFNDA:1\n");

        bad(
            "coveragepy-null-file.json",
            "coveragepy",
            r#"{"files":{"a.py":null}}"#,
        );
        let mut coverage_object = Map::new();
        assert!(
            coverage_array(&coverage_object, "missing")
                .unwrap()
                .is_none()
        );
        coverage_object.insert("null".to_owned(), Value::Null);
        assert!(coverage_array(&coverage_object, "null").unwrap().is_none());
        coverage_object.insert("bad".to_owned(), json!(true));
        assert!(coverage_array(&coverage_object, "bad").is_err());

        let statement_only = r#"{"a.js":{"statementMap":{"0":{"line":1}},"s":{"0":1}}}"#;
        assert!(
            parse_coverage_report(
                &write("statement-only.json", statement_only),
                "istanbul",
                None
            )
            .is_ok()
        );
        bad(
            "istanbul-missing-statement-map.json",
            "istanbul",
            r#"{"good.js":{"statementMap":{},"s":{}},"bad.js":{"s":{}}}"#,
        );
        bad(
            "istanbul-missing-statement-hits.json",
            "istanbul",
            r#"{"good.js":{"statementMap":{},"s":{}},"bad.js":{"statementMap":{}}}"#,
        );
        bad(
            "istanbul-null-file.json",
            "istanbul",
            r#"{"good.js":{"statementMap":{},"s":{}},"bad.js":null}"#,
        );
        bad(
            "istanbul-path-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"path":1}}"#,
        );
        assert!(location_line(&json!({"start":{}})).is_err());
        assert!(location_line(&json!({})).is_err());
        bad(
            "istanbul-missing-statement-hit.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{"0":{"line":1}},"s":{}}}"#,
        );
        bad(
            "istanbul-fnmap-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"fnMap":[],"f":{}}}"#,
        );
        bad(
            "istanbul-function-hits-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"fnMap":{},"f":[]}}"#,
        );
        assert!(
            parse_coverage_report(
                &write(
                    "istanbul-function-fallback.json",
                    r#"{"a.js":{"statementMap":{},"s":{},"fnMap":{"0":{"line":3}},"f":{"0":1}}}"#,
                ),
                "istanbul",
                None,
            )
            .is_ok()
        );
        bad(
            "istanbul-function-hit-missing.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"fnMap":{"0":{"line":3}},"f":{}}}"#,
        );
        bad(
            "istanbul-function-pair.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"fnMap":{}}}"#,
        );
        bad(
            "istanbul-branchmap-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"branchMap":[],"b":{}}}"#,
        );
        bad(
            "istanbul-branch-hits-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"branchMap":{},"b":[]}}"#,
        );
        bad(
            "istanbul-branch-hit-missing.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"branchMap":{"0":{"line":4}},"b":{}}}"#,
        );
        bad(
            "istanbul-branch-count-type.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"branchMap":{"0":{"line":4}},"b":{"0":1}}}"#,
        );
        bad(
            "istanbul-branch-pair.json",
            "istanbul",
            r#"{"a.js":{"statementMap":{},"s":{},"branchMap":{}}}"#,
        );

        bad("go-range-file.out", "go", "mode: set\nnot-a-range 1 1\n");
        bad(
            "go-range-separator.out",
            "go",
            "mode: set\na.go:1.1-2.1 2 1\n",
        );
        bad(
            "go-invalid-count.out",
            "go",
            "mode: set\na.go:2.1,1.1 -1 1\n",
        );
        bad(
            "go-large-range.out",
            "go",
            "mode: set\na.go:1.1,1000002.1 1 1\n",
        );
        bad(
            "go-position-format.out",
            "go",
            "mode: set\na.go:bad,1.1 1 1\n",
        );
        assert!(
            parse_coverage_report(&write("go-blank-line.out", "mode: set\n\n"), "go", None).is_ok()
        );
        assert!(go_position_line("bad").is_err());
        assert!(go_position_line("0.1").is_err());
        assert!(safe_i64(None).is_err());

        bad("llvm-unit-type.json", "llvm", r#"{"data":[1]}"#);
        bad("llvm-files-missing.json", "llvm", r#"{"data":[{}]}"#);
        bad("llvm-file-type.json", "llvm", r#"{"data":[{"files":[1]}]}"#);
        bad(
            "llvm-segments-type.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","segments":{}}]}]}"#,
        );
        bad(
            "llvm-segment-type.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","segments":[1]}]}]}"#,
        );
        bad(
            "llvm-short-segment.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","segments":[[1,0]]}]}]}"#,
        );
        bad(
            "llvm-branches-type.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","branches":{}}]}]}"#,
        );
        bad(
            "llvm-negative-branch.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","branches":[[0,0,0,0,1,0]]}]}]}"#,
        );
        bad(
            "llvm-summary-type.json",
            "llvm",
            r#"{"data":[{"files":[{"filename":"a.c","summary":[]}]}]}"#,
        );
        assert!(
            parse_coverage_report(
                &write(
                    "llvm-no-optional-fields.json",
                    r#"{"data":[{"files":[{"filename":"a.c"}]}]}"#
                ),
                "llvm",
                None,
            )
            .is_ok()
        );

        let mut builder = CoverageBuilder::new(None);
        builder.add_file_metrics("a.py", Map::new());
        let _ = builder.build("test", "test", Vec::new(), Value::Null);
    }

    #[test]
    fn parser_edge_inputs_cover_detection_and_error_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, content: &str| {
            let path = directory.path().join(name);
            std::fs::write(&path, content).expect("write fixture");
            path
        };
        let oversized = directory.path().join("oversized.lcov");
        std::fs::File::create(&oversized)
            .expect("create oversized fixture")
            .set_len(MAX_COVERAGE_REPORT_BYTES + 1)
            .expect("size oversized fixture");
        assert!(parse_coverage_report(&oversized, "lcov", None).is_err());
        let empty_lcov = write("empty.lcov", "TN:\n\nignored\nend_of_record\n");
        let report = parse_coverage_report(&empty_lcov, "auto", None).expect("empty lcov");
        assert_eq!(report.warnings.len(), 1);
        let rich_lcov = write(
            "rich.info",
            "ignored\nSF:a.py\nFN:1,func\nUNKNOWN:ignored\nFNDA:2,func\nFNDA:1,missing\nBRDA:1,0,0,-\nBRDA:1,0,1,2\nDA:1,2\nend_of_record\n",
        );
        let report = parse_coverage_report(&rich_lcov, "lcov", None).expect("rich lcov");
        assert!(report.total_functions() >= 1);
        assert!(report.total_branches() >= 1);
        let malformed_lcov = write("malformed.info", "SF:a.py\nDA:not,a\nend_of_record\n");
        assert!(parse_coverage_report(&malformed_lcov, "lcov", None).is_err());
        let malformed_branch_lcov = write(
            "malformed-branch.info",
            "SF:a.py\nBRDA:1,0\nend_of_record\n",
        );
        assert!(parse_coverage_report(&malformed_branch_lcov, "lcov", None).is_err());

        let coveragepy = write(
            "coverage.json",
            r#"{"files":{"a.py":{"executed_lines":[1],"missing_lines":[2],"summary":{"covered_lines":1},"executed_branches":[[1,0]],"missing_branches":[[2,0]]},"edge.py":{"executed_lines":[],"missing_lines":[],"executed_branches":[],"missing_branches":null},"non-numeric":{"executed_lines":[],"missing_lines":null},"no-lines":{}},"meta":{"version":1}}"#,
        );
        let report = parse_coverage_report(&coveragepy, "auto", None).expect("coveragepy");
        assert_eq!(report.format, "coveragepy");
        assert!(parse_coverage_report(&coveragepy, "coveragepy", None).is_ok());
        let bad_coveragepy = write("bad-coverage.json", "{}");
        assert!(parse_coverage_report(&bad_coveragepy, "coveragepy", None).is_err());
        assert!(parse_coverage_report(&bad_coveragepy, "auto", None).is_err());
        let malformed_coveragepy = write(
            "malformed-coverage.json",
            r#"{"files":{"a.py":{"executed_lines":["bad"]}}}"#,
        );
        assert!(parse_coverage_report(&malformed_coveragepy, "coveragepy", None).is_err());
        let malformed_branch_type = write(
            "malformed-branch-type.json",
            r#"{"files":{"a.py":{"executed_branches":[{}]}}}"#,
        );
        assert!(parse_coverage_report(&malformed_branch_type, "coveragepy", None).is_err());
        let malformed_branch_line = write(
            "malformed-branch-line.json",
            r#"{"files":{"a.py":{"executed_branches":[[]]}}}"#,
        );
        assert!(parse_coverage_report(&malformed_branch_line, "coveragepy", None).is_err());
        let scalar_json = write("scalar.json", "[]");
        assert!(parse_coverage_report(&scalar_json, "auto", None).is_err());

        let cobertura = write(
            "branch.xml",
            r#"<coverage><class name="fallback.py" line-rate="0.5" branch-rate="0.25"><lines><line number="1" hits="1" branch="true" branches-valid="4" branches-covered="3"/><line number="2" hits="0" branch="true" condition-coverage="50% (1/2)"/><line number="3" hits="0" branch="false"/></lines></class></coverage>"#,
        );
        let report = parse_coverage_report(&cobertura, "auto", None).expect("cobertura");
        assert_eq!(report.format, "cobertura");
        let malformed_cobertura = write(
            "malformed-cobertura.xml",
            r#"<coverage><class><lines><line number="1" hits="0"/></lines></class></coverage>"#,
        );
        assert!(parse_coverage_report(&malformed_cobertura, "cobertura", None).is_err());
        let jacoco = write(
            "plain.xml",
            r#"<report><package name=""><sourcefile name="A.java"><line nr="1" mi="1" ci="0" mb="0" cb="1"/></sourcefile></package><counter type="LINE" missed="0" covered="1"/></report>"#,
        );
        assert_eq!(
            parse_coverage_report(&jacoco, "auto", None).unwrap().format,
            "jacoco"
        );
        let jacoco_missing_name = write(
            "missing-name.xml",
            r#"<report><package><sourcefile/></package></report>"#,
        );
        assert!(parse_coverage_report(&jacoco_missing_name, "jacoco", None).is_err());
        let bad_xml = write("bad.xml", "<report>");
        assert!(parse_coverage_report(&bad_xml, "auto", None).is_err());
        let unknown_xml = write("unknown.xml", "<unknown/>");
        assert!(parse_coverage_report(&unknown_xml, "auto", None).is_err());

        let istanbul = write(
            "istanbul.json",
            r#"{"a.js":{"statementMap":{"0":{"start":{"line":1}},"1":{"line":2}},"s":{"0":1,"1":0},"fnMap":{"0":{"decl":{"line":2}}},"f":{"0":1},"branchMap":{"0":{"line":1}},"b":{"0":[1,0]}}}"#,
        );
        let report = parse_coverage_report(&istanbul, "auto", None).expect("istanbul");
        assert_eq!(report.format, "istanbul");
        let bad_istanbul = write("bad-istanbul.json", "[]");
        assert!(parse_coverage_report(&bad_istanbul, "istanbul", None).is_err());
        let not_istanbul = write("not-istanbul.json", r#"{"a.js":{}}"#);
        assert!(parse_coverage_report(&not_istanbul, "istanbul", None).is_err());
        let mixed_istanbul = write(
            "mixed-istanbul.json",
            r#"{"null.js":null,"missing-map":{"s":{"0":1}},"missing-location":{"statementMap":{"0":{}},"s":{"0":1}},"missing-start-line":{"statementMap":{"0":{"start":{}}},"s":{"0":1}},"bad-location":{"statementMap":{"0":null},"s":{}},"missing-statement-hit":{"statementMap":{"0":{"line":1},"1":{"line":2}},"s":{"0":1}},"missing-function":{"statementMap":{},"s":{},"fnMap":{"0":{}},"f":{"0":1}},"missing-branch":{"statementMap":{},"s":{},"branchMap":{"0":{}},"b":{"0":[1]}}}"#,
        );
        assert!(parse_coverage_report(&mixed_istanbul, "istanbul", None).is_err());
        let go = write(
            "cover.out",
            "mode: set\na.go:1.1,2.1 2 1\na.go:3.1,3.1 1 0\n",
        );
        assert_eq!(
            parse_coverage_report(&go, "auto", None).unwrap().format,
            "go"
        );
        let bad_go = write("bad.out", "mode-nope\n");
        assert!(parse_coverage_report(&bad_go, "go", None).is_err());
        let malformed_go = write("malformed-cover.out", "mode: set\nshort\n");
        assert!(parse_coverage_report(&malformed_go, "go", None).is_err());
        let llvm = write(
            "llvm.json",
            r#"{"data":[{"files":[{"filename":"a.c","segments":[[1,0,2,true],[2,0,0,false]],"branches":[[4,0,0,0,1,0],{"line_start":5,"trueCount":1,"falseCount":0}],"summary":{"lines":{"count":2,"covered":1},"branches":{"count":2,"covered":1},"functions":{"covered":1},"regions":{"count":2}}},{"filename":"no-summary.c","segments":[]}]}]}"#,
        );
        assert_eq!(
            parse_coverage_report(&llvm, "auto", None).unwrap().format,
            "llvm"
        );
        let llvm_edges = write(
            "llvm-edges.json",
            r#"{"data":[{"files":[{}, {"filename":"edge.c","segments":[null,[1,0,1,true]],"branches":[],"summary":{}}, {"filename":"no-segments.c","branches":{},"summary":{}}]}]}"#,
        );
        assert!(parse_coverage_report(&llvm_edges, "llvm", None).is_err());
        let llvm_bad_flag = write(
            "llvm-bad-flag.json",
            r#"{"data":[{"files":[{"filename":"a.c","segments":[[1,0,1,1]]}]}]}"#,
        );
        assert!(parse_coverage_report(&llvm_bad_flag, "llvm", None).is_err());
        let no_data = write("no-data.json", "{}");
        assert!(parse_coverage_report(&no_data, "auto", None).is_err());
        assert!(parse_coverage_report(&no_data, "llvm", None).is_err());
        let plain = write("plain.txt", "coverage");
        assert!(parse_coverage_report(&plain, "auto", None).is_err());

        let counter_xml = write(
            "counter.xml",
            r#"<report><counter type="LINE" missed="0" covered="1"/></report>"#,
        );
        assert_eq!(
            parse_coverage_report(&counter_xml, "auto", None)
                .unwrap()
                .format,
            "jacoco"
        );
        let invalid_xml = write("invalid.xml", "<root attr='unterminated>");
        assert!(parse_coverage_report(&invalid_xml, "xml", None).is_err());
        assert!(parse_xml(&invalid_xml).is_err());
        assert!(parse_xml(directory.path()).is_err());
        assert!(parse_xml(Path::new("\0")).is_err());
        let invalid_attribute = write("invalid-attribute.xml", "<root =\"bad\"/>");
        assert!(parse_xml(&invalid_attribute).is_err());
        let invalid_escape = write("invalid-escape.xml", "<root attr=\"&not-an-entity;\"/>");
        assert!(parse_xml(&invalid_escape).is_err());
        let comment_xml = write("comment.xml", "<!-- comment -->");
        assert!(parse_xml(&comment_xml).is_err());
        let unmatched_end = write("unmatched-end.xml", "</root>");
        assert!(parse_xml(&unmatched_end).is_err());
        let mut empty_stack = Vec::new();
        let mut empty_root = None;
        append_xml_end(&mut empty_stack, &mut empty_root);
        assert!(empty_root.is_none());

        let unsupported = write("unsupported.lcov", "TN:\n");
        assert!(parse_coverage_report(&unsupported, "lcov", None).is_ok());
        assert!(parse_selected("unsupported", &unsupported, None).is_err());
    }

    #[test]
    fn parser_private_helpers_cover_numeric_xml_and_branch_variants() {
        let node = XmlNode {
            name: "line".to_owned(),
            attrs: BTreeMap::new(),
            children: vec![],
        };
        assert_eq!(xml_name(b"{urn}line"), "line");
        assert_eq!(find_nodes(&node, "line").count(), 1);
        assert!(descendants(&node, "line").is_empty());
        assert_eq!(cobertura_branch_counts(&node).unwrap(), (0, 0));
        let mut attrs = BTreeMap::new();
        attrs.insert("branch".to_owned(), "true".to_owned());
        attrs.insert("condition-coverage".to_owned(), "50% (1/2)".to_owned());
        let branch = XmlNode {
            attrs,
            ..node.clone()
        };
        assert_eq!(cobertura_branch_counts(&branch).unwrap(), (2, 1));
        let mut explicit = branch.clone();
        explicit
            .attrs
            .insert("branches-valid".to_owned(), "4".to_owned());
        explicit
            .attrs
            .insert("branches-covered".to_owned(), "3".to_owned());
        assert_eq!(cobertura_branch_counts(&explicit).unwrap(), (4, 3));
        let mut malformed_branch = branch.clone();
        malformed_branch
            .attrs
            .insert("condition-coverage".to_owned(), "unknown".to_owned());
        assert!(cobertura_branch_counts(&malformed_branch).is_err());
        let mut incomplete_branch = branch.clone();
        incomplete_branch
            .attrs
            .insert("branches-valid".to_owned(), "2".to_owned());
        incomplete_branch.attrs.remove("condition-coverage");
        assert!(cobertura_branch_counts(&incomplete_branch).is_err());
        let mut branch_without_counts = node.clone();
        branch_without_counts
            .attrs
            .insert("branch".to_owned(), "true".to_owned());
        assert_eq!(
            cobertura_branch_counts(&branch_without_counts).unwrap(),
            (0, 0)
        );
        assert_eq!(location_line(&json!({"start":{"line":3}})).unwrap(), 3);
        assert_eq!(location_line(&json!({"line":4})).unwrap(), 4);
        assert!(location_line(&json!({"line":0})).is_err());
        assert!(value_i64(&json!(2.5)).is_err());
        assert!(value_i64(&json!("3.5")).is_err());
        assert!(value_i64(&json!(true)).is_err());
        assert!(safe_i64(Some("4.5")).is_err());
        assert!(safe_i64(Some("bad")).is_err());
        assert_eq!(
            numeric_line_numbers(&[json!(1), json!(2)])
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(numeric_line_numbers(&[json!(1), json!("bad")]).is_err());
        assert_eq!(
            llvm_branch_counts(&json!([1, 0, 0, 0, 2, 3])).unwrap(),
            (1, 2, 3)
        );
        assert_eq!(
            llvm_branch_counts(&json!({"line":2,"true_count":1,"false_count":0})).unwrap(),
            (2, 1, 0)
        );
        assert!(llvm_branch_counts(&json!({"line_start":0,"trueCount":1,"falseCount":1})).is_err());
        assert!(llvm_branch_counts(&Value::Null).is_err());
        assert!(llvm_branch_counts(&json!([1, 2])).is_err());
        assert!(llvm_branch_counts(&json!({"line": 2})).is_err());
        assert!(llvm_branch_counts(&json!({"trueCount": 1})).is_err());
        assert!(llvm_branch_counts(&json!({"line": 2, "trueCount": 1})).is_err());
        assert!(value_i64(&Value::Null).is_err());
        assert!(parse_f64("inf").is_err());
        assert!(numeric_i64(9_223_372_036_854_775_808.0).is_err());
        assert!(checked_len_i64(usize::MAX, "test count").is_err());
        let metrics = llvm_summary_metrics(
            json!({"lines":{"count":2,"covered":1},"branches":{"count":3,"covered":2},"functions":{"count":4,"covered":3},"regions":{"count":5,"covered":4},"other":{}})
                .as_object()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metrics["total_lines"], 2);
        assert_eq!(metrics["covered_lines"], 1);
        assert_eq!(metrics["total_branches"], 3);
        assert_eq!(metrics["covered_branches"], 2);
        assert_eq!(metrics["total_functions"], 4);
        assert_eq!(metrics["covered_functions"], 3);
        assert_eq!(metrics["total_regions"], 5);
        assert_eq!(metrics["covered_regions"], 4);
        assert!(metrics["llvm_summary"].is_object());
        let partial_metrics = llvm_summary_metrics(
            json!({"functions":{"covered":3},"regions":{"count":5}})
                .as_object()
                .unwrap(),
        )
        .unwrap();
        assert!(partial_metrics.get("total_functions").is_none());
        assert!(partial_metrics.get("covered_regions").is_none());
        assert!(
            parse_error("x".to_owned())
                .to_string()
                .contains("coverage parse error")
        );
    }
}
