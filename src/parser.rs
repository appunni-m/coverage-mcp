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
            builder.add_line(
                file,
                safe_i64(parts.next()),
                safe_i64(parts.next()),
                None,
                true,
                0,
                0,
                0,
                0,
                json!({}),
            );
        } else if let Some(payload) = line.strip_prefix("FN:") {
            if let Some((line_number, name)) = payload.split_once(',') {
                function_lines.insert(name.to_owned(), safe_i64(Some(line_number)));
            }
        } else if let Some(payload) = line.strip_prefix("FNDA:") {
            if let Some((hits, name)) = payload.split_once(',') {
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
                        i64::from(safe_i64(Some(hits)) > 0),
                        json!({}),
                    );
                }
            }
        } else if let Some(payload) = line.strip_prefix("BRDA:") {
            add_lcov_branch(&mut builder, file, payload);
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

fn add_lcov_branch(builder: &mut CoverageBuilder, file: &str, payload: &str) {
    let parts: Vec<&str> = payload.split(',').collect();
    if parts.len() < 4 {
        return;
    }
    let taken = parts[3];
    let covered = if taken == "-" {
        0
    } else {
        i64::from(safe_i64(Some(taken)) > 0)
    };
    builder.add_line(
        file,
        safe_i64(parts.first().copied()),
        0,
        Some(false),
        false,
        1,
        covered,
        0,
        0,
        json!({}),
    );
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
        let Some(payload) = payload.as_object() else {
            continue;
        };
        let mut line_numbers = Vec::new();
        for key in ["executed_lines", "missing_lines"] {
            if let Some(lines) = payload.get(key).and_then(Value::as_array) {
                line_numbers.extend(numeric_line_numbers(lines));
            }
        }
        line_numbers.sort_unstable();
        line_numbers.dedup();
        let executed: std::collections::BTreeSet<i64> = payload
            .get("executed_lines")
            .and_then(Value::as_array)
            .map(|lines| lines.iter().filter_map(Value::as_i64).collect())
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
            if let Some(values) = payload.get(key).and_then(Value::as_array) {
                for value in values {
                    if let Some(line) = value
                        .as_array()
                        .and_then(|items| items.first())
                        .and_then(Value::as_i64)
                    {
                        let entry = branches.entry(line).or_default();
                        if covered {
                            entry.1 += 1;
                        } else {
                            entry.0 += 1;
                        }
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

fn numeric_line_numbers(values: &[Value]) -> impl Iterator<Item = i64> + '_ {
    values.iter().filter_map(Value::as_i64)
}

fn parse_cobertura(path: &Path, repo_path: Option<&str>) -> AppResult<CoverageReport> {
    let root = parse_xml(path)?;
    let mut builder = CoverageBuilder::new(repo_path);
    for class_node in find_nodes(&root, "class") {
        let Some(file_path) = class_node
            .attrs
            .get("filename")
            .or_else(|| class_node.attrs.get("name"))
        else {
            continue;
        };
        for line_node in descendants(class_node, "line") {
            let line_number = safe_i64(line_node.attrs.get("number").map(String::as_str));
            let hits = safe_i64(line_node.attrs.get("hits").map(String::as_str));
            let (total_branches, covered_branches) = cobertura_branch_counts(line_node);
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
        if let Some(value) = class_node
            .attrs
            .get("line-rate")
            .and_then(|value| value.parse::<f64>().ok())
        {
            metrics.insert("line_rate".to_owned(), json!(value));
        }
        if let Some(value) = class_node
            .attrs
            .get("branch-rate")
            .and_then(|value| value.parse::<f64>().ok())
        {
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
            let Some(name) = source.attrs.get("name") else {
                continue;
            };
            let file_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            for line_node in descendants(source, "line") {
                let missed_instructions = safe_i64(line_node.attrs.get("mi").map(String::as_str));
                let covered_instructions = safe_i64(line_node.attrs.get("ci").map(String::as_str));
                let missed_branches = safe_i64(line_node.attrs.get("mb").map(String::as_str));
                let covered_branches = safe_i64(line_node.attrs.get("cb").map(String::as_str));
                builder.add_line(
                    &file_path,
                    safe_i64(line_node.attrs.get("nr").map(String::as_str)),
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
        let Some(payload) = payload.as_object() else {
            continue;
        };
        let Some(statement_map) = payload.get("statementMap").and_then(Value::as_object) else {
            continue;
        };
        let statement_hits = payload.get("s").and_then(Value::as_object);
        let file_path = payload.get("path").and_then(Value::as_str).unwrap_or(key);
        for (statement_id, location) in statement_map {
            let Some(line) = location_line(location) else {
                continue;
            };
            let hits = statement_hits
                .and_then(|values| values.get(statement_id))
                .map(value_i64)
                .unwrap_or(0);
            builder.add_line(file_path, line, hits, None, true, 0, 0, 0, 0, json!({}));
        }
        let function_map = payload.get("fnMap").and_then(Value::as_object);
        let function_hits = payload.get("f").and_then(Value::as_object);
        if let Some(function_map) = function_map {
            for (function_id, function) in function_map {
                let line = function
                    .get("loc")
                    .and_then(location_line)
                    .or_else(|| function.get("decl").and_then(location_line))
                    .or_else(|| location_line(function));
                let Some(line) = line else {
                    continue;
                };
                let hits = function_hits
                    .and_then(|values| values.get(function_id))
                    .map(value_i64)
                    .unwrap_or(0);
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
        let branch_map = payload.get("branchMap").and_then(Value::as_object);
        let branch_hits = payload.get("b").and_then(Value::as_object);
        if let Some(branch_map) = branch_map {
            for (branch_id, branch) in branch_map {
                let line = branch
                    .get("loc")
                    .and_then(location_line)
                    .or_else(|| location_line(branch));
                let Some(line) = line else {
                    continue;
                };
                let counts = branch_hits
                    .and_then(|values| values.get(branch_id))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let total = counts.len() as i64;
                let covered = counts.iter().filter(|value| value_i64(value) > 0).count() as i64;
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
        let parts: Vec<&str> = raw.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        let Some((file, range)) = parts[0].rsplit_once(':') else {
            continue;
        };
        let Some((start, end)) = range.split_once(',') else {
            continue;
        };
        let start_line = safe_i64(start.split('.').next());
        let end_line = safe_i64(end.split('.').next());
        let hits = safe_i64(parts.last().copied());
        for line in start_line..=end_line.max(start_line) {
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
    if let Some(units) = data.get("data").and_then(Value::as_array) {
        for unit in units {
            for file_payload in unit
                .get("files")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(file_path) = file_payload.get("filename").and_then(Value::as_str) else {
                    continue;
                };
                if let Some(segments) = file_payload.get("segments").and_then(Value::as_array) {
                    for segment in segments {
                        let Some(values) = segment.as_array() else {
                            continue;
                        };
                        if values.len() < 4 || !values[3].as_bool().unwrap_or(false) {
                            continue;
                        }
                        let line = value_i64(values.first().unwrap_or(&Value::Null));
                        let hits = value_i64(values.get(2).unwrap_or(&Value::Null));
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
                if let Some(branches) = file_payload.get("branches").and_then(Value::as_array) {
                    for branch in branches {
                        let (line, true_count, false_count) = llvm_branch_counts(branch);
                        let Some(line) = line else {
                            continue;
                        };
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
                if let Some(summary) = file_payload.get("summary").and_then(Value::as_object) {
                    builder.add_file_metrics(file_path, llvm_summary_metrics(summary));
                }
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

fn cobertura_branch_counts(node: &XmlNode) -> (i64, i64) {
    if !node
        .attrs
        .get("branch")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
    {
        return (0, 0);
    }
    if let (Some(total), Some(covered)) = (
        node.attrs.get("branches-valid"),
        node.attrs.get("branches-covered"),
    ) {
        return (safe_i64(Some(total)), safe_i64(Some(covered)));
    }
    let value = node
        .attrs
        .get("condition-coverage")
        .cloned()
        .unwrap_or_default();
    let value = value
        .trim_matches(|character| character != '(' && character != ')')
        .trim_matches(['(', ')']);
    if let Some((covered, total)) = value.split_once('/') {
        return (safe_i64(Some(total)), safe_i64(Some(covered)));
    }
    (0, 0)
}

fn looks_like_istanbul(object: &Map<String, Value>) -> bool {
    object
        .values()
        .any(|value| value.get("statementMap").is_some() && value.get("s").is_some())
}

fn location_line(value: &Value) -> Option<i64> {
    let object = value.as_object()?;
    if let Some(start) = object.get("start").and_then(Value::as_object) {
        let line = value_i64(start.get("line").unwrap_or(&Value::Null));
        return (line > 0).then_some(line);
    }
    let line = value_i64(object.get("line").unwrap_or(&Value::Null));
    (line > 0).then_some(line)
}

fn llvm_summary_metrics(summary: &Map<String, Value>) -> Map<String, Value> {
    let mut metrics = Map::new();
    for name in ["lines", "branches", "functions", "regions"] {
        if let Some(payload) = summary.get(name).and_then(Value::as_object) {
            if let Some(count) = payload.get("count") {
                metrics.insert(format!("total_{name}"), json!(value_i64(count)));
            }
            if let Some(covered) = payload.get("covered") {
                metrics.insert(format!("covered_{name}"), json!(value_i64(covered)));
            }
        }
    }
    metrics.insert("llvm_summary".to_owned(), Value::Object(summary.clone()));
    metrics
}

fn llvm_branch_counts(value: &Value) -> (Option<i64>, i64, i64) {
    if let Some(values) = value.as_array() {
        if values.len() >= 6 {
            return (
                Some(value_i64(&values[0])),
                value_i64(&values[4]),
                value_i64(&values[5]),
            );
        }
    }
    if let Some(object) = value.as_object() {
        return (
            ["line", "line_start"]
                .iter()
                .find_map(|key| object.get(*key))
                .map(value_i64)
                .filter(|line| *line > 0),
            ["true_count", "trueCount"]
                .iter()
                .find_map(|key| object.get(*key))
                .map(value_i64)
                .unwrap_or(0),
            ["false_count", "falseCount"]
                .iter()
                .find_map(|key| object.get(*key))
                .map(value_i64)
                .unwrap_or(0),
        );
    }
    (None, 0, 0)
}

fn value_i64(value: &Value) -> i64 {
    value
        .as_i64()
        .or_else(|| value.as_f64().map(|number| number as i64))
        .or_else(|| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .map(|number| number as i64)
        })
        .unwrap_or(0)
}

fn safe_i64(value: Option<&str>) -> i64 {
    value
        .and_then(|raw| raw.parse::<f64>().ok())
        .map(|number| number as i64)
        .unwrap_or(0)
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
    fn parser_edge_inputs_cover_detection_and_error_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, content: &str| {
            let path = directory.path().join(name);
            std::fs::write(&path, content).expect("write fixture");
            path
        };
        let empty_lcov = write("empty.lcov", "TN:\n\nignored\nend_of_record\n");
        let report = parse_coverage_report(&empty_lcov, "auto", None).expect("empty lcov");
        assert_eq!(report.warnings.len(), 1);
        let rich_lcov = write(
            "rich.info",
            "ignored\nSF:a.py\nFN:bad\nFN:1,func\nUNKNOWN:ignored\nFNDA:2,func\nFNDA:bad\nFNDA:1,missing\nBRDA:1,0,0,-\nBRDA:1,0,1,2\nBRDA:1,2\nDA:1,2\nDA:not,a\nend_of_record\n",
        );
        let report = parse_coverage_report(&rich_lcov, "lcov", None).expect("rich lcov");
        assert!(report.total_functions() >= 1);
        assert!(report.total_branches() >= 1);

        let coveragepy = write(
            "coverage.json",
            r#"{"files":{"a.py":{"executed_lines":[1,"bad"],"missing_lines":[2,"bad"],"summary":{"covered_lines":1},"executed_branches":[[1,0],"bad"],"missing_branches":[[2,0],null]},"edge.py":{"executed_lines":[],"missing_lines":[],"executed_branches":{},"missing_branches":null},"non-numeric":{"executed_lines":[],"missing_lines":[{}]},"no-lines":{},"ignored":null},"meta":{"version":1}}"#,
        );
        let report = parse_coverage_report(&coveragepy, "auto", None).expect("coveragepy");
        assert_eq!(report.format, "coveragepy");
        assert!(parse_coverage_report(&coveragepy, "coveragepy", None).is_ok());
        let bad_coveragepy = write("bad-coverage.json", "{}");
        assert!(parse_coverage_report(&bad_coveragepy, "coveragepy", None).is_err());
        assert!(parse_coverage_report(&bad_coveragepy, "auto", None).is_err());
        let scalar_json = write("scalar.json", "[]");
        assert!(parse_coverage_report(&scalar_json, "auto", None).is_err());

        let cobertura = write(
            "branch.xml",
            r#"<coverage><class name="fallback.py" line-rate="0.5" branch-rate="0.25"><lines><line number="1" hits="1" branch="true" branches-valid="4" branches-covered="3"/><line number="2" hits="0" branch="true" condition-coverage="50% (1/2)"/><line number="3" hits="0" branch="false"/></lines></class><class><lines><line number="4" hits="0"/></lines></class></coverage>"#,
        );
        let report = parse_coverage_report(&cobertura, "auto", None).expect("cobertura");
        assert_eq!(report.format, "cobertura");
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
        assert!(parse_coverage_report(&jacoco_missing_name, "jacoco", None).is_ok());
        let bad_xml = write("bad.xml", "<report>");
        assert!(parse_coverage_report(&bad_xml, "auto", None).is_err());
        let unknown_xml = write("unknown.xml", "<unknown/>");
        assert!(parse_coverage_report(&unknown_xml, "auto", None).is_err());

        let istanbul = write(
            "istanbul.json",
            r#"{"a.js":{"statementMap":{"0":{"start":{"line":1}},"1":{"line":2},"2":{}},"s":{"0":1,"1":0},"fnMap":{"0":{"decl":{"line":2}},"1":{"line":3}},"f":{"0":1},"branchMap":{"0":{"line":1},"1":{}},"b":{"0":[1,0]}}}"#,
        );
        let report = parse_coverage_report(&istanbul, "auto", None).expect("istanbul");
        assert_eq!(report.format, "istanbul");
        let bad_istanbul = write("bad-istanbul.json", "[]");
        assert!(parse_coverage_report(&bad_istanbul, "istanbul", None).is_err());
        let not_istanbul = write("not-istanbul.json", r#"{"a.js":{}}"#);
        assert!(parse_coverage_report(&not_istanbul, "istanbul", None).is_err());
        let mixed_istanbul = write(
            "mixed-istanbul.json",
            r#"{"null.js":null,"missing-map":{"s":{"0":1}},"missing-location":{"statementMap":{"0":{}},"s":{"0":1}},"missing-function":{"statementMap":{},"s":{},"fnMap":{"0":{}},"f":{"0":1}},"missing-branch":{"statementMap":{},"s":{},"branchMap":{"0":{}},"b":{"0":[1]}}}"#,
        );
        assert!(parse_coverage_report(&mixed_istanbul, "istanbul", None).is_ok());
        let go = write(
            "cover.out",
            "mode: set\nshort\nbad 1 2\na.go:bad 2 1\na.go:1.1,2.1 2 1\na.go:3.1 1 0\n",
        );
        assert_eq!(
            parse_coverage_report(&go, "auto", None).unwrap().format,
            "go"
        );
        let bad_go = write("bad.out", "mode-nope\n");
        assert!(parse_coverage_report(&bad_go, "go", None).is_err());
        let llvm = write(
            "llvm.json",
            r#"{"data":[{"files":[{"filename":"a.c","segments":[[1,0,2,true],[2,0,0,false],[3,0]],"branches":[[4,0,0,0,1,0],{"line_start":5,"trueCount":1,"falseCount":0},null],"summary":{"lines":{"count":2,"covered":1},"branches":{"count":2,"covered":1},"functions":{"covered":1},"regions":{"count":2}}},{"filename":"no-summary.c","segments":[]}]}]}"#,
        );
        assert_eq!(
            parse_coverage_report(&llvm, "auto", None).unwrap().format,
            "llvm"
        );
        let llvm_edges = write(
            "llvm-edges.json",
            r#"{"data":[{"files":[{}, {"filename":"edge.c","segments":[null,[1,0,1,true]],"branches":[],"summary":{}}, {"filename":"no-segments.c","branches":{},"summary":{}}]}]}"#,
        );
        assert!(parse_coverage_report(&llvm_edges, "llvm", None).is_ok());
        let no_data = write("no-data.json", "{}");
        assert!(parse_coverage_report(&no_data, "auto", None).is_err());
        assert!(parse_coverage_report(&no_data, "llvm", None).is_ok());
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
        assert_eq!(cobertura_branch_counts(&node), (0, 0));
        let mut attrs = BTreeMap::new();
        attrs.insert("branch".to_owned(), "true".to_owned());
        attrs.insert("condition-coverage".to_owned(), "50% (1/2)".to_owned());
        let branch = XmlNode {
            attrs,
            ..node.clone()
        };
        assert_eq!(cobertura_branch_counts(&branch), (2, 1));
        let mut explicit = branch.clone();
        explicit
            .attrs
            .insert("branches-valid".to_owned(), "4".to_owned());
        explicit
            .attrs
            .insert("branches-covered".to_owned(), "3".to_owned());
        assert_eq!(cobertura_branch_counts(&explicit), (4, 3));
        let mut malformed_branch = branch.clone();
        malformed_branch
            .attrs
            .insert("condition-coverage".to_owned(), "unknown".to_owned());
        assert_eq!(cobertura_branch_counts(&malformed_branch), (0, 0));
        assert_eq!(location_line(&json!({"start":{"line":3}})), Some(3));
        assert_eq!(location_line(&json!({"line":4})), Some(4));
        assert_eq!(location_line(&json!({"line":0})), None);
        assert_eq!(value_i64(&json!(2.5)), 2);
        assert_eq!(value_i64(&json!("3.5")), 3);
        assert_eq!(value_i64(&json!(true)), 0);
        assert_eq!(safe_i64(Some("4.5")), 4);
        assert_eq!(safe_i64(Some("bad")), 0);
        assert_eq!(
            numeric_line_numbers(&[json!(1), json!("bad"), json!(2)]).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            llvm_branch_counts(&json!([1, 0, 0, 0, 2, 3])),
            (Some(1), 2, 3)
        );
        assert_eq!(
            llvm_branch_counts(&json!({"line":2,"true_count":1,"false_count":0})),
            (Some(2), 1, 0)
        );
        assert_eq!(
            llvm_branch_counts(&json!({"line_start":0,"trueCount":1,"falseCount":1})),
            (None, 1, 1)
        );
        assert_eq!(llvm_branch_counts(&Value::Null), (None, 0, 0));
        assert_eq!(llvm_branch_counts(&json!([1, 2])), (None, 0, 0));
        let metrics = llvm_summary_metrics(
            json!({"lines":{"count":2,"covered":1},"branches":{"count":3,"covered":2},"functions":{"count":4,"covered":3},"regions":{"count":5,"covered":4},"other":{}})
                .as_object()
                .unwrap(),
        );
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
        );
        assert!(partial_metrics.get("total_functions").is_none());
        assert!(partial_metrics.get("covered_regions").is_none());
        assert!(
            parse_error("x".to_owned())
                .to_string()
                .contains("coverage parse error")
        );
    }
}
