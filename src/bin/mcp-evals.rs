//! Opt-in, deterministic evaluation runner for the agent-facing MCP contract.
//!
//! This binary is intentionally separate from the default test and CI lanes.
//! It creates a temporary Git repository and runs outcome, usability, safety,
//! reliability, and efficiency checks against the shared MCP dispatcher.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coverage_mcp::config::ServerConfig;
use coverage_mcp::mcp;
use coverage_mcp::service::{CoverageService, RequestContext, serialized_word_count};
use coverage_mcp::storage::CoverageStore;
use coverage_mcp::{SCHEMA_REVISION, VERSION};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CASES_JSON: &str = include_str!("../../evals/cases.json");
const REPORT_SCHEMA: &str = "coverage-mcp/eval-report@1";

fn main() {
    if let Err(error) = run() {
        eprintln!("mcp-evals: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let Some(report_path) = report_path()? else {
        return Ok(());
    };
    let cases = load_cases()?;
    let fixture = Fixture::new()?;
    let started = Instant::now();

    let suites = vec![
        run_section(
            "usability",
            "Independent usability and discovery",
            |section| evaluate_usability(section, &cases),
        ),
        run_section("outcomes", "Outcome-driven coverage workflows", |section| {
            evaluate_outcomes(section, &fixture)
        }),
        run_section(
            "efficiency",
            "Confusion, token, and compute efficiency",
            |section| evaluate_efficiency(section, &fixture),
        ),
        run_section(
            "protocol",
            "Protocol, envelope, and resource contract",
            |section| evaluate_protocol(section, &fixture),
        ),
        run_section(
            "safety",
            "Validation, safety, and no-silent-fallback behavior",
            |section| evaluate_safety(section, &fixture),
        ),
        run_section(
            "reliability",
            "Managed-run reliability and targeted evidence",
            |section| evaluate_reliability(section, &fixture),
        ),
    ];

    let summary = Summary::from_sections(&suites);
    let report = EvalReport {
        schema: REPORT_SCHEMA.to_owned(),
        version: VERSION.to_owned(),
        schema_revision: SCHEMA_REVISION,
        cases_schema: cases.schema,
        suites,
        summary,
    };
    write_report(&report_path, &report)?;

    println!(
        "MCP evals: {}/{} checks passed in {} ms; report: {}",
        report.summary.passed,
        report.summary.checks,
        started.elapsed().as_millis(),
        report_path.display()
    );
    for suite in &report.suites {
        println!(
            "  {:<12} {}/{} checks passed",
            suite.id,
            suite.passed,
            suite.checks.len()
        );
        for check in suite.checks.iter().filter(|check| !check.passed) {
            println!("    FAIL {}: {}", check.id, check.detail);
        }
    }
    if report.summary.failed > 0 {
        return Err(format!(
            "{} evaluation checks failed; inspect {}",
            report.summary.failed,
            report_path.display()
        ));
    }
    Ok(())
}

fn report_path() -> Result<Option<PathBuf>, String> {
    let mut arguments = env::args().skip(1);
    let mut report = PathBuf::from("target/evals/mcp-eval-report.json");
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => {
                println!(
                    "Usage: mcp-evals [--report PATH]\n\nRuns the opt-in deterministic MCP evaluation suite."
                );
                return Ok(None);
            }
            "--report" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--report requires a path".to_owned())?;
                report = PathBuf::from(value);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Some(report))
}

#[derive(Debug, Serialize)]
struct EvalReport {
    schema: String,
    version: String,
    schema_revision: u32,
    cases_schema: String,
    suites: Vec<SectionReport>,
    summary: Summary,
}

#[derive(Debug, Serialize)]
struct SectionReport {
    id: String,
    title: String,
    checks: Vec<CheckReport>,
    passed: usize,
    failed: usize,
    metrics: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct CheckReport {
    id: String,
    passed: bool,
    detail: String,
}

#[derive(Debug, Serialize)]
struct Summary {
    suites: usize,
    checks: usize,
    passed: usize,
    failed: usize,
}

impl Summary {
    fn from_sections(sections: &[SectionReport]) -> Self {
        let checks = sections.iter().map(|section| section.checks.len()).sum();
        let passed = sections.iter().map(|section| section.passed).sum();
        Self {
            suites: sections.len(),
            checks,
            passed,
            failed: checks.saturating_sub(passed),
        }
    }
}

struct SectionBuilder {
    id: String,
    title: String,
    started: Instant,
    checks: Vec<CheckReport>,
    metrics: BTreeMap<String, Value>,
}

impl SectionBuilder {
    fn new(id: &str, title: &str) -> Self {
        Self {
            id: id.to_owned(),
            title: title.to_owned(),
            started: Instant::now(),
            checks: Vec::new(),
            metrics: BTreeMap::new(),
        }
    }

    fn check(&mut self, id: impl Into<String>, passed: bool, detail: impl Into<String>) {
        self.checks.push(CheckReport {
            id: id.into(),
            passed,
            detail: detail.into(),
        });
    }

    fn metric(&mut self, key: &str, value: Value) {
        self.metrics.insert(key.to_owned(), value);
    }

    fn finish(mut self) -> SectionReport {
        self.metrics.insert(
            "duration_ms".to_owned(),
            json!(self.started.elapsed().as_millis()),
        );
        let passed = self.checks.iter().filter(|check| check.passed).count();
        let check_count = self.checks.len();
        SectionReport {
            id: self.id,
            title: self.title,
            checks: self.checks,
            passed,
            failed: check_count.saturating_sub(passed),
            metrics: self.metrics,
        }
    }
}

fn run_section<F>(id: &str, title: &str, evaluate: F) -> SectionReport
where
    F: FnOnce(&mut SectionBuilder) -> Result<(), String>,
{
    let mut section = SectionBuilder::new(id, title);
    if let Err(error) = evaluate(&mut section) {
        section.check("section_execution", false, error);
    }
    section.finish()
}

#[derive(Debug, Deserialize)]
struct CaseCorpus {
    schema: String,
    cases: Vec<EvalCase>,
}

#[derive(Debug, Deserialize)]
struct EvalCase {
    id: String,
    category: String,
    prompt: String,
    first_tool: String,
    view: String,
    outcome: String,
    required_terms: Vec<String>,
    follow_up_tools: Vec<String>,
}

fn load_cases() -> Result<CaseCorpus, String> {
    let cases: CaseCorpus = serde_json::from_str(CASES_JSON)
        .map_err(|error| format!("golden case corpus is invalid: {error}"))?;
    if cases.cases.is_empty() {
        return Err("golden case corpus must not be empty".to_owned());
    }
    Ok(cases)
}

struct Fixture {
    repo: PathBuf,
    service: CoverageService,
    base_snapshot_id: String,
    current_snapshot_id: String,
    _workspace: TempWorkspace,
}

impl Fixture {
    fn new() -> Result<Self, String> {
        let workspace = TempWorkspace::new()?;
        let repo = workspace.path.clone();
        initialize_git_repository(&repo)?;
        write_fixture_sources(&repo)?;
        write_text(
            &repo.join(".gitignore"),
            "coverage*.lcov\ncoverage.duckdb\ncoverage.duckdb.wal\ncoverage.duckdb.lock\nruns/\n",
        )?;
        run_git(&repo, &["add", "."])?;
        run_git(&repo, &["commit", "-m", "evaluation fixture"])?;
        let baseline_revision = git_revision(&repo)?;

        let store = CoverageStore::open(repo.join("coverage.duckdb"), fixture_config(&repo))
            .map_err(|error| format!("open evaluation store: {error}"))?;
        store
            .ensure_project(&repo)
            .map_err(|error| format!("ensure evaluation project: {error}"))?;

        let base_report = repo.join("coverage-base.lcov");
        write_coverage_report(&base_report, false)?;
        let base = store
            .ingest_report(
                &base_report,
                "lcov",
                Some(&repo),
                Some("main"),
                Some(&baseline_revision),
                None,
                "unit",
            )
            .map_err(|error| format!("ingest base fixture: {error}"))?;
        let current_report = repo.join("coverage-current.lcov");
        write_text(
            &repo.join("src/priority.rs"),
            "pub fn prioritize(values: &[i32]) -> i32 {\n    let mut total = 0;\n    for value in values {\n        if *value >= 0 {\n            total += *value;\n        } else {\n            total -= *value;\n        }\n    }\n    total\n}\n\npub fn review_marker() -> bool {\n    true\n}\n",
        )?;
        run_git(&repo, &["add", "src/priority.rs"])?;
        run_git(&repo, &["commit", "-m", "evaluation source change"])?;
        let current_revision = git_revision(&repo)?;
        write_coverage_report(&current_report, true)?;
        let current = store
            .ingest_report(
                &current_report,
                "lcov",
                Some(&repo),
                Some("main"),
                Some(&current_revision),
                None,
                "unit",
            )
            .map_err(|error| format!("ingest current fixture: {error}"))?;

        let base_snapshot_id = value_string(&base, "id", "base snapshot")?;
        let current_snapshot_id = value_string(&current, "id", "current snapshot")?;
        let project = store
            .project()
            .map_err(|error| format!("read evaluation project: {error}"))?;
        let service = CoverageService::new(
            store,
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );
        Ok(Self {
            repo,
            service,
            base_snapshot_id,
            current_snapshot_id,
            _workspace: workspace,
        })
    }
}

struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn new() -> Result<Self, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("read system clock: {error}"))?
            .as_nanos();
        let base = env::temp_dir().join(format!(
            "coverage-mcp-evals-{}-{timestamp}",
            std::process::id()
        ));
        for attempt in 0..100_u32 {
            let path = if attempt == 0 {
                base.clone()
            } else {
                base.with_extension(attempt.to_string())
            };
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(format!("create temporary evaluation workspace: {error}"));
                }
            }
        }
        Err("could not allocate a unique temporary evaluation workspace".to_owned())
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn fixture_config(root: &Path) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".to_owned(),
        port: 59_471,
        default_repository_path: None,
        common_db_path: root.join("common.duckdb"),
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

fn initialize_git_repository(root: &Path) -> Result<(), String> {
    run_git(root, &["init", "-b", "main"])?;
    run_git(root, &["config", "user.email", "mcp-evals@example.com"])?;
    run_git(root, &["config", "user.name", "Coverage MCP Evals"])
}

fn write_fixture_sources(root: &Path) -> Result<(), String> {
    write_text(
        &root.join("src/priority.rs"),
        "pub fn prioritize(values: &[i32]) -> i32 {\n    let mut total = 0;\n    for value in values {\n        if *value >= 0 {\n            total += *value;\n        } else {\n            total -= *value;\n        }\n    }\n    total\n}\n",
    )?;
    write_text(
        &root.join("src/parser.rs"),
        "pub fn parse_name(input: &str) -> usize {\n    let trimmed = input.trim();\n    if trimmed.is_empty() {\n        return 0;\n    }\n    trimmed.len()\n}\n",
    )?;
    for index in 0..40_u32 {
        write_text(
            &root.join(format!("src/feature_{index:02}.rs")),
            &format!(
                "pub fn feature_{index}() -> usize {{\n    let value = {index};\n    value\n}}\n"
            ),
        )?;
    }
    Ok(())
}

fn write_coverage_report(path: &Path, current: bool) -> Result<(), String> {
    let mut report = String::new();
    append_lcov_record(
        &mut report,
        "src/priority.rs",
        if current {
            &[
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 0),
                (6, 1),
                (7, 1),
                (8, 1),
                (9, 1),
                (10, 1),
                (11, 1),
            ]
        } else {
            &[
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 0),
                (7, 0),
                (8, 1),
                (9, 1),
                (10, 1),
                (11, 1),
            ]
        },
    );
    append_lcov_record(
        &mut report,
        "src/parser.rs",
        if current {
            &[(1, 1), (2, 1), (3, 1), (4, 0), (5, 1), (6, 1)]
        } else {
            &[(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
        },
    );
    for index in 0..40_u32 {
        append_lcov_record(
            &mut report,
            &format!("src/feature_{index:02}.rs"),
            if current {
                &[(1, 1), (2, 0), (3, 0), (4, 1)]
            } else {
                &[(1, 1), (2, 1), (3, 1), (4, 1)]
            },
        );
    }
    write_text(path, &report)
}

fn append_lcov_record(output: &mut String, path: &str, lines: &[(u32, u32)]) {
    output.push_str("TN:\nSF:");
    output.push_str(path);
    output.push('\n');
    for (line, hits) in lines {
        output.push_str(&format!("DA:{line},{hits}\n"));
    }
    output.push_str("end_of_record\n");
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(path, content).map_err(|error| format!("write {}: {error}", path.display()))
}

fn run_git(root: &Path, arguments: &[&str]) -> Result<(), String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .map_err(|error| format!("start git {}: {error}", arguments.join(" ")))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn git_revision(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|error| format!("run git rev-parse: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git rev-parse failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn evaluate_usability(section: &mut SectionBuilder, cases: &CaseCorpus) -> Result<(), String> {
    let initialize = dispatch(None, json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))?;
    let instructions = initialize["result"]["instructions"]
        .as_str()
        .ok_or_else(|| "initialize did not return instructions".to_owned())?;
    let instruction_terms = [
        "project_context",
        "approved",
        "wait=false",
        "poll_after_ms",
        "multiple calls",
        "max_words",
        "detailed=false",
    ];
    for term in instruction_terms {
        section.check(
            format!("instructions-mention-{term}"),
            instructions.contains(term),
            format!("initialize instructions contain {term}"),
        );
    }

    let tools_response = dispatch(None, json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))?;
    let tools = tools_response["result"]["tools"]
        .as_array()
        .ok_or_else(|| "tools/list did not return a tools array".to_owned())?;
    let mut names = BTreeSet::new();
    let mut descriptions = BTreeMap::new();
    for tool in tools {
        let name = tool["name"]
            .as_str()
            .ok_or_else(|| "tool has no name".to_owned())?;
        section.check(
            format!("tool-{name}-unique"),
            names.insert(name.to_owned()),
            "tool name is unique",
        );
        let description = tool["description"]
            .as_str()
            .ok_or_else(|| format!("{name} has no description"))?;
        descriptions.insert(name.to_owned(), description.to_ascii_lowercase());
        section.check(
            format!("tool-{name}-description"),
            description.split_whitespace().count() >= 8,
            "description explains use rather than only naming the endpoint",
        );
        for annotation in [
            "readOnlyHint",
            "destructiveHint",
            "idempotentHint",
            "openWorldHint",
        ] {
            section.check(
                format!("tool-{name}-annotation-{annotation}"),
                tool["annotations"][annotation].is_boolean(),
                format!("annotation {annotation} is explicit"),
            );
        }
        let properties = tool["inputSchema"]["properties"]
            .as_object()
            .ok_or_else(|| format!("{name} has no input properties object"))?;
        for (property, schema) in properties {
            section.check(
                format!("tool-{name}-property-{property}-description"),
                schema["description"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "input property has an explanation",
            );
        }
        let required = tool["outputSchema"]["required"]
            .as_array()
            .ok_or_else(|| format!("{name} has no output requirements"))?;
        section.check(
            format!("tool-{name}-stable-envelope"),
            ["context", "data", "page"]
                .iter()
                .all(|field| required.iter().any(|value| value == field)),
            "output schema requires context, data, and page",
        );
    }

    let required_tools = [
        "project_context",
        "register_test_command",
        "run_test",
        "run_review",
        "cancel_run",
        "coverage_review",
        "coverage_import",
    ];
    section.check(
        "public-tool-inventory-is-complete",
        required_tools.iter().all(|name| names.contains(*name)),
        "all documented outcome and workflow tools are discoverable",
    );

    let mut category_counts = BTreeMap::new();
    let mut total_followups = 0usize;
    for case in &cases.cases {
        *category_counts
            .entry(case.category.clone())
            .or_insert(0usize) += 1;
        total_followups += case.follow_up_tools.len();
        section.check(
            format!("case-{}-prompt", case.id),
            !case.prompt.trim().is_empty() && !case.outcome.trim().is_empty(),
            "golden case has a prompt and an observable outcome",
        );
        section.check(
            format!("case-{}-first-tool", case.id),
            names.contains(&case.first_tool),
            format!("{} is discoverable as the first tool", case.first_tool),
        );
        section.check(
            format!("case-{}-view", case.id),
            !case.view.trim().is_empty(),
            "golden case names the smallest intended projection",
        );
        let description = descriptions
            .get(&case.first_tool)
            .ok_or_else(|| format!("missing description for {}", case.first_tool))?;
        for term in &case.required_terms {
            let lowercase_term = term.to_ascii_lowercase();
            section.check(
                format!("case-{}-guidance-{}", case.id, term.replace(' ', "-")),
                description.contains(&lowercase_term)
                    || instructions.to_ascii_lowercase().contains(&lowercase_term),
                format!("tool guidance exposes {term}"),
            );
        }
        for follow_up in &case.follow_up_tools {
            section.check(
                format!("case-{}-follow-up-{follow_up}", case.id),
                names.contains(follow_up),
                format!("follow-up tool {follow_up} is discoverable"),
            );
        }
    }
    section.metric("tool_count", json!(tools.len()));
    section.metric("golden_case_count", json!(cases.cases.len()));
    section.metric("golden_follow_up_count", json!(total_followups));
    section.metric("case_categories", json!(category_counts));
    section.metric(
        "instruction_words",
        json!(instructions.split_whitespace().count()),
    );
    Ok(())
}

fn evaluate_outcomes(section: &mut SectionBuilder, fixture: &Fixture) -> Result<(), String> {
    let context = call_tool(
        &fixture.service,
        "project_context",
        &json!({"max_words":300}),
    )?;
    section.check(
        "project-context-is-first-class",
        path_exists(&context, "data.project") && path_exists(&context, "context.schema_revision"),
        "project_context returns identity and project state in the stable envelope",
    );

    let review = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"change",
            "measurement":{"snapshot_id":fixture.current_snapshot_id,"file_path":"src/priority.rs"},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "limits":{"max_files":3,"max_regions":5,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    let review_data = data(&review)?;
    section.check(
        "change-review-is-one-bounded-projection",
        review_data["claim_status"] == "supported"
            && review_data["change"]["current"]["id"].is_string()
            && review_data["change"]["regions"].is_array()
            && review_data["change"]["files"].is_array()
            && review_data["change"]["changed_code"]["status"] == "measured",
        "coverage_review returns a bounded changed-code result with baseline and current evidence",
    );
    let parent_review = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"change",
            "measurement":{"snapshot_id":fixture.current_snapshot_id,"file_path":"src/priority.rs"},
            "baseline":{"kind":"parent_commit"},
            "limits":{"max_files":3,"max_regions":5,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    section.check(
        "parent-commit-baseline-resolves",
        data(&parent_review)?["claim_status"] == "supported"
            && data(&parent_review)?["baseline"]["commit_sha"]
                == data(&review)?["baseline"]["commit_sha"],
        "parent_commit resolves the compatible snapshot at the current Git parent",
    );

    let history = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"history",
            "measurement":{"suite":"unit"},
            "history":{"detail_snapshots":2,"summary_window":10},
            "limits":{"max_words":700,"max_bytes":12000}
        }),
    )?;
    let history_data = data(&history)?;
    section.check(
        "history-review-separates-detail-and-summary",
        history_data["history"]["detail"].is_array()
            && history_data["history"]["summary"]["window"].is_number(),
        "history review returns detailed recent points and an aggregate window",
    );

    let broad_insight = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"insight",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "limits":{"max_regions":10,"max_words":400,"max_bytes":12000}
        }),
    )?;
    let broad_data = data(&broad_insight)?;
    let broad_targets_array = array_field(&broad_data["insight"], "items")?;
    section.check(
        "next-work-returns-ranked-targets",
        !broad_targets_array.is_empty()
            && broad_targets_array
                .iter()
                .all(|target| target["file_path"].is_string() || target["category"].is_string()),
        "insight returns bounded ranked findings without a raw line dump",
    );
    section.check(
        "targets-are-not-raw-line-dumps",
        !contains_key(broad_data, "selected_lines") && !contains_key(broad_data, "changed_lines"),
        "the next-work projection does not include unrelated exact line records",
    );

    let target = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"change",
            "measurement":{"snapshot_id":fixture.current_snapshot_id,"file_path":"src/priority.rs"},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "representation":"audit",
            "limits":{"max_files":3,"max_regions":10,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    let target_data = data(&target)?;
    let region = array_field(&target_data["change"], "regions")?
        .iter()
        .find(|region| region["status"] == "regressed")
        .ok_or_else(|| "target-specific query returned no red region".to_owned())?;
    let start = region["start"]
        .as_i64()
        .ok_or_else(|| "target region has no start".to_owned())?;
    let end = region["end"]
        .as_i64()
        .ok_or_else(|| "target region has no end".to_owned())?;
    section.check(
        "target-carries-source-follow-up-range",
        start >= 1 && end >= start,
        format!("changed-code review carries the bounded range {start}-{end}"),
    );

    let regression_regions = array_field(&target_data["change"], "regions")?
        .iter()
        .filter(|region| region["status"] == "regressed")
        .collect::<Vec<_>>();
    section.check(
        "previous-impact-returns-regressions",
        regression_regions
            .iter()
            .all(|region| region["status"] == "regressed")
            && regression_regions.iter().any(|region| {
                region["file_path"] == "src/priority.rs" && region["start"].as_i64() == Some(5)
            }),
        "change review exposes grouped previous-session regression impact",
    );

    let source = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"source",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "source":{"ranges":[{"file_path":"src/priority.rs","start":start,"end":end}]},
            "limits":{"max_words":600,"max_bytes":12000}
        }),
    )?;
    let source_data = data(&source)?;
    let source_group = source_data["source"]
        .as_array()
        .and_then(|groups| groups.first())
        .ok_or_else(|| "source review returned no file group".to_owned())?;
    let source_range = source_group["ranges"]
        .as_array()
        .and_then(|ranges| ranges.first())
        .ok_or_else(|| "source review returned no range".to_owned())?;
    let source_lines = array_field(source_range, "lines")?;
    section.check(
        "source-context-is-bounded-and-annotated",
        !source_lines.is_empty()
            && source_lines.iter().any(|line| line["marker"] == "red")
            && source_lines.iter().all(|line| line["marker"].is_string())
            && array_field(source_range, "red_regions")?
                .iter()
                .any(|region| {
                    region["start"].as_i64() == Some(start) || region["end"].as_i64() == Some(end)
                }),
        "source review returns numbered source with compact coverage markers",
    );
    let source_batch = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"source",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "source":{"ranges":[
                {"file_path":"src/priority.rs","start":start,"end":start},
                {"file_path":"src/priority.rs","start":end,"end":end}
            ]},
            "limits":{"max_words":600,"max_bytes":12000}
        }),
    )?;
    let source_batch_data = data(&source_batch)?;
    let source_batch_group = source_batch_data["source"]
        .as_array()
        .and_then(|groups| groups.first())
        .ok_or_else(|| "source batch returned no file group".to_owned())?;
    section.check(
        "source-context-batches-ranges",
        source_batch_group["ranges"].is_array()
            && source_batch_group["source_resolution"].is_string(),
        "source review can batch disjoint ranges and labels source provenance",
    );
    section.check(
        "file-view-separates-red-map-and-exact-lines",
        target_data["change"]["files"].is_array()
            && target_data["change"]["representation"] == "audit"
            && !contains_key(&target_data["change"], "selected_lines"),
        "change review returns compact file metrics without exact line dumps",
    );

    let history = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"history",
            "measurement":{"suite":"unit","file_path":"src/priority.rs"},
            "history":{"detail_snapshots":2,"summary_window":10},
            "limits":{"max_words":600,"max_bytes":12000}
        }),
    )?;
    let history_data = data(&history)?;
    section.check(
        "line-history-is-narrow",
        history_data["history"]["detail"].is_array(),
        "history review returns a narrow detailed projection",
    );
    section.check(
        "line-history-has-both-snapshots",
        history_data["history"]["detail"]
            .as_array()
            .is_some_and(|points| points.len() >= 2),
        "history detail contains the base and current observations",
    );

    let audit = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"audit",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "limits":{"max_files":3,"max_regions":10,"max_words":1000,"max_bytes":12000}
        }),
    )?;
    section.check(
        "exact-audit-is-explicit",
        data(&audit)?["task"] == "audit"
            && data(&audit)?["representation"] == "audit"
            && data(&audit)?["change"]["regions"].is_array(),
        "exact changed regions are available as a deliberate audit projection",
    );
    section.metric(
        "broad_target_words",
        json!(serialized_word_count(broad_data)),
    );
    section.metric(
        "source_review_words",
        json!(serialized_word_count(data(&source)?)),
    );
    section.metric("follow_up_call_count", json!(6));
    Ok(())
}

fn evaluate_efficiency(section: &mut SectionBuilder, fixture: &Fixture) -> Result<(), String> {
    let compact = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"change",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "representation":"compact",
            "limits":{"max_files":10,"max_regions":20,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    let detailed = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"audit",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "limits":{"max_files":10,"max_regions":20,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    let compact_words = serialized_word_count(data(&compact)?);
    let detailed_words = serialized_word_count(data(&detailed)?);
    section.check(
        "detailed-is-opt-in-cost",
        detailed_words >= compact_words,
        format!(
            "detailed projection uses {detailed_words} words versus {compact_words} compact words"
        ),
    );

    let history = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"history",
            "measurement":{"suite":"unit"},
            "history":{"detail_snapshots":2,"summary_window":10},
            "limits":{"max_words":600,"max_bytes":12000}
        }),
    )?;
    let history_data = data(&history)?;
    section.check(
        "history-detail-is-bounded",
        history_data["history"]["detail"]
            .as_array()
            .is_some_and(|points| points.len() <= 2)
            && history_data["history"]["summary"]["window"]
                .as_u64()
                .is_some_and(|window| window <= 10),
        "history keeps detailed points and aggregate windows bounded",
    );

    let mut cursor = None;
    let mut seen_commands = BTreeSet::new();
    let mut page_count = 0usize;
    let mut command_total = None;
    loop {
        page_count += 1;
        if page_count > 100 {
            return Err("project context pagination exceeded 100 pages".to_owned());
        }
        let mut arguments = json!({"max_words":50});
        if let Some(value) = cursor.as_deref() {
            arguments["cursor"] = json!(value);
        }
        let page = call_tool(&fixture.service, "project_context", &arguments)?;
        for command in array_field(data(&page)?, "commands")? {
            let id = command["id"]
                .as_str()
                .ok_or_else(|| "project context command has no id".to_owned())?;
            if !seen_commands.insert(id.to_owned()) {
                return Err(format!("project context pagination repeated {id}"));
            }
        }
        let page_metadata = page["page"]
            .as_object()
            .ok_or_else(|| "project context has no page metadata".to_owned())?;
        let word_count = page_metadata["word_count"]
            .as_u64()
            .ok_or_else(|| "project context page has no word_count".to_owned())?;
        section.check(
            format!("project-context-page-{page_count}-bounded"),
            word_count <= 50,
            format!("project context page uses {word_count} words"),
        );
        command_total = command_total.or_else(|| page_metadata["total"].as_u64());
        cursor = page_metadata["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    section.check(
        "project-context-pagination-is-complete",
        command_total == Some(seen_commands.len() as u64),
        format!(
            "collected {} unique commands from {:?} total",
            seen_commands.len(),
            command_total
        ),
    );

    let source = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"source",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "source":{"ranges":[
                {"file_path":"src/priority.rs","start":1,"end":1},
                {"file_path":"src/priority.rs","start":3,"end":3}
            ]},
            "limits":{"max_words":600,"max_bytes":12000}
        }),
    )?;
    let source_data = data(&source)?;
    section.check(
        "source-response-does-not-duplicate-groups",
        source_data["source"].is_array() && source_data.get("sources").is_none(),
        "source review emits one canonical grouped source array",
    );

    let compact_data = data(&compact)?;
    let audit_data = data(&detailed)?;
    let region_words = serialized_word_count(&compact_data["change"]["regions"]);
    let line_words = serialized_word_count(&audit_data["change"]);
    section.check(
        "grouped-regions-are-smaller-than-line-audit",
        region_words < line_words,
        format!("compact regions use {region_words} words versus {line_words} audit words"),
    );

    let consolidated = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({
            "task":"change",
            "measurement":{"snapshot_id":fixture.current_snapshot_id,"file_path":"src/priority.rs"},
            "baseline":{"kind":"explicit","snapshot_id":fixture.base_snapshot_id},
            "source":{"include":true,"context_lines":0},
            "representation":"compact",
            "limits":{"max_files":1,"max_regions":1,"max_source_lines":10,"max_words":1200,"max_bytes":12000}
        }),
    )?;
    let consolidated_bytes = serde_json::to_vec(&consolidated)
        .map_err(|error| format!("serialize consolidated benchmark: {error}"))?
        .len();
    section.check(
        "consolidated-workflow-is-bounded",
        consolidated_bytes <= 12_000,
        format!("consolidated review uses {consolidated_bytes} serialized bytes"),
    );
    section.metric(
        "bounded_change_workflow",
        json!({
            "calls":1,
            "serialized_bytes":consolidated_bytes,
            "failed_calls":0,
            "source_followups":0
        }),
    );

    let mut latencies = Vec::new();
    for index in 0..8 {
        let started = Instant::now();
        let _ = call_tool(
            &fixture.service,
            "coverage_review",
            &json!({
                "task":"insight",
                "measurement":{"snapshot_id":fixture.current_snapshot_id},
                "limits":{"max_regions":5,"max_words":300,"max_bytes":12000}
            }),
        )?;
        let latency = started.elapsed().as_millis() as u64;
        section.check(
            format!("bounded-insight-query-{}", index + 1),
            latency < 5_000,
            format!("bounded insight query completed in {latency} ms"),
        );
        latencies.push(latency);
    }
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p95 = percentile(&latencies, 95);
    let total_ms: u64 = latencies.iter().sum();
    section.check(
        "repeated-bounded-queries-have-a-compute-bound",
        total_ms < 30_000,
        format!("eight bounded queries completed in {total_ms} ms"),
    );
    section.metric("compact_words", json!(compact_words));
    section.metric("detailed_words", json!(detailed_words));
    section.metric("regions_words", json!(region_words));
    section.metric("exact_lines_words", json!(line_words));
    section.metric(
        "regions_to_lines_ratio",
        json!(ratio(region_words, line_words)),
    );
    section.metric(
        "query_latency_ms",
        json!({"p50":p50,"p95":p95,"samples":latencies}),
    );
    section.metric(
        "history_detail_count",
        json!(
            history_data["history"]["detail"]
                .as_array()
                .map_or(0, Vec::len)
        ),
    );
    Ok(())
}

fn evaluate_protocol(section: &mut SectionBuilder, fixture: &Fixture) -> Result<(), String> {
    let initialize = dispatch(None, json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))?;
    section.check(
        "json-rpc-initialize",
        initialize["jsonrpc"] == "2.0"
            && initialize["result"]["serverInfo"]["name"] == "coverage-mcp",
        "initialize returns a JSON-RPC result and server identity",
    );
    let tools = dispatch(None, json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))?;
    section.check(
        "json-rpc-tools-list",
        tools["result"]["tools"]
            .as_array()
            .is_some_and(|items| items.len() == 7),
        "tools/list returns the complete public inventory",
    );
    let resources = dispatch(
        None,
        json!({"jsonrpc":"2.0","id":3,"method":"resources/list"}),
    )?;
    let templates = dispatch(
        None,
        json!({"jsonrpc":"2.0","id":4,"method":"resources/templates/list"}),
    )?;
    section.check(
        "resources-are-discoverable",
        resources["result"]["resources"].is_array()
            && templates["result"]["resourceTemplates"].is_array(),
        "resources and resource templates have stable inventories",
    );
    section.check(
        "notifications-have-no-response",
        mcp::dispatch_json_rpc(
            None,
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .is_none(),
        "notifications remain response-less",
    );
    let missing_method = dispatch(None, json!({"jsonrpc":"2.0","id":5}))?;
    let unknown_method = dispatch(
        None,
        json!({"jsonrpc":"2.0","id":6,"method":"not-a-method"}),
    )?;
    section.check(
        "json-rpc-errors-are-explicit",
        missing_method["error"]["message"].is_string()
            && unknown_method["error"]["message"].is_string(),
        "missing and unknown methods return explicit errors",
    );

    let request = json!({
        "jsonrpc":"2.0",
        "id":7,
        "method":"tools/call",
        "params":{"name":"coverage_review","arguments":{
            "task":"insight",
            "measurement":{"snapshot_id":fixture.current_snapshot_id},
            "limits":{"max_regions":5,"max_words":250,"max_bytes":12000}
        }}
    });
    let wire = dispatch(Some(&fixture.service), request)?;
    let direct = call_tool(
        &fixture.service,
        "coverage_review",
        &json!({"task":"insight","measurement":{"snapshot_id":fixture.current_snapshot_id},"limits":{"max_regions":5,"max_words":250,"max_bytes":12000}}),
    )?;
    section.check(
        "dispatcher-and-tool-semantics-match",
        wire["result"]["isError"] == false && wire["result"]["structuredContent"] == direct,
        "JSON-RPC dispatch exposes the same structured result as direct service dispatch",
    );
    let content_text = wire["result"]["content"][0]["text"]
        .as_str()
        .ok_or_else(|| "tools/call did not return text content".to_owned())?;
    let content: Value = serde_json::from_str(content_text)
        .map_err(|error| format!("tools/call content is not JSON: {error}"))?;
    section.check(
        "structured-content-and-text-content-agree",
        content == direct,
        "structuredContent and text content carry the same bounded envelope",
    );
    let context_resource = dispatch(
        Some(&fixture.service),
        json!({"jsonrpc":"2.0","id":8,"method":"resources/read","params":{"uri":"coverage://context"}}),
    )?;
    let resource_text = context_resource["result"]["contents"][0]["text"]
        .as_str()
        .ok_or_else(|| "context resource has no text".to_owned())?;
    let resource: Value = serde_json::from_str(resource_text)
        .map_err(|error| format!("context resource is not JSON: {error}"))?;
    section.check(
        "context-resource-is-readable",
        resource["context"]["schema_revision"] == SCHEMA_REVISION,
        "coverage://context contains the versioned project envelope",
    );
    let snapshot_resource = dispatch(
        Some(&fixture.service),
        json!({
            "jsonrpc":"2.0",
            "id":9,
            "method":"resources/read",
            "params":{"uri":format!("coverage://snapshot/{}/summary", fixture.current_snapshot_id)}
        }),
    )?;
    section.check(
        "snapshot-resource-is-readable",
        snapshot_resource["result"]["contents"][0]["text"].is_string(),
        "snapshot summary resource returns bounded content",
    );
    section.metric("protocol_methods_checked", json!(9));
    Ok(())
}

fn evaluate_safety(section: &mut SectionBuilder, fixture: &Fixture) -> Result<(), String> {
    let invalid_cases = [
        ("null-arguments-rejected", "coverage_review", Value::Null),
        (
            "missing-task-budget-rejected",
            "coverage_review",
            json!({"limits":{"max_words":49}}),
        ),
        (
            "invalid-task-rejected",
            "coverage_review",
            json!({"task":"everything"}),
        ),
        (
            "invalid-budget-rejected",
            "coverage_review",
            json!({"task":"change","limits":{"max_words":49}}),
        ),
        (
            "wrong-representation-type-rejected",
            "coverage_review",
            json!({"task":"change","representation":false}),
        ),
        (
            "unknown-field-rejected",
            "coverage_review",
            json!({"task":"change","unexpected":true}),
        ),
        (
            "unknown-snapshot-rejected",
            "coverage_review",
            json!({"task":"change","measurement":{"snapshot_id":"missing-snapshot"}}),
        ),
        (
            "oversized-source-range-rejected",
            "coverage_review",
            json!({"task":"source","measurement":{"snapshot_id":fixture.current_snapshot_id},"source":{"ranges":[{"file_path":"src/priority.rs","start":1,"end":201}]}}),
        ),
        (
            "path-traversal-source-rejected",
            "coverage_review",
            json!({"task":"source","measurement":{"snapshot_id":fixture.current_snapshot_id},"source":{"ranges":[{"file_path":"../outside.rs","start":1,"end":2}]}}),
        ),
    ];
    for (id, tool, arguments) in &invalid_cases {
        section.check(
            *id,
            call_tool(&fixture.service, tool, arguments).is_err(),
            "invalid input returns an explicit error instead of a fallback result",
        );
    }

    let unapproved = call_tool(
        &fixture.service,
        "register_test_command",
        &json!({
            "name":"unapproved-eval-command",
            "command":"printf forbidden",
            "human_approved":false,
            "approved_by":"eval-suite",
            "approval_note":"must be rejected",
            "cwd":fixture.repo.to_string_lossy(),
            "shell":"/bin/sh"
        }),
    );
    section.check(
        "unapproved-command-is-rejected",
        unapproved.is_err(),
        "command registration requires explicit human approval",
    );

    let unknown_tool = dispatch(
        Some(&fixture.service),
        json!({
            "jsonrpc":"2.0",
            "id":20,
            "method":"tools/call",
            "params":{"name":"not-a-tool","arguments":{}}
        }),
    )?;
    section.check(
        "tool-errors-are-visible",
        unknown_tool["result"]["isError"] == true
            && unknown_tool["result"]["content"][0]["text"].is_string(),
        "tool failures are visible in the MCP result and are not silently converted",
    );

    let missing_service = dispatch(
        None,
        json!({
            "jsonrpc":"2.0",
            "id":21,
            "method":"tools/call",
            "params":{"name":"project_context","arguments":{}}
        }),
    )?;
    section.check(
        "project-selection-is-required",
        missing_service["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("selected project")),
        "data tools fail explicitly when no project is selected",
    );
    section.metric("invalid_cases", json!(invalid_cases.len()));
    Ok(())
}

fn evaluate_reliability(section: &mut SectionBuilder, fixture: &Fixture) -> Result<(), String> {
    let registered = call_tool(
        &fixture.service,
        "register_test_command",
        &json!({
            "name":"approved-eval-command",
            "command":"printf 'MCP_EVAL_STDOUT\\n'; printf 'MCP_EVAL_STDERR\\n' >&2",
            "human_approved":true,
            "approved_by":"eval-suite",
            "approval_note":"deterministic local evaluation command",
            "cwd":fixture.repo.to_string_lossy(),
            "shell":"/bin/sh",
            "max_words":250
        }),
    )?;
    let command_id = value_string(data(&registered)?, "id", "registered command")?;
    let first = call_tool(
        &fixture.service,
        "run_test",
        &json!({
            "command_ref":command_id,
            "wait":false,
            "idempotency_key":"mcp-eval-approved-run",
            "max_words":300
        }),
    )?;
    let first_id = value_string(data(&first)?, "id", "submitted run")?;
    let repeated = call_tool(
        &fixture.service,
        "run_test",
        &json!({
            "command_ref":command_id,
            "wait":false,
            "idempotency_key":"mcp-eval-approved-run",
            "max_words":300
        }),
    )?;
    section.check(
        "run-submission-is-idempotent",
        repeated["data"]["id"] == first_id && repeated["data"]["submission_reused"] == true,
        "reusing the idempotency key returns the same durable run",
    );
    let completed = wait_for_terminal(&fixture.service, &first_id)?;
    section.check(
        "async-run-reaches-terminal-success",
        completed["data"]["terminal"] == true && completed["data"]["status"] == "passed",
        "wait=false followed by bounded polling reaches a passed terminal state",
    );
    let fixture_status = Command::new("git")
        .arg("-C")
        .arg(&fixture.repo)
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_else(|error| format!("status error: {error}"));
    let fixture_clean = coverage_mcp::git::is_clean(&fixture.repo);
    section.check(
        "fixture-checkout-is-clean-for-reuse",
        fixture_clean,
        format!(
            "the unchanged-run reuse probe requires a clean checkout; status={fixture_status:?}"
        ),
    );
    let unchanged = call_tool(
        &fixture.service,
        "run_test",
        &json!({
            "command_ref":command_id,
            "wait":false,
            "reuse_if_unchanged":true,
            "max_words":300
        }),
    )?;
    section.check(
        "unchanged-run-is-reused-without-a-new-key",
        unchanged["data"]["id"] == first_id
            && unchanged["data"]["submission_reused"] == true
            && unchanged["data"]["reuse_reason"] == "unchanged_checkout",
        "the server returns the latest compatible run when the checkout is unchanged",
    );
    let logs = call_tool(
        &fixture.service,
        "run_review",
        &json!({
            "run_id":first_id,
            "view":"logs",
            "query":["MCP_EVAL_STDOUT","MCP_EVAL_STDERR"],
            "stream":"both",
            "context_lines":0,
            "max_matches":5,
            "max_words":250,
            "max_bytes":12000
        }),
    )?;
    section.check(
        "targeted-logs-return-evidence",
        data(&logs)?["match_count"].as_u64() == Some(2),
        "literal OR log search returns only the requested evidence windows",
    );
    section.check(
        "run-state-does-not-embed-full-logs",
        !contains_key(&completed, "MCP_EVAL_STDOUT")
            && !contains_key(&completed, "MCP_EVAL_STDERR"),
        "durable run state keeps logs targeted rather than embedding full output",
    );

    let failing_command = call_tool(
        &fixture.service,
        "register_test_command",
        &json!({
            "name":"failing-eval-command",
            "command":"printf 'MCP_EVAL_FAILURE\\n' >&2; exit 7",
            "human_approved":true,
            "approved_by":"eval-suite",
            "approval_note":"deterministic failure-path evaluation",
            "cwd":fixture.repo.to_string_lossy(),
            "shell":"/bin/sh"
        }),
    )?;
    let failing_id = value_string(data(&failing_command)?, "id", "failing command")?;
    let failing_run = call_tool(
        &fixture.service,
        "run_test",
        &json!({
            "command_ref":failing_id,
            "wait":false,
            "idempotency_key":"mcp-eval-failure",
            "max_words":250
        }),
    )?;
    let failing_run_id = value_string(data(&failing_run)?, "id", "failing run")?;
    let failed = wait_for_terminal(&fixture.service, &failing_run_id)?;
    section.check(
        "failed-run-is-terminal-and-distinct",
        failed["data"]["terminal"] == true && failed["data"]["status"] == "failed",
        "a failed command is terminalized as failed rather than left pending",
    );

    let cancel_command = call_tool(
        &fixture.service,
        "register_test_command",
        &json!({
            "name":"cancellable-eval-command",
            "command":"sleep 5",
            "human_approved":true,
            "approved_by":"eval-suite",
            "approval_note":"deterministic cancellation-path evaluation",
            "cwd":fixture.repo.to_string_lossy(),
            "shell":"/bin/sh"
        }),
    )?;
    let cancel_command_id = value_string(data(&cancel_command)?, "id", "cancellable command")?;
    let cancel_run = call_tool(
        &fixture.service,
        "run_test",
        &json!({
            "command_ref":cancel_command_id,
            "wait":false,
            "idempotency_key":"mcp-eval-cancel",
            "max_words":250
        }),
    )?;
    let cancel_run_id = value_string(data(&cancel_run)?, "id", "cancellable run")?;
    let cancellation = call_tool(
        &fixture.service,
        "cancel_run",
        &json!({"run_id":cancel_run_id,"max_words":250}),
    )?;
    let cancelled = wait_for_terminal(&fixture.service, &cancel_run_id)?;
    section.check(
        "cancellation-is-explicit-and-terminal",
        cancellation["data"]["cancellation_requested"] == true
            && cancelled["data"]["terminal"] == true
            && cancelled["data"]["status"] == "cancelled",
        "cancellation requests are observable and reach a cancelled terminal state",
    );
    section.metric("managed_runs", json!(3));
    section.metric("poll_after_ms_honored", json!(true));
    Ok(())
}

fn wait_for_terminal(service: &CoverageService, run_id: &str) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = call_tool(
            service,
            "run_review",
            &json!({"run_id":run_id,"view":"status","max_words":300,"max_bytes":12000}),
        )?;
        if state["data"]["terminal"] == true {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "run {run_id} did not reach terminal state within 15 seconds"
            ));
        }
        let poll_after_ms = state["data"]["poll_after_ms"]
            .as_u64()
            .unwrap_or(50)
            .clamp(1, 1_000);
        std::thread::sleep(Duration::from_millis(poll_after_ms));
    }
}

fn dispatch(service: Option<&CoverageService>, request: Value) -> Result<Value, String> {
    mcp::dispatch_json_rpc(service, &request)
        .ok_or_else(|| "request unexpectedly produced no response".to_owned())
}

fn call_tool(service: &CoverageService, name: &str, arguments: &Value) -> Result<Value, String> {
    mcp::call_tool(service, name, arguments).map_err(|error| format!("{name}: {error}"))
}

fn data(value: &Value) -> Result<&Value, String> {
    value
        .get("data")
        .ok_or_else(|| "response has no data field".to_owned())
}

fn array_field<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>, String> {
    if field.is_empty() {
        return value
            .as_array()
            .ok_or_else(|| "response data is not an array".to_owned());
    }
    value[field]
        .as_array()
        .ok_or_else(|| format!("response data field {field} is not an array"))
}

fn value_string(value: &Value, field: &str, label: &str) -> Result<String, String> {
    value[field]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{label} has no string field {field}"))
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => object
            .iter()
            .any(|(name, value)| name == key || contains_key(value, key)),
        Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
        _ => false,
    }
}

fn path_exists(value: &Value, path: &str) -> bool {
    let mut current = vec![value];
    for segment in path.split('.') {
        let array_segment = segment.ends_with("[]");
        let key = segment.strip_suffix("[]").unwrap_or(segment);
        let mut next = Vec::new();
        for value in current {
            let Some(child) = value.as_object().and_then(|object| object.get(key)) else {
                continue;
            };
            if array_segment {
                if let Some(children) = child.as_array() {
                    next.extend(children.iter());
                }
            } else {
                next.push(child);
            }
        }
        if next.is_empty() {
            return false;
        }
        current = next;
    }
    true
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index.min(values.len() - 1)]
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn write_report(path: &Path, report: &EvalReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create report directory {}: {error}", parent.display()))?;
    }
    let encoded = serde_json::to_vec_pretty(report)
        .map_err(|error| format!("serialize evaluation report: {error}"))?;
    fs::write(path, encoded).map_err(|error| format!("write evaluation report: {error}"))
}
