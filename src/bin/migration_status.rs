//! Render the repository-native migration evidence status.
//!
//! This binary deliberately consumes only manifest-indexed inputs and lane
//! artifacts. It does not execute an oracle, infer expected values, or turn a
//! Rust conformance test into cross-runtime parity evidence.

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const GENERATOR: &str = "migration-status@1";
const MANIFEST_RELATIVE: &str = "tests/fixtures/manifest.yaml";
const PARITY_MARKER_SCHEMA: &str = "migration-parity/lane-marker@1";
const PARITY_MARKER_RELATIVE: &str = "target/migration/parity-run.json";
const TARGET_PROFILE: &str = "rust-default";

type StatusResult<T> = Result<T, Box<dyn Error>>;

fn main() -> StatusResult<()> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let record_parity = arguments
        .first()
        .and_then(|argument| argument.to_str())
        .is_some_and(|argument| argument == "--record-parity");
    let repository_index = if record_parity { 1 } else { 0 };
    let repository_argument = arguments.get(repository_index);
    let repository = repository_argument
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    if arguments.len() > repository_index + 1 {
        return Err("usage: migration-status [--record-parity] [repository]".into());
    }
    let repository = fs::canonicalize(repository)?;
    let manifest_path = repository.join(MANIFEST_RELATIVE);
    let manifest_bytes = fs::read(&manifest_path)?;
    let manifest = String::from_utf8(manifest_bytes.clone())?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    let fixture_root = repository.join("tests/fixtures");
    let input_paths = indexed_inputs(&manifest)?;
    let cases = load_cases(&fixture_root, &input_paths)?;
    let coverage_input = load_indexed_json(&fixture_root, &input_paths, "coverage")?;
    let benchmark_input = load_indexed_json(&fixture_root, &input_paths, "benchmark")?;
    let target = target_identity(&repository)?;

    let migration_root = repository.join("target/migration");
    fs::create_dir_all(&migration_root)?;
    let parity_marker_path = repository.join(PARITY_MARKER_RELATIVE);
    if record_parity {
        let marker = json!({
            "schema":PARITY_MARKER_SCHEMA,
            "lane":"parity",
            "run_id":format!("local-parity-{}", &manifest_sha256[..16]),
            "manifest":{"path":MANIFEST_RELATIVE,"schema":"migration-parity/manifest@2","sha256":manifest_sha256},
            "target":target,
            "command":{"command_id":"parity","argv":["cargo","test","rust_migration"],"cwd":".","timeout_seconds":900},
            "created_at":Utc::now().to_rfc3339()
        });
        write_json(&parity_marker_path, &marker)?;
        println!(
            "recorded migration parity marker: {}",
            parity_marker_path.display()
        );
        return Ok(());
    }
    let (parity_marker, parity_marker_issue) =
        load_parity_marker(&parity_marker_path, &manifest_sha256, &target)?;
    let coverage_path = migration_root.join("coverage-raw.json");
    let benchmark_path = migration_root.join("benchmark-result.json");
    let coverage = coverage_path
        .is_file()
        .then(|| read_json(&coverage_path))
        .transpose()?;
    let benchmark = benchmark_path
        .is_file()
        .then(|| read_json(&benchmark_path))
        .transpose()?;
    let lane_context = LaneContext {
        repository: &repository,
        fixture_root: &fixture_root,
        manifest_sha256: &manifest_sha256,
        target: &target,
    };
    let dirty = target["dirty"].as_bool().unwrap_or(true);

    let coverage_artifact = coverage
        .as_ref()
        .map(|value| {
            let bytes = serde_json::to_vec(value)?;
            Ok::<_, Box<dyn Error>>(format!("local-{}", &sha256_hex(&bytes)[..16]))
        })
        .transpose()?;
    let benchmark_artifact = benchmark
        .as_ref()
        .map(|value| {
            let bytes = serde_json::to_vec(value)?;
            Ok::<_, Box<dyn Error>>(format!("local-{}", &sha256_hex(&bytes)[..16]))
        })
        .transpose()?;
    let parity_artifact = if parity_marker.is_some() {
        let artifact_id = format!("local-parity-{}", &manifest_sha256[..16]);
        let result = parity_result(&cases, &input_paths, &lane_context)?;
        write_json(&migration_root.join("parity-result.json"), &result)?;
        Some(artifact_id)
    } else {
        None
    };

    let coverage_result = coverage
        .as_ref()
        .map(|raw| {
            coverage_result(
                raw,
                &coverage_input,
                coverage_artifact.as_deref(),
                &lane_context,
            )
        })
        .transpose()?;
    let benchmark_result = benchmark
        .as_ref()
        .map(|raw| {
            benchmark_result(
                raw,
                &benchmark_input,
                benchmark_artifact.as_deref(),
                &lane_context,
            )
        })
        .transpose()?;
    let parity_outcome = "not_proven";
    let coverage_totals = coverage
        .as_ref()
        .map(coverage_dimensions)
        .unwrap_or_else(|| {
            json!({
                "function_coverage":{"covered":0,"total":0},
                "line_coverage":{"covered":0,"total":0},
                "region_coverage":{"covered":0,"total":0}
            })
        });
    let coverage_ingested = coverage_result
        .as_ref()
        .is_some_and(|result| result["collector"]["artifact_ingested"] == Value::Bool(true));
    let coverage_has_measurements = coverage_totals["function_coverage"]["total"]
        .as_u64()
        .is_some_and(|total| total > 0)
        && coverage_totals["line_coverage"]["total"]
            .as_u64()
            .is_some_and(|total| total > 0);
    let coverage_outcome = match coverage_result.as_ref() {
        Some(_)
            if coverage_ingested
                && coverage_has_measurements
                && coverage_totals["function_coverage"]["covered"]
                    == coverage_totals["function_coverage"]["total"]
                && coverage_totals["line_coverage"]["covered"]
                    == coverage_totals["line_coverage"]["total"]
                && !dirty =>
        {
            "pass"
        }
        Some(_) if dirty || !coverage_ingested => "not_proven",
        Some(_) => "fail",
        None => "not_proven",
    };
    let benchmark_outcome = match benchmark_result.as_ref() {
        Some(_) if parity_outcome == "pass" && !dirty => "pass",
        Some(_) => "not_proven",
        None => "not_proven",
    };

    let mut evidence = Vec::new();
    if let Some(id) = parity_artifact.as_deref() {
        evidence.push(json!({"lane":"parity","run_id":id,"snapshot_id":Value::Null}));
    }
    if let Some(id) = coverage_artifact.as_deref() {
        evidence.push(json!({"lane":"coverage","run_id":id,"snapshot_id":Value::Null}));
    }
    if let Some(id) = benchmark_artifact.as_deref() {
        evidence.push(json!({"lane":"benchmark","run_id":id,"snapshot_id":Value::Null}));
    }
    let mut stale = if dirty {
        evidence
            .iter()
            .map(|item| {
                json!({
                    "lane": item["lane"].clone(),
                    "run_id": item["run_id"].clone(),
                    "reason": "target working tree is dirty",
                    "identity_diff": ["target.dirty"]
                })
            })
            .collect()
    } else {
        Vec::new()
    };
    if let Some(issue) = parity_marker_issue {
        stale.push(issue);
    }

    let benchmark_summary = benchmark.as_ref().map(benchmark_metrics).unwrap_or_else(
        || json!({"sample_count":0,"median_latency_ms":Value::Null,"budget_outcome":"not_run"}),
    );
    let case_count = cases.len() as u64;
    let coverage_count = if coverage.is_some() { 1 } else { 0 };
    let benchmark_count = if benchmark.is_some() { 1 } else { 0 };
    let status_report = json!({
        "schema":"migration-parity/status-report@1",
        "manifest":{
            "path":MANIFEST_RELATIVE,
            "schema":"migration-parity/manifest@2",
            "sha256":manifest_sha256
        },
        "target_profiles":[target],
        "evidence":evidence,
        "completeness":[
            {"dimension":"inventory_representation","target_profile":TARGET_PROFILE,"numerator":cases.len(),"denominator":cases.len(),"evidence_id":Value::Null},
            {"dimension":"operation_contracts","target_profile":TARGET_PROFILE,"numerator":cases.len(),"denominator":cases.len(),"evidence_id":Value::Null},
            {"dimension":"parity_input_mapping","target_profile":TARGET_PROFILE,"numerator":case_count,"denominator":case_count,"evidence_id":Value::Null},
            {"dimension":"coverage_input_mapping","target_profile":TARGET_PROFILE,"numerator":coverage_count,"denominator":1,"evidence_id":coverage_artifact},
            {"dimension":"benchmark_input_mapping","target_profile":TARGET_PROFILE,"numerator":benchmark_count,"denominator":1,"evidence_id":benchmark_artifact},
            {"dimension":"parity_outcome","target_profile":TARGET_PROFILE,"numerator":if parity_outcome == "pass" {case_count} else {0},"denominator":case_count,"evidence_id":parity_artifact},
            {"dimension":"function_coverage","target_profile":TARGET_PROFILE,"numerator":coverage_totals["function_coverage"]["covered"],"denominator":coverage_totals["function_coverage"]["total"],"evidence_id":coverage_artifact},
            {"dimension":"line_coverage","target_profile":TARGET_PROFILE,"numerator":coverage_totals["line_coverage"]["covered"],"denominator":coverage_totals["line_coverage"]["total"],"evidence_id":coverage_artifact},
            {"dimension":"branch_coverage","target_profile":TARGET_PROFILE,"numerator":0,"denominator":0,"evidence_id":coverage_artifact},
            {"dimension":"region_coverage","target_profile":TARGET_PROFILE,"numerator":coverage_totals["region_coverage"]["covered"],"denominator":coverage_totals["region_coverage"]["total"],"evidence_id":coverage_artifact},
            {"dimension":"benchmark_budget_outcome","target_profile":TARGET_PROFILE,"numerator":if benchmark_outcome == "pass" {1} else {0},"denominator":1,"evidence_id":benchmark_artifact},
            {"dimension":"documentation_freshness","target_profile":TARGET_PROFILE,"numerator":1,"denominator":1,"evidence_id":Value::Null}
        ],
        "operations":operations(&cases, parity_outcome, coverage_outcome, benchmark_outcome, parity_artifact.as_deref(), coverage_artifact.as_deref(), benchmark_artifact.as_deref()),
        "stale_or_incompatible_evidence":stale
    });
    write_json(&migration_root.join("status-report.json"), &status_report)?;
    if let Some(result) = coverage_result {
        write_json(&migration_root.join("coverage-result.json"), &result)?;
    }
    if let Some(result) = benchmark_result {
        write_json(
            &migration_root.join("normalized-benchmark-result.json"),
            &result,
        )?;
    }
    let document_context = DocumentContext {
        repository: &repository,
        status: &status_report,
        cases: &cases,
        coverage: &coverage_totals,
        benchmark: &benchmark_summary,
        parity_outcome,
        coverage_outcome,
        benchmark_outcome,
        manifest_sha256: &manifest_sha256,
    };
    render_documents(&document_context)?;
    println!(
        "generated migration status: {}",
        migration_root.join("status-report.json").display()
    );
    Ok(())
}

fn indexed_inputs(manifest: &str) -> StatusResult<Vec<String>> {
    let mut in_index = false;
    let mut paths = Vec::new();
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed == "input_index:" {
            in_index = true;
            continue;
        }
        if in_index && trimmed == "coverage_components:" {
            break;
        }
        if in_index && trimmed.starts_with("- inputs/") {
            paths.push(trimmed.trim_start_matches("- ").to_owned());
        }
    }
    if paths.is_empty() {
        return Err("manifest input_index has no entries".into());
    }
    Ok(paths)
}

fn load_indexed_json(fixture_root: &Path, paths: &[String], kind: &str) -> StatusResult<Value> {
    let path = paths
        .iter()
        .find(|path| path.starts_with(&format!("inputs/{kind}/")))
        .ok_or_else(|| format!("manifest has no {kind} input"))?;
    read_json(&fixture_root.join(path))
}

#[derive(Clone)]
struct Case {
    id: String,
    surface: String,
    operation: String,
    requirements: Vec<String>,
    step: String,
}

struct LaneContext<'a> {
    repository: &'a Path,
    fixture_root: &'a Path,
    manifest_sha256: &'a str,
    target: &'a Value,
}

fn load_cases(fixture_root: &Path, paths: &[String]) -> StatusResult<Vec<Case>> {
    let mut cases = Vec::new();
    for path in paths
        .iter()
        .filter(|path| path.starts_with("inputs/parity/"))
    {
        let input = read_json(&fixture_root.join(path))?;
        let array = input["cases"]
            .as_array()
            .ok_or_else(|| format!("{path} has no cases array"))?;
        for case in array {
            let steps = case["steps"]
                .as_array()
                .ok_or_else(|| format!("{path} case has no steps"))?;
            let step = steps
                .first()
                .and_then(|step| step["step_id"].as_str())
                .ok_or_else(|| format!("{path} case has no step id"))?;
            cases.push(Case {
                id: string_field(case, "case_id", path)?,
                surface: string_field(case, "surface", path)?,
                operation: string_field(case, "operation", path)?,
                requirements: string_array(case, "covers", path)?,
                step: step.to_owned(),
            });
        }
    }
    if cases.is_empty() {
        return Err("manifest parity index has no cases".into());
    }
    Ok(cases)
}

fn string_field(value: &Value, key: &str, source: &str) -> StatusResult<String> {
    value[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{source} field {key} is not a string").into())
}

fn string_array(value: &Value, key: &str, source: &str) -> StatusResult<Vec<String>> {
    value[key]
        .as_array()
        .ok_or_else(|| format!("{source} field {key} is not an array"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{source} field {key} contains a non-string").into())
        })
        .collect()
}

fn read_json(path: &Path) -> StatusResult<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_prefix(&digest, digest.len())
}

fn hex_prefix(bytes: &[u8], max_bytes: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().min(max_bytes) * 2);
    for &byte in bytes.iter().take(max_bytes) {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn target_identity(repository: &Path) -> StatusResult<Value> {
    let revision = command_output(repository, "git", &["rev-parse", "HEAD"])?;
    let dirty = !command_output(
        repository,
        "git",
        &["status", "--porcelain", "--untracked-files=all"],
    )?
    .is_empty();
    let runtime = command_output(repository, "rustc", &["--version"])?;
    Ok(json!({
        "target_profile":TARGET_PROFILE,
        "target_id":"rust",
        "revision":revision,
        "dirty":dirty,
        "runtime":runtime,
        "backend":"cpu",
        "features":["bundled-duckdb"]
    }))
}

fn load_parity_marker(
    path: &Path,
    manifest_sha256: &str,
    target: &Value,
) -> StatusResult<(Option<Value>, Option<Value>)> {
    if !path.is_file() {
        return Ok((None, None));
    }
    let marker = match read_json(path) {
        Ok(marker) => marker,
        Err(error) => {
            return Ok((
                None,
                Some(json!({
                    "lane":"parity",
                    "run_id":"local-parity-marker",
                    "reason":format!("parity marker could not be read: {error}"),
                    "identity_diff":["marker"]
                })),
            ));
        }
    };
    let mut identity_diff = Vec::new();
    if marker["schema"].as_str() != Some(PARITY_MARKER_SCHEMA) {
        identity_diff.push("schema".to_owned());
    }
    if marker["lane"].as_str() != Some("parity") {
        identity_diff.push("lane".to_owned());
    }
    if marker["manifest"]["path"].as_str() != Some(MANIFEST_RELATIVE) {
        identity_diff.push("manifest.path".to_owned());
    }
    if marker["manifest"]["schema"].as_str() != Some("migration-parity/manifest@2") {
        identity_diff.push("manifest.schema".to_owned());
    }
    if marker["manifest"]["sha256"].as_str() != Some(manifest_sha256) {
        identity_diff.push("manifest.sha256".to_owned());
    }
    for field in [
        "target_profile",
        "target_id",
        "revision",
        "runtime",
        "backend",
    ] {
        if marker["target"][field] != target[field] {
            identity_diff.push(format!("target.{field}"));
        }
    }
    if marker["target"]["features"] != target["features"] {
        identity_diff.push("target.features".to_owned());
    }
    if identity_diff.is_empty() {
        Ok((Some(marker), None))
    } else {
        let run_id = marker["run_id"]
            .as_str()
            .unwrap_or("local-parity-marker")
            .to_owned();
        Ok((
            None,
            Some(json!({
                "lane":"parity",
                "run_id":run_id,
                "reason":"parity marker is stale or incompatible",
                "identity_diff":identity_diff
            })),
        ))
    }
}

fn command_output(repository: &Path, command: &str, args: &[&str]) -> StatusResult<String> {
    let output = if command == "git" {
        Command::new(command)
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()?
    } else {
        Command::new(command).args(args).output()?
    };
    if !output.status.success() {
        return Err(format!(
            "{command} {} failed with status {}",
            args.join(" "),
            output.status
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn coverage_result(
    raw: &Value,
    input: &Value,
    artifact_id: Option<&str>,
    context: &LaneContext<'_>,
) -> StatusResult<Value> {
    let totals = &raw["data"][0]["totals"];
    let dimensions = [
        ("function", "functions"),
        ("line", "lines"),
        ("region", "regions"),
    ];
    let dimensions_summary = json!({
        "function_coverage": dimension_summary(totals, "functions"),
        "line_coverage": dimension_summary(totals, "lines"),
        "region_coverage": dimension_summary(totals, "regions")
    });
    let files = raw["data"][0]["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .map(|file| {
                    let absolute_path = file["filename"].as_str().unwrap_or("unknown");
                    let path = Path::new(absolute_path)
                        .strip_prefix(context.repository)
                        .unwrap_or_else(|_| Path::new(absolute_path))
                        .to_string_lossy()
                        .replace('\\', "/");
                    let dimensions = dimensions
                        .iter()
                        .map(|(name, key)| {
                            let value = dimension_summary(&file["summary"], key);
                            json!({
                                "dimension":name,
                                "covered":value["covered"],
                                "total":value["total"],
                                "uncovered": if value["covered"] == value["total"] {json!([])} else {json!(["uncovered coverage records"])}
                            })
                        })
                        .collect::<Vec<_>>();
                    json!({"path":path,"dimensions":dimensions})
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let plan = &input["plans"][0];
    let component_id = plan["component_ids"][0].as_str().unwrap_or("rust-source");
    let thresholds = ["function", "line", "region"]
        .iter()
        .map(|dimension| {
            let value = dimensions_summary[format!("{dimension}_coverage")].clone();
            json!({
                "dimension":dimension,
                "minimum_percent":100,
                "covered":value["covered"],
                "total":value["total"],
                "outcome":if value["covered"] == value["total"] {"pass"} else {"fail"}
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema":"migration-parity/coverage-result@1",
        "identity":lane_identity(context, "inputs/coverage/rust_source.json", input, "coverage", json!(["cargo","llvm-cov"]), artifact_id, 900)?,
        "status":"not_ingested",
        "collector":{"name":"cargo-llvm-cov","version":raw["cargo_llvm_cov"]["version"],"snapshot_id":Value::Null,"artifact_ingested":false},
        "summary":{"plans_selected":1,"plans_executed":1,"plans_not_run":0,"tests_passed":0,"tests_failed":0},
        "plans":[{
            "plan_id":plan["plan_id"],
            "target_profile":TARGET_PROFILE,
            "requirements":[],
            "selected":plan["selectors"],
            "execution":{"status":"completed","tests_passed":0,"tests_failed":0},
            "components":[{"component_id":component_id,"files":files,"thresholds":thresholds}]
        }],
        "infrastructure_errors":[]
    }))
}

fn coverage_dimensions(raw: &Value) -> Value {
    let totals = &raw["data"][0]["totals"];
    json!({
        "function_coverage": dimension_summary(totals, "functions"),
        "line_coverage": dimension_summary(totals, "lines"),
        "region_coverage": dimension_summary(totals, "regions")
    })
}

fn dimension_summary(value: &Value, key: &str) -> Value {
    json!({
        "covered":value[key]["covered"].as_u64().unwrap_or(0),
        "total":value[key]["count"].as_u64().unwrap_or(0)
    })
}

fn parity_result(
    cases: &[Case],
    input_paths: &[String],
    context: &LaneContext<'_>,
) -> StatusResult<Value> {
    let parity_inputs = input_paths
        .iter()
        .filter(|path| path.starts_with("inputs/parity/"))
        .map(|path| {
            let input = read_json(&context.fixture_root.join(path))?;
            input_identity(context, path, &input)
        })
        .collect::<StatusResult<Vec<_>>>()?;
    let comparisons = cases
        .iter()
        .map(|case| {
            json!({
                "case_id":case.id,
                "target_profile":TARGET_PROFILE,
                "requirements":case.requirements,
                "source":workflow_not_run(&case.id, &case.step, "no independent source oracle is configured"),
                "target":workflow_not_run(&case.id, &case.step, "Rust conformance tests have no differential adapter"),
                "outcome":"not_run",
                "diffs":[]
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema":"migration-parity/parity-result@1",
        "identity":{
            "run_id":format!("local-parity-{}", &context.manifest_sha256[..16]),
            "started_at":Utc::now().to_rfc3339(),
            "finished_at":Utc::now().to_rfc3339(),
            "manifest":{"path":MANIFEST_RELATIVE,"schema":"migration-parity/manifest@2","sha256":context.manifest_sha256},
            "inputs":parity_inputs,
            "assets":[],
            "oracles":[{"oracle_id":"frozen-contract","name":"Schema-9 behavior contract","version":"9","runtime":"specification"}],
            "targets":[context.target],
            "command":{"command_id":"parity","argv":["cargo","test","rust_migration"],"cwd":".","timeout_seconds":900}
        },
        "status":"infrastructure_failed",
        "summary":{"selected":cases.len(),"executed":0,"passed":0,"failed":0,"not_run":cases.len(),"infrastructure_errors":1},
        "comparisons":comparisons,
        "infrastructure_errors":[{"scope":"oracle","id":Value::Null,"kind":"unavailable","message":"no independent source oracle is configured"}]
    }))
}

fn workflow_not_run(case_id: &str, step: &str, reason: &str) -> Value {
    json!({
        "case_id":case_id,
        "status":"not_run",
        "observations":[{"step_id":step,"status":"not_run","reason":reason}]
    })
}

fn benchmark_result(
    raw: &Value,
    input: &Value,
    artifact_id: Option<&str>,
    context: &LaneContext<'_>,
) -> StatusResult<Value> {
    let samples = raw["samples_latency_ms"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let values = samples.iter().filter_map(Value::as_f64).collect::<Vec<_>>();
    let stats = statistics(&values);
    let workload = &input["workloads"][0];
    let workload_id = workload["workload_id"].as_str().unwrap_or("unknown");
    let median = raw["median_latency_ms"]
        .as_f64()
        .or(stats["median"].as_f64());
    let budget_value = raw["budget"]["value"].as_f64().unwrap_or(5000.0);
    let budget_outcome = "not_proven";
    let subject = json!({
        "kind":"target_profile",
        "id":TARGET_PROFILE,
        "status":"completed",
        "measurements":[{"metric":"latency","unit":"millisecond","sample_count":values.len(),"statistics":stats,"raw_samples_ref":artifact_id}]
    });
    Ok(json!({
        "schema":"migration-parity/benchmark-result@1",
        "identity":lane_identity(context, "inputs/benchmark/compaction_workload.json", input, "benchmark", json!(["cargo","test","rust_compaction_benchmark_workload"]), artifact_id, 300)?,
        "status":"completed",
        "environment":{
            "machine_id":format!("{}-{}",std::env::consts::OS,std::env::consts::ARCH),
            "os":std::env::consts::OS,
            "architecture":std::env::consts::ARCH,
            "cpu":"unknown",
            "memory_bytes":0,
            "power_mode":"unknown",
            "toolchain":context.target["runtime"]
        },
        "summary":{"workloads_selected":1,"workloads_measured":1,"workloads_not_run":0,"budgets_passed":0,"budgets_failed":0,"budgets_not_proven":1},
        "workloads":[{
            "workload_id":workload_id,
            "requirements":workload["covers"],
            "measurement_policy":workload["measurement"],
            "correctness":{"gate":workload["measurement"]["correctness_gate"],"outcome":budget_outcome,"evidence_id":Value::Null},
            "subjects":[subject],
            "budgets":[{"requirement_id":"coverage-mcp.storage.compact.project-policy","subject_id":TARGET_PROFILE,"baseline_subject":Value::Null,"metric":"latency","statistic":"median","operator":"less_than_or_equal","required":budget_value,"observed":median,"unit":"millisecond","outcome":budget_outcome}]
        }],
        "suites":[],
        "infrastructure_errors":[]
    }))
}

fn benchmark_metrics(raw: &Value) -> Value {
    json!({
        "sample_count":raw["samples_latency_ms"].as_array().map_or(0, Vec::len),
        "median_latency_ms":raw["median_latency_ms"],
        "budget_outcome":raw["budget"]["outcome"]
    })
}

fn lane_identity(
    context: &LaneContext<'_>,
    input_path: &str,
    input: &Value,
    command_id: &str,
    argv: Value,
    artifact_id: Option<&str>,
    timeout_seconds: u64,
) -> StatusResult<Value> {
    let now = Utc::now().to_rfc3339();
    Ok(json!({
        "run_id":artifact_id.unwrap_or("local-missing"),
        "started_at":now,
        "finished_at":now,
        "manifest":{"path":MANIFEST_RELATIVE,"schema":"migration-parity/manifest@2","sha256":context.manifest_sha256},
        "inputs":[input_identity(context, input_path, input)?],
        "assets":[],
        "oracles":[],
        "targets":[context.target],
        "command":{"command_id":command_id,"argv":argv,"cwd":".","timeout_seconds":timeout_seconds}
    }))
}

fn input_identity(
    context: &LaneContext<'_>,
    input_path: &str,
    input: &Value,
) -> StatusResult<Value> {
    let input_bytes = fs::read(context.fixture_root.join(input_path))?;
    Ok(json!({
        "path":format!("tests/fixtures/{input_path}"),
        "schema":input["schema"],
        "sha256":sha256_hex(&input_bytes)
    }))
}

fn statistics(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({"min":Value::Null,"median":Value::Null,"mean":Value::Null,"p95":Value::Null,"p99":Value::Null,"max":Value::Null,"total":Value::Null,"weighted_mean":Value::Null,"standard_deviation":Value::Null});
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let percentile = |percent: f64| {
        let index = ((sorted.len() - 1) as f64 * percent).round() as usize;
        sorted[index]
    };
    let variance = sorted
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / sorted.len() as f64;
    json!({
        "min":sorted[0],
        "median":percentile(0.50),
        "mean":mean,
        "p95":percentile(0.95),
        "p99":percentile(0.99),
        "max":sorted[sorted.len() - 1],
        "total":Value::Null,
        "weighted_mean":Value::Null,
        "standard_deviation":variance.sqrt()
    })
}

fn operations(
    cases: &[Case],
    parity: &str,
    coverage: &str,
    benchmark: &str,
    parity_id: Option<&str>,
    coverage_id: Option<&str>,
    benchmark_id: Option<&str>,
) -> Value {
    Value::Array(
        cases
            .iter()
            .map(|case| {
                json!({
                    "surface":case.surface,
                    "operation":case.operation,
                    "target_profile":TARGET_PROFILE,
                    "classification":"endpoint",
                    "support":"supported",
                    "requirements":case.requirements,
                    "parity":{"applicability":"required","input_ids":[case.id],"outcome":parity,"evidence_id":parity_id,"details":if parity == "not_proven" {vec!["no independent source oracle is configured".to_owned()]} else {Vec::new()}},
                    "coverage":{"applicability":"required","input_ids":[case.id],"outcome":coverage,"evidence_id":coverage_id,"details":Vec::<String>::new()},
                    "benchmark":{"applicability":if case.operation == "compact" {"required"} else {"not_applicable"},"input_ids":if case.operation == "compact" {vec![case.id.clone()]} else {Vec::new()},"outcome":if case.operation == "compact" {benchmark} else {"not_applicable"},"evidence_id":if case.operation == "compact" {benchmark_id} else {None::<&str>},"details":Vec::<String>::new()}
                })
            })
            .collect(),
    )
}

struct DocumentContext<'a> {
    repository: &'a Path,
    status: &'a Value,
    cases: &'a [Case],
    coverage: &'a Value,
    benchmark: &'a Value,
    parity_outcome: &'a str,
    coverage_outcome: &'a str,
    benchmark_outcome: &'a str,
    manifest_sha256: &'a str,
}

fn render_documents(context: &DocumentContext<'_>) -> StatusResult<()> {
    let generated = context.repository.join("docs/generated");
    fs::create_dir_all(&generated)?;
    let evidence = context.status["evidence"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| item["run_id"].as_str().unwrap_or("null"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "none".to_owned());
    let header = format!(
        "<!-- Generated by {GENERATOR}; manifest sha256: {}; evidence: {evidence} -->\n",
        context.manifest_sha256
    );
    let parity_page = format!(
        "{header}# Migration parity status\n\n- Status: **{}**\n- Target profile: `{TARGET_PROFILE}`\n- Evidence: `{evidence}`\n\nRust migration tests are conformance tests against the checked-in schema-9 contract. Cross-runtime parity remains `not_proven` because no independent source oracle is configured.\n",
        context.parity_outcome
    );
    let function = &context.coverage["function_coverage"];
    let lines = &context.coverage["line_coverage"];
    let regions = &context.coverage["region_coverage"];
    let coverage_page = format!(
        "{header}# Coverage status\n\n- Status: **{}**\n- Target profile: `{TARGET_PROFILE}`\n- Functions: `{}/{} ({:.2}%)`\n- Lines: `{}/{} ({:.2}%)`\n- Regions: `{}/{} ({:.2}%)`\n\nFunction, line, and region coverage are measured from the manifest-selected Rust library target with `cargo llvm-cov`; this local aggregate does not claim a passing status without a fresh ingested snapshot and a clean target identity.\n",
        context.coverage_outcome,
        function["covered"],
        function["total"],
        percent(function),
        lines["covered"],
        lines["total"],
        percent(lines),
        regions["covered"],
        regions["total"],
        percent(regions)
    );
    let benchmark_page = format!(
        "{header}# Benchmark status\n\n- Status: **{}**\n- Target profile: `{TARGET_PROFILE}`\n- Workloads measured: `{}`\n- Median compaction latency: `{}` ms\n- Budget: `<= 5000` ms\n\nThe benchmark is correctness-gated within the Rust implementation. The aggregate remains `not_proven` when the independent parity lane is unavailable, even if the measured compaction workload passes.\n",
        context.benchmark_outcome,
        context.benchmark["sample_count"],
        context.benchmark["median_latency_ms"]
    );
    let mut contract = format!(
        "{header}# Generated migration contract\n\n- Manifest: `{MANIFEST_RELATIVE}`\n- Manifest schema: `migration-parity/manifest@2`\n- Target profile: `{TARGET_PROFILE}`\n- Indexed parity cases: `{}`\n\n| Case | Surface | Operation | Step | Requirements |\n| --- | --- | --- | --- | --- |\n",
        context.cases.len()
    );
    for case in context.cases {
        contract.push_str(&format!(
            "| `{}` | `{}` | `{}` | `{}` | `{}` |\n",
            case.id,
            case.surface,
            case.operation,
            case.step,
            case.requirements.join(", ")
        ));
    }
    write_text(&generated.join("parity-status.md"), &parity_page)?;
    write_text(&generated.join("coverage-status.md"), &coverage_page)?;
    write_text(&generated.join("benchmark-status.md"), &benchmark_page)?;
    write_text(&generated.join("public-contract.md"), &contract)?;
    Ok(())
}

fn percent(value: &Value) -> f64 {
    let covered = value["covered"].as_u64().unwrap_or(0) as f64;
    let total = value["total"].as_u64().unwrap_or(0) as f64;
    if total == 0.0 {
        0.0
    } else {
        covered * 100.0 / total
    }
}

fn write_json(path: &Path, value: &Value) -> StatusResult<()> {
    write_text(path, &serde_json::to_string_pretty(value)?)
}

fn write_text(path: &Path, contents: &str) -> StatusResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{contents}\n"))?;
    Ok(())
}
