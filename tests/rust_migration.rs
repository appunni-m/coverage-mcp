//! Rust migration parity tests.
//!
//! These tests exercise the migrated public behavior families: parsers,
//! normalized models, DuckDB storage, managed runs, projections, compaction,
//! REST, and MCP metadata/wire behavior.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use coverage_mcp::config::ServerConfig;
use coverage_mcp::http::CoverageServer;
use coverage_mcp::mcp;
use coverage_mcp::models::{CoverageBuilder, LineCoverage, normalize_report_path, rate};
use coverage_mcp::parser::{normalize_format, parse_coverage_report};
use coverage_mcp::service::{CoverageService, RequestContext};
use coverage_mcp::storage::{CoverageStore, ProjectSettingsPatch};
use coverage_mcp::{AppError, SCHEMA_REVISION};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

fn config() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".to_owned(),
        port: 59_471,
        db_path: None,
        common_db_path: std::env::temp_dir().join(format!(
            "coverage-mcp-test-common-{}.duckdb",
            std::process::id()
        )),
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

fn write_file(root: &Path, name: &str, content: &str) -> PathBuf {
    let path = root.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&path, content).unwrap();
    path
}

fn store(root: &Path) -> CoverageStore {
    let store = CoverageStore::open(root.join("coverage.duckdb"), config()).unwrap();
    store.ensure_project(root).unwrap();
    store
}

fn git_repo(root: &Path) {
    std::fs::create_dir_all(root).unwrap();
    run_git(root, &["init", "-b", "main"]);
    run_git(root, &["config", "user.email", "rust@example.com"]);
    run_git(root, &["config", "user.name", "Rust Tests"]);
    write_file(root, "src/a.py", "one\ntwo\nthree\n");
    run_git(root, &["add", "."]);
    run_git(root, &["commit", "-m", "base"]);
}

fn run_git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn ingest(store: &CoverageStore, report: &Path, branch: &str, commit: &str) -> Value {
    store
        .ingest_report(
            report,
            "lcov",
            None,
            Some(branch),
            Some(commit),
            None,
            "unit",
        )
        .unwrap()
}

#[test]
fn rust_migrates_all_parser_formats_and_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let fixtures = [
        (
            "lcov.info",
            "TN:\nSF:src/a.py\nDA:1,2\nDA:2,0\nBRDA:2,0,0,1\nend_of_record\n",
            "lcov",
        ),
        (
            "coverage.json",
            r#"{"files":{"src/a.py":{"executed_lines":[1],"missing_lines":[2],"executed_branches":[[1,0]],"missing_branches":[[2,0]]},"no-lines":{}}}"#,
            "coveragepy",
        ),
        (
            "cobertura.xml",
            r#"<coverage><packages><package><classes><class filename="src/a.py"><lines><line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/></lines></class></classes></package></packages></coverage>"#,
            "cobertura",
        ),
        (
            "jacoco.xml",
            r#"<report><package name="src"><sourcefile name="a.py"><line nr="1" mi="0" ci="2" mb="1" cb="1"/></sourcefile></package></report>"#,
            "jacoco",
        ),
        (
            "istanbul.json",
            r#"{"src/a.js":{"path":"src/a.js","statementMap":{"0":{"start":{"line":1},"end":{"line":1}}},"s":{"0":1},"fnMap":{"0":{"loc":{"start":{"line":1}}}},"f":{"0":1},"branchMap":{"0":{"loc":{"start":{"line":1}}}},"b":{"0":[1,0]}}}"#,
            "istanbul",
        ),
        ("cover.out", "mode: set\nsrc/a.go:1.1,2.1 2 1\n", "go"),
        (
            "llvm.json",
            r#"{"data":[{"files":[{"filename":"src/a.c","segments":[[1,0,2,true]],"branches":[{"line":1,"true_count":1,"false_count":0}],"summary":{"lines":{"count":1,"covered":1},"regions":{"count":1,"covered":1}}}]}]}"#,
            "llvm",
        ),
    ];
    for (name, content, expected) in fixtures {
        let path = write_file(directory.path(), name, content);
        let report = parse_coverage_report(&path, expected, None).unwrap();
        assert_eq!(report.format, expected);
        assert!(!report.files.is_empty(), "{expected} emitted no files");
        assert!(!report.lines.is_empty(), "{expected} emitted no lines");
        let auto = parse_coverage_report(&path, "auto", None);
        assert!(
            auto.is_ok(),
            "auto detection failed for {expected}: {auto:?}"
        );
    }
    assert_eq!(normalize_format("coverage.py").unwrap(), "coveragepy");
    assert_eq!(normalize_format("coverage-json").unwrap(), "coveragepy");
    assert_eq!(normalize_format("nyc").unwrap(), "istanbul");
    assert_eq!(normalize_format("go-coverprofile").unwrap(), "go");
    assert!(normalize_format("unknown").is_err());
    assert!(parse_coverage_report(&directory.path().join("missing"), "lcov", None).is_err());
}

#[test]
fn rust_migration_fixture_inputs_drive_registered_lanes() {
    let parser: Value =
        serde_json::from_str(include_str!("fixtures/inputs/parity/parser_formats.json")).unwrap();
    assert_eq!(parser["schema"], "migration-parity/parity-input@1");
    for format in parser["cases"][0]["steps"][0]["arguments"]["input"]["value"]["formats"]
        .as_array()
        .unwrap()
    {
        assert!(normalize_format(format.as_str().unwrap()).is_ok());
    }

    let storage: Value = serde_json::from_str(include_str!(
        "fixtures/inputs/parity/storage_compaction.json"
    ))
    .unwrap();
    assert_eq!(
        storage["cases"][0]["steps"][0]["arguments"]["input"]["value"]["default_after_days"],
        config().default_compaction_after_days
    );
    assert_eq!(
        storage["cases"][0]["steps"][0]["arguments"]["input"]["value"]["batch_size"],
        config().default_compaction_batch_size
    );

    let transport: Value = serde_json::from_str(include_str!(
        "fixtures/inputs/parity/transport_contract.json"
    ))
    .unwrap();
    assert_eq!(
        transport["cases"][0]["steps"][0]["arguments"]["input"]["value"]["schema_revision"],
        SCHEMA_REVISION
    );
    assert_eq!(
        transport["cases"][0]["steps"][0]["arguments"]["input"]["value"]["tools"],
        mcp::tools_list().as_array().unwrap().len()
    );

    let coverage: Value =
        serde_json::from_str(include_str!("fixtures/inputs/coverage/rust_source.json")).unwrap();
    assert_eq!(coverage["schema"], "migration-parity/coverage-input@1");
    assert_eq!(coverage["plans"][0]["component_ids"][0], "rust-source");
    assert_eq!(
        coverage["plans"][0]["selectors"]["parity_case_ids"]
            .as_array()
            .unwrap()
            .len(),
        3
    );

    let benchmark: Value = serde_json::from_str(include_str!(
        "fixtures/inputs/benchmark/compaction_workload.json"
    ))
    .unwrap();
    let measurement = &benchmark["workloads"][0]["measurement"];
    assert_eq!(benchmark["schema"], "migration-parity/benchmark-input@1");
    assert_eq!(measurement["boundary"], "observed_steps");
    assert_eq!(measurement["correctness_gate"], "parity_pass");
    assert_eq!(measurement["metrics"], json!(["latency"]));
    assert!(measurement["warmup_iterations"].as_u64().unwrap() >= 1);
    assert!(measurement["measurement_iterations"].as_u64().unwrap() >= 1);
    assert!(measurement["samples"].as_u64().unwrap() >= 1);

    let manifest = include_str!("fixtures/manifest.yaml");
    for input in [
        "inputs/parity/parser_formats.json",
        "inputs/parity/storage_compaction.json",
        "inputs/parity/transport_contract.json",
        "inputs/coverage/rust_source.json",
        "inputs/benchmark/compaction_workload.json",
    ] {
        assert!(
            manifest.contains(&format!("- {input}")),
            "fixture is not indexed: {input}"
        );
    }
}

fn run_compaction_benchmark_sample() -> (Duration, Value) {
    let directory = tempfile::tempdir().unwrap();
    write_file(directory.path(), "src/a.py", "one\ntwo\nthree\n");
    let report = write_file(
        directory.path(),
        "benchmark.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:3,1\nend_of_record\n",
    );
    let store = store(directory.path());
    let snapshot = ingest(&store, &report, "main", "benchmark");
    let snapshot_id = snapshot["id"].as_str().unwrap().to_owned();
    let db_path = store.db_path().to_owned();
    store.close().unwrap();
    {
        let connection = duckdb::Connection::open(&db_path).unwrap();
        connection
            .execute(
                "UPDATE snapshots SET created_at = CAST(current_timestamp AS TIMESTAMP) - INTERVAL '60 days', minute_bucket = CAST(current_timestamp AS TIMESTAMP) - INTERVAL '60 days' WHERE id = ?",
                duckdb::params![snapshot_id],
            )
            .unwrap();
    }
    let reopened = CoverageStore::open(db_path, config()).unwrap();
    reopened.ensure_project(directory.path()).unwrap();
    let started = Instant::now();
    let result = reopened.compact_now().unwrap();
    let elapsed = started.elapsed();
    assert_eq!(result["status"], "completed");
    assert_eq!(result["compacted_snapshots"], 1);
    reopened.close().unwrap();
    (elapsed, result)
}

fn maybe_write_benchmark_report(
    report_path: Option<PathBuf>,
    samples: &[Duration],
    median: Duration,
) {
    let Some(report_path) = report_path else {
        return;
    };
    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let sample_ms = samples
        .iter()
        .map(|sample| sample.as_secs_f64() * 1_000.0)
        .collect::<Vec<_>>();
    std::fs::write(
        report_path,
        serde_json::to_vec_pretty(&json!({
            "schema":"migration-parity/benchmark-result@1",
            "workload_id":"coverage-mcp.storage.compact.benchmark",
            "target_profile":"rust-default",
            "correctness_gate":"parity_pass",
            "samples_latency_ms":sample_ms,
            "median_latency_ms":median.as_secs_f64() * 1_000.0,
            "budget":{"operator":"less_than_or_equal","value":5000,"unit":"milliseconds","outcome":"pass"}
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn rust_compaction_benchmark_workload() {
    let workload: Value = serde_json::from_str(include_str!(
        "fixtures/inputs/benchmark/compaction_workload.json"
    ))
    .unwrap();
    let measurement = &workload["workloads"][0]["measurement"];
    let warmup_iterations = measurement["warmup_iterations"].as_u64().unwrap() as usize;
    let measurement_iterations = measurement["measurement_iterations"].as_u64().unwrap() as usize;
    let samples_requested = measurement["samples"].as_u64().unwrap() as usize;
    assert_eq!(measurement["concurrency"], 1);
    assert_eq!(measurement["correctness_gate"], "parity_pass");
    for _ in 0..warmup_iterations {
        let _ = run_compaction_benchmark_sample();
    }
    let mut samples = Vec::with_capacity(samples_requested * measurement_iterations);
    for _ in 0..samples_requested {
        for _ in 0..measurement_iterations {
            samples.push(run_compaction_benchmark_sample().0);
        }
    }
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    assert!(
        median <= Duration::from_secs(5),
        "compaction median exceeded the manifest budget: {median:?}"
    );
    maybe_write_benchmark_report(
        std::env::var_os("MIGRATION_BENCHMARK_REPORT").map(PathBuf::from),
        &samples,
        median,
    );
}

#[test]
fn rust_benchmark_report_can_be_written() {
    let directory = tempfile::tempdir().unwrap();
    let report_path = directory.path().join("nested/benchmark.json");
    maybe_write_benchmark_report(
        Some(report_path.clone()),
        &[Duration::from_millis(1), Duration::from_millis(2)],
        Duration::from_millis(2),
    );
    let report: Value = serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
    assert_eq!(report["schema"], "migration-parity/benchmark-result@1");
    assert_eq!(report["samples_latency_ms"][1], 2.0);
}

#[test]
fn rust_models_merge_metrics_and_path_normalization() {
    assert_eq!(rate(1, 2), Some(0.5));
    assert_eq!(rate(0, 0), None);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("src/a.py");
    write_file(directory.path(), "src/a.py", "one\n");
    assert_eq!(
        normalize_report_path(source.to_str().unwrap(), directory.path().to_str()),
        "src/a.py"
    );
    let outside = directory.path().join("../outside.py");
    assert_eq!(
        normalize_report_path(outside.to_str().unwrap(), directory.path().to_str()),
        outside.to_string_lossy().replace('\\', "/")
    );
    let outside_absolute = tempfile::NamedTempFile::new().unwrap();
    assert_eq!(
        normalize_report_path(
            outside_absolute.path().to_str().unwrap(),
            directory.path().to_str()
        ),
        outside_absolute.path().to_string_lossy().replace('\\', "/")
    );
    let mut builder = CoverageBuilder::new(Some(directory.path().to_str().unwrap()));
    builder.add_line(
        source.to_str().unwrap(),
        1,
        1,
        None,
        true,
        1,
        1,
        0,
        0,
        json!({"first":true}),
    );
    builder.add_line(
        "src/a.py",
        1,
        3,
        Some(true),
        true,
        0,
        0,
        1,
        1,
        json!({"second":true}),
    );
    builder.add_line("src/a.py", 0, 3, Some(true), true, 0, 0, 0, 0, json!({}));
    let mut metrics = serde_json::Map::new();
    metrics.insert("total_lines".to_owned(), json!(4));
    metrics.insert("covered_lines".to_owned(), json!(3));
    builder.add_file_metrics("src/a.py", metrics);
    let report = builder.build(
        "test",
        "fixture",
        vec!["warning".to_owned()],
        json!({"meta":1}),
    );
    assert_eq!(report.lines.len(), 1);
    assert_eq!(report.lines[0].hits, 3);
    assert_eq!(report.files[0].total_lines, 4);
    assert_eq!(report.files[0].covered_lines, 3);
    assert_eq!(report.warnings.len(), 1);

    let mut scalar = LineCoverage {
        file_path: "a.py".to_owned(),
        line_number: 1,
        hits: 1,
        covered: true,
        count_line: true,
        total_branches: 0,
        covered_branches: 0,
        total_functions: 0,
        covered_functions: 0,
        details: json!("scalar"),
    };
    let extra = scalar.clone();
    scalar.merge(&extra);
    assert_eq!(scalar.details, json!("scalar"));
}

#[test]
fn rust_storage_queries_compare_and_compacts_old_detail() {
    let directory = tempfile::tempdir().unwrap();
    write_file(directory.path(), "src/a.py", "one\ntwo\nthree\n");
    let base_report = write_file(
        directory.path(),
        "base.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nDA:2,1\nDA:4,1\nend_of_record\n",
    );
    let current_report = write_file(
        directory.path(),
        "current.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:3,1\nDA:4,0\nend_of_record\n",
    );
    let store = store(directory.path());
    let base = ingest(&store, &base_report, "main", "base");
    let current = ingest(&store, &current_report, "feature", "head");
    assert_eq!(current["total_lines"], 4);
    assert_eq!(
        store
            .latest_snapshot(None, Some("feature"), Some("unit"))
            .unwrap()
            .unwrap()["id"],
        current["id"]
    );
    assert_eq!(
        store.files(current["id"].as_str().unwrap(), 100).unwrap()[0]["file_path"],
        "src/a.py"
    );
    let gaps = store
        .file_gaps(current["id"].as_str().unwrap(), "src/a.py", 10)
        .unwrap();
    assert_eq!(gaps["uncovered_line_count"], 2);
    assert_eq!(gaps["ranges"][0]["start"], 2);
    assert_eq!(
        store
            .lines_in_ranges(current["id"].as_str().unwrap(), "src/a.py", &[(1, 2)])
            .unwrap()["line_count"],
        2
    );
    assert_eq!(
        store
            .source_lines(current["id"].as_str().unwrap(), "src/a.py", 2, 2)
            .unwrap()[0]["text"],
        "two"
    );
    assert!(
        store
            .source_lines(current["id"].as_str().unwrap(), "../missing", 1, 1)
            .is_err()
    );
    assert_eq!(
        store
            .line_history("src/a.py", 1, None, Some("unit"), 100)
            .unwrap()
            .len(),
        2
    );
    let comparison = store
        .compare(
            current["id"].as_str().unwrap(),
            base["id"].as_str().unwrap(),
            100,
            100,
        )
        .unwrap();
    assert_eq!(comparison["overall"]["total_lines_delta"], 1);
    assert_eq!(
        store
            .changed_lines(
                current["id"].as_str().unwrap(),
                base["id"].as_str().unwrap(),
                None,
                true,
                100
            )
            .unwrap()[0]["status"],
        "regressed"
    );
    let targets = store
        .targets(current["id"].as_str().unwrap(), "priority", 100)
        .unwrap();
    assert_eq!(targets[0]["file_path"], "src/a.py");
    assert_eq!(targets[0]["regions"][0]["start"], 2);
    assert!(
        store
            .targets(base["id"].as_str().unwrap(), "priority", 100)
            .unwrap()
            .is_empty()
    );
    for order_by in ["uncovered_lines", "line_rate", "file_path"] {
        assert!(
            store
                .targets(current["id"].as_str().unwrap(), order_by, 100)
                .is_ok()
        );
    }
    assert!(
        store
            .targets(current["id"].as_str().unwrap(), "invalid", 100)
            .is_err()
    );
    let regions = store
        .changed_regions(
            current["id"].as_str().unwrap(),
            base["id"].as_str().unwrap(),
            None,
            false,
            100,
        )
        .unwrap();
    assert!(regions.iter().any(|value| value["status"] == "regressed"));
    assert!(
        store
            .insights(
                current["id"].as_str().unwrap(),
                Some(base["id"].as_str().unwrap()),
                100
            )
            .unwrap()["summary"]["item_count"]
            .as_i64()
            .unwrap()
            >= 1
    );
    let snapshot_id = current["id"].as_str().unwrap().to_owned();
    let db_path = store.db_path().to_owned();
    store.close().unwrap();
    {
        let connection = duckdb::Connection::open(&db_path).unwrap();
        connection.execute("UPDATE snapshots SET created_at = CAST(current_timestamp AS TIMESTAMP) - INTERVAL '60 days', minute_bucket = CAST(current_timestamp AS TIMESTAMP) - INTERVAL '60 days' WHERE id = ?", duckdb::params![snapshot_id]).unwrap();
    }
    let reopened = CoverageStore::open(db_path, config()).unwrap();
    reopened.ensure_project(directory.path()).unwrap();
    reopened
        .update_project_settings(ProjectSettingsPatch {
            compaction_after_days: Some(30),
            compaction_interval_seconds: Some(3_600),
            compaction_batch_size: Some(10),
            ..Default::default()
        })
        .unwrap();
    let result = reopened.compact_now().unwrap();
    assert_eq!(result["compacted_snapshots"], 1);
    assert_eq!(reopened.files(&snapshot_id, 100).unwrap().len(), 1);
    assert_eq!(
        reopened.lines(&snapshot_id, "src/a.py", 100).unwrap().len(),
        4
    );
    assert_eq!(
        reopened.targets(&snapshot_id, "priority", 100).unwrap()[0]["regions"][0]["start"],
        2
    );
    assert_eq!(
        reopened.file_coverage(&snapshot_id, "src/a.py").unwrap()["file_path"],
        "src/a.py"
    );
    let gaps_report = write_file(
        directory.path(),
        "gaps.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nDA:4,0\nDA:5,0\nend_of_record\n",
    );
    let gaps_snapshot = reopened
        .ingest_report(
            &gaps_report,
            "lcov",
            Some(directory.path()),
            Some("main"),
            Some("gaps"),
            None,
            "unit",
        )
        .unwrap();
    let gap_ranges = reopened
        .file_gaps(gaps_snapshot["id"].as_str().unwrap(), "src/a.py", 10)
        .unwrap();
    assert_eq!(gap_ranges["ranges"].as_array().unwrap().len(), 2);
    assert!(reopened.file_coverage(&snapshot_id, "missing.py").is_err());
    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().unwrap();
        let outside_file = write_file(outside.path(), "outside.py", "outside\n");
        let link = directory.path().join("escape.py");
        std::os::unix::fs::symlink(&outside_file, &link).unwrap();
        assert!(
            reopened
                .source_lines(&snapshot_id, "escape.py", 1, 1)
                .is_err()
        );
    }
    let compacted_db = reopened.db_path().to_owned();
    assert!(
        reopened.project_summary().unwrap()["compaction"]["compaction_last_bytes_after"]
            .as_u64()
            .unwrap()
            > 0
    );
    reopened.close().unwrap();
    {
        let connection = duckdb::Connection::open(&compacted_db).unwrap();
        let invalid_json = zstd::encode_all("not-json".as_bytes(), 3).unwrap();
        let changed = connection
            .execute(
                "UPDATE coverage_compacted_payloads SET payload = ? WHERE snapshot_id = ?",
                duckdb::params![invalid_json, snapshot_id],
            )
            .unwrap();
        assert_eq!(changed, 1);
        let remaining: i64 = connection
            .query_row(
                "SELECT count(*) FROM files WHERE snapshot_id = ?",
                duckdb::params![snapshot_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);
    }
    let corrupted = CoverageStore::open(compacted_db.clone(), config()).unwrap();
    corrupted.ensure_project(directory.path()).unwrap();
    assert!(
        corrupted
            .compare_regions(&snapshot_id, base["id"].as_str().unwrap(), None, false, 100)
            .is_err()
    );
    let corrupted_result = corrupted.files(&snapshot_id, 100);
    assert!(corrupted_result.is_err());
    corrupted.close().unwrap();
    {
        let connection = duckdb::Connection::open(&compacted_db).unwrap();
        connection
            .execute(
                "UPDATE coverage_compacted_payloads SET payload = ? WHERE snapshot_id = ?",
                duckdb::params![vec![1_u8, 2, 3], snapshot_id],
            )
            .unwrap();
    }
    let undecodable = CoverageStore::open(compacted_db, config()).unwrap();
    undecodable.ensure_project(directory.path()).unwrap();
    assert!(undecodable.files(&snapshot_id, 100).is_err());
    undecodable.close().unwrap();
}

#[test]
fn rust_worktree_registration_and_lineage_guards() {
    let directory = tempfile::tempdir().unwrap();
    git_repo(directory.path());
    let report = write_file(
        directory.path(),
        "base.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n",
    );
    let store = store(directory.path());
    let no_baseline = store
        .register_worktree(directory.path(), "main", Some("before coverage"))
        .unwrap();
    let base = ingest(&store, &report, "main", &git_sha(directory.path()));
    assert!(no_baseline["baseline_snapshot_id"].is_null());
    assert!(
        store
            .compare_worktree(
                no_baseline["id"].as_str().unwrap(),
                Some(base["id"].as_str().unwrap()),
                100,
                100
            )
            .is_err()
    );
    let worktree = store
        .register_worktree(directory.path(), "main", Some("main checkout"))
        .unwrap();
    assert_eq!(worktree["baseline_snapshot_id"], base["id"]);
    assert_eq!(store.list_worktrees(100).unwrap().len(), 2);
    let progress = store
        .worktree_progress(worktree["id"].as_str().unwrap(), "unit", None, 100)
        .unwrap();
    assert_eq!(progress["baseline"]["id"], base["id"]);
    assert_eq!(progress["points"].as_array().unwrap().len(), 1);
    assert!(
        store
            .compare_worktree(
                worktree["id"].as_str().unwrap(),
                Some(base["id"].as_str().unwrap()),
                100,
                100
            )
            .is_ok()
    );
    assert!(
        store
            .compare("missing", base["id"].as_str().unwrap(), 100, 100)
            .is_err()
    );
    store.close().unwrap();
}

fn git_sha(root: &Path) -> String {
    String::from_utf8(
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned()
}

#[test]
fn rust_managed_runs_keep_idempotency_logs_and_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let artifact =
        json!({"coverage":{"path":"coverage.lcov","coverage_format":"lcov","suite":"unit"}});
    let command = store.register_command("unit", "printf 'TN:\\nSF:a.py\\nDA:1,1\\nend_of_record\\n' > coverage.lcov; echo passed; echo warning >&2", Some(directory.path()), "/bin/sh", Some(artifact), true, "tester", "approved Rust migration command", true).unwrap();
    let run = store
        .run_command(
            command["id"].as_str().unwrap(),
            Some(10),
            Some("one-run"),
            20,
        )
        .unwrap();
    assert_eq!(run["status"], "passed");
    assert!(run["terminal"].as_bool().unwrap());
    assert_eq!(run["parsed_summary"]["counters"]["passed"], 1);
    assert_eq!(run["parsed_summary"]["truncated"], false);
    assert!(run["parsed_summary"]["stdout_bytes"].as_u64().unwrap() > 0);
    assert_eq!(run["coverage_ingest"]["status"], "ingested");
    let repeated = store
        .submit_command(
            command["id"].as_str().unwrap(),
            Some(10),
            Some("one-run"),
            20,
        )
        .unwrap();
    assert_eq!(repeated["id"], run["id"]);
    assert_eq!(repeated["submission_reused"], true);
    let logs = store
        .search_run_logs(
            run["id"].as_str().unwrap(),
            &["passed".to_owned(), "warning".to_owned()],
            "both",
            0,
            10,
            false,
            200,
        )
        .unwrap();
    assert_eq!(logs["match_count"], 2);
    let artifact = store
        .latest_artifact("coverage", Some(command["id"].as_str().unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(artifact["ingest_status"], "ingested");
    assert!(store.cancel_run(run["id"].as_str().unwrap(), 20).is_err());
    store.close().unwrap();
}

#[test]
fn rust_storage_edge_paths_and_background_run_states_are_covered() {
    let directory = tempfile::tempdir().unwrap();
    assert!(
        CoverageStore::open(
            directory.path().join("invalid.duckdb"),
            ServerConfig {
                run_concurrency: 0,
                ..config()
            },
        )
        .is_err()
    );
    assert!(
        CoverageStore::open(
            directory.path().join("invalid-retention.duckdb"),
            ServerConfig {
                run_retention: 0,
                ..config()
            },
        )
        .is_err()
    );
    let path_blocker = directory.path().join("not-a-directory");
    std::fs::write(&path_blocker, "blocker").unwrap();
    assert!(CoverageStore::open(path_blocker.join("coverage.duckdb"), config()).is_err());
    let nested_store = CoverageStore::open(
        directory.path().join("nested/database/coverage.duckdb"),
        config(),
    )
    .unwrap();
    nested_store.close().unwrap();

    git_repo(directory.path());
    let base_report = write_file(
        directory.path(),
        "edge-base.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nDA:3,0\nSF:src/b.py\nDA:1,1\nBRDA:1,0,0,1\nSF:src/removed.py\nDA:1,1\nend_of_record\n",
    );
    let current_report = write_file(
        directory.path(),
        "edge-current.lcov",
        "TN:\nSF:src/a.py\nDA:1,0\nDA:2,1\nDA:3,1\nDA:4,0\nDA:5,0\nDA:6,0\nBRDA:1,0,0,1\nBRDA:1,0,1,0\nSF:src/b.py\nDA:1,1\nBRDA:1,0,0,1\nBRDA:1,0,1,1\nSF:src/c.py\nDA:1,1\nSF:src/zero.py\nDA:1,0\nend_of_record\n",
    );
    let other_suite_report = write_file(
        directory.path(),
        "edge-other.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n",
    );
    let mut edge_config = config();
    edge_config.run_concurrency = 1;
    edge_config.run_retention = 1;
    let store = CoverageStore::open(directory.path().join("edge.duckdb"), edge_config).unwrap();
    assert!(store.project().is_err());
    store.ensure_project(directory.path()).unwrap();
    assert!(
        store
            .ingest_report(
                &base_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some("blank-suite"),
                None,
                " ",
            )
            .is_err()
    );
    let base = ingest(&store, &base_report, "main", "base");
    let current = ingest(&store, &current_report, "feature", "head");
    let other = store
        .ingest_report(
            &other_suite_report,
            "lcov",
            Some(directory.path()),
            Some("feature"),
            Some("other"),
            None,
            "integration",
        )
        .unwrap();
    let base_id = base["id"].as_str().unwrap();
    let current_id = current["id"].as_str().unwrap();
    let other_id = other["id"].as_str().unwrap();

    assert!(store.targets(current_id, "priority", 100).unwrap().len() >= 2);

    assert_eq!(store.project_summary().unwrap()["snapshot_count"], 3);
    assert!(
        store
            .list_snapshots(None, Some("missing"), None, 0)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .latest_snapshot(None, Some("missing"), None)
            .unwrap()
            .is_none()
    );
    assert!(store.snapshot("missing").is_err());
    assert!(store.file_coverage(current_id, "missing.py").is_err());
    assert!(
        store
            .lines(current_id, "missing.py", 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.file_gaps(current_id, "missing.py", 10).unwrap()["ranges"],
        json!([])
    );
    assert!(store.source_lines(current_id, "src/a.py", 0, 2).is_err());
    assert!(store.source_lines(current_id, ".", 1, 2).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let unreadable_path = write_file(directory.path(), "unreadable.py", "hidden\n");
        let mut permissions = std::fs::metadata(&unreadable_path).unwrap().permissions();
        permissions.set_mode(0o0);
        std::fs::set_permissions(&unreadable_path, permissions).unwrap();
        assert!(
            store
                .source_lines(current_id, "unreadable.py", 1, 1)
                .is_err()
        );
    }
    assert!(store.source_lines(current_id, "src/a.py", 3, 2).is_err());
    assert!(
        store
            .line_history("missing.py", 1, None, Some("unit"), 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .line_history("src/a.py", 1, Some("missing"), Some("unit"), 10)
            .unwrap()
            .is_empty()
    );
    assert!(store.line_history("src/a.py", 1, None, None, 10).is_ok());
    assert!(
        store.lines_in_ranges(current_id, "src/a.py", &[]).unwrap()["line_count"]
            .as_i64()
            .unwrap()
            == 0
    );

    let changes = store
        .changed_lines(current_id, base_id, None, false, 100)
        .unwrap();
    let statuses = changes
        .iter()
        .filter_map(|value| value["status"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(statuses.contains("regressed"));
    assert!(statuses.contains("improved"));
    assert!(statuses.contains("new"));
    assert!(statuses.contains("removed"));
    assert!(statuses.contains("changed"));
    assert!(
        store
            .changed_lines(current_id, base_id, Some("src/a.py"), true, 100)
            .unwrap()
            .iter()
            .all(|value| value["status"] == "regressed")
    );
    assert!(store.compare(current_id, other_id, 100, 100).is_err());
    assert!(
        store
            .changed_lines(current_id, other_id, None, false, 100)
            .is_err()
    );
    assert!(
        store
            .changed_regions(current_id, other_id, None, false, 100)
            .is_err()
    );
    assert!(store.compare(current_id, base_id, 0, 0).is_ok());
    let initial_insights = store.insights(current_id, None, 100).unwrap();
    assert!(initial_insights["items"].is_array());
    let insight_categories = initial_insights["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["category"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(insight_categories.contains("zero-coverage-file"));
    assert!(insight_categories.contains("low-line-coverage"));
    assert!(insight_categories.contains("low-branch-coverage"));
    let healthy_report = write_file(
        directory.path(),
        "healthy.lcov",
        "TN:\nSF:src/healthy.py\nDA:1,1\nDA:2,1\nDA:3,1\nDA:4,1\nDA:5,1\nend_of_record\n",
    );
    let healthy_snapshot = store
        .ingest_report(
            &healthy_report,
            "lcov",
            Some(directory.path()),
            Some("feature"),
            Some("healthy"),
            None,
            "unit",
        )
        .unwrap();
    assert!(
        store
            .insights(healthy_snapshot["id"].as_str().unwrap(), None, 10)
            .unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let warning_report = write_file(directory.path(), "warning.lcov", "TN:\n");
    let warning_snapshot = store
        .ingest_report(
            &warning_report,
            "lcov",
            Some(directory.path()),
            Some("feature"),
            Some("warning"),
            None,
            "unit",
        )
        .unwrap();
    let warning_insights = store
        .insights(warning_snapshot["id"].as_str().unwrap(), None, 10)
        .unwrap();
    assert!(
        warning_insights["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["category"] == "parser-warning")
    );

    let missing_worktree = directory.path().join("missing");
    assert!(
        store
            .register_worktree(&missing_worktree, "main", None)
            .is_err()
    );
    let worktree = store
        .register_worktree(directory.path(), "main", None)
        .unwrap();
    let worktree_id = worktree["id"].as_str().unwrap();
    assert!(store.worktree("missing").is_err());
    assert!(
        store
            .worktree_progress(worktree_id, "unit", Some("src/a.py"), 10)
            .is_ok()
    );
    assert!(
        store
            .worktree_progress("missing-worktree", "unit", None, 10)
            .is_err()
    );
    assert!(
        store
            .trend(
                None,
                None,
                Some("unit"),
                Some("src/a.py"),
                Some(worktree_id),
                10,
            )
            .is_ok()
    );
    assert!(
        store
            .compare_worktree(worktree_id, Some(current_id), 100, 100)
            .is_ok()
    );
    assert!(store.compare_worktree(worktree_id, None, 100, 100).is_ok());

    assert!(
        store
            .register_command(
                "denied",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                false,
                "tester",
                "no",
                true,
            )
            .is_err()
    );
    assert!(
        store
            .register_command(
                "blank",
                " ",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .is_err()
    );
    assert!(
        store
            .register_command(
                " ",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .is_err()
    );
    assert!(
        store
            .register_command(
                "missing-approver",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                " ",
                "approved",
                true,
            )
            .is_err()
    );
    assert!(
        store
            .register_command(
                "missing-note",
                "true",
                Some(directory.path()),
                "/bin/sh",
                None,
                true,
                "tester",
                " ",
                true,
            )
            .is_err()
    );
    let cwd_file = write_file(directory.path(), "cwd-file", "not a directory");
    assert!(
        store
            .register_command(
                "file-cwd",
                "true",
                Some(&cwd_file),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .is_err()
    );
    let missing_cwd = directory.path().join("missing");
    assert!(
        store
            .register_command(
                "missing-cwd",
                "true",
                Some(&missing_cwd),
                "/bin/sh",
                None,
                true,
                "tester",
                "approved",
                true,
            )
            .is_err()
    );
    let disabled = store
        .register_command(
            "disabled",
            "true",
            Some(directory.path()),
            "/bin/sh",
            None,
            true,
            "tester",
            "approved",
            false,
        )
        .unwrap();
    assert!(store.registered_command("disabled").is_ok());
    assert!(store.registered_command("missing-command").is_err());
    assert!(
        store
            .submit_command(disabled["id"].as_str().unwrap(), None, None, 20)
            .is_err()
    );
    assert!(
        store
            .submit_command("missing-command", None, None, 20)
            .is_err()
    );
    assert!(
        store
            .submit_command(disabled["id"].as_str().unwrap(), None, None, 0)
            .is_err()
    );

    let basic = store
        .register_command(
            "basic",
            "printf 'passed edge\\n'",
            Some(directory.path()),
            "/bin/sh",
            None,
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let basic_id = basic["id"].as_str().unwrap();
    assert!(store.submit_command(basic_id, Some(0), None, 20).is_err());
    assert!(store.submit_command(basic_id, None, Some(" "), 20).is_err());
    assert!(
        store
            .submit_command(basic_id, None, Some(&"x".repeat(201)), 20)
            .is_err()
    );
    let first_run = store
        .run_command(basic_id, Some(20), Some("first"), 20)
        .unwrap();
    let second_run = store
        .run_command(basic_id, Some(20), Some("second"), 20)
        .unwrap();
    assert!(first_run["terminal"].as_bool().unwrap());
    assert!(second_run["terminal"].as_bool().unwrap());
    assert!(store.latest_run(Some("basic")).unwrap().is_some());
    assert!(store.latest_run(None).unwrap().is_some());
    assert!(
        store
            .search_run_logs(
                second_run["id"].as_str().unwrap(),
                &[],
                "both",
                1,
                5,
                false,
                100,
            )
            .is_err()
    );
    assert!(
        store
            .search_run_logs(
                second_run["id"].as_str().unwrap(),
                &(0..21).map(|n| n.to_string()).collect::<Vec<_>>(),
                "both",
                1,
                5,
                false,
                100,
            )
            .is_err()
    );
    assert!(
        store
            .search_run_logs(
                second_run["id"].as_str().unwrap(),
                &["PASSED".to_owned()],
                "stderr",
                0,
                5,
                true,
                100,
            )
            .unwrap()["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let case_sensitive = store
        .search_run_logs(
            second_run["id"].as_str().unwrap(),
            &["passed".to_owned()],
            "stdout",
            1,
            1,
            true,
            100,
        )
        .unwrap();
    assert_eq!(case_sensitive["match_count"], 1);
    assert!(store.latest_artifact("missing", None).unwrap().is_none());
    assert!(store.cancel_run("missing", 20).is_err());
    assert!(store.execute_run("missing-run").is_err());

    let failed = store
        .register_command(
            "failed",
            "false",
            Some(directory.path()),
            "/bin/sh",
            None,
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let failed_run = store
        .run_command(failed["id"].as_str().unwrap(), Some(20), None, 20)
        .unwrap();
    assert_eq!(failed_run["status"], "failed");

    let bad_artifact = write_file(directory.path(), "bad-artifact.lcov", "not a report\n");
    let artifact_command = store
        .register_command(
            "artifact-failure",
            "true",
            Some(directory.path()),
            "/bin/sh",
            Some(json!({"coverage":{"path":bad_artifact.file_name().unwrap().to_string_lossy(),"coverage_format":"unsupported","suite":"unit"}})),
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let artifact_run = store
        .run_command(artifact_command["id"].as_str().unwrap(), Some(20), None, 20)
        .unwrap();
    assert_eq!(artifact_run["coverage_ingest"]["status"], "failed");
    assert!(
        store
            .latest_artifact("coverage", Some(artifact_command["id"].as_str().unwrap()))
            .unwrap()
            .is_some()
    );
    let missing_artifact_command = store
        .register_command(
            "artifact-missing",
            "true",
            Some(directory.path()),
            "/bin/sh",
            Some(json!({"coverage":{"path":"does-not-exist.lcov","coverage_format":"lcov","suite":"unit"}})),
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let missing_artifact_run = store
        .run_command(
            missing_artifact_command["id"].as_str().unwrap(),
            Some(20),
            None,
            20,
        )
        .unwrap();
    assert_eq!(missing_artifact_run["coverage_ingest"]["status"], "failed");
    let absolute_artifact_command = store
        .register_command(
            "artifact-absolute",
            "true",
            Some(directory.path()),
            "/bin/sh",
            Some(json!({"coverage":{"path":bad_artifact.to_string_lossy(),"coverage_format":"unsupported","suite":"unit"}})),
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    assert_eq!(
        store
            .run_command(
                absolute_artifact_command["id"].as_str().unwrap(),
                Some(20),
                None,
                20,
            )
            .unwrap()["coverage_ingest"]["status"],
        "failed"
    );
    let skipped_artifact_command = store
        .register_command(
            "artifact-skipped",
            "false",
            Some(directory.path()),
            "/bin/sh",
            Some(json!({"coverage":{"path":bad_artifact.file_name().unwrap().to_string_lossy(),"coverage_format":"lcov","suite":"unit"}})),
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let skipped_artifact_run = store
        .run_command(
            skipped_artifact_command["id"].as_str().unwrap(),
            Some(20),
            None,
            20,
        )
        .unwrap();
    assert_eq!(skipped_artifact_run["coverage_ingest"]["status"], "partial");

    let metadata_only_command = store
        .register_command(
            "metadata-only-artifact",
            "true",
            Some(directory.path()),
            "/bin/sh",
            Some(json!({"log":{"path":"metadata.log"}})),
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let metadata_only_run = store
        .run_command(
            metadata_only_command["id"].as_str().unwrap(),
            Some(20),
            None,
            20,
        )
        .unwrap();
    assert_eq!(metadata_only_run["coverage_ingest"]["configured"], 0);

    let sleep_command = store
        .register_command(
            "sleep",
            "sleep 5",
            Some(directory.path()),
            "/bin/sh",
            None,
            true,
            "tester",
            "approved",
            true,
        )
        .unwrap();
    let pending = store
        .submit_command(sleep_command["id"].as_str().unwrap(), Some(20), None, 20)
        .unwrap();
    let _pending_state = store
        .run_result(pending["id"].as_str().unwrap(), 20)
        .unwrap();
    for _ in 0..200 {
        if store
            .run_result(pending["id"].as_str().unwrap(), 20)
            .unwrap()["status"]
            == "running"
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let pending_id = pending["id"].as_str().unwrap();
    let cancellation_request = store.cancel_run(pending_id, 20).unwrap();
    assert_eq!(cancellation_request["cancellation_requested"], true);
    let cancelled = loop {
        let result = store.run_result(pending_id, 20).unwrap();
        if result["terminal"].as_bool().unwrap_or(false) {
            break result;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(cancelled["status"], "cancelled");
    let _queue = store.list_run_queue(10).unwrap();
    assert!(
        store
            .run_command(sleep_command["id"].as_str().unwrap(), Some(1), None, 20)
            .unwrap()["status"]
            .as_str()
            .is_some()
    );

    let other_repository = tempfile::tempdir().unwrap();
    git_repo(other_repository.path());
    let other_store =
        CoverageStore::open(other_repository.path().join("coverage.duckdb"), config()).unwrap();
    other_store.ensure_project(other_repository.path()).unwrap();
    let empty_worktree = other_store
        .register_worktree(other_repository.path(), "main", Some("empty"))
        .unwrap();
    assert!(
        other_store
            .worktree_progress(
                empty_worktree["id"].as_str().unwrap(),
                "unit",
                Some("src/a.py"),
                10,
            )
            .is_ok()
    );
    assert!(
        other_store
            .compare_worktree(empty_worktree["id"].as_str().unwrap(), None, 10, 10)
            .is_err()
    );
    other_store.close().unwrap();
    let db_path = store.db_path().to_owned();
    store.close().unwrap();
    {
        let connection = duckdb::Connection::open(&db_path).unwrap();
        connection
            .execute(
                "UPDATE snapshots SET repo_key = 'different-repo', repo_path = ? WHERE id = ?",
                duckdb::params![other_repository.path().to_string_lossy(), current_id],
            )
            .unwrap();
    }
    let mutated = CoverageStore::open(db_path, config()).unwrap();
    mutated.ensure_project(directory.path()).unwrap();
    assert!(
        mutated
            .compare_worktree(worktree_id, Some(current_id), 10, 10,)
            .is_err()
    );
    assert!(mutated.compare(current_id, base_id, 10, 10).is_err());
    assert!(
        mutated
            .changed_lines(current_id, base_id, None, false, 10)
            .is_err()
    );

    let mutated_db = mutated.db_path().to_owned();
    mutated.close().unwrap();
    mutated.close().unwrap();
    assert!(mutated.project_summary().is_err());
    {
        let connection = duckdb::Connection::open(&mutated_db).unwrap();
        connection
            .execute(
                "UPDATE snapshots SET repo_path = ? WHERE id = ?",
                duckdb::params![
                    directory
                        .path()
                        .join("missing-repository")
                        .to_string_lossy(),
                    current_id
                ],
            )
            .unwrap();
    }
    let broken = CoverageStore::open(mutated_db, config()).unwrap();
    broken.ensure_project(directory.path()).unwrap();
    assert!(broken.source_lines(current_id, "src/a.py", 1, 1).is_err());
    broken.close().unwrap();
}

#[test]
fn rust_service_worktree_comparison_views_match_storage_contract() {
    let directory = tempfile::tempdir().unwrap();
    git_repo(directory.path());
    let base_report = write_file(
        directory.path(),
        "service-base.lcov",
        "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n",
    );
    let current_report = write_file(
        directory.path(),
        "service-current.lcov",
        "TN:\nSF:src/a.py\nDA:1,0\nDA:2,1\nend_of_record\n",
    );
    let store = store(directory.path());
    let base = ingest(&store, &base_report, "main", "base");
    let current = ingest(&store, &current_report, "feature", "head");
    let worktree = store
        .register_worktree(directory.path(), "main", Some("service worktree"))
        .unwrap();
    let project = store.project().unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: project.repo_key,
            checkout_path: project.repo_path,
            suite: None,
        },
    );
    assert!(
        service
            .worktree_registration(
                directory.path().to_str().unwrap(),
                "main",
                Some("service-registered"),
            )
            .is_ok()
    );
    let worktree_id = worktree["id"].as_str().unwrap();
    let current_id = current["id"].as_str().unwrap();
    let base_id = base["id"].as_str().unwrap();
    let progress = service
        .coverage_comparison(
            "progress",
            None,
            None,
            Some(worktree_id),
            Some("unit"),
            Some("src/a.py"),
            false,
            None,
            600,
            false,
        )
        .unwrap();
    assert!(progress["data"]["points"].is_array());
    assert!(
        service
            .coverage_comparison(
                "progress",
                None,
                None,
                Some(worktree_id),
                Some("unit"),
                None,
                false,
                None,
                600,
                true,
            )
            .is_ok()
    );
    assert!(
        service
            .coverage_comparison(
                "progress",
                None,
                None,
                Some(worktree_id),
                Some("unit"),
                None,
                false,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                Some(current_id),
                None,
                Some(worktree_id),
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_ok()
    );
    assert!(
        service
            .coverage_comparison(
                "regions",
                Some(current_id),
                None,
                Some(worktree_id),
                Some("unit"),
                Some("src/a.py"),
                true,
                None,
                600,
                false,
            )
            .is_ok()
    );
    assert!(
        service
            .coverage_comparison(
                "regions",
                None,
                None,
                Some("missing-worktree"),
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "regions",
                None,
                None,
                Some(worktree_id),
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_ok()
    );
    assert!(
        service
            .coverage_comparison(
                "progress",
                None,
                None,
                None,
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "progress",
                None,
                None,
                Some("missing-worktree"),
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                Some("missing-snapshot"),
                Some(base_id),
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "files",
                Some(current_id),
                Some(base_id),
                None,
                None,
                None,
                false,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "progress",
                None,
                None,
                Some(worktree_id),
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                Some(current_id),
                None,
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                None,
                Some(base_id),
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    store.close().unwrap();
}

#[test]
fn rust_service_pagination_projection_and_mcp_contract_match() {
    let directory = tempfile::tempdir().unwrap();
    git_repo(directory.path());
    write_file(directory.path(), "a.py", "one\ntwo\nthree\n");
    let report = write_file(
        directory.path(),
        "coverage.lcov",
        "TN:\nSF:a.py\nDA:1,1\nDA:2,0\nend_of_record\n",
    );
    let store = store(directory.path());
    let snapshot = ingest(&store, &report, "main", "head");
    let project = store.project().unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: project.repo_key,
            checkout_path: project.repo_path,
            suite: None,
        },
    );
    let context = service.project_context(None, 600, false).unwrap();
    assert_eq!(context["context"]["schema_revision"], SCHEMA_REVISION);
    assert!(context["data"]["project"]["compaction"].is_object());
    assert_eq!(service.context(Some("unit")).suite.as_deref(), Some("unit"));
    let duplicate_values = vec![
        json!({"value": "one ".repeat(60)}),
        json!({"value": "one ".repeat(60)}),
        json!({"value": "one ".repeat(60)}),
    ];
    let (_, first_page) = service
        .page(&duplicate_values, None, 130, "duplicate-values", None)
        .unwrap();
    let cursor = first_page["next_cursor"].as_str().unwrap().to_owned();
    assert!(
        service
            .page(
                &duplicate_values,
                Some(&cursor),
                130,
                "duplicate-values",
                None
            )
            .is_ok()
    );
    service.validate_repository_path(None).unwrap();
    service
        .validate_repository_path(Some(directory.path().to_str().unwrap()))
        .unwrap();
    let other = tempfile::tempdir().unwrap();
    assert!(
        service
            .validate_repository_path(other.path().to_str())
            .is_err()
    );
    let other_git = tempfile::tempdir().unwrap();
    git_repo(other_git.path());
    assert!(
        service
            .validate_repository_path(other_git.path().to_str())
            .is_err()
    );
    let mismatched_service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: "deliberately-mismatched-repo".to_owned(),
            checkout_path: directory.path().to_string_lossy().into_owned(),
            suite: None,
        },
    );
    assert!(
        mismatched_service
            .ingest("coverage.lcov", "lcov", "mismatch", None, None, None, false)
            .is_err()
    );
    assert!(
        service
            .ingest("missing.lcov", "lcov", "unit", None, None, None, false)
            .is_err()
    );
    assert!(
        service
            .ingest("coverage.lcov", "lcov", " ", None, None, None, false)
            .is_err()
    );

    let second_report = write_file(
        directory.path(),
        "coverage-second.lcov",
        "TN:\nSF:a.py\nDA:1,0\nDA:2,1\nDA:3,1\nend_of_record\n",
    );
    let second = service
        .ingest(
            second_report.to_str().unwrap(),
            "lcov",
            "unit",
            Some("main"),
            Some("head-2"),
            None,
            true,
        )
        .unwrap();
    let second_id = second["data"]["id"].as_str().unwrap();
    let first_id = snapshot["id"].as_str().unwrap();
    assert!(
        service
            .coverage_query(
                "summary",
                Some(second_id),
                None,
                Some("unit"),
                None,
                None,
                None,
                None,
                None,
                49,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                Some(second_id),
                Some(first_id),
                None,
                Some("unit"),
                None,
                false,
                None,
                49,
                false,
            )
            .is_err()
    );
    let other_suite_snapshot = service
        .ingest(
            "coverage.lcov",
            "lcov",
            "other-suite",
            Some("main"),
            Some("other-head"),
            None,
            false,
        )
        .unwrap();
    let other_suite_id = other_suite_snapshot["data"]["id"].as_str().unwrap();
    let targets = service
        .coverage_query_ordered(
            "targets",
            Some(second_id),
            None,
            Some("unit"),
            None,
            None,
            None,
            None,
            Some("uncovered_lines"),
            None,
            600,
            false,
        )
        .unwrap();
    assert_eq!(targets["data"]["order_by"], "uncovered_lines");
    assert_eq!(targets["data"]["targets"][0]["regions"][0]["start"], 1);
    let filtered_targets = service
        .coverage_query_ordered(
            "targets",
            Some(second_id),
            None,
            Some("unit"),
            None,
            Some("a.py"),
            None,
            None,
            None,
            None,
            600,
            false,
        )
        .unwrap();
    assert_eq!(
        filtered_targets["data"]["targets"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        service
            .coverage_query_ordered(
                "targets",
                Some(second_id),
                None,
                Some("unit"),
                None,
                None,
                None,
                None,
                None,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query_ordered(
                "targets",
                Some(second_id),
                None,
                Some("unit"),
                None,
                None,
                None,
                None,
                Some("invalid"),
                None,
                600,
                false,
            )
            .is_err()
    );
    let compact_regions = service
        .coverage_comparison(
            "regions",
            None,
            None,
            None,
            Some("unit"),
            None,
            false,
            None,
            600,
            false,
        )
        .unwrap();
    assert!(compact_regions["data"]["regions"].is_array());
    assert!(compact_regions["data"]["region_change_count"].is_number());
    assert!(
        service
            .coverage_comparison(
                "regions",
                None,
                None,
                None,
                Some("missing-suite"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "regions",
                Some(second_id),
                Some(other_suite_id),
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "summary",
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_ok()
    );
    assert!(
        service
            .coverage_query(
                "files",
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                None,
                None,
                None,
                600,
                true,
            )
            .is_ok()
    );
    let file_view = service
        .coverage_query(
            "file",
            Some(second_id),
            None,
            Some("unit"),
            None,
            Some("a.py"),
            None,
            Some(vec![(1, 2)]),
            None,
            600,
            false,
        )
        .unwrap();
    assert!(file_view["data"]["selected_lines"].is_array());
    assert_eq!(file_view["data"]["red_regions"][0]["start"], 1);
    assert!(
        service
            .coverage_query(
                "file",
                Some(second_id),
                None,
                Some("unit"),
                None,
                Some("a.py"),
                None,
                None,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    let insights = service
        .coverage_query(
            "insights",
            Some(second_id),
            Some(first_id),
            Some("unit"),
            None,
            None,
            None,
            None,
            None,
            600,
            true,
        )
        .unwrap();
    assert!(insights["data"]["items"].is_array());
    assert!(
        service
            .coverage_query(
                "insights",
                Some(second_id),
                None,
                Some("unit"),
                None,
                None,
                None,
                None,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    let history = service
        .coverage_query(
            "line_history",
            None,
            None,
            Some("unit"),
            Some("main"),
            Some("a.py"),
            Some(1),
            None,
            None,
            600,
            false,
        )
        .unwrap();
    assert_eq!(history["data"].as_array().unwrap().len(), 2);
    assert!(
        service
            .coverage_query(
                "line_history",
                None,
                None,
                Some("unit"),
                Some("main"),
                Some("a.py"),
                Some(1),
                None,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "line_history",
                None,
                None,
                None,
                None,
                Some("a.py"),
                Some(1),
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "file",
                Some(second_id),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );

    for (view, detailed, only_regressions) in [
        ("overview", false, false),
        ("files", true, false),
        ("lines", false, false),
        ("lines", false, true),
    ] {
        assert!(
            service
                .coverage_comparison(
                    view,
                    Some(second_id),
                    Some(first_id),
                    None,
                    Some("unit"),
                    Some("a.py"),
                    only_regressions,
                    None,
                    600,
                    detailed,
                )
                .is_ok()
        );
    }
    assert!(
        service
            .coverage_comparison(
                "invalid",
                Some(second_id),
                Some(first_id),
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                Some(second_id),
                Some(first_id),
                None,
                Some("other-suite"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_comparison(
                "overview",
                None,
                Some(first_id),
                None,
                None,
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );

    let source = service.source(second_id, "a.py", 1, 2, None, 600).unwrap();
    assert_eq!(source["data"]["lines"].as_array().unwrap().len(), 2);
    assert_eq!(source["data"]["lines"][0]["marker"], "red");
    assert_eq!(source["data"]["lines"][1]["marker"], "green");
    assert_eq!(source["data"]["red_regions"][0]["start"], 1);
    assert!(service.source(second_id, "a.py", 2, 1, None, 600).is_err());
    assert!(
        service
            .source(second_id, "a.py", 1, 2, Some("invalid"), 600)
            .is_err()
    );
    assert!(
        service
            .file_detail(second_id, "a.py", None, 600, false)
            .unwrap()["data"]["lines"]
            .is_array()
    );
    assert!(
        service
            .file_detail(second_id, "a.py", None, 600, true)
            .unwrap()["data"]["lines"]
            .is_array()
    );
    assert!(
        service
            .file_detail(second_id, "a.py", Some("invalid"), 600, false)
            .is_err()
    );

    let command = service
        .command_registration(
            "service-unit",
            "printf 'service passed\\n'",
            true,
            "tester",
            "approved for Rust service test",
            None,
            "/bin/sh",
            None,
            true,
        )
        .unwrap();
    let command_id = command["data"]["id"].as_str().unwrap();
    let context_detailed = service.project_context(None, 600, true).unwrap();
    assert!(context_detailed["data"]["commands"].is_array());
    let run = service
        .run_submission(command_id, Some(20), Some("service-wait"), true, true)
        .unwrap();
    let run_id = run["data"]["id"].as_str().unwrap();
    assert!(service.run_state(run_id, "status", true).is_ok());
    assert!(
        service
            .search_logs(
                run_id,
                vec!["passed".to_owned()],
                "stdout",
                1,
                5,
                600,
                false,
            )
            .is_ok()
    );
    assert!(
        service
            .search_logs(
                "missing",
                vec!["missing".to_owned()],
                "stdout",
                0,
                5,
                600,
                false,
            )
            .is_err()
    );
    assert!(service.run_state(run_id, "unknown", false).is_err());
    let queued = service
        .run_submission(command_id, Some(20), Some("service-queued"), false, false)
        .unwrap();
    assert!(queued["data"]["id"].is_string());
    let active_command = service
        .command_registration(
            "service-active",
            "sleep 2",
            true,
            "tester",
            "approved for Rust service queue test",
            None,
            "/bin/sh",
            None,
            false,
        )
        .unwrap();
    let active_run = service
        .run_submission(
            active_command["data"]["id"].as_str().unwrap(),
            Some(20),
            Some("service-active"),
            false,
            false,
        )
        .unwrap();
    let active_context = service.project_context(None, 600, false).unwrap();
    assert!(active_context["data"]["active_runs"].is_array());
    assert!(
        service
            .run_state(active_run["data"]["id"].as_str().unwrap(), "cancel", false)
            .is_ok()
    );
    assert!(
        service
            .command_registration(
                "bad-cwd",
                "true",
                true,
                "tester",
                "approved",
                Some(other.path().to_str().unwrap()),
                "/bin/sh",
                None,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .command_registration(
                "bad-artifacts",
                "true",
                true,
                "tester",
                "approved",
                None,
                "/bin/sh",
                Some(json!({"bad": 1})),
                false,
            )
            .is_err()
    );
    service
        .update_project_settings(ProjectSettingsPatch {
            compaction_batch_size: Some(5),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        service.compact_now().unwrap()["data"]["status"],
        "completed"
    );

    let large = vec![
        json!({"payload": vec!["word"; 80]}),
        json!({"payload": vec!["word"; 80]}),
    ];
    let (first_page, page) = service.page(&large, None, 50, "large", None).unwrap();
    assert_eq!(first_page.len(), 1);
    let next = page["next_cursor"].as_str().unwrap();
    assert_eq!(
        service
            .page(&large, Some(next), 50, "large", None)
            .unwrap()
            .0
            .len(),
        1
    );
    let two_small = vec![
        json!({"payload": vec!["word"; 30]}),
        json!({"payload": vec!["word"; 30]}),
    ];
    assert_eq!(
        service
            .page(&two_small, None, 50, "two-small", None)
            .unwrap()
            .0
            .len(),
        1
    );
    assert!(service.page(&large, None, 50, "large", Some(3)).is_err());
    let summary = service
        .coverage_query(
            "summary",
            Some(snapshot["id"].as_str().unwrap()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            600,
            false,
        )
        .unwrap();
    assert!(summary["data"]["line_rate"].is_number());
    let detailed = service
        .coverage_query(
            "summary",
            Some(snapshot["id"].as_str().unwrap()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            600,
            true,
        )
        .unwrap();
    assert!(detailed["data"]["report_path"].is_string());
    let files = service
        .coverage_query(
            "files",
            Some(snapshot["id"].as_str().unwrap()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            50,
            false,
        )
        .unwrap();
    assert_eq!(files["page"]["max_words"], 50);
    assert!(
        service
            .coverage_query(
                "files",
                Some(snapshot["id"].as_str().unwrap()),
                None,
                None,
                None,
                None,
                None,
                None,
                Some("invalid"),
                600,
                false,
            )
            .is_err()
    );
    let cursor = files["page"]["next_cursor"].clone();
    assert!(cursor.is_null() || cursor.is_string());
    assert!(
        service
            .coverage_query(
                "unknown", None, None, None, None, None, None, None, None, 600, false
            )
            .is_err()
    );
    assert!(
        service
            .apply_budget(json!({"data":{"large":vec!["word"; 100]} }), 50)
            .is_err()
    );
    assert!(
        service
            .page(&[json!({"id":1})], Some("invalid"), 50, "scope", None)
            .is_err()
    );
    let wrong_cursor = coverage_mcp::service::encode_cursor(&"a".repeat(64), "scope", 1).unwrap();
    assert!(
        service
            .page(&[json!({"id":1})], Some(&wrong_cursor), 50, "scope", None)
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "files",
                None,
                None,
                Some("missing-suite"),
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "line_history",
                None,
                None,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "line_history",
                None,
                None,
                Some("unit"),
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .coverage_query(
                "line_history",
                None,
                None,
                Some("unit"),
                None,
                Some("a.py"),
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    let empty_directory = tempfile::tempdir().unwrap();
    let empty_store =
        CoverageStore::open(empty_directory.path().join("coverage.duckdb"), config()).unwrap();
    empty_store.ensure_project(empty_directory.path()).unwrap();
    let empty_project = empty_store.project().unwrap();
    let empty_service = CoverageService::new(
        empty_store.clone(),
        RequestContext {
            repo_key: empty_project.repo_key,
            checkout_path: empty_project.repo_path,
            suite: None,
        },
    );
    assert!(
        empty_service
            .coverage_query(
                "files", None, None, None, None, None, None, None, None, 600, false,
            )
            .is_err()
    );
    assert!(
        empty_service
            .coverage_query(
                "summary",
                Some("missing"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        empty_service
            .coverage_query(
                "file",
                Some("missing"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(
        empty_service
            .coverage_query(
                "insights",
                Some("missing"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    empty_store.close().unwrap();
    assert!(
        empty_service
            .coverage_query(
                "line_history",
                None,
                None,
                Some("unit"),
                None,
                Some("a.py"),
                Some(1),
                None,
                None,
                600,
                false,
            )
            .is_err()
    );
    let tools = mcp::tools_list();
    let tools = tools.as_array().unwrap();
    assert_eq!(tools.len(), 11);
    let names = tools
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(names.contains("project_context") && names.contains("source_context"));
    assert!(
        mcp::resources_list()
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["uri"] == "coverage://context")
    );
    assert!(
        mcp::resource_templates_list()
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["uriTemplate"] == "coverage://snapshot/{snapshot_id}/summary")
    );
    let call = mcp::call_tool(
        &service,
        "coverage_query",
        &json!({"view":"summary","snapshot_id":snapshot["id"]}),
    )
    .unwrap();
    assert_eq!(call["data"]["total_lines"], 2);
    let mcp_command = mcp::call_tool(
        &service,
        "register_test_command",
        &json!({
            "name":"mcp-unit",
            "command":"printf 'mcp passed\\n'",
            "human_approved":true,
            "approved_by":"tester",
            "approval_note":"approved for Rust MCP test",
            "cwd":directory.path(),
            "shell":"/bin/sh"
        }),
    )
    .unwrap();
    let mcp_command_id = mcp_command["data"]["id"].as_str().unwrap();
    let mcp_run = mcp::call_tool(
        &service,
        "run_test",
        &json!({"command_ref":mcp_command_id,"wait":true,"idempotency_key":"mcp-one"}),
    )
    .unwrap();
    let mcp_run_id = mcp_run["data"]["id"].as_str().unwrap();
    assert!(mcp::call_tool(&service, "get_run_data", &json!({"run_id":mcp_run_id})).is_ok());
    assert!(
        mcp::call_tool(
            &service,
            "search_test_logs",
            &json!({"run_id":mcp_run_id,"query":["mcp","missing"],"stream":"both"})
        )
        .is_ok()
    );
    assert!(mcp::call_tool(&service, "cancel_run", &json!({"run_id":mcp_run_id})).is_err());

    let cancel_command = mcp::call_tool(
        &service,
        "register_test_command",
        &json!({
            "name":"mcp-cancel",
            "command":"sleep 2",
            "human_approved":true,
            "approved_by":"tester",
            "approval_note":"approved for Rust MCP cancellation test",
            "cwd":directory.path(),
            "shell":"/bin/sh"
        }),
    )
    .unwrap();
    let pending = mcp::call_tool(
        &service,
        "run_test",
        &json!({"command_ref":cancel_command["data"]["id"],"wait":false}),
    )
    .unwrap();
    assert!(
        mcp::call_tool(
            &service,
            "cancel_run",
            &json!({"run_id":pending["data"]["id"]})
        )
        .is_ok()
    );
    assert!(mcp::call_tool(&service, "register_test_command", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "run_test", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "get_run_data", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "cancel_run", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "search_test_logs", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "ingest_coverage", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "register_worktree", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "source_context", &json!({})).is_err());
    assert!(mcp::call_tool(&service, "coverage_query", &Value::Null).is_err());
    assert!(mcp::call_tool(&service, "coverage_compare", &json!({})).is_err());
    let mcp_ingest = mcp::call_tool(
        &service,
        "ingest_coverage",
        &json!({"report_path":"coverage-second.lcov","format":"lcov","suite":"mcp"}),
    )
    .unwrap();
    let mcp_snapshot_id = mcp_ingest["data"]["id"].as_str().unwrap();
    assert!(
        service
            .coverage_comparison(
                "regions",
                Some(mcp_snapshot_id),
                None,
                None,
                Some("mcp"),
                None,
                false,
                None,
                600,
                false,
            )
            .is_err()
    );
    assert!(mcp::call_tool(
        &service,
        "coverage_query",
        &json!({"view":"file","snapshot_id":mcp_snapshot_id,"file_path":"a.py","line_ranges":[{"start":1,"end":2}]})
    )
    .is_ok());
    let mcp_targets = mcp::call_tool(
        &service,
        "coverage_query",
        &json!({"view":"targets","snapshot_id":mcp_snapshot_id,"order_by":"priority"}),
    )
    .unwrap();
    assert!(mcp_targets["data"]["targets"].is_array());
    assert!(
        mcp::call_tool(
            &service,
            "coverage_query",
            &json!({"view":"insights","snapshot_id":mcp_snapshot_id})
        )
        .is_ok()
    );
    assert!(
        mcp::call_tool(
            &service,
            "coverage_query",
            &json!({"view":"line_history","file_path":"a.py","line_number":1,"suite":"unit"})
        )
        .is_ok()
    );
    assert!(
        mcp::call_tool(
            &service,
            "coverage_compare",
            &json!({"view":"files","snapshot_id":second_id,"baseline_snapshot_id":first_id})
        )
        .is_ok()
    );
    assert!(mcp::call_tool(
        &service,
        "coverage_compare",
        &json!({"view":"lines","snapshot_id":second_id,"baseline_snapshot_id":first_id,"only_regressions":true})
    )
    .is_ok());
    let mcp_regions = mcp::call_tool(
        &service,
        "coverage_compare",
        &json!({"view":"regions","snapshot_id":second_id,"baseline_snapshot_id":first_id}),
    )
    .unwrap();
    assert!(mcp_regions["data"]["regions"].is_array());
    let mcp_worktree = mcp::call_tool(
        &service,
        "register_worktree",
        &json!({"path":directory.path(),"base_ref":"main"}),
    )
    .unwrap();
    assert!(mcp_worktree["data"]["id"].is_string());
    assert!(
        mcp::call_tool(
            &service,
            "source_context",
            &json!({"snapshot_id":second_id,"file_path":"a.py","start":1,"end":2})
        )
        .is_ok()
    );
    assert!(mcp::read_resource(&service, "coverage://unknown").is_err());
    assert!(mcp::call_tool(&service, "not-a-tool", &json!({})).is_err());
    store.close().unwrap();
}

#[tokio::test]
async fn rust_http_rest_dashboard_health_and_mcp_wire_are_live() {
    let directory = tempfile::tempdir().unwrap();
    let mut invalid_host = config();
    invalid_host.host = "0.0.0.0".to_owned();
    assert!(
        CoverageServer::new(invalid_host)
            .unwrap()
            .run()
            .await
            .is_err()
    );
    let report = write_file(
        directory.path(),
        "coverage.lcov",
        "TN:\nSF:a.py\nDA:1,1\nend_of_record\n",
    );
    let probe_listener = match TcpListener::bind(("127.0.0.1", 0)).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("could not bind probe listener: {error}"),
    };
    let probe_address = probe_listener.local_addr().unwrap();
    let run_server = CoverageServer::new(config()).unwrap();
    let run_task = tokio::spawn(run_server.serve_listener(probe_listener));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let _run_health = http_exchange(probe_address, "GET", "/health", None, None).await;
    assert!(
        http_raw(probe_address, "GET", "/api/snapshots", None, None,)
            .await
            .contains("400 Bad Request")
    );
    assert!(
        http_raw(
            probe_address,
            "GET",
            "/api/snapshots?repo_path=%00",
            None,
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    run_task.abort();
    let _ = run_task.await;
    let mut server_config = config();
    server_config.db_path = Some(directory.path().join("coverage.duckdb"));
    let server = CoverageServer::new(server_config).unwrap();
    let listener = match TcpListener::bind(("127.0.0.1", 0)).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping live HTTP assertion because this sandbox forbids local listeners");
            return;
        }
        Err(error) => panic!("could not bind local test listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(server.clone().serve_listener(listener));
    let invalid_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let invalid_address = invalid_listener.local_addr().unwrap();
    let invalid_server = tokio::spawn(async move {
        let (mut socket, _) = invalid_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 8\r\n\r\nnot-json")
            .await
            .unwrap();
    });
    assert!(
        tokio::spawn(http_exchange(invalid_address, "GET", "/", None, None))
            .await
            .unwrap_err()
            .is_panic()
    );
    invalid_server.await.unwrap();
    let mut malformed = TcpStream::connect(address).await.unwrap();
    malformed
        .write_all(b"not an HTTP request\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    drop(malformed);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let health = http_exchange(address, "GET", "/health", None, None).await;
    assert_eq!(health["schema_revision"], 7);
    assert!(
        http_raw(address, "GET", "/mcp/", None, None)
            .await
            .contains("405 Method Not Allowed")
    );
    assert!(
        http_raw(address, "GET", "/favicon.ico", None, None)
            .await
            .contains("204 No Content")
    );
    assert!(
        http_raw(address, "GET", "/not-a-route", None, None)
            .await
            .contains("404 Not Found")
    );
    assert!(
        http_raw(address, "POST", "/health", Some(&json!({})), None)
            .await
            .contains("404 Not Found")
    );
    assert!(
        http_raw(
            address,
            "GET",
            "/health",
            None,
            Some(("Host", "evil.example")),
        )
        .await
        .contains("400 Bad Request")
    );
    let dashboard = http_raw(address, "GET", "/", None, None).await;
    assert!(dashboard.contains("projectSelect") && dashboard.contains("coverageViewer"));
    let body = json!({"report_path":report.to_string_lossy(),"repo_path":directory.path(),"format":"lcov","suite":"unit","branch":"main","commit_sha":"head"});
    let ingested = http_exchange(address, "POST", "/api/ingest", Some(&body), None).await;
    let snapshot_id = ingested["data"]["id"].as_str().unwrap().to_owned();
    assert!(
        http_raw_payload(address, "POST", "/api/ingest", "not-json", None)
            .await
            .contains("500 Internal Server Error")
    );
    assert!(
        http_raw(address, "POST", "/api/ingest", Some(&json!({})), None)
            .await
            .contains("400 Bad Request")
    );
    let second_report = write_file(
        directory.path(),
        "coverage-second.lcov",
        "TN:\nSF:a.py\nDA:1,0\nDA:2,1\nend_of_record\n",
    );
    let second_body = json!({"report_path":second_report.to_string_lossy(),"repo_path":directory.path(),"format":"lcov","suite":"unit","branch":"main","commit_sha":"head-2"});
    let second = http_exchange(address, "POST", "/api/ingest", Some(&second_body), None).await;
    let second_snapshot_id = second["data"]["id"].as_str().unwrap().to_owned();
    let snapshots = http_exchange(address, "GET", "/api/snapshots", None, None).await;
    assert_eq!(snapshots["data"].as_array().unwrap().len(), 2);
    let _filtered_snapshots = http_raw(
        address,
        "GET",
        &format!(
            "/api/snapshots?repo_path={}&branch=main&suite=unit&cursor=invalid",
            directory.path().display()
        ),
        None,
        None,
    )
    .await;
    let _projects_cursor =
        http_raw(address, "GET", "/api/projects?cursor=invalid", None, None).await;
    let _projects = http_exchange(address, "GET", "/api/projects?max_words=5000", None, None).await;
    let _project = http_exchange(address, "GET", "/api/projects/project", None, None).await;
    assert!(
        http_raw(address, "GET", "/api/projects/unknown-project", None, None)
            .await
            .contains("404 Not Found")
    );
    let _latest = http_exchange(
        address,
        "GET",
        "/api/snapshots/latest?suite=unit",
        None,
        None,
    )
    .await;
    assert!(
        http_raw(
            address,
            "GET",
            "/api/snapshots/latest?suite=missing",
            None,
            None
        )
        .await
        .contains("404 Not Found")
    );
    let _snapshot = http_exchange(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}"),
        None,
        None,
    )
    .await;
    let _files = http_exchange(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files"),
        None,
        None,
    )
    .await;
    let _file_filter = http_exchange(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files?file_path=a.py&detailed=true"),
        None,
        None,
    )
    .await;
    let _file_filter_cursor = http_raw(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files?file_path=a.py&cursor=invalid"),
        None,
        None,
    )
    .await;
    let _file_detail = http_exchange(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files/a.py"),
        None,
        None,
    )
    .await;
    let _file_detail_cursor = http_raw(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files/a.py?cursor=invalid"),
        None,
        None,
    )
    .await;
    let _files_cursor = http_raw(
        address,
        "GET",
        &format!("/api/snapshots/{snapshot_id}/files?cursor=invalid"),
        None,
        None,
    )
    .await;
    let _insights = http_exchange(
        address,
        "GET",
        &format!("/api/snapshots/{second_snapshot_id}/insights?baseline_snapshot_id={snapshot_id}"),
        None,
        None,
    )
    .await;
    let _trend = http_exchange(
        address,
        "GET",
        "/api/trend?suite=unit&limit=10&file_path=a.py",
        None,
        None,
    )
    .await;
    let compare_query =
        format!("/api/compare?snapshot_id={second_snapshot_id}&baseline_snapshot_id={snapshot_id}");
    let _compare = http_exchange(address, "GET", &compare_query, None, None).await;
    let _compare_post = http_exchange(
        address,
        "POST",
        "/api/compare",
        Some(&json!({"snapshot_id":second_snapshot_id,"baseline_snapshot_id":snapshot_id})),
        None,
    )
    .await;
    let changed_query = format!(
        "/api/changed-lines?snapshot_id={second_snapshot_id}&baseline_snapshot_id={snapshot_id}&only_regressions=true"
    );
    let _changed = http_exchange(address, "GET", &changed_query, None, None).await;
    assert!(
        http_raw(address, "GET", "/api/changed-lines", None, None)
            .await
            .contains("400 Bad Request")
    );
    let _history = http_exchange(
        address,
        "GET",
        "/api/line-history?file_path=a.py&line_number=1&suite=unit",
        None,
        None,
    )
    .await;
    assert!(
        http_raw(
            address,
            "GET",
            "/api/line-history?file_path=a.py&line_number=bad",
            None,
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    let _source = http_exchange(
        address,
        "GET",
        &format!("/api/source-lines?snapshot_id={second_snapshot_id}&file_path=a.py&start=1&end=2&cursor=invalid"),
        None,
        None,
    )
    .await;
    let _source_bad_end = http_raw(
        address,
        "GET",
        &format!(
            "/api/source-lines?snapshot_id={second_snapshot_id}&file_path=a.py&start=1&end=bad"
        ),
        None,
        None,
    )
    .await;
    let _worktrees = http_exchange(address, "GET", "/api/worktrees", None, None).await;
    let _registered_worktree = http_exchange(
        address,
        "POST",
        "/api/worktrees/register",
        Some(&json!({"path":directory.path(),"base_ref":"main","name":"http-worktree"})),
        None,
    )
    .await;
    assert!(
        http_raw(
            address,
            "POST",
            "/api/worktrees/register",
            Some(&json!({})),
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    let created_project = http_exchange(
        address,
        "POST",
        "/api/projects",
        Some(&json!({"repo_path":directory.path(),"compaction_after_days":7,"compaction_enabled":false})),
        None,
    )
    .await;
    assert_eq!(created_project["data"]["compaction_after_days"], 7);
    let edited_project = http_exchange(
        address,
        "PATCH",
        "/api/projects/project",
        Some(&json!({"compaction":{"compaction_enabled":true,"compaction_batch_size":5}})),
        None,
    )
    .await;
    assert_eq!(edited_project["data"]["compaction_batch_size"], 5);
    let compacted =
        http_exchange(address, "POST", "/api/projects/project/compact", None, None).await;
    assert_eq!(compacted["data"]["status"], "completed");
    let command = http_exchange(
        address,
        "POST",
        "/api/commands/register",
        Some(&json!({"name":"http-unit","command":"printf 'passed\\n'","cwd":directory.path(),"shell":"/bin/sh","human_approved":true,"approved_by":"tester","approval_note":"approved for Rust transport test","artifact_paths":{}})),
        None,
    )
    .await;
    let command_id = command["data"]["id"].as_str().unwrap().to_owned();
    let _commands = http_exchange(address, "GET", "/api/commands", None, None).await;
    let _command = http_exchange(
        address,
        "GET",
        &format!("/api/commands/{command_id}"),
        None,
        None,
    )
    .await;
    let run = http_exchange(address, "POST", "/api/runs/profiled", Some(&json!({"command_ref":command_id,"wait":true,"idempotency_key":"http-one","timeout_seconds":20})), None).await;
    let run_id = run["data"]["id"].as_str().unwrap().to_owned();
    let _queue = http_exchange(address, "GET", "/api/runs/queue", None, None).await;
    let _latest_run = http_exchange(
        address,
        "GET",
        &format!("/api/runs/latest?command_ref={command_id}"),
        None,
        None,
    )
    .await;
    let run_state = http_exchange(
        address,
        "GET",
        &format!("/api/runs/{run_id}?detailed=true"),
        None,
        None,
    )
    .await;
    assert_eq!(run_state["data"]["parsed_summary"]["truncated"], false);
    let _logs = http_exchange(
        address,
        "GET",
        &format!("/api/runs/{run_id}/logs/search?query=passed&stream=stdout&context_lines=1&max_matches=5&case_sensitive=true"),
        None,
        None,
    )
    .await;
    let _artifact = http_exchange(
        address,
        "GET",
        "/api/artifacts/latest?kind=coverage",
        None,
        None,
    )
    .await;
    assert!(
        http_raw(address, "GET", "/api/artifacts/latest", None, None)
            .await
            .contains("400 Bad Request")
    );
    let _run_topology = http_exchange(
        address,
        "GET",
        &format!("/api/topology/run/{run_id}"),
        None,
        None,
    )
    .await;
    let _snapshot_topology = http_exchange(
        address,
        "GET",
        &format!("/api/topology/snapshot/{second_snapshot_id}"),
        None,
        None,
    )
    .await;
    assert!(
        http_raw(address, "GET", "/api/topology/unknown/id", None, None)
            .await
            .contains("400 Bad Request")
    );
    let _cancel = http_exchange(
        address,
        "POST",
        &format!("/api/runs/{run_id}/cancel"),
        None,
        None,
    )
    .await;
    let _bad_worktree = http_exchange(
        address,
        "POST",
        "/api/worktrees/register",
        Some(&json!({"path":directory.path(),"base_ref":"main"})),
        None,
    )
    .await;
    let _bad_progress = http_exchange(
        address,
        "GET",
        "/api/worktrees/missing/progress?suite=unit",
        None,
        None,
    )
    .await;
    assert!(
        http_raw(
            address,
            "GET",
            "/api/worktrees/missing/progress",
            None,
            None
        )
        .await
        .contains("400 Bad Request")
    );
    assert!(
        http_raw(address, "GET", "/api/worktrees/missing/compare", None, None)
            .await
            .contains("404 Not Found")
    );
    assert!(
        http_raw(
            address,
            "GET",
            "/api/source-lines?snapshot_id=missing&file_path=a.py&start=bad&end=2",
            None,
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    assert!(
        http_raw(address, "GET", "/api/compare", None, None)
            .await
            .contains("400 Bad Request")
    );
    assert!(
        http_raw(
            address,
            "GET",
            &format!("/api/compare?snapshot_id={second_snapshot_id}"),
            None,
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    assert!(
        http_raw(address, "POST", "/api/compare", Some(&json!({})), None,)
            .await
            .contains("400 Bad Request")
    );
    assert!(
        http_raw(
            address,
            "POST",
            "/api/compare",
            Some(&json!({"snapshot_id":second_snapshot_id})),
            None,
        )
        .await
        .contains("400 Bad Request")
    );
    assert!(
        http_raw(
            address,
            "GET",
            &format!("/api/snapshots/{second_snapshot_id}/files/missing.py"),
            None,
            None,
        )
        .await
        .contains("404 Not Found")
    );
    assert!(
        http_raw(address, "GET", "/api/unknown", None, None)
            .await
            .contains("404 Not Found")
    );
    for (method, path, body) in [
        ("GET", "/api/projects?max_words=49", None),
        ("GET", "/api/snapshots/missing", None),
        ("GET", "/api/snapshots/missing/files", None),
        ("GET", "/api/snapshots/missing/insights", None),
        ("GET", "/api/trend?worktree_id=missing", None),
        (
            "GET",
            "/api/compare?snapshot_id=missing&baseline_snapshot_id=missing",
            None,
        ),
        (
            "GET",
            "/api/changed-lines?snapshot_id=missing&baseline_snapshot_id=missing",
            None,
        ),
        (
            "GET",
            "/api/source-lines?snapshot_id=missing&file_path=a.py&start=1&end=2",
            None,
        ),
        ("GET", "/api/commands/missing", None),
        ("GET", "/api/runs/latest?command_ref=missing", None),
        ("GET", "/api/runs/missing", None),
        ("POST", "/api/runs/missing/cancel", None),
        ("GET", "/api/runs/missing/logs/search?query=missing", None),
        ("GET", "/api/topology/run/missing", None),
        ("GET", "/api/topology/snapshot/missing", None),
    ] {
        let _ = http_raw(address, method, path, body, None).await;
    }
    let summary = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})),
        None,
    )
    .await;
    assert_eq!(summary["result"]["serverInfo"]["name"], "coverage-mcp");
    let tools = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})),
        None,
    )
    .await;
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 11);
    let _resources = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":4,"method":"resources/list","params":{}})),
        None,
    )
    .await;
    let _templates = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":5,"method":"resources/templates/list","params":{}})),
        None,
    )
    .await;
    let _context_resource = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"coverage://context"}})), None).await;
    let _snapshot_resource = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":7,"method":"resources/read","params":{"uri":format!("coverage://snapshot/{second_snapshot_id}/summary")}})), None).await;
    let _notification = http_raw(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","method":"notifications/initialized"})),
        None,
    )
    .await;
    let _unknown_method = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":8,"method":"unknown"})),
        None,
    )
    .await;
    let _mcp_context = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"project_context","arguments":{}}})), None).await;
    let _mcp_files = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"coverage_query","arguments":{"view":"files","snapshot_id":second_snapshot_id}}})), None).await;
    let mcp_targets = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":101,"method":"tools/call","params":{"name":"coverage_query","arguments":{"view":"targets","snapshot_id":second_snapshot_id}}})), None).await;
    assert_eq!(mcp_targets["result"]["isError"], false);
    assert!(mcp_targets["result"]["structuredContent"]["data"]["targets"].is_array());
    let _mcp_compare = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"coverage_compare","arguments":{"view":"overview","snapshot_id":second_snapshot_id,"baseline_snapshot_id":snapshot_id}}})), None).await;
    let mcp_regions = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":102,"method":"tools/call","params":{"name":"coverage_compare","arguments":{"view":"regions","snapshot_id":second_snapshot_id,"baseline_snapshot_id":snapshot_id}}})), None).await;
    assert_eq!(mcp_regions["result"]["isError"], false);
    assert!(mcp_regions["result"]["structuredContent"]["data"]["regions"].is_array());
    let _mcp_source = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"source_context","arguments":{"snapshot_id":second_snapshot_id,"file_path":"a.py","start":1,"end":2}}})), None).await;
    let _mcp_missing_uri = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":13,"method":"resources/read","params":{}})),
        None,
    )
    .await;
    let _mcp_unknown_resource = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":14,"method":"resources/read","params":{"uri":"coverage://unknown"}})), None).await;
    let _mcp_missing_name = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":15,"method":"tools/call","params":{}})),
        None,
    )
    .await;
    let _mcp_missing_params = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":17,"method":"tools/call"})),
        None,
    )
    .await;
    let _mcp_unknown_tool = http_exchange(
        address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":16,"method":"tools/call","params":{"name":"unknown"}})),
        None,
    )
    .await;
    assert!(
        http_raw(address, "POST", "/mcp/", None, None)
            .await
            .contains("400 Bad Request")
    );
    let mcp_summary = http_exchange(address, "POST", "/mcp/", Some(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"coverage_query","arguments":{"view":"summary","snapshot_id":snapshot_id}}})), None).await;
    assert_eq!(mcp_summary["result"]["isError"], false);
    task.abort();
    let _ = task.await;

    let mut common_config = config();
    common_config.common_db_path = directory.path().join("common-registry.duckdb");
    let common_server = CoverageServer::new(common_config).unwrap();
    let common_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let common_address = common_listener.local_addr().unwrap();
    let common_task = tokio::spawn(common_server.serve_listener(common_listener));
    let _empty_projects = http_exchange(common_address, "GET", "/api/projects", None, None).await;
    let repo_header = ("x-coverage-mcp-repo", directory.path().to_str().unwrap());
    let _empty_latest_run = http_exchange(
        common_address,
        "GET",
        "/api/runs/latest",
        None,
        Some(repo_header),
    )
    .await;
    let selected = http_exchange(
        common_address,
        "GET",
        "/api/projects",
        None,
        Some(repo_header),
    )
    .await;
    let project_id = selected["data"][0]["id"].as_str().unwrap().to_owned();
    let selected_by_id = http_exchange(
        common_address,
        "GET",
        &format!("/api/projects/{project_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(selected_by_id["data"]["id"], project_id);
    let updated_by_id = http_exchange(
        common_address,
        "PATCH",
        &format!("/api/projects/{project_id}"),
        Some(&json!({"compaction":{"compaction_batch_size":5}})),
        None,
    )
    .await;
    assert_eq!(updated_by_id["data"]["compaction_batch_size"], 5);
    let compacted_by_id = http_exchange(
        common_address,
        "POST",
        &format!("/api/projects/{project_id}/compact"),
        None,
        None,
    )
    .await;
    assert_eq!(compacted_by_id["data"]["status"], "completed");
    assert!(
        http_raw(
            common_address,
            "GET",
            "/api/projects/unknown-project",
            None,
            None,
        )
        .await
        .contains("404 Not Found")
    );
    let _unscoped_with_store =
        http_exchange(common_address, "GET", "/api/projects", None, None).await;
    assert!(
        http_raw(common_address, "GET", "/api/snapshots", None, None)
            .await
            .contains("400 Bad Request")
    );
    common_task.abort();
    let _ = common_task.await;

    let bad_directory = tempfile::tempdir().unwrap();
    git_repo(bad_directory.path());
    let bad_db_directory = bad_directory.path().join("database-directory");
    std::fs::create_dir_all(&bad_db_directory).unwrap();
    let mut bad_config = config();
    bad_config.db_path = Some(bad_db_directory);
    let bad_server = CoverageServer::new(bad_config).unwrap();
    let bad_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let bad_address = bad_listener.local_addr().unwrap();
    let bad_task = tokio::spawn(bad_server.serve_listener(bad_listener));
    let _bad_resource = http_exchange(
        bad_address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":20,"method":"resources/read","params":{"uri":"coverage://context"}})),
        None,
    )
    .await;
    let _bad_tool = http_exchange(
        bad_address,
        "POST",
        "/mcp/",
        Some(&json!({"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"project_context","arguments":{}}})),
        None,
    )
    .await;
    bad_task.abort();
    let _ = bad_task.await;
}

async fn http_raw(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Option<&Value>,
    header: Option<(&str, &str)>,
) -> String {
    let payload = body.map(Value::to_string).unwrap_or_default();
    http_raw_payload(address, method, path, &payload, header).await
}

async fn http_raw_payload(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    payload: &str,
    header: Option<(&str, &str)>,
) -> String {
    let mut stream = None;
    for _ in 0..50 {
        match TcpStream::connect(address).await {
            Ok(value) => {
                stream = Some(value);
                break;
            }
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    }
    let mut stream = stream.expect("HTTP test server did not start");
    let (host, extra) = match header {
        Some(("Host", value)) => (value.to_owned(), String::new()),
        Some((key, value)) => ("127.0.0.1".to_owned(), format!("{key}: {value}\r\n")),
        None => ("127.0.0.1".to_owned(), String::new()),
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    String::from_utf8(bytes).unwrap()
}

async fn http_exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Option<&Value>,
    header: Option<(&str, &str)>,
) -> Value {
    let raw = http_raw(address, method, path, body, header).await;
    let payload = raw.split("\r\n\r\n").nth(1).unwrap_or_default();
    serde_json::from_str(payload).unwrap_or_else(|error| panic!("invalid response {error}: {raw}"))
}

#[test]
fn rust_validation_errors_are_stable() {
    let error = AppError::Validation("bad input".to_owned());
    assert_eq!(error.status_code(), 400);
    assert_eq!(AppError::NotFound("missing".to_owned()).status_code(), 404);
    assert_eq!(AppError::Runtime("boom".to_owned()).status_code(), 500);
    assert_eq!(
        AppError::Busy {
            resource: "database".to_owned(),
            holder: String::new(),
        }
        .status_code(),
        503
    );
    assert_eq!(
        AppError::Timeout {
            operation: "query".to_owned(),
            timeout_ms: 1,
        }
        .status_code(),
        504
    );
    assert!(format!("{error}").contains("bad input"));
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    assert!(
        store
            .update_project_settings(ProjectSettingsPatch {
                compaction_after_days: Some(0),
                ..Default::default()
            })
            .is_err()
    );
    assert!(store.lines_in_ranges("missing", "a.py", &[(0, 1)]).is_err());
    assert!(
        store
            .lines_in_ranges("missing", "a.py", &[(1, 1); 11])
            .is_err()
    );
    store.close().unwrap();
}

#[test]
fn rust_cli_entrypoint_executes_compaction_and_host_validation() {
    let directory = tempfile::tempdir().unwrap();
    git_repo(directory.path());
    let binary = std::env::var_os("CARGO_BIN_EXE_coverage-mcp")
        .map(PathBuf::from)
        .or_else(|| {
            let fallback = std::env::current_exe()
                .expect("test executable path")
                .parent()
                .and_then(Path::parent)
                .expect("Cargo target directory")
                .join("coverage-mcp");
            fallback.exists().then_some(fallback)
        });
    let Some(binary) = binary else {
        return;
    };
    let compact = Command::new(&binary)
        .args([
            "compact",
            "--repo",
            directory.path().to_str().unwrap(),
            "--older-than-days",
            "1",
        ])
        .output()
        .unwrap();
    assert!(compact.status.success(), "compact failed: {compact:?}");
    assert!(String::from_utf8_lossy(&compact.stdout).contains("completed"));
    let compact_with_default_policy = Command::new(&binary)
        .args(["compact", "--repo", directory.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        compact_with_default_policy.status.success(),
        "default-policy compact failed: {compact_with_default_policy:?}"
    );
    let invalid_host = Command::new(&binary)
        .args(["serve", "--host", "0.0.0.0"])
        .output()
        .unwrap();
    assert!(!invalid_host.status.success());
    assert!(String::from_utf8_lossy(&invalid_host.stderr).contains("daemon host"));
    let mut daemon = Command::new(&binary)
        .args(["serve", "--port", "0"])
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    daemon.kill().unwrap();
    let _ = daemon.wait().unwrap();
}
