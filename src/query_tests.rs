//! Outcome-oriented tests for the report reader and its shared MCP boundary.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::{config::ServerConfig, mcp, parser::parse_coverage_report};
use tempfile::{TempDir, tempdir};

fn fixture() -> TempDir {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("a.rs"), "fn parse() {}\n").unwrap();
    dir
}
fn llvm(root: &Path, name: &str, hits: [i64; 3]) {
    let regions: Vec<_> = hits
        .into_iter()
        .enumerate()
        .map(|(i, n)| json!([i + 1, 1, i + 1, 8, n, 0, 0, 0]))
        .collect();
    let covered = hits.iter().filter(|n| **n > 0).count();
    let document = json!({"data":[{"functions":[{"name":"parse","count":hits[0],"filenames":["a.rs"],"regions":regions,"branches":[[2,1,2,8,hits[1],hits[2],0,0,4]]}],"files":[{"filename":"a.rs","segments":[[1,1,hits[0],true,true,false],[2,1,hits[1],true,true,false],[3,1,hits[2],true,true,false],[4,1,0,false,false,false]],"summary":{"regions":{"count":3,"covered":covered},"lines":{"count":3,"covered":covered},"functions":{"count":1,"covered":1},"branches":{"count":2,"covered":hits[1..].iter().filter(|n| **n>0).count()}}}]}]});
    std::fs::write(root.join(name), document.to_string()).unwrap();
}
fn receipt(root: &Path, report: &str, build: &str, scope: &str, status: &str) {
    let value = json!({"report_sha256":digest(&std::fs::read(root.join(report)).unwrap()),"source_hashes":{"a.rs":digest(&std::fs::read(root.join("a.rs")).unwrap())},"build_id":build,"scope":scope,"test_status":status});
    std::fs::write(
        root.join(format!("{report}.context.json")),
        value.to_string(),
    )
    .unwrap();
}
fn ask(dir: &TempDir, name: &str, args: Value) -> Value {
    read_reports(dir.path(), name, &args).unwrap()["data"].clone()
}
fn reader(dir: &TempDir) -> Reader<'static> {
    Reader {
        context: RequestContext {
            repo_key: "test".into(),
            checkout_path: dir.path().to_string_lossy().into(),
            suite: None,
        },
        store: None,
    }
}
fn config() -> ServerConfig {
    ServerConfig::from_environment(None, None, None).unwrap()
}

#[test]
fn native_gaps_are_grouped_without_source_dump_or_denominator_changes() {
    let dir = fixture();
    llvm(dir.path(), "coverage.json", [1, 0, 0]);
    let gaps = ask(
        &dir,
        "coverage_gaps",
        json!({"measurement":"coverage.json","query":"PARSE"}),
    );
    assert_eq!(gaps["status"], "measured");
    assert_eq!(
        gaps["measurement"]["coverage"],
        json!({"metric":"regions","covered":1,"total":3,"missing":2})
    );
    assert_eq!(gaps["group_count"], 1);
    assert_eq!(gaps["groups"][0]["function"], "parse");
    assert_eq!(gaps["groups"][0]["count"], 2);
    assert_eq!(
        gaps["groups"][0]["locations"][0]["span"],
        json!([2, 1, 2, 8])
    );
    assert!(!gaps.to_string().contains("fn parse"));
    assert_eq!(gaps["measurement"]["tests"], "unknown");
    assert_eq!(
        ask(
            &dir,
            "coverage_gaps",
            json!({"measurement":"coverage.json","line":1})
        )["group_count"],
        0
    );
    let branch = ask(
        &dir,
        "coverage_gaps",
        json!({"measurement":"coverage.json","metric":"branches"}),
    );
    assert_eq!(branch["groups"][0]["count"], 2);
    assert_eq!(branch["groups"][0]["locations"][0]["arm"], "false");
    assert_eq!(
        ask(
            &dir,
            "coverage_gaps",
            json!({"measurement":"coverage.json","metric":"lines","query":"a.rs","limit":1})
        )["next_offset"],
        1
    );
    assert_eq!(
        ask(
            &dir,
            "coverage_gaps",
            json!({"measurement":"coverage.json","metric":"lines","offset":1})
        )["groups"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(render(&json!({"data":gaps})).contains("a.rs:2"));
}

#[test]
fn comparison_separates_subsets_full_regressions_failed_tests_and_conflicts() {
    let dir = fixture();
    llvm(dir.path(), "base.json", [1, 0, 1]);
    llvm(dir.path(), "after.json", [1, 1, 0]);
    let args = json!({"measurement":"after.json","baseline":"base.json"});
    let limited = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(limited["status"], "limited");
    assert_eq!(limited["changes"]["newly_observed"], 1);
    assert_eq!(limited["changes"]["not_observed"], 1);
    assert!(limited["changes"]["regressed"].is_null());
    receipt(dir.path(), "base.json", "build-a", "full", "passed");
    receipt(
        dir.path(),
        "after.json",
        "build-a",
        "selected_tests",
        "passed",
    );
    let subset = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(subset["status"], "improved");
    assert_eq!(subset["scope"], "selected_tests");
    let mut full_args = args.clone();
    full_args["scope"] = json!("full");
    assert_eq!(
        ask(&dir, "coverage_compare", full_args.clone())["scope"],
        "selected_tests"
    );
    receipt(dir.path(), "after.json", "build-a", "full", "passed");
    let full = ask(&dir, "coverage_compare", full_args.clone());
    assert_eq!(full["status"], "regression");
    assert_eq!(full["changes"]["regressed"], 1);
    receipt(dir.path(), "after.json", "build-a", "full", "failed");
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "tests_failed"
    );
    receipt(dir.path(), "after.json", "build-b", "full", "passed");
    let conflict = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(conflict["status"], "incomparable");
    assert!(conflict["changes"].is_null());
    assert_eq!(conflict["gained_group_count"], 0);
    receipt(dir.path(), "after.json", "build-a", "full", "passed");
    let same = ask(
        &dir,
        "coverage_compare",
        json!({"measurement":"after.json","baseline":"after.json","scope":"full"}),
    );
    assert_eq!(same["status"], "no_gain");
    std::fs::write(dir.path().join("a.rs"), "changed source\n").unwrap();
    assert_eq!(
        ask(&dir, "coverage_gaps", json!({"measurement":"base.json"}))["measurement"]["source"],
        "changed_or_missing"
    );
    receipt(dir.path(), "after.json", "build-a", "full", "passed");
    assert_eq!(
        ask(&dir, "coverage_compare", args)["status"],
        "incomparable"
    );
    assert!(render(&json!({"data":full})).contains("Newly observed: 1"));
}

#[test]
fn line_reports_missing_details_and_input_errors_are_explicit() {
    let dir = fixture();
    std::fs::write(
        dir.path().join("coverage.lcov"),
        "TN:\nSF:a.rs\nDA:1,1\nDA:2,0\nend_of_record\n",
    )
    .unwrap();
    let gaps = ask(
        &dir,
        "coverage_gaps",
        json!({"measurement":"coverage.lcov"}),
    );
    assert_eq!(gaps["measurement"]["coverage"]["metric"], "lines");
    assert!(gaps["groups"][0]["function"].is_null());
    assert_eq!(
        ask(
            &dir,
            "coverage_gaps",
            json!({"measurement":"coverage.lcov","metric":"branches"})
        )["status"],
        "detail_unavailable"
    );
    assert_eq!(
        ask(
            &dir,
            "coverage_compare",
            json!({"measurement":"coverage.lcov","baseline":"coverage.lcov","metric":"regions"})
        )["status"],
        "incomparable"
    );
    for args in [
        json!({}),
        json!({"measurement":""}),
        json!({"measurement":"missing.json"}),
        json!({"measurement":"coverage.lcov","line":0}),
        json!({"measurement":"coverage.lcov","metric":"functions"}),
        json!({"measurement":"coverage.lcov","query":""}),
        json!({"measurement":"coverage.lcov","query":"x".repeat(257)}),
        json!({"limit":0}),
        json!({"limit":21}),
        json!({"limit":-1}),
        json!({"unexpected":true}),
        json!({"measurement":7}),
    ] {
        assert!(
            read_reports(dir.path(), "coverage_gaps", &args).is_err(),
            "{args}"
        );
    }
    assert!(read_reports(dir.path(), "unknown", &json!({})).is_err());
    assert!(read_reports(dir.path(), "coverage_compare", &json!({})).is_err());
    assert!(
        read_reports(
            dir.path(),
            "coverage_compare",
            &json!({"measurement":"coverage.lcov","baseline":"coverage.lcov","scope":"all"})
        )
        .is_err()
    );
    let outside = fixture();
    std::fs::write(
        outside.path().join("out.lcov"),
        "TN:\nSF:a.rs\nDA:1,0\nend_of_record\n",
    )
    .unwrap();
    assert!(
        read_reports(
            dir.path(),
            "coverage_gaps",
            &json!({"measurement":outside.path().join("out.lcov")})
        )
        .is_err()
    );
    assert!(render(&json!({})).contains("unknown"));
}

#[test]
fn receipts_bind_artifact_and_require_complete_source_context() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [1, 0, 0]);
    receipt(dir.path(), "r.json", "b", "full", "passed");
    let path = dir.path().join("r.json.context.json");
    let original: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for (field, value) in [
        ("report_sha256", json!("wrong")),
        ("source_hashes", json!(null)),
        ("build_id", json!("")),
        ("test_status", json!("maybe")),
        ("scope", json!("maybe")),
    ] {
        let mut invalid = original.clone();
        invalid[field] = value;
        std::fs::write(&path, invalid.to_string()).unwrap();
        assert!(
            read_reports(
                dir.path(),
                "coverage_gaps",
                &json!({"measurement":"r.json"})
            )
            .is_err(),
            "{field}"
        );
    }
    let mut missing = original.clone();
    missing["source_hashes"] = json!({});
    std::fs::write(&path, missing.to_string()).unwrap();
    assert_eq!(
        ask(&dir, "coverage_gaps", json!({"measurement":"r.json"}))["measurement"]["source"],
        "unverified"
    );
    std::fs::write(&path, original.to_string()).unwrap();
    std::fs::remove_file(dir.path().join("a.rs")).unwrap();
    assert_eq!(
        ask(&dir, "coverage_gaps", json!({"measurement":"r.json"}))["measurement"]["source"],
        "changed_or_missing"
    );
    std::fs::write(&path, " ".repeat(1024 * 1024 + 1)).unwrap();
    assert!(load(&reader(&dir), Some("r.json"), None).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(load(&reader(&dir), Some("r.json"), None).is_err());
    let huge = std::fs::File::create(dir.path().join("huge.json")).unwrap();
    huge.set_len(64 * 1024 * 1024 + 1).unwrap();
    assert!(load(&reader(&dir), Some("huge.json"), None).is_err());
}

#[test]
fn saved_measurements_survive_compaction_and_latest_requires_unambiguous_suite() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [1, 0, 0]);
    let store = CoverageStore::open(dir.path().join("db.duckdb"), config()).unwrap();
    let git = store.ensure_project(dir.path()).unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
    );
    assert!(call_tool(&service, "coverage_gaps", &json!({})).is_err());
    let snapshot = store
        .ingest_report(
            &dir.path().join("r.json"),
            "llvm",
            Some(dir.path()),
            None,
            None,
            None,
            "unit",
        )
        .unwrap();
    let args = json!({"measurement":snapshot["id"]});
    let before = call_tool(&service, "coverage_gaps", &args).unwrap();
    assert_eq!(before["data"]["groups"][0]["count"], 2);
    assert_eq!(
        call_tool(&service, "coverage_gaps", &json!({})).unwrap()["data"]["measurement"]["ref"],
        snapshot["id"]
    );
    assert!(
        call_tool(
            &service,
            "coverage_gaps",
            &json!({"measurement":snapshot["id"],"suite":"other"})
        )
        .is_err()
    );
    store
        .ingest_report(
            &dir.path().join("r.json"),
            "llvm",
            Some(dir.path()),
            None,
            None,
            None,
            "other",
        )
        .unwrap();
    assert!(call_tool(&service, "coverage_gaps", &json!({})).is_err());
    assert!(call_tool(&service, "coverage_gaps", &json!({"suite":"unit"})).is_ok());
    store
        .update_project_settings(crate::storage::ProjectSettingsPatch {
            compaction_after_days: Some(1),
            ..Default::default()
        })
        .unwrap();
    store
        .execute_sql_for_test(
            "UPDATE snapshots SET created_at = current_timestamp - INTERVAL 2 DAY",
        )
        .unwrap();
    store.compact_now().unwrap();
    assert_eq!(call_tool(&service, "coverage_gaps", &args).unwrap(), before);
    assert!(call_tool(&service, "coverage_gaps", &json!({"measurement":"unknown"})).is_err());
    store.close().unwrap();
    assert!(call_tool(&service, "coverage_gaps", &args).is_err());
}

#[test]
fn protocol_is_read_only_strict_and_does_not_duplicate_evidence() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [1, 0, 0]);
    let store = CoverageStore::open(dir.path().join("db.duckdb"), config()).unwrap();
    let git = store.ensure_project(dir.path()).unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
    );
    let inventory = mcp::tools_list();
    assert_eq!(inventory.as_array().unwrap().len(), 2);
    for tool in inventory.as_array().unwrap() {
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
    for request in [
        json!({}),
        json!({"id":1,"method":"unknown"}),
        json!({"id":2,"method":"resources/read","params":{"uri":"coverage://snapshot/old"}}),
        json!({"id":3,"method":"resources/read"}),
        json!({"id":4,"method":"tools/call"}),
    ] {
        assert!(mcp::dispatch_json_rpc(Some(&service), &request).unwrap()["error"].is_object());
    }
    assert!(mcp::dispatch_json_rpc(None, &json!({"method":"notifications/initialized"})).is_none());
    for method in [
        "initialize",
        "tools/list",
        "resources/list",
        "resources/templates/list",
        "ping",
    ] {
        let response = mcp::dispatch_json_rpc(None, &json!({"id":1,"method":method})).unwrap();
        assert!(response["result"].is_object(), "{response}");
    }
    let request = json!({"id":1,"method":"tools/call","params":{"name":"coverage_gaps","arguments":{"measurement":"r.json"}}});
    assert!(mcp::dispatch_json_rpc(None, &request).unwrap()["error"].is_object());
    let success = mcp::dispatch_json_rpc(Some(&service), &request).unwrap();
    assert_eq!(success["result"]["isError"], false);
    assert_eq!(
        success["result"]["structuredContent"],
        mcp::call_tool(&service, "coverage_gaps", &json!({"measurement":"r.json"})).unwrap()
    );
    assert!(
        success["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .len()
            < 80
    );
    for name in [
        "project_context",
        "coverage_review",
        "run_test",
        "coverage_import",
        "register_test_command",
        "run_review",
        "cancel_run",
        "find_duplicate_coverage_tests",
    ] {
        assert!(mcp::call_tool(&service, name, &json!({})).is_err());
    }
    assert_eq!(
        mcp::dispatch_json_rpc(
            Some(&service),
            &json!({"id":1,"method":"tools/call","params":{"name":"coverage_gaps"}})
        )
        .unwrap()["result"]["isError"],
        true
    );
    store.close().unwrap();
}

#[test]
fn native_record_validation_deduplication_and_ordering_preserve_evidence() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [0, 0, 0]);
    let mut report = parse_coverage_report(
        &dir.path().join("r.json"),
        "llvm",
        Some(dir.path().to_str().unwrap()),
    )
    .unwrap();
    let original = report.files[0].raw_metrics["llvm_functions"][0].clone();
    for (key, value) in [
        ("name", json!(null)),
        ("filenames", json!(null)),
        ("regions", json!([false])),
        ("regions", json!([[0, 1, 1, 1, 0, 0, 0, 0]])),
        ("regions", json!([[3, 1, 2, 1, 0, 0, 0, 0]])),
        ("regions", json!([[1, 1, 2, 1, 0, 99, 0, 0]])),
        ("regions", json!([[1, 1, 2, 1, -1, 0, 0, 0]])),
    ] {
        let mut invalid = original.clone();
        invalid[key] = value;
        report.files[0].raw_metrics["llvm_functions"] = json!([invalid]);
        assert!(
            observations(&report, dir.path().to_str().unwrap()).is_err(),
            "{key}"
        );
    }
    let mut other = original.clone();
    other["regions"] = json!([[1, 1, 2, 1, 0, 0, 0, 1]]);
    other["branches"] = Value::Null;
    report.files[0].raw_metrics["llvm_functions"] = json!([other]);
    assert!(
        !observations(&report, "")
            .unwrap()
            .values()
            .any(|row| row.kind == "regions")
    );
    let mut other = original.clone();
    other["filenames"] = json!(["different.rs"]);
    report.files[0].raw_metrics["llvm_functions"] = json!([other]);
    assert!(
        !observations(&report, "")
            .unwrap()
            .values()
            .any(|row| row.kind == "regions")
    );
    let mut hit = original.clone();
    hit["regions"][0][4] = json!(5);
    report.files[0].raw_metrics["llvm_functions"] = json!([original, hit]);
    let indexed = observations(&report, "").unwrap();
    assert_eq!(
        indexed
            .values()
            .filter(|row| row.kind == "regions" && row.span[0] == 1)
            .count(),
        1
    );
    assert_eq!(
        indexed
            .values()
            .find(|row| row.kind == "regions" && row.span[0] == 1)
            .unwrap()
            .hits,
        5
    );
    let mut multiple = Vec::new();
    for index in 0..25 {
        let mut row = indexed
            .values()
            .find(|row| row.kind == "regions")
            .unwrap()
            .clone();
        row.path = format!("{index:02}.rs");
        row.symbol = if index % 2 == 0 {
            "_ZN4test4name17h0123456789abcdefE".into()
        } else {
            "plain".into()
        };
        row.span = [1, 1, 1, 2];
        multiple.push(row);
    }
    let (page, total) = groups(multiple.iter(), 5, 5);
    assert_eq!(total, 25);
    assert_eq!(page[0]["path"], "05.rs");
    let (none, total) = groups(multiple.iter(), 5, 100);
    assert_eq!(total, 25);
    assert!(none.is_empty());
}

#[test]
fn response_byte_budget_preserves_counts_and_continuation() {
    let groups: Vec<_> = (0..20)
        .map(|n| json!({"path":format!("{n}{}","x".repeat(1200)),"count":5,"locations":[]}))
        .collect();
    for key in ["groups", "gained", "remaining"] {
        let mut data = json!({"group_count":20,"next_offset":null,"gained_group_count":20,"remaining_group_count":20});
        data[key] = json!(groups);
        let result = bounded(json!({"data":data})).unwrap();
        assert!(result.to_string().len() <= 12_288);
        assert_eq!(result["data"]["group_count"], 20);
        if key == "groups" {
            assert_eq!(
                result["data"]["next_offset"],
                result["data"][key].as_array().unwrap().len()
            );
        }
    }
    assert!(bounded(json!({"metadata":"x".repeat(13_000)})).is_err());
    let report = CoverageReport {
        format: "x".into(),
        report_path: "x".into(),
        files: vec![],
        lines: vec![],
        test_names: vec![],
        test_coverage: vec![],
        warnings: vec![],
        metadata: json!({}),
    };
    assert_eq!(
        source_state("", &report, &json!({"source_hashes":{}})).unwrap(),
        "unverified"
    );
}

#[test]
fn composite_unknown_evidence_stays_visible_and_corrupt_inventory_is_rejected() {
    let dir = fixture();
    let source_hash = digest(&std::fs::read(dir.path().join("a.rs")).unwrap());
    let snapshot = json!({"total_regions":3,"covered_regions":1,"mapping_version":"v1","inventory_hash":"inv","reasons":["missing component"]});
    let rows:Vec<_>=["covered","uncovered","missing_artifact"].into_iter().enumerate().map(|(i,state)|json!({"path":"a.rs","start_line":i+1,"end_line":i+1,"source_hash":source_hash,"state":state,"region_key":format!("{i}")})).collect();
    let measurement = decode_composite(
        dir.path().to_str().unwrap(),
        "saved",
        &snapshot,
        rows.clone(),
    )
    .unwrap();
    let observations = measurement_observations(&measurement, "").unwrap();
    assert_eq!(observations.len(), 3);
    let (gaps, count) = groups(observations.values().filter(|row| row.hits <= 0), 5, 0);
    assert_eq!(count, 2);
    assert!(
        gaps.iter()
            .any(|group| group["unmeasured_reason"] == "missing_artifact")
    );
    assert_eq!(measurement.report.total_regions(), 3);
    assert_eq!(measurement.source_state, "matches_receipt");
    assert!(render(&json!({"data":{"measurement":summary(&measurement,"regions"),"groups":gaps,"group_count":2,"next_offset":2}})).contains("Evidence missing"));
    for key in ["path", "start_line", "end_line", "region_key"] {
        let mut bad = rows.clone();
        bad[0][key] = Value::Null;
        assert!(decode_composite("", "saved", &snapshot, bad).is_err());
    }
    let mut conflict = rows.clone();
    conflict[1]["source_hash"] = json!("other");
    assert!(decode_composite("", "saved", &snapshot, conflict).is_err());
    assert!(decode_composite("", "saved", &snapshot, vec![]).is_err());
    std::fs::remove_file(dir.path().join("a.rs")).unwrap();
    std::fs::create_dir(dir.path().join("a.rs")).unwrap();
    assert!(decode_composite(dir.path().to_str().unwrap(), "saved", &snapshot, rows).is_err());
}

#[test]
fn saved_composite_queries_validate_ownership_and_keep_missing_components() {
    let dir = fixture();
    let store = CoverageStore::open(dir.path().join("db.duckdb"), config()).unwrap();
    let git = store.ensure_project(dir.path()).unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key.clone(),
            checkout_path: git.repo_path.clone(),
            suite: None,
        },
    );
    let row = json!({"path":"a.rs","start_line":1,"end_line":1,"source_hash":digest(b"fn parse() {}\n"),"state":"missing_artifact","region_key":"one"});
    let quote = |value: &str| value.replace('\'', "''");
    store.execute_sql_for_test(&format!("INSERT INTO composite_snapshots VALUES ('composite',current_timestamp,'run','cmd','{}','{}','partial','v1','inventory',NULL,false,0,1,0,'[\"missing component\"]','[]','{{}}','{{}}','[]','[]'); INSERT INTO composite_regions VALUES ('composite','one','{}')",quote(&git.repo_path),quote(&git.repo_key),quote(&row.to_string()))).unwrap();
    let args = json!({"measurement":"composite"});
    for skip in 0..8 {
        store.inject_query_fault_after(skip);
        let _ = load_composite(
            &Reader {
                context: service.context(None),
                store: Some(&store),
            },
            "composite",
        );
    }
    store.clear_query_fault();
    let gaps = call_tool(&service, "coverage_gaps", &args).unwrap();
    assert_eq!(gaps["data"]["status"], "incomplete");
    for state in [
        "unmeasured",
        "source_mismatch",
        "uncovered",
        "missing_artifact",
    ] {
        let mut region = row.clone();
        region["state"] = json!(state);
        store
            .execute_sql_for_test(&format!(
                "UPDATE composite_regions SET region='{}'",
                quote(&region.to_string())
            ))
            .unwrap();
        let legacy = service
            .composite_review(
                "insight",
                "composite",
                None,
                None,
                5,
                600,
                12_000,
                "compact",
                10,
            )
            .unwrap();
        assert!(!legacy["data"].is_null());
    }
    assert_eq!(
        gaps["data"]["groups"][0]["unmeasured_reason"],
        "missing_artifact"
    );
    assert_eq!(gaps["data"]["measurement"]["coverage"]["total"], 1);
    let comparison = call_tool(
        &service,
        "coverage_compare",
        &json!({"measurement":"composite","baseline":"composite"}),
    )
    .unwrap();
    assert_eq!(comparison["data"]["status"], "incomparable");
    store
        .execute_sql_for_test("UPDATE composite_snapshots SET repo_key='foreign'")
        .unwrap();
    assert!(call_tool(&service, "coverage_gaps", &args).is_err());
    llvm(dir.path(), "r.json", [1, 0, 0]);
    let snapshot = store
        .ingest_report(
            &dir.path().join("r.json"),
            "llvm",
            Some(dir.path()),
            None,
            None,
            None,
            "unit",
        )
        .unwrap();
    store
        .execute_sql_for_test("UPDATE snapshots SET repo_key='foreign'")
        .unwrap();
    assert!(
        call_tool(
            &service,
            "coverage_gaps",
            &json!({"measurement":snapshot["id"]})
        )
        .is_err()
    );
    store.close().unwrap();
}

#[test]
fn report_and_receipt_failures_do_not_turn_into_empty_evidence() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [1, 0, 0]);
    assert!(read_reports(Path::new("\0"), "coverage_gaps", &json!({})).is_err());
    assert!(report_path(Path::new("/missing-coverage-repo"), "r.json").is_err());
    assert!(load(&reader(&dir), Some("unknown"), None).is_err());
    std::fs::create_dir(dir.path().join("directory.json")).unwrap();
    assert!(load(&reader(&dir), Some("directory.json"), None).is_err());
    std::fs::write(dir.path().join("invalid.json"), "{").unwrap();
    assert!(load(&reader(&dir), Some("invalid.json"), None).is_err());
    std::fs::write(dir.path().join("r.json.context.json"), "{").unwrap();
    assert!(load(&reader(&dir), Some("r.json"), None).is_err());
    std::fs::remove_file(dir.path().join("r.json.context.json")).unwrap();
    #[cfg(unix)]
    {
        let other = fixture();
        std::fs::write(other.path().join("receipt.json"), "{}").unwrap();
        std::os::unix::fs::symlink(
            other.path().join("receipt.json"),
            dir.path().join("r.json.context.json"),
        )
        .unwrap();
        assert!(load(&reader(&dir), Some("r.json"), None).is_err());
        std::fs::remove_file(dir.path().join("r.json.context.json")).unwrap();
        std::os::unix::fs::symlink(
            "r.json.context.json",
            dir.path().join("r.json.context.json"),
        )
        .unwrap();
        assert!(load(&reader(&dir), Some("r.json"), None).is_err());
        std::fs::remove_file(dir.path().join("r.json.context.json")).unwrap();
    }
    receipt(dir.path(), "r.json", "b", "full", "passed");
    std::fs::remove_file(dir.path().join("a.rs")).unwrap();
    std::fs::create_dir(dir.path().join("a.rs")).unwrap();
    assert!(load(&reader(&dir), Some("r.json"), None).is_err());
    std::fs::remove_file(dir.path().join("r.json.context.json")).unwrap();
    for args in [
        json!({"measurement":"r.json","baseline":"r.json","limit":0}),
        json!({"measurement":"invalid.json","baseline":"r.json"}),
        json!({"measurement":"r.json","baseline":"invalid.json"}),
        json!({"measurement":"r.json","baseline":"r.json","metric":"bad"}),
    ] {
        assert!(read_reports(dir.path(), "coverage_compare", &args).is_err());
    }
    assert!(
        bounded(json!({"data":{"groups":[{"path":"x".repeat(13_000)}],"group_count":1}})).is_err()
    );
    let m = load(&reader(&dir), Some("r.json"), None).unwrap();
    let mut a = load(&reader(&dir), Some("r.json"), None).unwrap();
    let mut b = load(&reader(&dir), Some("r.json"), None).unwrap();
    a.context = json!({"build_id":"b"});
    b.context = json!({"build_id":"b"});
    assert!(!compatible_context(&a, &b));
    a.context["source_hashes"] = json!({});
    assert!(!compatible_context(&a, &b));
    assert!(!compatible_context(&m, &a));
}

#[test]
fn malformed_native_coordinates_propagate_through_queries() {
    let dir = fixture();
    llvm(dir.path(), "good.json", [1, 0, 0]);
    let mut document: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("good.json")).unwrap()).unwrap();
    let original = document["data"][0]["functions"][0]["regions"][0].clone();
    for index in [0, 1, 2, 3, 5, 7] {
        let mut row = original.clone();
        row[index] = json!(-1);
        document["data"][0]["functions"][0]["regions"] = json!([row]);
        std::fs::write(dir.path().join("bad.json"), document.to_string()).unwrap();
        assert!(
            read_reports(
                dir.path(),
                "coverage_gaps",
                &json!({"measurement":"bad.json"})
            )
            .is_err()
        );
        assert!(
            read_reports(
                dir.path(),
                "coverage_compare",
                &json!({"measurement":"bad.json","baseline":"good.json"})
            )
            .is_err()
        );
        assert!(
            read_reports(
                dir.path(),
                "coverage_compare",
                &json!({"measurement":"good.json","baseline":"bad.json"})
            )
            .is_err()
        );
        for args in [
            json!({"scope":"incremental","baseline":"bad.json","measurement":"good.json"}),
            json!({"scope":"incremental","baseline":"good.json","measurement":"bad.json"}),
            json!({"scope":"incremental","baseline":"good.json","measurement":"good.json","previous":["bad.json"]}),
        ] {
            assert!(read_reports(dir.path(), "coverage_compare", &args).is_err());
        }
    }
    assert!(load_composite(&reader(&dir), "missing").is_err());
    assert_eq!(
        ask(
            &dir,
            "coverage_gaps",
            json!({"measurement":"good.json","line":2})
        )["group_count"],
        1
    );
    let mut normalized =
        parse_coverage_report(&dir.path().join("good.json"), "llvm", None).unwrap();
    normalized.lines[0].count_line = false;
    assert!(
        !observations(&normalized, dir.path().to_str().unwrap())
            .unwrap()
            .values()
            .any(|row| row.kind == "lines" && row.span[0] == 1)
    );
    let tied = ["b", "a"].map(|name| Observation {
        path: "same.rs".into(),
        symbol: name.into(),
        span: [1, 1, 1, 2],
        kind: "regions",
        arm: "",
        hits: 0,
        identity: None,
        cause: None,
    });
    assert_eq!(groups(tied.iter(), 5, 0).0[0]["function"], "a");
    let rows = (1..=6)
        .map(|line| Observation {
            path: "a.rs".into(),
            symbol: "symbol".repeat(30),
            span: [line, 1, line, 9],
            kind: "regions",
            arm: "",
            hits: 0,
            identity: None,
            cause: None,
        })
        .collect::<Vec<_>>();
    let (groups, count) = groups(rows.iter(), 5, 0);
    assert_eq!(count, 1);
    assert_eq!(groups[0]["locations_omitted"], 3);
    assert!(groups[0]["function"].as_str().unwrap().ends_with('…'));
    assert!(render(&json!({"data":{"groups":groups,"group_count":1}})).contains("more locations"));
}

#[test]
fn history_ambiguity_and_query_storage_failures_are_actionable() {
    let dir = fixture();
    llvm(dir.path(), "r.json", [1, 0, 0]);
    let store = CoverageStore::open(dir.path().join("db.duckdb"), config()).unwrap();
    let git = store.ensure_project(dir.path()).unwrap();
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
    );
    let snapshot = store
        .ingest_report(
            &dir.path().join("r.json"),
            "llvm",
            Some(dir.path()),
            None,
            None,
            None,
            "unit",
        )
        .unwrap();
    store.execute_sql_for_test("INSERT INTO snapshots SELECT id || '-' || i AS id, snapshots.* EXCLUDE(id) FROM snapshots, range(5001) r(i)").unwrap();
    let error = call_tool(&service, "coverage_gaps", &json!({})).unwrap_err();
    assert!(error.to_string().contains("large history"));
    for skip in 0..12 {
        store.inject_query_fault_after(skip);
        let _ = call_tool(
            &service,
            "coverage_gaps",
            &json!({"suite":"unit","measurement":snapshot["id"]}),
        );
    }
    store.clear_query_fault();
    store.close().unwrap();
    assert!(call_tool(&service, "coverage_gaps", &json!({})).is_err());
}

#[test]
fn incremental_unions_batches_without_losing_baseline_or_double_counting() {
    let dir = fixture();
    for (name, hits, scope) in [
        ("base.json", [1, 0, 0], "full"),
        ("one.json", [0, 1, 0], "selected_tests"),
        ("two.json", [0, 1, 1], "selected_tests"),
    ] {
        llvm(dir.path(), name, hits);
        receipt(dir.path(), name, "build-a", scope, "passed");
    }
    let args = json!({"baseline":"base.json","measurement":"one.json","scope":"incremental"});
    let result = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(result["status"], "improved");
    assert_eq!(
        result["incremental"]["accepted"],
        json!({"covered":1,"total":3,"missing":2})
    );
    assert_eq!(
        result["incremental"]["combined"],
        json!({"covered":2,"total":3,"missing":1})
    );
    assert_eq!(result["incremental"]["newly_covered"], 1);
    assert_eq!(
        result["incremental"]["remaining_gap_reduction_percent"],
        50.0
    );
    assert_eq!(result["remaining"][0]["locations"][0]["span"][0], 3);
    assert_eq!(result["regression_checked"], false);
    assert!(result["changes"]["regressed"].is_null());
    assert!(result["changes"]["not_observed"].is_null());
    assert!(render(&json!({"data":result})).contains("Combined: 2 / 3 regions"));
    let mut args = args;
    args["measurement"] = json!("two.json");
    args["previous"] = json!(["one.json", "one.json"]);
    let union = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(union["incremental"]["newly_covered"], 1);
    assert_eq!(union["incremental"]["already_covered"], 1);
    assert_eq!(union["incremental"]["combined"]["covered"], 3);
    assert_eq!(union["remaining_group_count"], 0);
    args["query"] = json!("does-not-match");
    let filtered = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(filtered["incremental"], union["incremental"]);
    assert_eq!(filtered["gained_group_count"], 0);
    args["previous"] = json!(["two.json"]);
    let repeated = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(repeated["status"], "no_gain");
    assert_eq!(repeated["incremental"]["newly_covered"], 0);
    assert_eq!(
        repeated["incremental"]["remaining_gap_reduction_percent"],
        0.0
    );
    args["metric"] = json!("branches");
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["incremental"]["combined"]["total"],
        2
    );
    args["metric"] = json!("lines");
    assert_eq!(
        ask(&dir, "coverage_compare", args)["incremental"]["combined"]["total"],
        3
    );
}

#[test]
fn incremental_rejects_failed_stale_incomplete_and_changed_inventory() {
    let dir = fixture();
    llvm(dir.path(), "base.json", [1, 0, 0]);
    llvm(dir.path(), "batch.json", [0, 1, 0]);
    let args = json!({"baseline":"base.json","measurement":"batch.json","scope":"incremental"});
    let unverified = ask(&dir, "coverage_compare", args.clone());
    assert_eq!(unverified["status"], "limited");
    assert_eq!(unverified["incremental"]["baseline_scope"], "unknown");
    assert_eq!(unverified["incremental"]["evidence"], "unverified");
    for (base_status, batch_status, expected) in [
        ("failed", "passed", "tests_failed"),
        ("passed", "failed", "tests_failed"),
        ("passed", "unknown", "limited"),
    ] {
        receipt(dir.path(), "base.json", "a", "full", base_status);
        receipt(
            dir.path(),
            "batch.json",
            "a",
            "selected_tests",
            batch_status,
        );
        let result = ask(&dir, "coverage_compare", args.clone());
        assert_eq!(result["status"], expected);
        if expected == "tests_failed" {
            assert!(result["incremental"].is_null());
        }
    }
    let mut prior = args.clone();
    prior["previous"] = json!(["batch.json"]);
    receipt(dir.path(), "batch.json", "a", "selected_tests", "failed");
    let failed = ask(&dir, "coverage_compare", prior.clone());
    assert_eq!(failed["status"], "tests_failed");
    assert!(failed["reason"].as_str().unwrap().contains("Earlier batch"));
    prior["previous"] = json!(["missing.json"]);
    assert!(read_reports(dir.path(), "coverage_compare", &prior).is_err());
    receipt(dir.path(), "batch.json", "b", "selected_tests", "passed");
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "incomparable"
    );
    receipt(dir.path(), "batch.json", "a", "selected_tests", "passed");
    std::fs::write(dir.path().join("a.rs"), "changed source").unwrap();
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "incomparable"
    );
    receipt(dir.path(), "base.json", "a", "full", "passed");
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "incomparable"
    );
    receipt(dir.path(), "batch.json", "a", "selected_tests", "passed");
    for (file, change, reason) in [
        ("base.json", "summary", "Baseline detail"),
        ("batch.json", "summary", "Batch detail"),
        ("batch.json", "inventory", "outside the baseline"),
    ] {
        llvm(dir.path(), file, [1, 0, 0]);
        let path = dir.path().join(file);
        let mut document: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if change == "summary" {
            document["data"][0]["files"][0]["summary"]["regions"]["count"] = json!(4);
        } else {
            document["data"][0]["functions"][0]["regions"][2] = json!([5, 1, 5, 8, 0, 0, 0, 0]);
        }
        std::fs::write(&path, document.to_string()).unwrap();
        receipt(dir.path(), file, "a", "selected_tests", "passed");
        let result = ask(&dir, "coverage_compare", args.clone());
        assert_eq!(result["status"], "incomparable");
        assert!(
            result["reason"].as_str().unwrap().contains(reason),
            "{result}"
        );
        assert!(result["incremental"].is_null());
        llvm(dir.path(), file, [1, 0, 0]);
        receipt(dir.path(), file, "a", "full", "passed");
    }
    for extra in [
        json!({"previous":vec!["batch.json";17]}),
        json!({"scope":"full","previous":["batch.json"]}),
    ] {
        let mut invalid = args.clone();
        for (key, value) in extra.as_object().unwrap() {
            invalid[key] = value.clone();
        }
        assert!(read_reports(dir.path(), "coverage_compare", &invalid).is_err());
    }
}

#[test]
fn incremental_coalesces_generic_locations_and_accepts_partial_batch_reports() {
    let dir = fixture();
    llvm(dir.path(), "base.json", [1, 0, 0]);
    llvm(dir.path(), "batch.json", [0, 1, 0]);
    for file in ["base.json", "batch.json"] {
        let path = dir.path().join(file);
        let mut document: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let mut duplicate = document["data"][0]["functions"][0].clone();
        duplicate["name"] = json!("parse_generic_instance");
        document["data"][0]["functions"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        std::fs::write(path, document.to_string()).unwrap();
        receipt(dir.path(), file, "a", "selected_tests", "passed");
    }
    let args = json!({"baseline":"base.json","measurement":"batch.json","scope":"incremental"});
    assert_eq!(
        ask(&dir, "coverage_compare", args)["incremental"]["combined"]["covered"],
        2
    );
    std::fs::write(
        dir.path().join("base.lcov"),
        "TN:\nSF:a.rs\nDA:1,1\nDA:2,0\nDA:3,0\nend_of_record\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("batch.lcov"),
        "TN:\nSF:a.rs\nDA:2,1\nend_of_record\n",
    )
    .unwrap();
    let args = json!({"baseline":"base.lcov","measurement":"batch.lcov","scope":"incremental"});
    assert_eq!(
        ask(&dir, "coverage_compare", args)["incremental"]["combined"],
        json!({"total":3,"covered":2,"missing":1})
    );
    let mismatch = json!({"baseline":"base.json","measurement":"batch.lcov","metric":"lines","scope":"incremental"});
    assert_eq!(
        ask(&dir, "coverage_compare", mismatch)["status"],
        "incomparable"
    );
    let mut prior = json!({"baseline":"base.json","measurement":"batch.json","scope":"incremental","previous":["batch.json"]});
    std::fs::remove_file(dir.path().join("batch.json.context.json")).unwrap();
    assert_eq!(
        ask(&dir, "coverage_compare", prior.clone())["status"],
        "limited"
    );
    prior["metric"] = json!("branches");
    assert_eq!(
        ask(&dir, "coverage_compare", prior)["incremental"]["combined"]["covered"],
        1
    );
}

#[test]
fn incremental_preserves_known_conflicts_even_with_partial_receipts() {
    let dir = fixture();
    for name in ["base.json", "one.json", "two.json"] {
        llvm(dir.path(), name, [1, 0, 0]);
    }
    receipt(
        dir.path(),
        "one.json",
        "build-a",
        "selected_tests",
        "passed",
    );
    receipt(
        dir.path(),
        "two.json",
        "build-b",
        "selected_tests",
        "passed",
    );
    let mut args = json!({"scope":"incremental","baseline":"base.json","measurement":"two.json","previous":["one.json"]});
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "incomparable"
    );
    args["previous"] = json!(["one.json", "two.json"]);
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "incomparable"
    );
    receipt(
        dir.path(),
        "two.json",
        "build-a",
        "selected_tests",
        "passed",
    );
    assert_eq!(
        ask(&dir, "coverage_compare", args.clone())["status"],
        "limited"
    );
    std::fs::remove_file(dir.path().join("one.json.context.json")).unwrap();
    args["previous"] = json!(["one.json"]);
    assert_eq!(ask(&dir, "coverage_compare", args)["status"], "limited");

    std::fs::write(dir.path().join("b.rs"), "new source").unwrap();
    std::fs::write(
        dir.path().join("partial.lcov"),
        "TN:\nSF:a.rs\nDA:1,1\nend_of_record\nSF:b.rs\nDA:1,0\nend_of_record\n",
    )
    .unwrap();
    let report = dir.path().join("partial.lcov");
    let receipt = json!({"report_sha256":digest(&std::fs::read(&report).unwrap()),"source_hashes":{"b.rs":digest(b"old source")},"build_id":"a","test_status":"passed"});
    std::fs::write(
        dir.path().join("partial.lcov.context.json"),
        receipt.to_string(),
    )
    .unwrap();
    let gaps = ask(&dir, "coverage_gaps", json!({"measurement":"partial.lcov"}));
    assert_eq!(gaps["measurement"]["source"], "changed_or_missing");
    let result = ask(
        &dir,
        "coverage_compare",
        json!({"scope":"incremental","baseline":"partial.lcov","measurement":"partial.lcov"}),
    );
    assert_eq!(result["status"], "incomparable");
    assert!(result["incremental"].is_null());
}
