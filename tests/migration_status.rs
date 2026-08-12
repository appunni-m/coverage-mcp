//! End-to-end checks for the Rust migration evidence command.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::{TempDir, tempdir};

fn status_binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_migration-status")
        .map(PathBuf::from)
        .expect("Cargo must provide the migration-status binary to integration tests")
}

fn fixture_repository() -> TempDir {
    let directory = tempdir().unwrap();
    let files = [
        (
            "tests/fixtures/manifest.yaml",
            include_str!("fixtures/manifest.yaml"),
        ),
        (
            "tests/fixtures/inputs/benchmark/compaction_workload.json",
            include_str!("fixtures/inputs/benchmark/compaction_workload.json"),
        ),
        (
            "tests/fixtures/inputs/coverage/rust_source.json",
            include_str!("fixtures/inputs/coverage/rust_source.json"),
        ),
        (
            "tests/fixtures/inputs/parity/parser_formats.json",
            include_str!("fixtures/inputs/parity/parser_formats.json"),
        ),
        (
            "tests/fixtures/inputs/parity/storage_compaction.json",
            include_str!("fixtures/inputs/parity/storage_compaction.json"),
        ),
        (
            "tests/fixtures/inputs/parity/transport_contract.json",
            include_str!("fixtures/inputs/parity/transport_contract.json"),
        ),
    ];
    for (relative, contents) in files {
        let path = directory.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }
    directory
}

fn run_status(root: &Path, record_parity: bool) -> std::process::Output {
    let mut command = Command::new(status_binary());
    if record_parity {
        command.arg("--record-parity");
    }
    command.arg(root);
    command.output().unwrap()
}

#[test]
fn status_generator_keeps_missing_evidence_explicit() {
    let directory = fixture_repository();
    let marker = run_status(directory.path(), true);
    assert!(marker.status.success(), "marker failed: {marker:?}");

    let status = run_status(directory.path(), false);
    assert!(status.status.success(), "status failed: {status:?}");

    let report: Value = serde_json::from_slice(
        &std::fs::read(directory.path().join("target/migration/status-report.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["schema"], "migration-parity/status-report@1");
    let operations = report["operations"].as_array().unwrap();
    let compact = operations
        .iter()
        .find(|operation| operation["operation"] == "compact")
        .unwrap();
    assert_eq!(compact["parity"]["outcome"], "not_proven");
    assert_eq!(compact["coverage"]["outcome"], "not_proven");
    assert_eq!(compact["benchmark"]["outcome"], "not_proven");
    assert!(
        report["stale_or_incompatible_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["lane"] == "parity"
                    && item["identity_diff"]
                        .as_array()
                        .unwrap()
                        .contains(&Value::String("target.dirty".to_owned()))
            })
    );
    for page in [
        "parity-status.md",
        "coverage-status.md",
        "benchmark-status.md",
        "public-contract.md",
    ] {
        assert!(directory.path().join("docs/generated").join(page).is_file());
    }
}
