//! End-to-end smoke tests for the storage and native MCP transports.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use coverage_mcp::storage::ProjectSettingsPatch;
use coverage_mcp::{CoverageStore, ServerConfig};
use tempfile::tempdir;

fn config() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".to_owned(),
        port: 59471,
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
        default_compaction_interval_seconds: 3600,
        default_compaction_batch_size: 100,
    }
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

fn init_git_repository(root: &Path, marker: &str) {
    std::fs::create_dir_all(root).unwrap();
    run_git(root, &["init", "-b", "main"]);
    run_git(root, &["config", "user.email", "rust@example.com"]);
    run_git(root, &["config", "user.name", "Rust Tests"]);
    std::fs::write(root.join("marker.txt"), format!("{marker}\n")).unwrap();
    run_git(root, &["add", "."]);
    run_git(root, &["commit", "-m", "base"]);
}

fn unused_loopback_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

fn spawn_shared_connector(repo: &Path, common_db: &Path, port: u16) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
        .args(["connect", "--repo"])
        .arg(repo)
        .env("COVERAGE_MCP_HOST", "127.0.0.1")
        .env("COVERAGE_MCP_PORT", port.to_string())
        .env("COVERAGE_MCP_COMMON_DB", common_db)
        .env_remove("COVERAGE_MCP_DB")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"project_context\",\"arguments\":{\"detailed\":false}}}\n"
    );
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child
}

fn connector_responses(output: Output) -> Vec<serde_json::Value> {
    assert!(
        output.status.success(),
        "connector failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[cfg(unix)]
struct DaemonGuard {
    lock_path: PathBuf,
}

#[cfg(unix)]
impl DaemonGuard {
    fn pid(&self) -> Option<String> {
        std::fs::read_to_string(&self.lock_path)
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("pid=").map(str::to_owned))
    }
}

#[cfg(unix)]
impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let Some(pid) = self.pid() else {
            return;
        };
        let _ = Command::new("kill")
            .args(["-TERM", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if !Command::new("kill")
                .args(["-0", &pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        let _ = Command::new("kill")
            .args(["-KILL", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[test]
fn storage_smoke() {
    let directory = tempdir().unwrap();
    run_git(directory.path(), &["init", "-b", "main"]);
    run_git(
        directory.path(),
        &["config", "user.email", "rust@example.com"],
    );
    run_git(directory.path(), &["config", "user.name", "Rust Tests"]);
    std::fs::write(directory.path().join("a.py"), "one\ntwo\n").unwrap();
    run_git(directory.path(), &["add", "."]);
    run_git(directory.path(), &["commit", "-m", "base"]);
    let report = directory.path().join("coverage.lcov");
    std::fs::write(&report, "TN:\nSF:a.py\nDA:1,1\nDA:2,0\nend_of_record\n").unwrap();
    let store = CoverageStore::open(directory.path().join("coverage.duckdb"), config()).unwrap();
    store.ensure_project(directory.path()).unwrap();
    let no_baseline = store
        .register_worktree(directory.path(), "main", Some("before coverage"))
        .unwrap();
    let snapshot = store
        .ingest_report(
            &report,
            "lcov",
            Some(directory.path()),
            Some("main"),
            Some("head"),
            None,
            "unit",
        )
        .unwrap();
    assert_eq!(snapshot["total_lines"], 2);
    assert!(no_baseline["baseline_snapshot_id"].is_null());
    assert!(
        store
            .compare_worktree(
                no_baseline["id"].as_str().unwrap(),
                Some(snapshot["id"].as_str().unwrap()),
                100,
                100
            )
            .is_err()
    );
    assert_eq!(
        store
            .files(snapshot["id"].as_str().unwrap(), 100)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .lines(snapshot["id"].as_str().unwrap(), "a.py", 100)
            .unwrap()
            .len(),
        2
    );
    store
        .update_project_settings(ProjectSettingsPatch {
            compaction_interval_seconds: Some(3_600),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(store.project_settings().unwrap().compaction_after_days, 30);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let unreadable_path = directory.path().join("unreadable.py");
        std::fs::write(&unreadable_path, "hidden\n").unwrap();
        let mut permissions = std::fs::metadata(&unreadable_path).unwrap().permissions();
        permissions.set_mode(0o0);
        std::fs::set_permissions(&unreadable_path, permissions).unwrap();
        assert!(
            store
                .source_lines(snapshot["id"].as_str().unwrap(), "unreadable.py", 1, 1)
                .is_err()
        );
    }
    assert!(store.execute_run("missing-run").is_err());
    store.close().unwrap();
}

#[test]
fn standalone_stdio_mcp_smoke() {
    let directory = tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
        .args(["connect", "--repo", env!("CARGO_MANIFEST_DIR"), "--db"])
        .arg(directory.path().join("stdio.duckdb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/list\"}\n",
        "not-json\n"
    );
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stdio failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let responses = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "coverage-mcp");
    assert_eq!(
        responses[1]["result"]["tools"].as_array().unwrap().len(),
        11
    );
    assert!(responses[2]["result"]["resources"].is_array());
    assert_eq!(responses[3]["error"]["code"], -32000);
}

#[cfg(unix)]
#[test]
fn shared_daemon_stdio_connectors_route_multiple_repositories() {
    let directory = tempdir().unwrap();
    let repository_a = directory.path().join("repository-a");
    let repository_b = directory.path().join("repository-b");
    init_git_repository(&repository_a, "a");
    init_git_repository(&repository_b, "b");
    let common_db = directory.path().join("daemon").join("common.duckdb");
    let guard = DaemonGuard {
        lock_path: common_db.parent().unwrap().join("daemon.lock"),
    };
    let port = unused_loopback_port();

    let connector_a = spawn_shared_connector(&repository_a, &common_db, port);
    let connector_b = spawn_shared_connector(&repository_b, &common_db, port);
    let responses_a = connector_responses(connector_a.wait_with_output().unwrap());
    let responses_b = connector_responses(connector_b.wait_with_output().unwrap());

    for (responses, repository) in [
        (&responses_a, repository_a.as_path()),
        (&responses_b, repository_b.as_path()),
    ] {
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["result"]["serverInfo"]["name"], "coverage-mcp");
        assert_eq!(
            responses[1]["result"]["structuredContent"]["context"]["checkout_path"],
            repository
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
    }
    assert!(
        repository_a
            .join(".coverage-mcp")
            .join("coverage.duckdb")
            .exists()
    );
    assert!(
        repository_b
            .join(".coverage-mcp")
            .join("coverage.duckdb")
            .exists()
    );
    assert!(!common_db.parent().unwrap().join("projects").exists());
    let metadata = std::fs::read_to_string(&guard.lock_path).unwrap();
    assert_eq!(
        metadata
            .lines()
            .filter(|line| line.starts_with("pid="))
            .count(),
        1
    );
    let daemon_pid = guard.pid().unwrap();
    assert!(
        Command::new("kill")
            .args(["-0", &daemon_pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success()),
        "shared daemon exited while stdio clients disconnected"
    );
}
