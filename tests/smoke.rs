//! End-to-end smoke tests for the storage and native MCP transports.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use coverage_mcp::storage::ProjectSettingsPatch;
use coverage_mcp::{CoverageStore, ServerConfig};
use coverage_mcp::{
    http::DAEMON_HANDOFF_PATH,
    lock::{daemon_lock_path, held_daemon_owner},
};
use tempfile::tempdir;

fn config() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".to_owned(),
        port: 59471,
        default_repository_path: None,
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

fn connector_request<W: Write, R: BufRead>(
    stdin: &mut W,
    stdout: &mut R,
    request: serde_json::Value,
) -> serde_json::Value {
    writeln!(stdin, "{request}").unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

fn loopback_request(port: u16, method: &str, path: &str, body: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(
        format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn wait_for_health(port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(response) = loopback_request(port, "GET", "/health", "") {
            if response.contains("200 OK") {
                return response;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("daemon did not become healthy on port {port}");
}

fn serve_fixed_health(listener: TcpListener, body: String, requests: usize) {
    for _ in 0..requests {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1_024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).unwrap();
            assert!(read > 0, "client closed before completing HTTP headers");
            request.extend_from_slice(&chunk[..read]);
        }
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .unwrap();
    }
}

#[cfg(unix)]
struct DaemonGuard {
    lock_path: PathBuf,
    armed: bool,
}

#[cfg(unix)]
impl DaemonGuard {
    fn pid(&self) -> Option<String> {
        std::fs::read_to_string(&self.lock_path)
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("pid=").map(str::to_owned))
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
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
        .ensure_lineage_baseline(directory.path(), "main", Some("before coverage"))
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
fn cli_rejects_direct_database_modes() {
    let directory = tempdir().unwrap();
    for subcommand in ["connect", "serve"] {
        let help = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
            .args([subcommand, "--help"])
            .output()
            .unwrap();
        assert!(help.status.success());
        assert!(!String::from_utf8(help.stdout).unwrap().contains("--db"));

        let rejected = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
            .arg(subcommand)
            .arg("--db")
            .arg(directory.path().join("direct.duckdb"))
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(2));
        assert!(String::from_utf8(rejected.stderr).unwrap().contains("--db"));
    }
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
        armed: true,
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

#[cfg(unix)]
#[test]
fn existing_stdio_connector_recovers_a_crashed_daemon_and_stale_lock_file() {
    let directory = tempdir().unwrap();
    let repository = directory.path().join("repository");
    init_git_repository(&repository, "recovery");
    let common_db = directory.path().join("daemon").join("common.duckdb");
    let lock_path = daemon_lock_path(&common_db);
    let guard = DaemonGuard {
        lock_path: lock_path.clone(),
        armed: true,
    };
    let port = unused_loopback_port();
    let mut connector = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
        .args(["connect", "--repo"])
        .arg(&repository)
        .env("COVERAGE_MCP_HOST", "127.0.0.1")
        .env("COVERAGE_MCP_PORT", port.to_string())
        .env("COVERAGE_MCP_COMMON_DB", &common_db)
        .env("COVERAGE_MCP_RUN_CONCURRENCY", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = connector.stdin.take().unwrap();
    let mut stdout = BufReader::new(connector.stdout.take().unwrap());

    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n")
        .unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let initialize: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(initialize["result"]["serverInfo"]["name"], "coverage-mcp");

    let register = |id, name: &str, command: &str| {
        serde_json::json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"tools/call",
            "params":{
                "name":"register_test_command",
                "arguments":{
                    "name":name,
                    "command":command,
                    "cwd":repository,
                    "shell":"/bin/sh",
                    "human_approved":true,
                    "approved_by":"smoke-test",
                    "approval_note":"approved daemon-restart recovery fixture"
                }
            }
        })
    };
    let running_command = connector_request(
        &mut stdin,
        &mut stdout,
        register(
            2,
            "restart-running",
            "while [ ! -f release-running ]; do sleep 0.05; done",
        ),
    );
    let running_command_id = running_command["result"]["structuredContent"]["data"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let queued_command = connector_request(
        &mut stdin,
        &mut stdout,
        register(3, "restart-queued", "printf resumed"),
    );
    let queued_command_id = queued_command["result"]["structuredContent"]["data"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let submit = |id, command_ref: &str, key: &str| {
        serde_json::json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"tools/call",
            "params":{
                "name":"run_test",
                "arguments":{
                    "command_ref":command_ref,
                    "wait":false,
                    "idempotency_key":key
                }
            }
        })
    };
    let running_submission = connector_request(
        &mut stdin,
        &mut stdout,
        submit(4, &running_command_id, "restart-running"),
    );
    let running_id = running_submission["result"]["structuredContent"]["data"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let queued_submission = connector_request(
        &mut stdin,
        &mut stdout,
        submit(5, &queued_command_id, "restart-queued"),
    );
    let queued_id = queued_submission["result"]["structuredContent"]["data"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let run_state = |id, run_id: &str| {
        serde_json::json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"tools/call",
            "params":{"name":"run_review","arguments":{"run_id":run_id,"view":"status"}}
        })
    };
    let state_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let running = connector_request(&mut stdin, &mut stdout, run_state(6, &running_id));
        let queued = connector_request(&mut stdin, &mut stdout, run_state(7, &queued_id));
        if running["result"]["structuredContent"]["data"]["status"] == "running"
            && queued["result"]["structuredContent"]["data"]["status"] == "queued"
        {
            break;
        }
        assert!(
            Instant::now() < state_deadline,
            "managed runs did not reach running/queued restart fixture state: running={running}, queued={queued}"
        );
        thread::sleep(Duration::from_millis(50));
    }

    wait_for_health(port);
    let first_pid = guard.pid().expect("first daemon pid");
    let status = Command::new("kill")
        .args(["-KILL", &first_pid])
        .status()
        .unwrap();
    assert!(status.success());
    std::fs::write(repository.join("release-running"), "release\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let listener_gone = TcpStream::connect(("127.0.0.1", port)).is_err();
        if listener_gone && held_daemon_owner(&lock_path).unwrap().is_none() {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        held_daemon_owner(&lock_path).unwrap().is_none(),
        "crashed daemon lease remained held"
    );
    assert!(lock_path.exists(), "the stale metadata file should remain");

    line.clear();
    let tools = connector_request(
        &mut stdin,
        &mut stdout,
        serde_json::json!({"jsonrpc":"2.0","id":8,"method":"tools/list"}),
    );
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 7);
    wait_for_health(port);
    let second_pid = guard.pid().expect("replacement daemon pid");
    assert_ne!(first_pid, second_pid);

    let interrupted = connector_request(&mut stdin, &mut stdout, run_state(9, &running_id));
    assert_eq!(
        interrupted["result"]["structuredContent"]["data"]["status"],
        "interrupted"
    );
    assert_eq!(
        interrupted["result"]["structuredContent"]["data"]["terminal"],
        true
    );
    let resume_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let resumed = connector_request(&mut stdin, &mut stdout, run_state(10, &queued_id));
        if resumed["result"]["structuredContent"]["data"]["terminal"] == true {
            assert_eq!(
                resumed["result"]["structuredContent"]["data"]["status"],
                "passed"
            );
            break;
        }
        assert!(
            Instant::now() < resume_deadline,
            "queued run did not resume after daemon restart"
        );
        thread::sleep(Duration::from_secs(1));
    }

    drop(stdin);
    assert!(connector.wait().unwrap().success());
}

#[cfg(unix)]
#[test]
fn daemon_handoff_endpoint_closes_the_owned_process_and_releases_its_lease() {
    let directory = tempdir().unwrap();
    let common_db = directory.path().join("daemon").join("common.duckdb");
    let lock_path = daemon_lock_path(&common_db);
    let port = unused_loopback_port();
    let mut guard = DaemonGuard {
        lock_path: lock_path.clone(),
        armed: true,
    };
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .args(["--common-db"])
        .arg(&common_db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let health = wait_for_health(port);
    let owner = held_daemon_owner(&lock_path)
        .unwrap()
        .expect("held daemon owner");
    assert_eq!(owner.pid, daemon.id());
    assert!(health.contains(owner.instance_id.as_deref().expect("instance id")));
    assert!(!health.contains(owner.handoff_token.as_deref().expect("handoff token")));

    let rejected =
        loopback_request(port, "POST", DAEMON_HANDOFF_PATH, r#"{"token":"wrong"}"#).unwrap();
    assert!(rejected.contains("403 Forbidden"));
    assert!(daemon.try_wait().unwrap().is_none());

    let accepted = loopback_request(
        port,
        "POST",
        DAEMON_HANDOFF_PATH,
        &serde_json::json!({"token":owner.handoff_token}).to_string(),
    )
    .unwrap();
    assert!(accepted.contains("202 Accepted"));

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = daemon.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "daemon did not finish handoff");
        thread::sleep(Duration::from_millis(25));
    };
    assert!(status.success());
    assert!(held_daemon_owner(&lock_path).unwrap().is_none());
    guard.disarm();
}

#[test]
fn connector_refuses_to_replace_an_unlocked_health_lookalike() {
    let directory = tempdir().unwrap();
    let common_db = directory.path().join("common.duckdb");
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = serde_json::json!({
        "status":"ok",
        "version":"0.8.6",
        "schema_revision":9,
        "common_db_path":common_db,
        "daemon_path":"/tmp/not-an-owned-coverage-mcp"
    })
    .to_string();
    let server = thread::spawn(move || serve_fixed_health(listener, body, 2));

    let output = Command::new(env!("CARGO_BIN_EXE_coverage-mcp"))
        .args(["connect", "--repo", env!("CARGO_MANIFEST_DIR")])
        .env("COVERAGE_MCP_HOST", "127.0.0.1")
        .env("COVERAGE_MCP_PORT", port.to_string())
        .env("COVERAGE_MCP_COMMON_DB", &common_db)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    server.join().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("automatic recovery refused"));
    assert!(stderr.contains("does not hold the expected ownership lease"));
}
