//! Deterministic user-question evaluations for the two-query coverage contract.
use coverage_mcp::{AppResult, mcp, query};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf, time::Instant};

fn main() {
    if let Err(error) = run() {
        eprintln!("mcp-evals: {error}");
        std::process::exit(1);
    }
}
fn run() -> AppResult<()> {
    let mut args = env::args().skip(1);
    let report = match args.next().as_deref() {
        None => PathBuf::from("target/evals/mcp-eval-report.json"),
        Some("--report") => PathBuf::from(args.next().ok_or_else(|| {
            coverage_mcp::AppError::Validation("--report requires a path".into())
        })?),
        _ => {
            return Err(coverage_mcp::AppError::Validation(
                "usage: mcp-evals [--report PATH]".into(),
            ));
        }
    };
    if args.next().is_some() {
        return Err(coverage_mcp::AppError::Validation(
            "unexpected argument".into(),
        ));
    }
    let fixture =
        Fixture(env::temp_dir().join(format!("coverage-query-eval-{}", uuid::Uuid::new_v4())));
    fs::create_dir_all(&fixture.0)?;
    fs::write(fixture.0.join("a.rs"), "fn parse() {}\n")?;
    for (name, hits) in [
        ("base", [1, 0, 1]),
        ("candidate", [1, 1, 0]),
        ("identical", [1, 0, 1]),
        ("failed", [1, 1, 1]),
    ] {
        let covered = hits.iter().filter(|n| **n > 0).count();
        let document = json!({"data":[{"functions":[{"name":"parse","count":1,"filenames":["a.rs"],"regions":[[1,1,1,9,hits[0],0,0,0],[2,1,2,9,hits[1],0,0,0],[3,1,3,9,hits[2],0,0,0]]}],"files":[{"filename":"a.rs","segments":[[1,1,hits[0],true,true,false],[2,1,hits[1],true,true,false],[3,1,hits[2],true,true,false],[4,1,0,false,false,false]],"summary":{"regions":{"count":3,"covered":covered},"lines":{"count":3,"covered":covered}}}]}]});
        let bytes = document.to_string();
        fs::write(fixture.0.join(format!("{name}.json")), &bytes)?;
        let sha = |bytes: &[u8]| {
            const HEX: &[u8] = b"0123456789abcdef";
            let mut output = String::new();
            for byte in Sha256::digest(bytes) {
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 15) as usize] as char);
            }
            output
        };
        let receipt = json!({"report_sha256":sha(bytes.as_bytes()),"source_hashes":{"a.rs":sha(b"fn parse() {}\n")},"build_id":"same-build","scope":"full","test_status":if name=="failed"{"failed"}else{"passed"}});
        fs::write(
            fixture.0.join(format!("{name}.json.context.json")),
            receipt.to_string(),
        )?;
    }
    fs::copy(
        fixture.0.join("candidate.json"),
        fixture.0.join("unverified.json"),
    )?;
    let corpus: Value = serde_json::from_str(include_str!("../../evals/cases.json"))?;
    let mut checks = Vec::new();
    let started = Instant::now();
    for case in corpus["cases"].as_array().into_iter().flatten() {
        let name = case["tool"].as_str().unwrap_or_default();
        let result = query::read_reports(&fixture.0, name, &case["arguments"]);
        let (passed, bytes, detail) = match result {
            Ok(response) => {
                let matched = case["expect"].as_object().is_some_and(|expected| {
                    expected
                        .iter()
                        .all(|(pointer, value)| response.pointer(pointer) == Some(value))
                });
                let bytes = response.to_string().len();
                (
                    matched && bytes <= 12_288,
                    bytes,
                    response["data"]["status"].clone(),
                )
            }
            Err(error) => (case["error"] == true, 0, json!(error.to_string())),
        };
        checks.push(json!({"id":case["id"],"passed":passed,"visible_bytes":bytes,"result":detail}));
    }
    let contract = mcp::tools_list();
    checks.push(json!({"id":"two-read-only-tools","passed":contract.as_array().is_some_and(|tools|tools.len()==2 && tools.iter().all(|tool|tool["annotations"]["readOnlyHint"]==true)),"visible_bytes":contract.to_string().len()}));
    let passed = checks
        .iter()
        .filter(|check| check["passed"] == true)
        .count();
    let output = json!({"schema":"coverage-mcp/eval-report@2","version":coverage_mcp::VERSION,"contract":mcp::public_contract_metadata(),"summary":{"passed":passed,"checks":checks.len(),"elapsed_ms":started.elapsed().as_millis()},"checks":checks,"measurement_note":"Visible serialized bytes are a response-size proxy, not billed tokens. These are deterministic outcome checks, not a live-model A/B trial."});
    if let Some(parent) = report.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(&report, serde_json::to_string_pretty(&output)?)?;
    println!(
        "{passed}/{} checks passed; {}",
        checks.len(),
        report.display()
    );
    if passed != checks.len() {
        return Err(coverage_mcp::AppError::Runtime(
            "coverage outcome evaluation failed".into(),
        ));
    }
    Ok(())
}
struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
