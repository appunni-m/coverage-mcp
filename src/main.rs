//! Coverage MCP command-line entry point.

#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use coverage_mcp::config::default_db_path;
use coverage_mcp::error::AppResult;
use coverage_mcp::mcp;
use coverage_mcp::service::{CoverageService, RequestContext};
use coverage_mcp::{CoverageServer, CoverageStore, ServerConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

#[derive(Debug, Parser)]
#[command(
    name = "coverage-mcp",
    version,
    about = "Local-first coverage MCP server"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the shared HTTP daemon.
    Serve {
        /// Bind host, defaulting to COVERAGE_MCP_HOST or 127.0.0.1.
        #[arg(long, env = "COVERAGE_MCP_HOST")]
        host: Option<String>,
        /// Bind port, defaulting to COVERAGE_MCP_PORT or 59471.
        #[arg(long, env = "COVERAGE_MCP_PORT")]
        port: Option<u16>,
        /// Use one standalone repository database instead of lazy project routing.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Override the daemon-wide common registry database.
        #[arg(long, env = "COVERAGE_MCP_COMMON_DB")]
        common_db: Option<PathBuf>,
    },
    /// Run the MCP JSON-RPC stdio transport in this process.
    #[command(alias = "stdio")]
    Connect {
        /// Repository checkout used for project selection, defaulting to `.`.
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        /// Optional repository database; otherwise `.coverage-mcp/coverage.duckdb` is used.
        #[arg(long, env = "COVERAGE_MCP_DB")]
        db: Option<PathBuf>,
    },
    /// Run one compaction pass for a project and exit.
    Compact {
        /// Repository checkout or project path.
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        /// Override the number of days before a coverage event is compacted.
        #[arg(long)]
        older_than_days: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve {
        host: None,
        port: None,
        db: None,
        common_db: None,
    }) {
        Command::Serve {
            host,
            port,
            db,
            common_db,
        } => {
            let config = ServerConfig::from_environment(host, port, db, common_db)?;
            CoverageServer::new(config)?.run().await?;
        }
        Command::Connect { repo, db } => run_stdio(repo, db).await?,
        Command::Compact {
            repo,
            older_than_days,
        } => {
            let config = ServerConfig::for_repository(repo.clone())?;
            let db_path = standalone_db_path(&config)?;
            let store = coverage_mcp::CoverageStore::open(db_path, config.clone())?;
            store.ensure_project(&repo)?;
            if let Some(days) = older_than_days {
                store.update_project_settings(coverage_mcp::storage::ProjectSettingsPatch {
                    compaction_after_days: Some(days),
                    ..Default::default()
                })?;
            }
            let result = store.compact_now()?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            store.close()?;
        }
    }
    Ok(())
}

async fn run_stdio(repo: PathBuf, db: Option<PathBuf>) -> AppResult<()> {
    let mut config = ServerConfig::for_repository(repo.clone())?;
    if let Some(db) = db {
        config.db_path = Some(db);
    }
    let repo_path = config
        .db_path
        .clone()
        .unwrap_or_else(|| default_db_path(&repo));
    let store = CoverageStore::open(repo_path, config)?;
    let git = store.ensure_project(&repo)?;
    let service = CoverageService::new(
        store.clone(),
        RequestContext {
            repo_key: git.repo_key,
            checkout_path: git.repo_path,
            suite: None,
        },
    );

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let stdout = BufWriter::new(tokio::io::stdout());
    let mut stdout = stdout;
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => mcp::dispatch_json_rpc(Some(&service), &request),
            Err(error) => Some(json_rpc_error(None, coverage_mcp::AppError::Json(error))),
        };
        if let Some(response) = response {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    store.close()?;
    Ok(())
}

fn json_rpc_error(id: Option<Value>, error: coverage_mcp::AppError) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id.unwrap_or(Value::Null),
        "error":{"code":-32000,"message":error.to_string()}
    })
}

fn standalone_db_path(config: &ServerConfig) -> AppResult<PathBuf> {
    config.db_path.clone().ok_or_else(|| {
        coverage_mcp::AppError::Validation(
            "standalone compaction requires a repository database path".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_db_path_enforces_repository_mode() {
        let mut config = ServerConfig::from_environment(
            None,
            None,
            Some(PathBuf::from("coverage.duckdb")),
            None,
        )
        .expect("config");
        assert_eq!(
            standalone_db_path(&config).unwrap(),
            PathBuf::from("coverage.duckdb")
        );
        config.db_path = None;
        assert!(standalone_db_path(&config).is_err());
    }
}
