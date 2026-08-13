//! Rust implementation of the Coverage MCP service.
//!
//! This crate owns the production daemon, storage, parsers, REST surface, MCP
//! transports, dashboard, and background maintenance lifecycle.

#![warn(missing_docs)]
// Unit tests intentionally use concise assertions. Production targets are
// still checked with unwrap/expect denied by the package lint policy.
#![cfg_attr(
    test,
    allow(clippy::expect_used, clippy::unwrap_in_result, clippy::unwrap_used)
)]

use sha2::{Digest, Sha256};

pub mod compaction;
/// Runtime configuration and defaults.
pub mod config;
/// Application error types.
pub mod error;
/// Git identity and ancestry helpers.
pub mod git;
/// Hyper-based REST and MCP transport.
pub mod http;
/// Process and database lease management.
pub mod lock;
/// Stateless MCP tool contract.
pub mod mcp;
/// Coverage domain models.
pub mod models;
/// Coverage report parsers.
pub mod parser;
/// Bounded DuckDB pooling and query interruption.
pub mod pool;
/// Transport-neutral service orchestration.
pub mod service;
/// DuckDB storage and execution engine.
pub mod storage;

pub use config::ServerConfig;
pub use error::{AppError, AppResult};
pub use http::CoverageServer;
pub use storage::{CoverageStore, ProjectSettings, ProjectSettingsPatch};

/// Returns the stable short identifier used to address a canonical project.
pub(crate) fn stable_project_id(repo_key: &str) -> String {
    let digest = Sha256::digest(repo_key.as_bytes());
    hex_prefix(&digest, 12)
}

/// Encodes up to `max_bytes` of a digest as lowercase hexadecimal.
pub(crate) fn hex_prefix(bytes: &[u8], max_bytes: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().min(max_bytes) * 2);
    for &byte in bytes.iter().take(max_bytes) {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

// The migration suite remains an integration test target, but compiling the
// same cases inside the library is intentional: Rust does not attribute
// coverage counters from an uninstrumented dependency rlib to the integration
// executable. This keeps the parity suite runnable in both modes and makes
// the measured library coverage complete and reproducible.
#[cfg(test)]
extern crate self as coverage_mcp;

#[cfg(test)]
#[path = "../tests/rust_migration.rs"]
mod rust_migration;

/// Public Coverage MCP contract revision for the duplicate-coverage query.
pub const SCHEMA_REVISION: u32 = 8;

/// Rust daemon version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
