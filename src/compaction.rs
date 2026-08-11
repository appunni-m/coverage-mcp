//! Background coverage-detail compaction.
//!
//! The storage implementation owns the transaction because compaction must
//! share the same DuckDB lock as ingest and queries. This module contains the
//! public result and policy types used by the HTTP and CLI layers.

use serde::{Deserialize, Serialize};

/// Per-project policy for compressing older coverage event detail.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompactionPolicy {
    /// Whether background compaction is enabled.
    pub enabled: bool,
    /// Age threshold in days before an event is eligible.
    pub older_than_days: u32,
    /// Background maintenance interval in seconds.
    pub interval_seconds: u64,
    /// Maximum events handled in one pass.
    pub batch_size: u32,
}

/// Result of one compaction pass.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CompactionResult {
    /// Project key handled by the pass.
    pub repo_key: String,
    /// Number of snapshots moved to compressed detail storage.
    pub compacted_snapshots: u64,
    /// Bytes represented by the uncompressed detail rows.
    pub bytes_before: u64,
    /// Bytes occupied by compressed payloads.
    pub bytes_after: u64,
    /// Whether a DuckDB checkpoint was requested.
    pub checkpointed: bool,
    /// Timestamp of the pass.
    pub completed_at: String,
    /// Stable status string for project context and health.
    pub status: String,
}
