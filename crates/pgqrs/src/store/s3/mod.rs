//! S3-backed store scaffolding.
//!
//! This module defines configuration and mode types used by the upcoming
//! S3-backed SQLite implementation.

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::error::{Error, Result};

/// Durability behavior for S3-backed stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Reads and writes use local SQLite state. Flushes happen asynchronously.
    #[default]
    Local,
    /// Writes wait for successful object-store flush before acknowledging.
    Durable,
}

/// Sync task runtime configuration for S3-backed SQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3SyncConfig {
    /// Periodic flush interval in milliseconds.
    pub flush_interval_ms: u64,
    /// Threshold for pending local operations before forcing a flush.
    pub max_pending_ops: usize,
    /// Maximum retry backoff in milliseconds for transient sync errors.
    pub max_backoff_ms: u64,
}

impl Default for S3SyncConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: 1000,
            max_pending_ops: 100,
            max_backoff_ms: 30_000,
        }
    }
}

/// Map an `s3://...` DSN to a deterministic local SQLite cache DSN.
pub fn sqlite_cache_dsn_from_s3_dsn(dsn: &str) -> Result<String> {
    let key = dsn
        .strip_prefix("s3://")
        .or_else(|| dsn.strip_prefix("s3:"))
        .ok_or_else(|| Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Invalid S3 DSN format: {}", dsn),
        })?;

    if key.trim().is_empty() {
        return Err(Error::InvalidConfig {
            field: "dsn".to_string(),
            message: "S3 DSN cannot be empty".to_string(),
        });
    }

    let base_dir = std::env::var("PGQRS_S3_LOCAL_CACHE_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("CARGO_TARGET_TMPDIR").map(PathBuf::from))
        .unwrap_or_else(|_| std::env::temp_dir().join("pgqrs_s3_cache"));
    std::fs::create_dir_all(&base_dir).map_err(|e| Error::InvalidConfig {
        field: "PGQRS_S3_LOCAL_CACHE_DIR".to_string(),
        message: format!("Failed to create S3 cache directory: {}", e),
    })?;

    let mut hasher = DefaultHasher::new();
    dsn.hash(&mut hasher);
    let hash = hasher.finish();

    let path = base_dir.join(format!("s3_cache_{hash:016x}.db"));
    Ok(format!("sqlite://{}?mode=rwc", path.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use super::sqlite_cache_dsn_from_s3_dsn;

    #[test]
    fn sqlite_cache_mapping_is_deterministic() {
        let a = sqlite_cache_dsn_from_s3_dsn("s3://bucket/queue.db").unwrap();
        let b = sqlite_cache_dsn_from_s3_dsn("s3://bucket/queue.db").unwrap();
        assert_eq!(a, b);
        assert!(a.starts_with("sqlite://"));
    }

    #[test]
    fn sqlite_cache_mapping_rejects_invalid_input() {
        let err = sqlite_cache_dsn_from_s3_dsn("sqlite://foo").unwrap_err();
        assert!(err.to_string().contains("Invalid S3 DSN format"));
    }
}
