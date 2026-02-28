//! S3-backed store scaffolding.
//!
//! This module defines configuration and mode types used by the upcoming
//! S3-backed SQLite implementation.

pub mod client;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::sqlite::SqliteStore;

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

/// Initial S3-backed store scaffold.
///
/// Current implementation delegates to a local SQLite cache DB derived from the S3 DSN.
#[derive(Debug, Clone)]
pub struct S3Store {
    sqlite: SqliteStore,
    mode: DurabilityMode,
    sync_config: S3SyncConfig,
    source_dsn: String,
    sqlite_cache_dsn: String,
}

impl S3Store {
    /// Open an S3-backed store against a local SQLite cache.
    pub async fn open(
        s3_dsn: &str,
        config: &Config,
        mode: DurabilityMode,
        sync_config: S3SyncConfig,
    ) -> Result<Self> {
        let sqlite_cache_dsn = sqlite_cache_dsn_from_s3_dsn(s3_dsn)?;
        let sqlite = SqliteStore::new(&sqlite_cache_dsn, config).await?;
        Ok(Self {
            sqlite,
            mode,
            sync_config,
            source_dsn: s3_dsn.to_string(),
            sqlite_cache_dsn,
        })
    }

    /// Underlying SQLite cache store used for reads/writes.
    pub fn sqlite(&self) -> &SqliteStore {
        &self.sqlite
    }

    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    pub fn sync_config(&self) -> &S3SyncConfig {
        &self.sync_config
    }

    pub fn source_dsn(&self) -> &str {
        &self.source_dsn
    }

    pub fn sqlite_cache_dsn(&self) -> &str {
        &self.sqlite_cache_dsn
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
    use super::{sqlite_cache_dsn_from_s3_dsn, DurabilityMode, S3Store, S3SyncConfig};
    use crate::config::Config;

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

    #[tokio::test]
    async fn s3_store_open_uses_sqlite_cache() {
        let config = Config::from_dsn("sqlite::memory:");
        let store = S3Store::open(
            "s3://bucket/queue.sqlite",
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");

        assert_eq!(store.mode(), DurabilityMode::Local);
        assert_eq!(store.source_dsn(), "s3://bucket/queue.sqlite");
        assert!(store.sqlite_cache_dsn().starts_with("sqlite://"));
    }
}
