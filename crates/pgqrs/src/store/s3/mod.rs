//! S3-backed store scaffolding.
//!
//! This module defines configuration and mode types used by the upcoming
//! S3-backed SQLite implementation.

pub mod client;
pub mod state;
pub mod sync;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::sqlite::SqliteStore;
use client::ObjectStoreClient;
use state::LocalDbState;
use std::sync::Arc;
use std::time::Duration;
use sync::{run_sync_loop, wake_channel, SyncCoordinator, SyncWakeSender};
use tokio::sync::{mpsc, Mutex};

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
    state: LocalDbState,
    sync: Arc<SyncCoordinator>,
    last_etag: Arc<Mutex<Option<String>>>,
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
        let state = LocalDbState::from_cache_dsn(&sqlite_cache_dsn, mode)?;
        state.ensure_files()?;
        let sqlite = SqliteStore::new(&state.write_dsn(), config).await?;
        Ok(Self {
            sqlite,
            state,
            sync: Arc::new(SyncCoordinator::new()),
            last_etag: Arc::new(Mutex::new(None)),
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

    pub fn state(&self) -> &LocalDbState {
        &self.state
    }

    /// Reserve a local write sequence for a mutating operation.
    pub fn reserve_write_sequence(&self) -> u64 {
        self.sync.next_sequence()
    }

    /// Mark a sequence as durable after a successful flush.
    pub fn mark_flushed(&self, seq: u64) {
        self.sync.mark_flushed(seq);
    }

    /// Wait until a sequence is durable.
    pub async fn wait_until_flushed(&self, seq: u64, timeout: Duration) -> Result<()> {
        self.sync.wait_until_flushed(seq, timeout).await
    }

    pub fn current_sequence(&self) -> u64 {
        self.sync.current_sequence()
    }

    pub fn last_flushed_sequence(&self) -> u64 {
        self.sync.last_flushed_sequence()
    }

    /// Flush local write DB to object storage using optimistic CAS.
    pub async fn flush_once<C: ObjectStoreClient>(&self, client: &C, key: &str) -> Result<()> {
        let payload = std::fs::read(self.state.write_path()).map_err(|e| Error::Internal {
            message: format!(
                "Failed to read write_db '{}': {}",
                self.state.write_path().display(),
                e
            ),
        })?;

        let expected = {
            let guard = self.last_etag.lock().await;
            guard.clone()
        };

        let new_etag = client
            .put_object_if_match(key, &payload, expected.as_deref())
            .await?;

        self.state.promote_write_to_read()?;

        {
            let mut guard = self.last_etag.lock().await;
            *guard = Some(new_etag);
        }

        self.mark_flushed(self.current_sequence());
        Ok(())
    }

    /// Create a wake channel for the background sync loop.
    pub fn create_wake_channel(&self) -> (SyncWakeSender, mpsc::Receiver<()>) {
        wake_channel(self.sync_config.max_pending_ops.max(1))
    }

    /// Run the background sync loop for this store.
    pub async fn run_sync_task<C: ObjectStoreClient>(
        &self,
        client: Arc<C>,
        key: String,
        wake_rx: mpsc::Receiver<()>,
    ) {
        let store = self.clone();
        let interval_ms = self.sync_config.flush_interval_ms.max(1);
        run_sync_loop(wake_rx, Duration::from_millis(interval_ms), move || {
            let store = store.clone();
            let client = client.clone();
            let key = key.clone();
            async move { store.flush_once(client.as_ref(), &key).await }
        })
        .await;
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
    use crate::error::Error;
    use crate::store::s3::client::{InMemoryObjectStore, ObjectStoreClient};
    use std::sync::Arc;

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
        assert_eq!(store.current_sequence(), 0);
        assert_eq!(store.last_flushed_sequence(), 0);
    }

    #[tokio::test]
    async fn s3_store_flush_sequence_tracking() {
        let config = Config::from_dsn("sqlite::memory:");
        let store = S3Store::open(
            "s3://bucket/queue.sqlite",
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");

        let seq = store.reserve_write_sequence();
        assert_eq!(seq, 1);
        assert_eq!(store.current_sequence(), 1);
        assert_eq!(store.last_flushed_sequence(), 0);

        store.mark_flushed(seq);
        store
            .wait_until_flushed(seq, std::time::Duration::from_millis(100))
            .await
            .expect("wait should complete");
        assert_eq!(store.last_flushed_sequence(), 1);
    }

    #[tokio::test]
    async fn flush_once_promotes_and_marks_flushed() {
        let config = Config::from_dsn("sqlite::memory:");
        let store = S3Store::open(
            "s3://bucket/flush_ok.sqlite",
            &config,
            DurabilityMode::Durable,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");

        std::fs::write(store.state().write_path(), b"hello-world").expect("write test db bytes");
        let seq = store.reserve_write_sequence();
        let client = InMemoryObjectStore::new();

        store
            .flush_once(&client, "bucket/flush_ok.sqlite")
            .await
            .expect("flush should succeed");

        store
            .wait_until_flushed(seq, std::time::Duration::from_millis(100))
            .await
            .expect("seq should be durable");

        let read_bytes = std::fs::read(store.state().read_path()).expect("read read_db");
        assert_eq!(read_bytes, b"hello-world");
        let obj = client
            .get_object("bucket/flush_ok.sqlite")
            .await
            .expect("object should exist");
        assert_eq!(obj.bytes, b"hello-world");
    }

    #[tokio::test]
    async fn flush_once_reports_conflict() {
        let config = Config::from_dsn("sqlite::memory:");
        let client = InMemoryObjectStore::new();

        let store_a = S3Store::open(
            "s3://bucket/conflict_a.sqlite",
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open a");
        std::fs::write(store_a.state().write_path(), b"v1").expect("write v1");
        store_a.reserve_write_sequence();
        store_a
            .flush_once(&client, "bucket/shared.sqlite")
            .await
            .expect("first flush");

        let store_b = S3Store::open(
            "s3://bucket/conflict_b.sqlite",
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open b");
        std::fs::write(store_b.state().write_path(), b"v2").expect("write v2");
        store_b.reserve_write_sequence();

        let err = store_b
            .flush_once(&client, "bucket/shared.sqlite")
            .await
            .expect_err("second store should conflict (stale/no etag)");
        assert!(matches!(err, Error::Conflict { .. }));
    }

    #[tokio::test]
    async fn run_sync_task_flushes_on_wake() {
        let config = Config::from_dsn("sqlite::memory:");
        let store = S3Store::open(
            "s3://bucket/sync_task.sqlite",
            &config,
            DurabilityMode::Local,
            S3SyncConfig {
                flush_interval_ms: 60_000,
                ..S3SyncConfig::default()
            },
        )
        .await
        .expect("open should succeed");

        std::fs::write(store.state().write_path(), b"sync-wake").expect("write test bytes");
        let seq = store.reserve_write_sequence();
        let client = Arc::new(InMemoryObjectStore::new());
        let (wake, wake_rx) = store.create_wake_channel();

        let task_store = store.clone();
        let task_client = client.clone();
        let handle = tokio::spawn(async move {
            task_store
                .run_sync_task(task_client, "bucket/sync_task.sqlite".to_string(), wake_rx)
                .await;
        });

        wake.wake();
        store
            .wait_until_flushed(seq, std::time::Duration::from_millis(500))
            .await
            .expect("wake-triggered flush should complete");

        let obj = client
            .get_object("bucket/sync_task.sqlite")
            .await
            .expect("object should exist after wake flush");
        assert_eq!(obj.bytes, b"sync-wake");

        drop(wake);
        handle.await.expect("sync loop task should exit");
    }
}
