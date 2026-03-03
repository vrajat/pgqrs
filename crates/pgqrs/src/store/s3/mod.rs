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
#[cfg(test)]
use client::InMemoryObjectStore;
use client::{build_aws_s3_client, AwsS3ClientConfig, AwsS3ObjectStore, ObjectStoreClient};
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
    Local,
    /// Writes wait for successful object-store flush before acknowledging.
    #[default]
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
#[derive(Clone)]
pub struct S3Store {
    sqlite: SqliteStore,
    state: LocalDbState,
    sync: Arc<SyncCoordinator>,
    last_etag: Arc<Mutex<Option<String>>>,
    object_key: String,
    wake_tx: SyncWakeSender,
    durable_wait_timeout: Duration,
    mode: DurabilityMode,
    sync_config: S3SyncConfig,
    source_dsn: String,
    sqlite_cache_dsn: String,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("mode", &self.mode)
            .field("source_dsn", &self.source_dsn)
            .field("sqlite_cache_dsn", &self.sqlite_cache_dsn)
            .field("object_key", &self.object_key)
            .finish()
    }
}

impl S3Store {
    /// Open an S3-backed store against a local SQLite cache.
    pub async fn open(
        s3_dsn: &str,
        config: &Config,
        mode: DurabilityMode,
        sync_config: S3SyncConfig,
    ) -> Result<Self> {
        let (bucket, object_key) = parse_s3_bucket_and_key(s3_dsn)?;
        let sqlite_cache_dsn = sqlite_cache_dsn_from_s3_dsn(s3_dsn)?;
        let state = LocalDbState::from_cache_dsn(&sqlite_cache_dsn, mode)?;
        state.ensure_files()?;
        let object_store = build_object_store_from_env(&bucket).await?;
        let last_etag = Arc::new(Mutex::new(None));

        let sqlite = SqliteStore::new(&state.write_dsn(), config).await?;
        let sync = Arc::new(SyncCoordinator::new());
        let (wake_tx, wake_rx) = wake_channel(sync_config.max_pending_ops.max(1));
        let durable_wait_timeout = Duration::from_secs(config.connection_timeout_seconds.max(1));

        let store = Self {
            sqlite,
            state,
            sync,
            last_etag,
            object_key: object_key.clone(),
            wake_tx,
            durable_wait_timeout,
            mode,
            sync_config,
            source_dsn: s3_dsn.to_string(),
            sqlite_cache_dsn,
        };

        let task_state = store.state.clone();
        let task_sync = store.sync.clone();
        let task_last_etag = store.last_etag.clone();
        let interval_ms = store.sync_config.flush_interval_ms.max(1);
        let max_backoff = Duration::from_millis(store.sync_config.max_backoff_ms.max(1));
        tokio::spawn(async move {
            run_sync_loop(
                wake_rx,
                Duration::from_millis(interval_ms),
                max_backoff,
                move || {
                    let client = object_store.clone();
                    let key = object_key.clone();
                    let state = task_state.clone();
                    let sync = task_sync.clone();
                    let last_etag = task_last_etag.clone();
                    async move {
                        flush_once_inner(&state, sync.as_ref(), &last_etag, client.as_ref(), &key)
                            .await
                    }
                },
            )
            .await;
        });

        Ok(store)
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
    pub async fn flush_once<C: ObjectStoreClient + ?Sized>(
        &self,
        client: &C,
        key: &str,
    ) -> Result<()> {
        flush_once_inner(
            &self.state,
            self.sync.as_ref(),
            &self.last_etag,
            client,
            key,
        )
        .await
    }

    /// Create a wake channel for the background sync loop.
    pub fn create_wake_channel(&self) -> (SyncWakeSender, mpsc::Receiver<()>) {
        wake_channel(self.sync_config.max_pending_ops.max(1))
    }

    /// Run the background sync loop for this store.
    pub async fn run_sync_task<C: ObjectStoreClient + ?Sized>(
        &self,
        client: Arc<C>,
        key: String,
        wake_rx: mpsc::Receiver<()>,
    ) {
        let state = self.state.clone();
        let sync = self.sync.clone();
        let last_etag = self.last_etag.clone();
        let interval_ms = self.sync_config.flush_interval_ms.max(1);
        let max_backoff = Duration::from_millis(self.sync_config.max_backoff_ms.max(1));
        run_sync_loop(
            wake_rx,
            Duration::from_millis(interval_ms),
            max_backoff,
            move || {
                let client = client.clone();
                let key = key.clone();
                let state = state.clone();
                let sync = sync.clone();
                let last_etag = last_etag.clone();
                async move {
                    flush_once_inner(&state, sync.as_ref(), &last_etag, client.as_ref(), &key).await
                }
            },
        )
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

    /// Notify S3Store that a local mutation committed.
    ///
    /// In durable mode this waits until the flush sequence is committed to object storage.
    pub async fn record_mutation_and_maybe_wait(&self) -> Result<()> {
        let seq = self.reserve_write_sequence();
        self.wake_tx.wake();
        if self.mode == DurabilityMode::Durable {
            self.wait_until_flushed(seq, self.durable_wait_timeout)
                .await?;
        }
        Ok(())
    }

    /// Initialize local state from remote object if it exists.
    ///
    /// Returns `Ok(true)` when remote state was loaded and `Ok(false)` when object was missing.
    pub async fn bootstrap_from_remote<C: ObjectStoreClient + ?Sized>(
        &self,
        client: &C,
        key: &str,
    ) -> Result<bool> {
        match client.get_object(key).await {
            Ok(remote) => {
                self.state.restore_from_remote_bytes(&remote.bytes)?;
                let mut guard = self.last_etag.lock().await;
                *guard = remote.etag;
                Ok(true)
            }
            Err(Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

async fn flush_once_inner<C: ObjectStoreClient + ?Sized>(
    state: &LocalDbState,
    sync: &SyncCoordinator,
    last_etag: &Mutex<Option<String>>,
    client: &C,
    key: &str,
) -> Result<()> {
    let payload = std::fs::read(state.write_path()).map_err(|e| Error::Internal {
        message: format!(
            "Failed to read write_db '{}': {}",
            state.write_path().display(),
            e
        ),
    })?;

    let expected = {
        let guard = last_etag.lock().await;
        guard.clone()
    };

    let new_etag = match client
        .put_object_if_match(key, &payload, expected.as_deref())
        .await
    {
        Ok(etag) => etag,
        Err(Error::Conflict { message }) => {
            sync.mark_conflict(sync.current_sequence());
            // Fast-fail callers, but first reset local state to latest remote baseline.
            let _ = recover_from_remote_inner(state, last_etag, client, key).await;
            return Err(Error::Conflict { message });
        }
        Err(e) => return Err(e),
    };

    state.promote_write_to_read()?;

    {
        let mut guard = last_etag.lock().await;
        *guard = Some(new_etag);
    }

    sync.mark_flushed(sync.current_sequence());
    Ok(())
}

async fn recover_from_remote_inner<C: ObjectStoreClient + ?Sized>(
    state: &LocalDbState,
    last_etag: &Mutex<Option<String>>,
    client: &C,
    key: &str,
) -> Result<()> {
    let remote = client.get_object(key).await?;
    state.restore_from_remote_bytes(&remote.bytes)?;
    let mut guard = last_etag.lock().await;
    *guard = remote.etag;
    Ok(())
}

fn parse_s3_bucket_and_key(dsn: &str) -> Result<(String, String)> {
    let full = dsn
        .strip_prefix("s3://")
        .or_else(|| dsn.strip_prefix("s3:"))
        .ok_or_else(|| Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Invalid S3 DSN format: {}", dsn),
        })?;

    let mut parts = full.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().trim();
    let key = parts.next().unwrap_or_default().trim();

    if bucket.is_empty() {
        return Err(Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("S3 DSN missing bucket: {}", dsn),
        });
    }
    if key.is_empty() {
        return Err(Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("S3 DSN missing object key: {}", dsn),
        });
    }

    Ok((bucket.to_string(), key.to_string()))
}

async fn build_object_store_from_env(bucket: &str) -> Result<Arc<dyn ObjectStoreClient>> {
    let endpoint = std::env::var("PGQRS_S3_ENDPOINT").ok();
    if endpoint.as_ref().is_none_or(|v| v.trim().is_empty()) {
        #[cfg(test)]
        {
            return Ok(Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStoreClient>);
        }
    }
    let region = required_non_empty_env("PGQRS_S3_REGION")?;
    let endpoint = required_non_empty_env("PGQRS_S3_ENDPOINT")?;
    validate_s3_auth_env(&endpoint)?;

    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let client = build_aws_s3_client(AwsS3ClientConfig {
        region,
        endpoint: Some(endpoint),
        access_key,
        secret_key,
        force_path_style: true,
        credentials_provider_name: "pgqrs-s3",
    })
    .await;
    Ok(Arc::new(AwsS3ObjectStore::new(client, bucket.to_string())) as Arc<dyn ObjectStoreClient>)
}

fn required_non_empty_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::InvalidConfig {
            field: name.to_string(),
            message: format!("S3 backend requires {} to be set", name),
        })
}

fn validate_s3_auth_env(endpoint: &str) -> Result<()> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
    let access_present = access_key.as_ref().is_some_and(|v| !v.trim().is_empty());
    let secret_present = secret_key.as_ref().is_some_and(|v| !v.trim().is_empty());
    if access_present ^ secret_present {
        return Err(Error::InvalidConfig {
            field: "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY".to_string(),
            message: "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither".to_string(),
        });
    }

    // For LocalStack/custom local endpoints, force explicit static creds so tests do not silently
    // fall back to non-deterministic provider-chain behavior.
    let endpoint_lc = endpoint.to_ascii_lowercase();
    let is_local_endpoint = endpoint_lc.contains("localhost")
        || endpoint_lc.contains("127.0.0.1")
        || endpoint_lc.contains("localstack");
    if is_local_endpoint && !(access_present && secret_present) {
        return Err(Error::InvalidConfig {
            field: "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY".to_string(),
            message: format!(
                "Local S3 endpoint '{}' requires explicit AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
                endpoint
            ),
        });
    }

    Ok(())
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
    use uuid::Uuid;

    fn unique_s3_dsn(prefix: &str) -> String {
        format!("s3://bucket/{}_{}.sqlite", prefix, Uuid::new_v4())
    }

    fn unique_object_key(prefix: &str) -> String {
        format!("bucket/{}_{}.sqlite", prefix, Uuid::new_v4())
    }

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
        let s3_dsn = unique_s3_dsn("queue");
        let store = S3Store::open(
            &s3_dsn,
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");

        assert_eq!(store.mode(), DurabilityMode::Local);
        assert_eq!(store.source_dsn(), s3_dsn);
        assert!(store.sqlite_cache_dsn().starts_with("sqlite://"));
        assert_eq!(store.current_sequence(), 0);
        assert_eq!(store.last_flushed_sequence(), 0);
    }

    #[tokio::test]
    async fn s3_store_flush_sequence_tracking() {
        let config = Config::from_dsn("sqlite::memory:");
        let s3_dsn = unique_s3_dsn("flush_sequence");
        let store = S3Store::open(
            &s3_dsn,
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
        let s3_dsn = unique_s3_dsn("flush_ok");
        let object_key = unique_object_key("flush_ok");
        let store = S3Store::open(
            &s3_dsn,
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
            .flush_once(&client, &object_key)
            .await
            .expect("flush should succeed");

        store
            .wait_until_flushed(seq, std::time::Duration::from_millis(100))
            .await
            .expect("seq should be durable");

        let read_bytes = std::fs::read(store.state().read_path()).expect("read read_db");
        assert_eq!(read_bytes, b"hello-world");
        let obj = client
            .get_object(&object_key)
            .await
            .expect("object should exist");
        assert_eq!(obj.bytes, b"hello-world");
    }

    #[tokio::test]
    async fn flush_once_reports_conflict() {
        let config = Config::from_dsn("sqlite::memory:");
        let client = InMemoryObjectStore::new();
        let s3_dsn_a = unique_s3_dsn("conflict_a");
        let s3_dsn_b = unique_s3_dsn("conflict_b");
        let shared_key = unique_object_key("shared_conflict");

        let store_a = S3Store::open(
            &s3_dsn_a,
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open a");
        std::fs::write(store_a.state().write_path(), b"v1").expect("write v1");
        store_a.reserve_write_sequence();
        store_a
            .flush_once(&client, &shared_key)
            .await
            .expect("first flush");

        let store_b = S3Store::open(
            &s3_dsn_b,
            &config,
            DurabilityMode::Local,
            S3SyncConfig::default(),
        )
        .await
        .expect("open b");
        std::fs::write(store_b.state().write_path(), b"v2").expect("write v2");
        let seq = store_b.reserve_write_sequence();

        let err = store_b
            .flush_once(&client, &shared_key)
            .await
            .expect_err("second store should conflict (stale/no etag)");
        assert!(matches!(err, Error::Conflict { .. }));
        let wait_err = store_b
            .wait_until_flushed(seq, std::time::Duration::from_millis(100))
            .await
            .expect_err("pending waiters should fail-fast after conflict");
        assert!(matches!(wait_err, Error::Conflict { .. }));

        // store_b should have been reset to remote state from store_a
        let write_bytes =
            std::fs::read(store_b.state().write_path()).expect("read recovered write");
        assert_eq!(write_bytes, b"v1");

        // retry from refreshed baseline should succeed
        std::fs::write(store_b.state().write_path(), b"v2").expect("rewrite v2");
        store_b.reserve_write_sequence();
        store_b
            .flush_once(&client, &shared_key)
            .await
            .expect("retry flush should succeed after recovery");
    }

    #[tokio::test]
    async fn run_sync_task_flushes_on_wake() {
        let config = Config::from_dsn("sqlite::memory:");
        let s3_dsn = unique_s3_dsn("sync_task");
        let object_key = unique_object_key("sync_task");
        let store = S3Store::open(
            &s3_dsn,
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
        let task_key = object_key.clone();

        let task_store = store.clone();
        let task_client = client.clone();
        let handle = tokio::spawn(async move {
            task_store
                .run_sync_task(task_client, task_key, wake_rx)
                .await;
        });

        wake.wake();
        store
            .wait_until_flushed(seq, std::time::Duration::from_millis(500))
            .await
            .expect("wake-triggered flush should complete");

        let obj = client
            .get_object(&object_key)
            .await
            .expect("object should exist after wake flush");
        assert_eq!(obj.bytes, b"sync-wake");

        drop(wake);
        handle.await.expect("sync loop task should exit");
    }

    #[tokio::test]
    async fn bootstrap_from_remote_loads_existing_state_and_etag() {
        let config = Config::from_dsn("sqlite::memory:");
        let s3_dsn = unique_s3_dsn("bootstrap_existing");
        let object_key = unique_object_key("bootstrap_existing");
        let store = S3Store::open(
            &s3_dsn,
            &config,
            DurabilityMode::Durable,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");
        let client = InMemoryObjectStore::new();
        client
            .put_object_if_match(&object_key, b"remote-v1", None)
            .await
            .expect("seed remote object");

        let loaded = store
            .bootstrap_from_remote(&client, &object_key)
            .await
            .expect("bootstrap should succeed");
        assert!(loaded);
        assert_eq!(
            std::fs::read(store.state().write_path()).unwrap(),
            b"remote-v1"
        );
        assert_eq!(
            std::fs::read(store.state().read_path()).unwrap(),
            b"remote-v1"
        );

        // Verify ETag baseline was set by bootstrap: subsequent flush should succeed.
        std::fs::write(store.state().write_path(), b"remote-v2").expect("write v2");
        store.reserve_write_sequence();
        store
            .flush_once(&client, &object_key)
            .await
            .expect("flush after bootstrap should succeed");
    }

    #[tokio::test]
    async fn bootstrap_from_remote_missing_object_returns_false() {
        let config = Config::from_dsn("sqlite::memory:");
        let s3_dsn = unique_s3_dsn("bootstrap_missing");
        let object_key = unique_object_key("bootstrap_missing");
        let store = S3Store::open(
            &s3_dsn,
            &config,
            DurabilityMode::Durable,
            S3SyncConfig::default(),
        )
        .await
        .expect("open should succeed");
        let client = InMemoryObjectStore::new();

        std::fs::write(store.state().write_path(), b"local-only").expect("seed local");
        let loaded = store
            .bootstrap_from_remote(&client, &object_key)
            .await
            .expect("bootstrap should not fail when object missing");
        assert!(!loaded);
        assert_eq!(
            std::fs::read(store.state().write_path()).unwrap(),
            b"local-only"
        );
    }
}
