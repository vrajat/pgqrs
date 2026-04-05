use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::dblock::DbLock;
use crate::store::s3::{
    ensure_s3_local_cache_dir, parse_s3_bucket_and_key, sanitize_cache_component, SyncDb, SyncState,
};
use crate::store::sqlite::SqliteTables;
use crate::store::{DbOpFuture, DbTables};
use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, UpdateVersion};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
/// Object-store-backed single-handle SQLite snapshot database.
///
/// # Guarantees
/// - Reads and writes are executed against the same local SQLite file.
/// - `snapshot()` pulls remote object bytes to the local file, then reopens the SQLite handle.
/// - `sync()` publishes the local SQLite file to remote object storage, then reopens the
///   SQLite handle.
/// - SnapshotDb enforces `config.sqlite.use_wal = false` internally because sync unit is the
///   whole DB file.
///
/// # Non-Guarantees
/// - No automatic background synchronization.
/// - No cross-process coordination.
/// - No cross-handle consistency guarantees beyond the local SQLite file and remote object.
///
/// # Application Responsibility
/// The app decides when to call `sync()`, `refresh()`, and `snapshot()`.
///
/// ```rust,no_run
/// # #[cfg(feature = "s3")]
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use pgqrs::config::Config;
/// use pgqrs::store::s3::S3Store;
/// use serde_json::json;
///
/// let rt = tokio::runtime::Runtime::new()?;
/// rt.block_on(async {
///     // Requires S3 env vars (endpoint/region/credentials) to be configured.
///     let config = Config::from_dsn("s3://pgqrs-test-bucket/snapshot-doc.sqlite");
///     let mut store = S3Store::new(&config).await?;
///
///     let queue_name = "jobs";
///     pgqrs::admin(&store).create_queue(queue_name).await?;
///     let producer = pgqrs::producer("host", 9001, queue_name).create(&store).await?;
///     let consumer = pgqrs::consumer("host", 9002, queue_name).create(&store).await?;
///
///     pgqrs::enqueue()
///         .message(&json!({ "job": "example" }))
///         .worker(&producer)
///         .execute(&store)
///         .await?;
///
///     store.sync().await?;
///
///     let after = pgqrs::tables(&store).messages().count().await?;
///     assert_eq!(after, 1);
///
///     let got = pgqrs::dequeue().worker(&consumer).fetch_all(&store).await?;
///     assert_eq!(got.len(), 1);
///
///     Ok::<(), pgqrs::error::Error>(())
/// })?;
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "s3"))]
/// # fn main() {}
/// ```
pub struct SnapshotDb {
    inner: Arc<RwLock<SnapshotState>>,
    write_gate: Arc<Mutex<()>>,
    config: Config,
    cache_dir: PathBuf,
}

struct SnapshotState {
    object_store: Arc<dyn ObjectStore>,
    object_key: String,
    last_etag: Option<String>,
    is_dirty: bool,
    write_version: u64,
    sqlite_tables: SqliteTables,
}

impl SnapshotDb {
    /// Build a `SnapshotDb` from config.
    ///
    /// - DSN is parsed as `s3://bucket/key` from `config.dsn`.
    /// - Object store is created from environment (`PGQRS_S3_*`, `AWS_*`).
    /// - A local SQLite handle is created in snapshot mode.
    pub async fn new(config: &Config) -> Result<Self> {
        let (bucket, object_key) = parse_s3_bucket_and_key(&config.dsn)?;
        let object_store = build_object_store_from_env(&bucket)?;
        let mut config = config.clone();
        config.sqlite.use_wal = false;
        let cache_dir = ensure_s3_local_cache_dir(&config.s3.cache_id)?;

        let sqlite_dsn = sqlite_dsn_for_revision(&cache_dir, None)?;
        let sqlite_tables = SqliteTables::new(&sqlite_dsn, &config).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(SnapshotState {
                object_store,
                object_key,
                last_etag: None,
                is_dirty: false,
                write_version: 0,
                sqlite_tables,
            })),
            write_gate: Arc::new(Mutex::new(())),
            config,
            cache_dir,
        })
    }

    pub(crate) fn write_gate(&self) -> &Arc<Mutex<()>> {
        &self.write_gate
    }

    pub async fn state(&self) -> Result<SyncState> {
        let (object_store, object_key, local_etag, is_dirty) = {
            let guard = self.inner.read().await;
            (
                guard.object_store.clone(),
                guard.object_key.clone(),
                guard.last_etag.clone(),
                guard.is_dirty,
            )
        };

        let sqlite_path = sqlite_path_for_revision(&self.cache_dir, local_etag.as_deref())?;
        if !sqlite_path.exists() {
            return Ok(SyncState::LocalMissing);
        }

        let object_path = ObjectPath::from(object_key.as_str());
        let remote_head = object_store.head(&object_path).await;
        match remote_head {
            Ok(head) => {
                let remote_etag = head.e_tag;
                match (remote_etag == local_etag, is_dirty) {
                    (true, false) => Ok(SyncState::InSync),
                    (true, true) => Ok(SyncState::LocalChanges),
                    (false, false) => Ok(SyncState::RemoteChanges),
                    (false, true) => Ok(SyncState::ConcurrentChanges),
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(SyncState::RemoteMissing {
                local_dirty: is_dirty,
            }),
            Err(e) => Err(map_object_store_error("head", &object_key, &e)),
        }
    }
}

pub(crate) fn build_object_store_from_env(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    let region = std::env::var("AWS_REGION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let endpoint = std::env::var("AWS_ENDPOINT_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    let endpoint_is_local = endpoint.as_ref().is_some_and(|ep| {
        let ep_lc = ep.to_ascii_lowercase();
        ep_lc.contains("localhost") || ep_lc.contains("127.0.0.1") || ep_lc.contains("localstack")
    });
    if !endpoint_is_local && std::env::var_os("SSL_CERT_FILE").is_none() {
        let fallback_paths = [
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/cert.pem",
        ];
        if let Some(path) = fallback_paths
            .iter()
            .find_map(|path| std::path::Path::new(path).exists().then_some(*path))
        {
            std::env::set_var("SSL_CERT_FILE", path);
        }
    }

    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_virtual_hosted_style_request(false);

    if let Some(region) = region {
        builder = builder.with_region(region);
    }

    if let Some(ep) = endpoint {
        if ep.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_endpoint(ep);
    }

    let store = builder.build().map_err(|e| Error::InvalidConfig {
        field: "dsn".to_string(),
        message: format!("Failed to build AmazonS3 object store: {}", e),
    })?;
    Ok(Arc::new(store))
}

#[async_trait]
impl DbLock for SnapshotDb {
    fn config(&self) -> &Config {
        &self.config
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        crate::store::ConcurrencyModel::SingleProcess
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> DbOpFuture<'a, R> + Send,
    {
        let guard = self.inner.read().await;
        f(&guard.sqlite_tables).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> DbOpFuture<'a, R> + Send,
    {
        let mut guard = self.inner.write().await;
        let out = f(&guard.sqlite_tables).await?;
        guard.is_dirty = true;
        guard.write_version = guard.write_version.saturating_add(1);
        Ok(out)
    }
}

#[async_trait]
impl SyncDb for SnapshotDb {
    async fn snapshot(&mut self) -> Result<()> {
        let (object_store, object_key, local_etag, is_dirty) = {
            let guard = self.inner.read().await;
            (
                guard.object_store.clone(),
                guard.object_key.clone(),
                guard.last_etag.clone(),
                guard.is_dirty,
            )
        };

        if is_dirty {
            return Err(Error::Conflict {
                message: format!(
                    "Snapshot refused for key '{}': local store has unsynced writes",
                    object_key
                ),
            });
        }

        let object_path = ObjectPath::from(object_key.as_str());
        let head = object_store
            .head(&object_path)
            .await
            .map_err(|e| map_object_store_error("head", &object_key, &e))?;
        let remote_etag = head.e_tag.clone();

        if remote_etag == local_etag {
            return Ok(());
        }

        let remote = object_store
            .get(&object_path)
            .await
            .map_err(|e| map_object_store_error("get", &object_key, &e))?;
        let remote_bytes = remote.bytes().await.map_err(|e| Error::Internal {
            message: format!("snapshot get bytes failed for key '{}': {}", object_key, e),
        })?;

        let sqlite_path = sqlite_path_for_revision(&self.cache_dir, remote_etag.as_deref())?;
        std::fs::write(&sqlite_path, remote_bytes.as_ref()).map_err(|e| Error::Internal {
            message: format!(
                "Failed writing snapshot db {}: {}",
                sqlite_path.display(),
                e
            ),
        })?;

        let sqlite_dsn = sqlite_dsn_for_revision(&self.cache_dir, remote_etag.as_deref())?;
        let new_tables = SqliteTables::new(&sqlite_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("snapshot reopen sqlite store failed: {e}"),
            })?;

        let mut guard = self.inner.write().await;
        if guard.last_etag != local_etag {
            return Err(Error::Conflict {
                message: format!(
                    "Snapshot CAS failed for key '{}': local etag changed from {:?} to {:?}",
                    object_key, local_etag, guard.last_etag
                ),
            });
        }

        guard.last_etag = remote_etag;
        guard.is_dirty = false;
        guard.sqlite_tables = new_tables;
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        let (object_store, object_key, start_etag, is_dirty, start_write_version) = {
            let guard = self.inner.read().await;
            (
                guard.object_store.clone(),
                guard.object_key.clone(),
                guard.last_etag.clone(),
                guard.is_dirty,
                guard.write_version,
            )
        };

        if !is_dirty {
            return Ok(());
        }

        let sqlite_path = sqlite_path_for_revision(&self.cache_dir, start_etag.as_deref())?;
        let payload = std::fs::read(&sqlite_path).map_err(|e| Error::Internal {
            message: format!(
                "Failed reading snapshot db {}: {}",
                sqlite_path.display(),
                e
            ),
        })?;
        let payload_for_next_revision = payload.clone();

        let object_path = ObjectPath::from(object_key.as_str());
        let sync_state = self.state().await?;
        match sync_state {
            SyncState::LocalMissing => {
                return Err(Error::Conflict {
                    message: format!(
                        "Sync CAS failed for key '{}': local cache file is missing",
                        object_key
                    ),
                });
            }
            SyncState::ConcurrentChanges => {
                return Err(Error::Conflict {
                    message: format!(
                        "Sync CAS failed for key '{}': local and remote changed concurrently from baseline {:?}",
                        object_key, start_etag
                    ),
                });
            }
            _ => {}
        }

        let put_result = match &start_etag {
            Some(last_etag) => {
                let version = UpdateVersion {
                    e_tag: Some(last_etag.clone()),
                    version: None,
                };
                match object_store
                    .put_opts(
                        &object_path,
                        payload.clone().into(),
                        PutMode::Update(version).into(),
                    )
                    .await
                {
                    Ok(put_result) => put_result,
                    Err(object_store::Error::NotFound { .. }) => object_store
                        .put_opts(&object_path, payload.into(), PutMode::Create.into())
                        .await
                        .map_err(|e| map_object_store_error("put", &object_key, &e))?,
                    Err(e) => return Err(map_object_store_error("put", &object_key, &e)),
                }
            }
            None => object_store
                .put_opts(&object_path, payload.into(), PutMode::Create.into())
                .await
                .map_err(|e| map_object_store_error("put", &object_key, &e))?,
        };
        let next_etag = put_result.e_tag;
        let next_path = sqlite_path_for_revision(&self.cache_dir, next_etag.as_deref())?;
        let previous_path = sqlite_path_for_revision(&self.cache_dir, start_etag.as_deref())?;
        if previous_path != next_path {
            std::fs::write(&next_path, &payload_for_next_revision).map_err(|e| {
                Error::Internal {
                    message: format!(
                        "Failed to materialize snapshot db {} after sync from {}: {}",
                        next_path.display(),
                        previous_path.display(),
                        e
                    ),
                }
            })?;
        }
        let sqlite_dsn = sqlite_dsn_for_revision(&self.cache_dir, next_etag.as_deref())?;
        let new_tables = SqliteTables::new(&sqlite_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("sync reopen sqlite store failed: {e}"),
            })?;

        let mut guard = self.inner.write().await;
        if guard.last_etag != start_etag
            || guard.write_version != start_write_version
            || !guard.is_dirty
        {
            return Err(Error::Conflict {
                message: format!(
                    "Sync CAS failed for key '{}': local state changed during sync",
                    object_key
                ),
            });
        }

        guard.last_etag = next_etag;
        guard.is_dirty = false;
        guard.sqlite_tables = new_tables;
        Ok(())
    }
}

fn map_object_store_error(operation: &str, key: &str, err: &object_store::Error) -> Error {
    match err {
        object_store::Error::NotFound { .. } => Error::NotFound {
            entity: "object".to_string(),
            id: key.to_string(),
        },
        object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. } => {
            Error::Conflict {
                message: format!(
                    "ObjectStore {} conflict for key '{}': {}",
                    operation, key, err
                ),
            }
        }
        object_store::Error::Generic { source, .. } => {
            let msg = source.to_string().to_ascii_lowercase();
            if msg.contains("412")
                || msg.contains("precondition")
                || msg.contains("if-match")
                || msg.contains("condition")
            {
                Error::Conflict {
                    message: format!(
                        "ObjectStore {} conflict for key '{}': {}",
                        operation, key, source
                    ),
                }
            } else if msg.contains("timeout")
                || msg.contains("timed out")
                || msg.contains("connection refused")
            {
                Error::Timeout {
                    operation: format!("object_store:{} key={}", operation, key),
                }
            } else {
                Error::Internal {
                    message: format!(
                        "ObjectStore {} failed for key '{}': {}",
                        operation, key, source
                    ),
                }
            }
        }
        _ => Error::Internal {
            message: format!(
                "ObjectStore {} failed for key '{}': {}",
                operation, key, err
            ),
        },
    }
}

fn sqlite_dsn_for_revision(cache_dir: &std::path::Path, etag: Option<&str>) -> Result<String> {
    let path = sqlite_path_for_revision(cache_dir, etag)?;
    Ok(format!("sqlite://{}?mode=rwc", path.display()))
}

fn sqlite_path_for_revision(cache_dir: &std::path::Path, etag: Option<&str>) -> Result<PathBuf> {
    let filename = match etag {
        Some(etag) if !etag.trim().is_empty() => {
            format!("cache_{}.sqlite", sanitize_cache_component(etag))
        }
        _ => "cache.sqlite".to_string(),
    };
    Ok(cache_dir.join(filename))
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_s3_local_cache_dir, parse_s3_bucket_and_key, sqlite_dsn_for_revision,
        sqlite_path_for_revision,
    };

    fn cache_dir_for_test() -> std::path::PathBuf {
        ensure_s3_local_cache_dir("snapshot-tests").unwrap()
    }

    #[test]
    fn parse_s3_bucket_and_key_accepts_valid_s3_url() {
        let (bucket, key) = parse_s3_bucket_and_key("s3://my-bucket/path/to/queue.db").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/queue.db");
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_missing_bucket() {
        let err = parse_s3_bucket_and_key("s3:///queue.db").unwrap_err();
        assert!(err.to_string().contains("missing bucket"));
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_missing_key() {
        let err = parse_s3_bucket_and_key("s3://my-bucket").unwrap_err();
        assert!(err.to_string().contains("missing object key"));
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_wrong_scheme() {
        let err = parse_s3_bucket_and_key("sqlite://my-bucket/queue.db").unwrap_err();
        assert!(err.to_string().contains("Invalid S3 DSN"));
    }

    #[test]
    fn sqlite_revision_paths_use_bootstrap_before_first_sync() {
        let cache_dir = cache_dir_for_test();
        let path = sqlite_path_for_revision(&cache_dir, None).unwrap();
        assert_eq!(path.file_name().unwrap().to_string_lossy(), "cache.sqlite");

        let dsn = sqlite_dsn_for_revision(&cache_dir, None).unwrap();
        assert!(dsn.ends_with("cache.sqlite?mode=rwc"));
    }

    #[test]
    fn sqlite_revision_paths_use_sanitized_etag_filename() {
        let cache_dir = cache_dir_for_test();
        let path = sqlite_path_for_revision(&cache_dir, Some("\"etag:1/2\"")).unwrap();
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            "cache__etag_1_2_.sqlite"
        );
    }
}
