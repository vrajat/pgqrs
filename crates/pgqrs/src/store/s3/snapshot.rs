use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::s3::{StoreOpFuture, SyncDb};
use crate::store::sqlite::SqliteStore;
use crate::store::Store;
use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, UpdateVersion};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
/// Object-store-backed two-handle SQLite snapshot database (`write` + `read`).
///
/// # Guarantees
/// - Writes are executed against local `write`.
/// - Reads are executed against local `read`.
/// - `snapshot()` pulls remote object bytes to local `write`, publishes `write -> read`, then
///   reopens `read`.
/// - `sync()` publishes local `write` to remote object storage, then publishes `write -> read`,
///   then reopens `read`.
/// - SnapshotDb enforces `config.sqlite.use_wal = false` internally because sync unit is the
///   whole DB file.
///
/// # Non-Guarantees
/// - No automatic background synchronization.
/// - No cross-process coordination.
/// - No read-your-writes unless the app calls `sync()` / `refresh()` at the right boundaries.
///
/// # Application Responsibility
/// The app decides when to call `sync()` and `refresh()`.
/// Typical producer/consumer sequence:
/// 1. Producer enqueues message (write-side update).
/// 2. Consumer checks read-side state and sees nothing yet.
/// 3. App calls `sync()` (publish write-side snapshot).
/// 4. App calls `refresh()` (reopen read-side handle).
/// 5. Consumer retries read/dequeue and now sees the item.
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
///     // Read-side still stale before app sync/refresh.
///     let before = pgqrs::tables(&store).messages().count().await?;
///     assert_eq!(before, 0);
///
///     store.sync().await?;
///     store.refresh().await?;
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
    object_store: Arc<dyn ObjectStore>,
    object_key: String,
    last_etag: Option<String>,
    read_store: SqliteStore,
    write_store: SqliteStore,
    read_dsn: String,
    write_dsn: String,
    read_path: PathBuf,
    write_path: PathBuf,
    config: Config,
}

impl SnapshotDb {
    /// Build a `SnapshotDb` from config.
    ///
    /// - DSN is parsed as `s3://bucket/key` from `config.dsn`.
    /// - Object store is created from environment (`PGQRS_S3_*`, `AWS_*`).
    /// - Local read/write SQLite handles are created in snapshot mode.
    pub async fn new(config: &Config) -> Result<Self> {
        let (bucket, object_key) = parse_s3_bucket_and_key(&config.dsn)?;
        let object_store = build_object_store_from_env(&bucket)?;
        let mut config = config.clone();
        // SnapshotDb sync unit is the whole SQLite DB file; force rollback journal mode.
        config.sqlite.use_wal = false;

        let (read_path, write_path) = make_snapshot_paths(&object_key);
        let read_dsn = sqlite_dsn_from_path(&read_path);
        let write_dsn = sqlite_dsn_from_path(&write_path);

        let read_store = SqliteStore::new(&read_dsn, &config).await?;
        let write_store = SqliteStore::new(&write_dsn, &config).await?;

        let this = Self {
            object_store,
            object_key,
            last_etag: None,
            read_store,
            write_store,
            read_dsn,
            write_dsn,
            read_path,
            write_path,
            config,
        };

        Ok(this)
    }

    pub fn read_path(&self) -> &Path {
        &self.read_path
    }

    pub fn write_path(&self) -> &Path {
        &self.write_path
    }
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

fn build_object_store_from_env(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    let region = std::env::var("PGQRS_S3_REGION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());
    let endpoint = std::env::var("PGQRS_S3_ENDPOINT")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| Some("http://localhost:4566".to_string()));

    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    if access_key.is_some() ^ secret_key.is_some() {
        return Err(Error::InvalidConfig {
            field: "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY".to_string(),
            message: "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither".to_string(),
        });
    }
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

    let (access_key, secret_key) = match (access_key, secret_key, endpoint_is_local) {
        (None, None, true) => (Some("test".to_string()), Some("test".to_string())),
        (ak, sk, _) => (ak, sk),
    };

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_virtual_hosted_style_request(false);

    if let Some(ep) = endpoint {
        if ep.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_endpoint(ep);
    }

    if let (Some(ak), Some(sk)) = (access_key, secret_key) {
        builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
    }

    let store = builder.build().map_err(|e| Error::InvalidConfig {
        field: "dsn".to_string(),
        message: format!("Failed to build AmazonS3 object store: {}", e),
    })?;
    Ok(Arc::new(store))
}

#[async_trait]
impl SyncDb for SnapshotDb {
    fn config(&self) -> &Config {
        &self.config
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        self.write_store.concurrency_model()
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        f(&self.read_store)
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        f(&self.write_store)
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        f(&self.read_store).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        f(&self.write_store).await
    }

    async fn snapshot(&mut self) -> Result<()> {
        let object_path = ObjectPath::from(self.object_key.as_str());
        let remote = self
            .object_store
            .get(&object_path)
            .await
            .map_err(|e| map_object_store_error("get", &self.object_key, &e))?;
        self.last_etag = remote.meta.e_tag.clone();
        let remote_bytes = remote.bytes().await.map_err(|e| Error::Internal {
            message: format!(
                "snapshot get bytes failed for key '{}': {}",
                self.object_key, e
            ),
        })?;

        std::fs::write(&self.write_path, remote_bytes.as_ref()).map_err(|e| Error::Internal {
            message: format!(
                "Failed writing snapshot write db {}: {}",
                self.write_path.display(),
                e
            ),
        })?;

        self.write_store = SqliteStore::new(&self.write_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("snapshot reopen write store failed: {}", e),
            })?;
        copy_sqlite_db(
            &self.write_path,
            &self.read_path,
            "snapshot-write->snapshot-read",
        )?;
        self.refresh().await
    }

    async fn refresh(&mut self) -> Result<()> {
        let placeholder_path = std::env::temp_dir().join(format!(
            "pgqrs-refresh-placeholder-{}.db",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let placeholder_dsn = sqlite_dsn_from_path(&placeholder_path);
        let placeholder = SqliteStore::new(&placeholder_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("refresh placeholder open failed: {}", e),
            })?;
        let old = std::mem::replace(&mut self.read_store, placeholder);
        drop(old);
        self.read_store = SqliteStore::new(&self.read_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("refresh reopen read store failed: {}", e),
            })?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.write_store
            .execute_raw("PRAGMA wal_checkpoint(TRUNCATE)")
            .await?;
        self.reopen_write_store().await?;
        self.write_store
            .execute_raw("PRAGMA wal_checkpoint(TRUNCATE)")
            .await?;

        let payload = std::fs::read(&self.write_path).map_err(|e| Error::Internal {
            message: format!(
                "Failed reading snapshot write db {}: {}",
                self.write_path.display(),
                e
            ),
        })?;

        let object_path = ObjectPath::from(self.object_key.as_str());
        let put_result = match &self.last_etag {
            Some(last_etag) => {
                let version = UpdateVersion {
                    e_tag: Some(last_etag.clone()),
                    version: None,
                };
                match self
                    .object_store
                    .put_opts(
                        &object_path,
                        payload.clone().into(),
                        PutMode::Update(version).into(),
                    )
                    .await
                {
                    Ok(put_result) => put_result,
                    Err(object_store::Error::NotFound { .. }) => self
                        .object_store
                        .put_opts(&object_path, payload.into(), PutMode::Create.into())
                        .await
                        .map_err(|e| map_object_store_error("put", &self.object_key, &e))?,
                    Err(e) => return Err(map_object_store_error("put", &self.object_key, &e)),
                }
            }
            None => self
                .object_store
                .put_opts(&object_path, payload.into(), PutMode::Create.into())
                .await
                .map_err(|e| map_object_store_error("put", &self.object_key, &e))?,
        };
        self.last_etag = put_result.e_tag;

        copy_sqlite_db(
            &self.write_path,
            &self.read_path,
            "snapshot-write->snapshot-read",
        )?;
        self.refresh().await
    }
}

impl SnapshotDb {
    async fn reopen_write_store(&mut self) -> Result<()> {
        let placeholder_path = std::env::temp_dir().join(format!(
            "pgqrs-write-placeholder-{}.db",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let placeholder_dsn = sqlite_dsn_from_path(&placeholder_path);
        let placeholder = SqliteStore::new(&placeholder_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("sync placeholder write open failed: {}", e),
            })?;
        let old = std::mem::replace(&mut self.write_store, placeholder);
        drop(old);
        self.write_store = SqliteStore::new(&self.write_dsn, &self.config)
            .await
            .map_err(|e| Error::Internal {
                message: format!("sync reopen write store failed: {}", e),
            })?;
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

fn make_snapshot_paths(object_key: &str) -> (PathBuf, PathBuf) {
    let name = object_key
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("pgqrs-object.sqlite");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let base = std::env::temp_dir();
    let read_path = base.join(format!("pgqrs-snapshot-{}-{}-read.db", unique, name));
    let write_path = base.join(format!("pgqrs-snapshot-{}-{}-write.db", unique, name));
    (read_path, write_path)
}

fn sqlite_dsn_from_path(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}

fn copy_sqlite_db(src: &Path, dst: &Path, label: &str) -> Result<()> {
    if src == dst {
        return Ok(());
    }
    std::fs::copy(src, dst).map_err(|e| Error::Internal {
        message: format!(
            "Failed sqlite copy {} ({} -> {}): {}",
            label,
            src.display(),
            dst.display(),
            e
        ),
    })?;

    let src_wal = PathBuf::from(format!("{}-wal", src.display()));
    let dst_wal = PathBuf::from(format!("{}-wal", dst.display()));
    if src_wal.exists() {
        std::fs::copy(&src_wal, &dst_wal).map_err(|e| Error::Internal {
            message: format!(
                "Failed sqlite WAL copy {} ({} -> {}): {}",
                label,
                src_wal.display(),
                dst_wal.display(),
                e
            ),
        })?;
    } else if dst_wal.exists() {
        let _ = std::fs::remove_file(&dst_wal);
    }

    let src_shm = PathBuf::from(format!("{}-shm", src.display()));
    let dst_shm = PathBuf::from(format!("{}-shm", dst.display()));
    if src_shm.exists() {
        std::fs::copy(&src_shm, &dst_shm).map_err(|e| Error::Internal {
            message: format!(
                "Failed sqlite SHM copy {} ({} -> {}): {}",
                label,
                src_shm.display(),
                dst_shm.display(),
                e
            ),
        })?;
    } else if dst_shm.exists() {
        let _ = std::fs::remove_file(&dst_shm);
    }

    Ok(())
}
