//! S3-backed queue store facade.
//!
//! This module provides the public `S3Store` entrypoint and common S3-oriented
//! configuration/model types. The implementation uses an internal durability-backed
//! DB holder and `SyncStore<...>` for store/table wiring.

pub mod client;
pub mod consistent;
pub mod snapshot;
pub mod syncstore;
pub mod tables;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::s3::tables::Tables;
use crate::store::{
    AnyStore, ConcurrencyModel, MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store,
    WorkerTable, WorkflowTable,
};
use crate::workers::{Admin, Worker};

pub type StoreOpFuture<'a, R> = Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>>;

#[async_trait]
pub trait SyncDb: Clone + Send + Sync + 'static {
    fn config(&self) -> &Config;
    fn concurrency_model(&self) -> crate::store::ConcurrencyModel;
    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;
    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;
    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;
    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;
    async fn snapshot(&mut self) -> Result<()>;
    async fn refresh(&mut self) -> Result<()>;
    async fn sync(&mut self) -> Result<()>;
}

/// Durability behavior for S3-backed stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Strict mode: write operations are synchronized before returning.
    #[default]
    Durable,
    /// Local snapshot mode: write/read handles are split and app drives sync/refresh boundaries.
    Local,
}

#[derive(Clone)]
enum DurabilityStore {
    Local {
        db: Arc<RwLock<snapshot::SnapshotDb>>,
        config: Config,
    },
    Durable(consistent::ConsistentDb),
}

#[async_trait]
impl SyncDb for DurabilityStore {
    fn config(&self) -> &Config {
        match self {
            Self::Local { config, .. } => config,
            Self::Durable(db) => db.config(),
        }
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        match self {
            Self::Local { .. } => crate::store::ConcurrencyModel::SingleProcess,
            Self::Durable(db) => db.concurrency_model(),
        }
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        match self {
            Self::Local { db, .. } => db.blocking_read().with_read_ref(f),
            Self::Durable(db) => db.with_read_ref(f),
        }
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        match self {
            Self::Local { db, .. } => db.blocking_write().with_write_ref(f),
            Self::Durable(db) => db.with_write_ref(f),
        }
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        match self {
            Self::Local { db, .. } => db.read().await.with_read(f).await,
            Self::Durable(db) => db.with_read(f).await,
        }
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        match self {
            Self::Local { db, .. } => db.write().await.with_write(f).await,
            Self::Durable(db) => db.with_write(f).await,
        }
    }

    async fn snapshot(&mut self) -> Result<()> {
        match self {
            Self::Local { db, .. } => db.write().await.snapshot().await,
            Self::Durable(db) => db.snapshot().await,
        }
    }

    async fn refresh(&mut self) -> Result<()> {
        match self {
            Self::Local { db, .. } => db.write().await.refresh().await,
            Self::Durable(db) => db.refresh().await,
        }
    }

    async fn sync(&mut self) -> Result<()> {
        match self {
            Self::Local { db, .. } => db.write().await.sync().await,
            Self::Durable(db) => db.sync().await,
        }
    }
}

/// S3-backed store entrypoint.
#[derive(Clone)]
pub struct S3Store {
    db: DurabilityStore,
    tables: Tables<DurabilityStore>,
    config: Config,
    mode: DurabilityMode,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store").field("mode", &self.mode).finish()
    }
}

impl S3Store {
    /// Create an S3-backed store.
    ///
    /// Internally this initializes mode-specific DB wiring:
    /// - `DurabilityMode::Local` -> `SnapshotDb`
    /// - `DurabilityMode::Durable` -> `ConsistentDb`
    pub async fn new(config: &Config) -> Result<Self> {
        let mode = config.s3.mode;
        let db = match mode {
            DurabilityMode::Local => DurabilityStore::Local {
                db: Arc::new(RwLock::new(snapshot::SnapshotDb::new(config).await?)),
                config: config.clone(),
            },
            DurabilityMode::Durable => {
                DurabilityStore::Durable(consistent::ConsistentDb::new(config).await?)
            }
        };
        let tables = Tables::new(db.clone());
        Ok(Self {
            db,
            tables,
            config: config.clone(),
            mode,
        })
    }

    /// Access the mode captured from configuration.
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Synchronize state from object storage on demand.
    ///
    /// For `S3Store` this is a compatibility shim over `SyncStore<SnapshotDb>`.
    pub async fn snapshot(&mut self) -> Result<()> {
        self.db.snapshot().await
    }

    /// Reload local read handle from the current backing files.
    ///
    /// For `S3Store` this is a compatibility shim over `SyncStore<SnapshotDb>`.
    pub async fn refresh(&mut self) -> Result<()> {
        self.db.refresh().await
    }

    /// Sync local write state to object storage.
    ///
    /// For `S3Store` this is a compatibility shim over `SyncStore<SnapshotDb>`.
    pub async fn sync(&mut self) -> Result<()> {
        self.db.sync().await
    }

    /// Parse `sqlite://` cache dsn from an `s3://` DSN.
    pub fn sqlite_cache_dsn_from_s3_dsn(dsn: &str) -> Result<String> {
        parse_and_cache_s3_dsn(dsn)
    }
}

#[async_trait]
impl Store for S3Store {
    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        let sql = sql.to_string();
        self.db
            .with_write(|store| Box::pin(async move { store.execute_raw(&sql).await }))
            .await
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        let sql = sql.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move { store.execute_raw_with_i64(&sql, param).await })
            })
            .await
    }

    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        let sql = sql.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move { store.execute_raw_with_two_i64(&sql, param1, param2).await })
            })
            .await
    }

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        let sql = sql.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.query_int(&sql).await }))
            .await
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        let sql = sql.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.query_string(&sql).await }))
            .await
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        let sql = sql.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.query_bool(&sql).await }))
            .await
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn queues(&self) -> &dyn QueueTable {
        &self.tables
    }

    fn messages(&self) -> &dyn MessageTable {
        &self.tables
    }

    fn workers(&self) -> &dyn WorkerTable {
        &self.tables
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        &self.tables
    }

    fn workflow_runs(&self) -> &dyn RunRecordTable {
        &self.tables
    }

    fn workflow_steps(&self) -> &dyn StepRecordTable {
        &self.tables
    }

    async fn bootstrap(&self) -> crate::error::Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.bootstrap().await }))
            .await
    }

    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn crate::Admin>> {
        let hostname = hostname.to_string();
        let config = config.clone();
        let inner_admin = self
            .db
            .with_write(|store| {
                Box::pin(async move { store.admin(&hostname, port, &config).await })
            })
            .await?;
        Ok(Box::new(S3Admin {
            db: self.db.clone(),
            inner: Arc::from(inner_admin),
        }))
    }

    async fn admin_ephemeral(
        &self,
        config: &Config,
    ) -> crate::error::Result<Box<dyn crate::Admin>> {
        let config = config.clone();
        let inner_admin = self
            .db
            .with_write(|store| Box::pin(async move { store.admin_ephemeral(&config).await }))
            .await?;
        Ok(Box::new(S3Admin {
            db: self.db.clone(),
            inner: Arc::from(inner_admin),
        }))
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<crate::Producer> {
        let queue = queue.to_string();
        let hostname = hostname.to_string();
        let validation_config = config.validation_config.clone();

        let (queue_info, worker_record) = self
            .db
            .with_write(|store| {
                Box::pin(async move {
                    let queue_info = store.queues().get_by_name(&queue).await?;
                    let worker_record = store
                        .workers()
                        .register(Some(queue_info.id), &hostname, port)
                        .await?;
                    Ok((queue_info, worker_record))
                })
            })
            .await?;

        Ok(crate::workers::Producer::new(
            AnyStore::S3(self.clone()),
            queue_info,
            worker_record,
            validation_config,
        ))
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        _config: &Config,
    ) -> crate::error::Result<crate::Consumer> {
        let queue = queue.to_string();
        let hostname = hostname.to_string();

        let (queue_info, worker_record) = self
            .db
            .with_write(|store| {
                Box::pin(async move {
                    let queue_info = store.queues().get_by_name(&queue).await?;
                    let worker_record = store
                        .workers()
                        .register(Some(queue_info.id), &hostname, port)
                        .await?;
                    Ok((queue_info, worker_record))
                })
            })
            .await?;

        Ok(crate::workers::Consumer::new(
            AnyStore::S3(self.clone()),
            queue_info,
            worker_record,
        ))
    }

    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
        let name = name.to_string();
        self.db
            .with_write(|store| Box::pin(async move { store.queue(&name).await }))
            .await
    }

    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
        let name = name.to_string();
        self.db
            .with_write(|store| Box::pin(async move { store.workflow(&name).await }))
            .await
    }

    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<crate::Run> {
        let record = self
            .db
            .with_write(|store| {
                Box::pin(async move {
                    let run = store.run(message).await?;
                    Ok(run.record().clone())
                })
            })
            .await?;

        Ok(crate::workers::Run::new(AnyStore::S3(self.clone()), record))
    }

    async fn worker(&self, id: i64) -> crate::error::Result<Box<dyn crate::Worker>> {
        let worker_record = self
            .db
            .with_read(|store| Box::pin(async move { store.workers().get(id).await }))
            .await?;
        Ok(Box::new(S3Worker {
            db: self.db.clone(),
            worker_record,
        }))
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.db.concurrency_model()
    }

    fn backend_name(&self) -> &'static str {
        "s3"
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<crate::Producer> {
        let queue = queue.to_string();
        let validation_config = config.validation_config.clone();

        let (queue_info, worker_record) = self
            .db
            .with_write(|store| {
                Box::pin(async move {
                    let queue_info = store.queues().get_by_name(&queue).await?;
                    let worker_record = store
                        .workers()
                        .register_ephemeral(Some(queue_info.id))
                        .await?;
                    Ok((queue_info, worker_record))
                })
            })
            .await?;

        Ok(crate::workers::Producer::new(
            AnyStore::S3(self.clone()),
            queue_info,
            worker_record,
            validation_config,
        ))
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        _config: &Config,
    ) -> crate::error::Result<crate::Consumer> {
        let queue = queue.to_string();

        let (queue_info, worker_record) = self
            .db
            .with_write(|store| {
                Box::pin(async move {
                    let queue_info = store.queues().get_by_name(&queue).await?;
                    let worker_record = store
                        .workers()
                        .register_ephemeral(Some(queue_info.id))
                        .await?;
                    Ok((queue_info, worker_record))
                })
            })
            .await?;

        Ok(crate::workers::Consumer::new(
            AnyStore::S3(self.clone()),
            queue_info,
            worker_record,
        ))
    }
}

struct S3Admin<DB>
where
    DB: SyncDb,
{
    db: DB,
    inner: Arc<dyn Admin>,
}

#[async_trait]
impl<DB> Worker for S3Admin<DB>
where
    DB: SyncDb,
{
    fn worker_record(&self) -> &crate::types::WorkerRecord {
        self.inner.worker_record()
    }

    async fn status(&self) -> crate::error::Result<crate::types::WorkerStatus> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.status().await }))
            .await
    }

    async fn suspend(&self) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.suspend().await }))
            .await
    }

    async fn resume(&self) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.resume().await }))
            .await
    }

    async fn shutdown(&self) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.shutdown().await }))
            .await
    }

    async fn heartbeat(&self) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.heartbeat().await }))
            .await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> crate::error::Result<bool> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.is_healthy(max_age).await }))
            .await
    }
}

struct S3Worker<DB>
where
    DB: SyncDb,
{
    db: DB,
    worker_record: crate::types::WorkerRecord,
}

#[async_trait]
impl<DB> Worker for S3Worker<DB>
where
    DB: SyncDb,
{
    fn worker_record(&self) -> &crate::types::WorkerRecord {
        &self.worker_record
    }

    async fn status(&self) -> crate::error::Result<crate::types::WorkerStatus> {
        let worker_id = self.worker_record.id;
        self.db
            .with_read(|store| Box::pin(async move { store.workers().get_status(worker_id).await }))
            .await
    }

    async fn suspend(&self) -> crate::error::Result<()> {
        let worker_id = self.worker_record.id;
        self.db
            .with_write(|store| Box::pin(async move { store.workers().suspend(worker_id).await }))
            .await
    }

    async fn resume(&self) -> crate::error::Result<()> {
        let worker_id = self.worker_record.id;
        self.db
            .with_write(|store| Box::pin(async move { store.workers().resume(worker_id).await }))
            .await
    }

    async fn shutdown(&self) -> crate::error::Result<()> {
        let worker_id = self.worker_record.id;
        self.db
            .with_write(|store| Box::pin(async move { store.workers().shutdown(worker_id).await }))
            .await
    }

    async fn heartbeat(&self) -> crate::error::Result<()> {
        let worker_id = self.worker_record.id;
        self.db
            .with_write(|store| Box::pin(async move { store.workers().heartbeat(worker_id).await }))
            .await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> crate::error::Result<bool> {
        let worker_id = self.worker_record.id;
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workers().is_healthy(worker_id, max_age).await })
            })
            .await
    }
}

#[async_trait]
impl<DB> Admin for S3Admin<DB>
where
    DB: SyncDb,
{
    async fn verify(&self) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.verify().await }))
            .await
    }

    async fn delete_queue(
        &self,
        queue_info: &crate::types::QueueRecord,
    ) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        let queue_info = queue_info.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.delete_queue(&queue_info).await }))
            .await
    }

    async fn purge_queue(&self, name: &str) -> crate::error::Result<()> {
        let inner = self.inner.clone();
        let name = name.to_string();
        self.db
            .with_write(|_| Box::pin(async move { inner.purge_queue(&name).await }))
            .await
    }

    async fn dlq(&self) -> crate::error::Result<Vec<i64>> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.dlq().await }))
            .await
    }

    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::stats::QueueMetrics> {
        let inner = self.inner.clone();
        let name = name.to_string();
        self.db
            .with_read(|_| Box::pin(async move { inner.queue_metrics(&name).await }))
            .await
    }

    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::stats::QueueMetrics>> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.all_queues_metrics().await }))
            .await
    }

    async fn system_stats(&self) -> crate::error::Result<crate::stats::SystemStats> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.system_stats().await }))
            .await
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::stats::WorkerHealthStats>> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| {
                Box::pin(async move {
                    inner
                        .worker_health_stats(heartbeat_timeout, group_by_queue)
                        .await
                })
            })
            .await
    }

    async fn worker_stats(
        &self,
        queue_name: &str,
    ) -> crate::error::Result<crate::stats::WorkerStats> {
        let inner = self.inner.clone();
        let queue_name = queue_name.to_string();
        self.db
            .with_read(|_| Box::pin(async move { inner.worker_stats(&queue_name).await }))
            .await
    }

    async fn delete_worker(&self, worker_id: i64) -> crate::error::Result<u64> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.delete_worker(worker_id).await }))
            .await
    }

    async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> crate::error::Result<Vec<crate::types::QueueMessage>> {
        let inner = self.inner.clone();
        self.db
            .with_read(|_| Box::pin(async move { inner.get_worker_messages(worker_id).await }))
            .await
    }

    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<chrono::Duration>,
    ) -> crate::error::Result<u64> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| {
                Box::pin(async move { inner.reclaim_messages(queue_id, older_than).await })
            })
            .await
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> crate::error::Result<u64> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.purge_old_workers(older_than).await }))
            .await
    }

    async fn release_worker_messages(&self, worker_id: i64) -> crate::error::Result<u64> {
        let inner = self.inner.clone();
        self.db
            .with_write(|_| Box::pin(async move { inner.release_worker_messages(worker_id).await }))
            .await
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

fn cache_prefix() -> String {
    if let Ok(prefix) = std::env::var("PGQRS_S3_CACHE_PREFIX") {
        let trimmed = prefix.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "host".to_string());
    format!("{}_{}", host, std::process::id())
}

fn sanitize_cache_component(input: &str) -> String {
    let out: String = input
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => c,
            _ => '_',
        })
        .collect();
    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}

fn parse_and_cache_s3_dsn(dsn: &str) -> Result<String> {
    let (bucket, key) = parse_s3_bucket_and_key(dsn)?;

    let base_dir = std::env::var("PGQRS_S3_LOCAL_CACHE_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("CARGO_TARGET_TMPDIR").map(PathBuf::from))
        .unwrap_or_else(|_| std::env::temp_dir().join("pgqrs_s3_cache"));
    std::fs::create_dir_all(&base_dir).map_err(|e| Error::InvalidConfig {
        field: "PGQRS_S3_LOCAL_CACHE_DIR".to_string(),
        message: format!("Failed to create S3 cache directory: {}", e),
    })?;

    let prefix = cache_prefix();
    let mut path = base_dir
        .join(sanitize_cache_component(&prefix))
        .join(sanitize_cache_component(&bucket));
    for segment in key.split('/') {
        path = path.join(sanitize_cache_component(segment));
    }

    Ok(format!("sqlite://{}?mode=rwc", path.to_string_lossy()))
}

/// Map an `s3://...` DSN to a deterministic local cache DSN.
pub fn sqlite_cache_dsn_from_s3_dsn(dsn: &str) -> Result<String> {
    parse_and_cache_s3_dsn(dsn)
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
