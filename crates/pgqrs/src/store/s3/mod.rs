//! S3-backed queue store facade.
//!
//! This module provides the public `S3Store` entrypoint and common S3-oriented
//! configuration/model types. The implementation uses an internal durability-backed
//! DB holder and direct `S3Store` wiring for table and worker operations.

pub mod consistent;
pub mod snapshot;
pub mod tables;

use async_trait::async_trait;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::s3::tables::Tables;
use crate::store::{
    AnyStore, ConcurrencyModel, MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store,
    WorkerTable, WorkflowTable,
};
use crate::workers::{Admin, Worker};

pub type StoreOpFuture<'a, R> = Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>>;

/// Internal stateful DB contract for the S3-backed store layers.
///
/// This trait sits below [`S3Store`] and above the concrete SQLite/object-store state machine.
/// It is responsible for two things:
///
/// 1. exposing queue/table operations against the current local SQLite view
/// 2. exposing explicit synchronization operations (`snapshot()` and `sync()`)
///
/// The important semantic boundary is:
///
/// - `with_read(...)` reads from the current local SQLite state
/// - `with_write(...)` mutates local SQLite state
/// - `snapshot(...)` pulls remote state into the local cache
/// - `sync(...)` pushes local state to remote object storage
///
/// Implementations decide how much consistency policy they add on top:
///
/// - `SnapshotDb` is explicit and caller-driven
/// - `ConsistentDb` sequences durable writes automatically
///
/// Engineers should treat this trait as the internal durability/replication boundary for the
/// S3 backend, not as a generic cross-backend store abstraction.
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
    async fn sync(&mut self) -> Result<()>;
}

/// Durability behavior for S3-backed stores.
///
/// This enum controls how much synchronization policy the S3 backend adds on top of the local
/// SQLite cache.
///
/// - `Durable`
///   - writes are treated as durable writes
///   - the store sequences local write -> remote sync before returning
///   - callers do not need to call `sync()` after ordinary write operations
///
/// - `Local`
///   - writes only affect the local SQLite cache
///   - callers explicitly decide when to call `sync()` or `snapshot()`
///   - useful for tests, staged synchronization, and explicit state management
///
/// This mode does not change the queue API surface; it changes the consistency semantics of the
/// S3-backed implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Strict mode: write operations are synchronized before returning.
    #[default]
    Durable,
    /// Local snapshot mode: write/read handles are split and app drives sync/refresh boundaries.
    Local,
}

/// Derived consistency status for an S3-backed store.
///
/// This is a diagnostic/read-only view computed on demand from:
/// - the presence of the current local cache file
/// - local dirty state
/// - local last observed etag
/// - remote object existence and remote etag
///
/// It is intentionally higher-level than the internal fields tracked by `SnapshotDb`.
/// The enum does **not** attempt to prove which side is authoritative when state diverges; it
/// only reports the observable relationship between local cache state and remote object state.
///
/// Interpretation:
///
/// - `LocalMissing`
///   - the expected local cache file is missing
///   - usually indicates local cleanup or corruption rather than a normal steady state
///
/// - `RemoteMissing { local_dirty }`
///   - the local cache exists, but the remote object does not
///   - `local_dirty = true` means there are unsynced local writes and a future `sync()` would
///     recreate remote state
///   - `local_dirty = false` means the local cache is clean and `sync()` is a no-op under the
///     current semantics
///
/// - `InSync`
///   - the local cache exists
///   - the remote object exists
///   - remote etag matches the local etag baseline
///   - local state is not dirty
///
/// - `Diverged { local_dirty }`
///   - local and remote are both present, but they are not currently aligned
///   - this may mean remote advanced, local has unsynced writes, or both
///   - the enum deliberately does not try to declare a winner/head revision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
    LocalMissing,
    RemoteMissing { local_dirty: bool },
    InSync,
    Diverged { local_dirty: bool },
}

#[derive(Clone)]
enum DurabilityStore {
    Local(snapshot::SnapshotDb),
    Durable(consistent::ConsistentDb),
}

#[async_trait]
impl SyncDb for DurabilityStore {
    fn config(&self) -> &Config {
        match self {
            Self::Local(db) => db.config(),
            Self::Durable(db) => db.config(),
        }
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        match self {
            Self::Local(db) => db.concurrency_model(),
            Self::Durable(db) => db.concurrency_model(),
        }
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        match self {
            Self::Local(db) => db.with_read_ref(f),
            Self::Durable(db) => db.with_read_ref(f),
        }
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        match self {
            Self::Local(db) => db.with_write_ref(f),
            Self::Durable(db) => db.with_write_ref(f),
        }
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        match self {
            Self::Local(db) => db.with_read(f).await,
            Self::Durable(db) => db.with_read(f).await,
        }
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        match self {
            Self::Local(db) => db.with_write(f).await,
            Self::Durable(db) => db.with_write(f).await,
        }
    }

    async fn snapshot(&mut self) -> Result<()> {
        match self {
            Self::Local(db) => db.snapshot().await,
            Self::Durable(db) => db.snapshot().await,
        }
    }

    async fn sync(&mut self) -> Result<()> {
        match self {
            Self::Local(db) => db.sync().await,
            Self::Durable(db) => db.sync().await,
        }
    }
}

/// S3-backed store entrypoint.
#[derive(Clone)]
pub struct S3Store {
    db: DurabilityStore,
    tables: Tables<DurabilityStore>,
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
            DurabilityMode::Local => {
                DurabilityStore::Local(snapshot::SnapshotDb::new(config).await?)
            }
            DurabilityMode::Durable => {
                DurabilityStore::Durable(consistent::ConsistentDb::new(config).await?)
            }
        };
        let tables = Tables::new(db.clone());
        Ok(Self { db, tables, mode })
    }

    /// Access the mode captured from configuration.
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Synchronize state from object storage on demand.
    ///
    /// Synchronize state from object storage on demand.
    pub async fn snapshot(&mut self) -> Result<()> {
        self.db.snapshot().await
    }

    /// Sync local write state to object storage.
    ///
    /// Sync local write state to object storage.
    pub async fn sync(&mut self) -> Result<()> {
        self.db.sync().await
    }

    pub async fn state(&self) -> Result<SyncState> {
        match &self.db {
            DurabilityStore::Local(db) => db.state().await,
            DurabilityStore::Durable(db) => db.state().await,
        }
    }

    /// Parse `sqlite://` cache dsn from an `s3://` DSN.
    pub fn sqlite_cache_dsn_from_s3_dsn(dsn: &str) -> Result<String> {
        parse_and_cache_s3_dsn(dsn)
    }

    pub fn object_store_from_env(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        snapshot::build_object_store_from_env(bucket)
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
        self.db.config()
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
            worker_record: inner_admin.worker_record().clone(),
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
            worker_record: inner_admin.worker_record().clone(),
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
    worker_record: crate::types::WorkerRecord,
}

#[async_trait]
impl<DB> Worker for S3Admin<DB>
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
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.verify().await
                })
            })
            .await
    }

    async fn delete_queue(
        &self,
        queue_info: &crate::types::QueueRecord,
    ) -> crate::error::Result<()> {
        let config = self.db.config().clone();
        let queue_info = queue_info.clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.delete_queue(&queue_info).await
                })
            })
            .await
    }

    async fn purge_queue(&self, name: &str) -> crate::error::Result<()> {
        let config = self.db.config().clone();
        let name = name.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.purge_queue(&name).await
                })
            })
            .await
    }

    async fn dlq(&self) -> crate::error::Result<Vec<i64>> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.dlq().await
                })
            })
            .await
    }

    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::stats::QueueMetrics> {
        let config = self.db.config().clone();
        let name = name.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.queue_metrics(&name).await
                })
            })
            .await
    }

    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::stats::QueueMetrics>> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.all_queues_metrics().await
                })
            })
            .await
    }

    async fn system_stats(&self) -> crate::error::Result<crate::stats::SystemStats> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.system_stats().await
                })
            })
            .await
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::stats::WorkerHealthStats>> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin
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
        let config = self.db.config().clone();
        let queue_name = queue_name.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.worker_stats(&queue_name).await
                })
            })
            .await
    }

    async fn delete_worker(&self, worker_id: i64) -> crate::error::Result<u64> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.delete_worker(worker_id).await
                })
            })
            .await
    }

    async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> crate::error::Result<Vec<crate::types::QueueMessage>> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.get_worker_messages(worker_id).await
                })
            })
            .await
    }

    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<chrono::Duration>,
    ) -> crate::error::Result<u64> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.reclaim_messages(queue_id, older_than).await
                })
            })
            .await
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> crate::error::Result<u64> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.purge_old_workers(older_than).await
                })
            })
            .await
    }

    async fn release_worker_messages(&self, worker_id: i64) -> crate::error::Result<u64> {
        let config = self.db.config().clone();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    let admin = store.admin_ephemeral(&config).await?;
                    admin.release_worker_messages(worker_id).await
                })
            })
            .await
    }
}

pub fn parse_s3_bucket_and_key(dsn: &str) -> Result<(String, String)> {
    let url = Url::parse(dsn).map_err(|e| Error::InvalidConfig {
        field: "dsn".to_string(),
        message: format!("Invalid S3 DSN format: {dsn} ({e})"),
    })?;

    if url.scheme() != "s3" {
        return Err(Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Invalid S3 DSN scheme '{}': {}", url.scheme(), dsn),
        });
    }

    let bucket = url.host_str().unwrap_or_default().trim();
    let key = url.path().trim_start_matches('/').trim();

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

    Ok((bucket.to_owned(), key.to_owned()))
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
    let dir = s3_local_cache_dir_for_dsn(dsn)?;
    Ok(format!(
        "sqlite://{}?mode=rwc",
        dir.join("bootstrap.sqlite").to_string_lossy()
    ))
}

pub(crate) fn s3_local_cache_dir_for_dsn(dsn: &str) -> Result<PathBuf> {
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

    std::fs::create_dir_all(&path).map_err(|e| Error::InvalidConfig {
        field: "PGQRS_S3_LOCAL_CACHE_DIR".to_string(),
        message: format!("Failed to create S3 cache path {}: {}", path.display(), e),
    })?;

    Ok(path)
}

/// Map an `s3://...` DSN to a deterministic local cache DSN.
pub fn sqlite_cache_dsn_from_s3_dsn(dsn: &str) -> Result<String> {
    parse_and_cache_s3_dsn(dsn)
}

#[cfg(test)]
mod tests {
    use super::{parse_s3_bucket_and_key, sqlite_cache_dsn_from_s3_dsn};

    #[test]
    fn parse_s3_bucket_and_key_accepts_valid_s3_url() {
        let (bucket, key) = parse_s3_bucket_and_key("s3://bucket/path/to/queue.db").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "path/to/queue.db");
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_missing_bucket() {
        let err = parse_s3_bucket_and_key("s3:///queue.db").unwrap_err();
        assert!(err.to_string().contains("missing bucket"));
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_missing_key() {
        let err = parse_s3_bucket_and_key("s3://bucket").unwrap_err();
        assert!(err.to_string().contains("missing object key"));
    }

    #[test]
    fn parse_s3_bucket_and_key_rejects_wrong_scheme() {
        let err = parse_s3_bucket_and_key("sqlite://bucket/queue.db").unwrap_err();
        assert!(err.to_string().contains("Invalid S3 DSN"));
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
        assert!(err.to_string().contains("Invalid S3 DSN"));
    }
}
