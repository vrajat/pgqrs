use crate::error::Result;
use crate::store::s3::anytables::AnyTables;
use crate::store::s3::consistent::ConsistentDb;
use crate::store::s3::snapshot::SnapshotDb;
use crate::store::s3::SyncDb;
use crate::store::{
    ConcurrencyModel, MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store,
    WorkerTable, WorkflowTable,
};
use async_trait::async_trait;

/// Store adapter over a SnapshotDb-like core.
///
/// This is the Store-shaped layer for builders and higher-level APIs.
#[derive(Clone)]
pub struct SyncStore<DB>
where
    DB: SyncDb,
{
    tables: AnyTables<DB>,
}

impl<DB> SyncStore<DB>
where
    DB: SyncDb,
{
    pub fn db(&self) -> &DB {
        self.tables.store()
    }

    pub async fn refresh(&mut self) -> Result<()> {
        self.tables.store_mut().refresh().await
    }

    pub async fn snapshot(&mut self) -> Result<()> {
        self.tables.store_mut().snapshot().await
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.tables.store_mut().sync().await
    }
}

impl SyncStore<SnapshotDb> {
    pub async fn new(config: &crate::Config) -> Result<Self> {
        let db = SnapshotDb::new(config).await?;
        Ok(Self {
            tables: AnyTables::new(db),
        })
    }
}

impl SyncStore<ConsistentDb> {
    pub async fn new(config: &crate::Config) -> Result<Self> {
        let db = ConsistentDb::new(config).await?;
        Ok(Self {
            tables: AnyTables::new(db),
        })
    }
}

#[async_trait]
impl<DB> Store for SyncStore<DB>
where
    DB: SyncDb,
{
    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        let sql = sql.to_string();
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.execute_raw(&sql).await }))
            .await
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        let sql = sql.to_string();
        self.tables
            .store()
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
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.execute_raw_with_two_i64(&sql, param1, param2).await })
            })
            .await
    }

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        let sql = sql.to_string();
        self.tables
            .store()
            .with_read(|store| Box::pin(async move { store.query_int(&sql).await }))
            .await
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        let sql = sql.to_string();
        self.tables
            .store()
            .with_read(|store| Box::pin(async move { store.query_string(&sql).await }))
            .await
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        let sql = sql.to_string();
        self.tables
            .store()
            .with_read(|store| Box::pin(async move { store.query_bool(&sql).await }))
            .await
    }

    fn config(&self) -> &crate::Config {
        self.tables.store().config()
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
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.bootstrap().await }))
            .await
    }

    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &crate::Config,
    ) -> crate::error::Result<Box<dyn crate::Admin>> {
        let hostname = hostname.to_string();
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.admin(&hostname, port, &config).await })
            })
            .await
    }

    async fn admin_ephemeral(
        &self,
        config: &crate::Config,
    ) -> crate::error::Result<Box<dyn crate::Admin>> {
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.admin_ephemeral(&config).await }))
            .await
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &crate::Config,
    ) -> crate::error::Result<crate::Producer> {
        let queue = queue.to_string();
        let hostname = hostname.to_string();
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.producer(&queue, &hostname, port, &config).await })
            })
            .await
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &crate::Config,
    ) -> crate::error::Result<crate::Consumer> {
        let queue = queue.to_string();
        let hostname = hostname.to_string();
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.consumer(&queue, &hostname, port, &config).await })
            })
            .await
    }

    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
        let name = name.to_string();
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.queue(&name).await }))
            .await
    }

    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
        let name = name.to_string();
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.workflow(&name).await }))
            .await
    }

    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<crate::Run> {
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.run(message).await }))
            .await
    }

    async fn worker(&self, id: i64) -> crate::error::Result<Box<dyn crate::Worker>> {
        self.tables
            .store()
            .with_write(|store| Box::pin(async move { store.worker(id).await }))
            .await
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.tables.store().concurrency_model()
    }

    fn backend_name(&self) -> &'static str {
        "sync"
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &crate::Config,
    ) -> crate::error::Result<crate::Producer> {
        let queue = queue.to_string();
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.producer_ephemeral(&queue, &config).await })
            })
            .await
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &crate::Config,
    ) -> crate::error::Result<crate::Consumer> {
        let queue = queue.to_string();
        let config = config.clone();
        self.tables
            .store()
            .with_write(|store| {
                Box::pin(async move { store.consumer_ephemeral(&queue, &config).await })
            })
            .await
    }
}
