//! Postgres implementation of the Store trait.

use crate::store::{
    Admin as AdminTrait, ArchiveTable, Consumer as ConsumerTrait, MessageTable,
    Producer as ProducerTrait, QueueTable, Store, Worker as WorkerTrait, WorkerTable,
    Workflow as WorkflowTrait, WorkflowTable,
};
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::pgqrs_archive::Archive as PostgresArchiveTable;
use self::tables::pgqrs_messages::Messages as PostgresMessageTable;
use self::tables::pgqrs_queues::Queues as PostgresQueueTable;
use self::tables::pgqrs_workers::Workers as PostgresWorkerTable;
use self::tables::pgqrs_workflows::Workflows as PostgresWorkflowTable;

use self::worker::admin::Admin as PostgresAdmin;
use self::worker::consumer::Consumer as PostgresConsumer;
use self::worker::producer::Producer as PostgresProducer;
use self::workflow::guard::StepGuard as PostgresStepGuard;
use self::workflow::handle::Workflow as PostgresWorkflow;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
    config: Config,
    queues: Arc<PostgresQueueTable>,
    messages: Arc<PostgresMessageTable>,
    workers: Arc<PostgresWorkerTable>,
    archive: Arc<PostgresArchiveTable>,
    workflows: Arc<PostgresWorkflowTable>,
}

impl PostgresStore {
    pub fn new(pool: PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            queues: Arc::new(PostgresQueueTable::new(pool.clone())),
            messages: Arc::new(PostgresMessageTable::new(pool.clone())),
            workers: Arc::new(PostgresWorkerTable::new(pool.clone())),
            archive: Arc::new(PostgresArchiveTable::new(pool.clone())),
            workflows: Arc::new(PostgresWorkflowTable::new(pool)),
        }
    }

    /// Get access to the underlying PgPool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl Store for PostgresStore {
    type Db = sqlx::Postgres;

    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        sqlx::raw_sql(sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        // Use proper parameter binding to prevent SQL injection
        sqlx::query(sql).bind(param).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        // Use proper parameter binding to prevent SQL injection
        sqlx::query(sql)
            .bind(param1)
            .bind(param2)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn queues(&self) -> &dyn QueueTable {
        self.queues.as_ref()
    }

    fn messages(&self) -> &dyn MessageTable {
        self.messages.as_ref()
    }

    fn workers(&self) -> &dyn WorkerTable {
        self.workers.as_ref()
    }

    fn archive(&self) -> &dyn ArchiveTable {
        self.archive.as_ref()
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        self.workflows.as_ref()
    }

    async fn admin(&self, config: &Config) -> crate::error::Result<Box<dyn AdminTrait>> {
        let admin = PostgresAdmin::new(config).await?;
        Ok(Box::new(admin))
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ProducerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let producer =
            PostgresProducer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ConsumerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let consumer =
            PostgresConsumer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(consumer))
    }

    fn workflow(&self, id: i64) -> Box<dyn WorkflowTrait> {
        Box::new(PostgresWorkflow::new(self.pool.clone(), id))
    }

    fn worker(&self, id: i64) -> Box<dyn WorkerTrait> {
        Box::new(self::worker::WorkerHandle::new(self.pool.clone(), id))
    }

    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        let result =
            PostgresStepGuard::acquire::<serde_json::Value>(&self.pool, workflow_id, step_id)
                .await?;
        Ok(result)
    }

    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> crate::error::Result<Box<dyn WorkflowTrait>> {
        let workflow = PostgresWorkflow::create(self.pool.clone(), name, input).await?;
        Ok(Box::new(workflow))
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        crate::store::ConcurrencyModel::MultiProcess
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ProducerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let producer =
            PostgresProducer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ConsumerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let consumer =
            PostgresConsumer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(consumer))
    }
}
