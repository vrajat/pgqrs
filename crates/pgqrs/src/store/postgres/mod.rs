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
    fn pool(&self) -> sqlx::PgPool {
        self.pool.clone()
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
        use crate::store::postgres::workflow::guard::StepResult;
        let result =
            PostgresStepGuard::acquire::<serde_json::Value>(&self.pool, workflow_id, step_id)
                .await?;
        match result {
            StepResult::Execute(guard) => Ok(crate::store::StepResult::Execute(Box::new(guard))),
            StepResult::Skipped(val) => Ok(crate::store::StepResult::Skipped(val)),
        }
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
