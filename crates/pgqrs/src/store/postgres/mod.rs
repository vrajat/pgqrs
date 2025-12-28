//! Postgres implementation of the Store trait.

use crate::error::Result;
use crate::store::{
    ArchiveTable, ConcurrencyModel, MessageTable, QueueTable, Store, WorkerTable, WorkflowTable,
    Admin as AdminTrait, Producer as ProducerTrait, Consumer as ConsumerTrait,
    Workflow as WorkflowTrait, StepGuard as StepGuardTrait,
};
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::pgqrs_queues::Queues as PostgresQueueTable;
use self::tables::pgqrs_messages::Messages as PostgresMessageTable;
use self::tables::pgqrs_workers::Workers as PostgresWorkerTable;
use self::tables::pgqrs_archive::Archive as PostgresArchiveTable;
use self::tables::pgqrs_workflows::Workflows as PostgresWorkflowTable;

use self::worker::admin::Admin as PostgresAdmin;
use self::worker::producer::Producer as PostgresProducer;
use self::worker::consumer::Consumer as PostgresConsumer;
use self::workflow::handle::Workflow as PostgresWorkflow;
use self::workflow::guard::StepGuard as PostgresStepGuard;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
    queues: Arc<PostgresQueueTable>,
    messages: Arc<PostgresMessageTable>,
    workers: Arc<PostgresWorkerTable>,
    archive: Arc<PostgresArchiveTable>,
    workflows: Arc<PostgresWorkflowTable>,
}

impl PostgresStore {
    pub fn new(pool: PgPool, config: &Config) -> Self {
        Self {
            queues: Arc::new(PostgresQueueTable::new(pool.clone())),
            messages: Arc::new(PostgresMessageTable::new(
                pool.clone(),
                config.default_max_read_ct().unwrap_or(100),
            )),
            workers: Arc::new(PostgresWorkerTable::new(pool.clone())),
            archive: Arc::new(PostgresArchiveTable::new(pool.clone())),
            workflows: Arc::new(PostgresWorkflowTable::new(pool.clone())),
            pool,
        }
    }

    /// Get access to the underlying PgPool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl Store for PostgresStore {
    type QueueTable = PostgresQueueTable;
    type MessageTable = PostgresMessageTable;
    type WorkerTable = PostgresWorkerTable;
    type ArchiveTable = PostgresArchiveTable;
    type WorkflowTable = PostgresWorkflowTable;

    type Admin = PostgresAdmin;
    type Producer = PostgresProducer;
    type Consumer = PostgresConsumer;
    type Workflow = PostgresWorkflow;
    type StepGuard = PostgresStepGuard;

    fn queues(&self) -> &Self::QueueTable {
        self.queues.as_ref()
    }

    fn messages(&self) -> &Self::MessageTable {
        self.messages.as_ref()
    }

    fn workers(&self) -> &Self::WorkerTable {
        self.workers.as_ref()
    }

    fn archive(&self) -> &Self::ArchiveTable {
        self.archive.as_ref()
    }

    fn workflows(&self) -> &Self::WorkflowTable {
        self.workflows.as_ref()
    }

    async fn admin(&self, config: &Config) -> crate::error::Result<Self::Admin> {
        PostgresAdmin::new(config).await
    }

    async fn producer(&self, queue: &str, hostname: &str, port: i32, config: &Config) -> crate::error::Result<Self::Producer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        PostgresProducer::new(self.pool.clone(), &queue_info, hostname, port, config).await
    }

    async fn consumer(&self, queue: &str, hostname: &str, port: i32, config: &Config) -> crate::error::Result<Self::Consumer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        PostgresConsumer::new(self.pool.clone(), &queue_info, hostname, port, config).await
    }

    fn workflow(&self, id: i64) -> Self::Workflow {
        PostgresWorkflow::new(self.pool.clone(), id)
    }

    async fn acquire_step(&self, workflow_id: i64, step_id: &str) -> crate::error::Result<Option<Self::StepGuard>> {
         use crate::store::postgres::workflow::guard::StepResult;
         let result = PostgresStepGuard::acquire::<serde_json::Value>(&self.pool, workflow_id, step_id).await?;
         match result {
             StepResult::Execute(guard) => Ok(Some(guard)),
             StepResult::Skipped(_) => Ok(None),
         }
    }
}
