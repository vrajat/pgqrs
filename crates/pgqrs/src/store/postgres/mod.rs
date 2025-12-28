//! Postgres implementation of the Store trait.

use crate::error::Result;
use crate::store::{
    ArchiveTable, ConcurrencyModel, MessageTable, QueueTable, Store, WorkerTable, WorkflowTable,
};
use crate::types::{QueueMessage, SystemStats, WorkerHealthStats, WorkerPurgeResult};
use async_trait::async_trait;
use chrono::Duration;
use sqlx::PgPool;
use std::sync::Arc;

pub mod archive;
pub mod messages;
pub mod queues;
pub mod workers;
pub mod workflows;

#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    queues: Arc<queues::PostgresQueueTable>,
    messages: Arc<messages::PostgresMessageTable>,
    workers: Arc<workers::PostgresWorkerTable>,
    archive: Arc<archive::PostgresArchiveTable>,
    workflows: Arc<workflows::PostgresWorkflowTable>,
}

impl PostgresStore {
    pub fn new(pool: PgPool, max_read_ct: u32) -> Self {
        Self {
            queues: Arc::new(queues::PostgresQueueTable::new(pool.clone())),
            messages: Arc::new(messages::PostgresMessageTable::new(pool.clone(), max_read_ct)),
            workers: Arc::new(workers::PostgresWorkerTable::new(pool.clone())),
            archive: Arc::new(archive::PostgresArchiveTable::new(pool.clone())),
            workflows: Arc::new(workflows::PostgresWorkflowTable::new(pool.clone())),
            pool,
        }
    }
}

#[async_trait]
impl Store for PostgresStore {
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

    async fn install(&self) -> Result<()> {
        // TODO: Move migration logic from Admin here
        todo!("Implement install()")
    }

    async fn verify(&self) -> Result<()> {
        // TODO: Move verification logic from Admin here
        todo!("Implement verify()")
    }

    async fn dlq_batch(&self, _max_attempts: i32) -> Result<Vec<i64>> {
        // TODO: Move DLQ logic from Admin here
        todo!("Implement dlq_batch()")
    }

    async fn replay_from_dlq(&self, _archived_id: i64) -> Result<Option<QueueMessage>> {
        // TODO: Move replay logic from Producer here
        todo!("Implement replay_from_dlq()")
    }

    async fn release_zombie_messages(&self, _worker_timeout: Duration) -> Result<u64> {
        // TODO: Implement zombie message release
        todo!("Implement release_zombie_messages()")
    }

    async fn purge_stale_workers(&self, _timeout: Duration) -> Result<WorkerPurgeResult> {
        // TODO: Implement stale worker purge
        todo!("Implement purge_stale_workers()")
    }

    async fn get_system_stats(&self) -> Result<SystemStats> {
        // TODO: Move system stats from Admin here
        todo!("Implement get_system_stats()")
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::MultiProcess
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }
}
