
//! Postgres implementation of the Store trait.

use crate::store::{Store, QueueStore, MessageStore, WorkerStore, ArchiveStore};
use sqlx::PgPool;
use std::sync::Arc;

pub mod queues;
pub mod messages;
pub mod workers;
pub mod workflows;
pub mod archive;

#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    queues: Arc<queues::PostgresQueueStore>,
    messages: Arc<messages::PostgresMessageStore>,
    workers: Arc<workers::PostgresWorkerStore>,
    archive: Arc<archive::PostgresArchiveStore>,
    workflows: Arc<workflows::PostgresWorkflowStore>,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool: pool.clone(),
            queues: Arc::new(queues::PostgresQueueStore::new(pool.clone())),
            messages: Arc::new(messages::PostgresMessageStore::new(pool.clone())),
            workers: Arc::new(workers::PostgresWorkerStore::new(pool.clone())),
            archive: Arc::new(archive::PostgresArchiveStore::new(pool.clone())),
            workflows: Arc::new(workflows::PostgresWorkflowStore::new(pool)),
        }
    }
}

#[async_trait::async_trait]
impl Store for PostgresStore {
    type Error = sqlx::Error;

    type QueueStore = queues::PostgresQueueStore;
    type MessageStore = messages::PostgresMessageStore;
    type WorkerStore = workers::PostgresWorkerStore;
    type ArchiveStore = archive::PostgresArchiveStore;
    type WorkflowStore = workflows::PostgresWorkflowStore;

    fn queues(&self) -> &Self::QueueStore {
        &self.queues
    }

    fn messages(&self) -> &Self::MessageStore {
        &self.messages
    }

    fn workers(&self) -> &Self::WorkerStore {
        &self.workers
    }

    fn archive(&self) -> &Self::ArchiveStore {
        &self.archive
    }

    fn workflows(&self) -> &Self::WorkflowStore {
        &self.workflows
    }
}

// Ensure the submodules are accessible appropriately
// (We will create these files next)
