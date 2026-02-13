pub mod admin;
pub mod consumer;
pub mod producer;

use crate::error::Result;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::Worker;
use crate::types::{WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

pub struct TursoWorkerHandle {
    worker_record: WorkerRecord,
    workers: Arc<TursoWorkerTable>,
}

impl TursoWorkerHandle {
    pub fn new(db: Arc<Database>, worker_record: WorkerRecord) -> Self {
        Self {
            workers: Arc::new(TursoWorkerTable::new(db)),
            worker_record,
        }
    }
}

#[async_trait]
impl Worker for TursoWorkerHandle {
    fn worker_record(&self) -> &WorkerRecord {
        &self.worker_record
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_record.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers
            .is_healthy(self.worker_record.id, max_age)
            .await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_record.id).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_record.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_record.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_record.id).await
    }
}
