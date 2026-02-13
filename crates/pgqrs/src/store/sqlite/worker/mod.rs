pub mod admin;
pub mod consumer;
pub mod producer;

use crate::error::Result;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::Worker;
use crate::types::{WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::sync::Arc;

pub struct SqliteWorkerHandle {
    worker_record: WorkerRecord,
    workers: Arc<SqliteWorkerTable>,
}

impl SqliteWorkerHandle {
    pub fn new(pool: SqlitePool, worker_record: WorkerRecord) -> Self {
        Self {
            workers: Arc::new(SqliteWorkerTable::new(pool)),
            worker_record,
        }
    }
}

#[async_trait]
impl Worker for SqliteWorkerHandle {
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
