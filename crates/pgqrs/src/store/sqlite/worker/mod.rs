pub mod admin;
pub mod consumer;
pub mod producer;

use crate::error::Result;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::Worker;
use crate::types::WorkerStatus;
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::sync::Arc;

pub struct SqliteWorkerHandle {
    id: i64,
    workers: Arc<SqliteWorkerTable>,
}

impl SqliteWorkerHandle {
    pub fn new(pool: SqlitePool, id: i64) -> Self {
        Self {
            workers: Arc::new(SqliteWorkerTable::new(pool)),
            id,
        }
    }
}

#[async_trait]
impl Worker for SqliteWorkerHandle {
    fn worker_id(&self) -> i64 {
        self.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers.is_healthy(self.id, max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.id).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.id).await
    }
}
