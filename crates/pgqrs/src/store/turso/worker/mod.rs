pub mod admin;
pub mod consumer;
pub mod producer;

use crate::error::Result;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::Worker;
use crate::types::WorkerStatus;
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

pub struct TursoWorkerHandle {
    id: i64,
    workers: Arc<TursoWorkerTable>,
}

impl TursoWorkerHandle {
    pub fn new(db: Arc<Database>, id: i64) -> Self {
        Self {
            workers: Arc::new(TursoWorkerTable::new(db)),
            id,
        }
    }
}

#[async_trait]
impl Worker for TursoWorkerHandle {
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
