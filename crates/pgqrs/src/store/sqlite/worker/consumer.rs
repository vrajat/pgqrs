use crate::error::Result;
use crate::store::sqlite::tables::messages::SqliteMessageTable;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::{MessageTable, WorkerTable};
use crate::types::{QueueMessage, QueueRecord, WorkerRecord, WorkerStatus};

use async_trait::async_trait;
use sqlx::sqlite::SqlitePool;
use std::sync::Arc;

pub struct SqliteConsumer {
    pub pool: SqlitePool,
    queue_info: QueueRecord,
    worker_record: WorkerRecord,
    _config: crate::config::Config,
    workers: Arc<SqliteWorkerTable>,
    messages: Arc<SqliteMessageTable>,
}

impl SqliteConsumer {
    pub async fn new(
        pool: SqlitePool,
        queue_info: &QueueRecord,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_record = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }

    pub async fn new_ephemeral(
        pool: SqlitePool,
        queue_info: &QueueRecord,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_record = workers.register_ephemeral(Some(queue_info.id)).await?;
        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl crate::store::Worker for SqliteConsumer {
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
        let pending = self
            .messages
            .count_pending_for_queue_and_worker(self.queue_info.id, self.worker_record.id)
            .await?;

        if pending > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending as u64,
                reason: format!("Consumer has {} pending messages", pending),
            });
        }
        self.workers.shutdown(self.worker_record.id).await
    }
}

#[async_trait]
impl crate::store::Consumer for SqliteConsumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, 5).await // Default 5s VT like Postgres
    }

    async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    async fn dequeue_many_with_delay(&self, limit: usize, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_at(limit, vt, chrono::Utc::now()).await
    }

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<QueueMessage>> {
        self.messages
            .dequeue_at(
                self.queue_info.id,
                limit,
                vt,
                self.worker_record.id,
                now,
                self._config.max_read_ct,
            )
            .await
    }

    async fn extend_visibility(&self, message_id: i64, additional_seconds: u32) -> Result<bool> {
        let c = self
            .messages
            .extend_visibility(message_id, self.worker_record.id, additional_seconds)
            .await?;
        Ok(c > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows = self
            .messages
            .delete_owned(message_id, self.worker_record.id)
            .await?;
        Ok(rows > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        self.messages
            .delete_many_owned(&message_ids, self.worker_record.id)
            .await
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        self.messages.archive(msg_id, self.worker_record.id).await
    }

    async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        self.messages
            .archive_many(&msg_ids, self.worker_record.id)
            .await
    }

    async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
        let res = self
            .messages
            .release_messages_by_ids(message_ids, self.worker_record.id)
            .await?;
        Ok(res.iter().filter(|&&b| b).count() as u64)
    }

    async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        let count = self
            .messages
            .release_with_visibility(message_id, self.worker_record.id, visible_at)
            .await?;
        Ok(count > 0)
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for SqliteConsumer {
    fn drop(&mut self) {
        if self.worker_record.hostname.starts_with("__ephemeral__") {
            let workers = self.workers.clone();
            let worker_id = self.worker_record.id;

            // Best effort spawn
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            }
        }
    }
}
