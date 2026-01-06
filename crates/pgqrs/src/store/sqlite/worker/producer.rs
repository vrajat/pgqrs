use crate::error::Result;
use crate::store::sqlite::tables::archive::SqliteArchiveTable;
use crate::store::sqlite::tables::messages::SqliteMessageTable;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::WorkerTable;
use crate::types::{QueueInfo, QueueMessage, WorkerStatus};
use crate::validation::PayloadValidator;
use async_trait::async_trait;
use chrono::Utc;
use sqlx::SqlitePool;
use std::sync::Arc;

/// Producer interface for enqueueing messages to a specific queue.
pub struct SqliteProducer {
    pub pool: SqlitePool,
    queue_info: QueueInfo,
    worker_info: crate::types::WorkerInfo,
    config: crate::config::Config,
    validator: PayloadValidator,
    messages: Arc<SqliteMessageTable>,
    workers: Arc<SqliteWorkerTable>,
    archive: Arc<SqliteArchiveTable>, // Needed for replay_dlq
}

impl SqliteProducer {
    pub async fn new(
        pool: SqlitePool,
        queue_info: &QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers_arc = Arc::new(SqliteWorkerTable::new(pool.clone()));

        let worker_info = workers_arc
            .register(Some(queue_info.id), hostname, port)
            .await?;

        tracing::debug!(
            "Registered producer worker {} ({}:{}) for queue '{}'",
            worker_info.id,
            hostname,
            port,
            queue_info.queue_name
        );
        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));
        let archive = Arc::new(SqliteArchiveTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info: worker_info.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers: workers_arc,
            messages,
            archive,
        })
    }

    pub async fn new_ephemeral(
        pool: SqlitePool,
        queue_info: &QueueInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers_arc = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_info = workers_arc.register_ephemeral(Some(queue_info.id)).await?;

        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));
        let archive = Arc::new(SqliteArchiveTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info: worker_info.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers: workers_arc,
            messages,
            archive,
        })
    }

    pub fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

#[async_trait]
impl crate::store::Worker for SqliteProducer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers.is_healthy(self.worker_info.id, max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_info.id).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_info.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_info.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_info.id).await
    }
}

#[async_trait]
impl crate::store::Producer for SqliteProducer {
    async fn get_message_by_id(&self, msg_id: i64) -> Result<QueueMessage> {
        use crate::store::MessageTable;
        self.messages.get(msg_id).await
    }

    async fn enqueue(&self, payload: &serde_json::Value) -> Result<QueueMessage> {
        self.enqueue_delayed(payload, 0).await
    }

    async fn enqueue_delayed(
        &self,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        use crate::store::MessageTable;
        self.validator.validate(payload)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));
        let id = self.insert_message(payload, now, vt).await?;
        self.messages.get(id).await
    }

    async fn batch_enqueue(&self, payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        self.batch_enqueue_delayed(payloads, 0).await
    }

    async fn batch_enqueue_delayed(
        &self,
        payloads: &[serde_json::Value],
        delay_seconds: u32,
    ) -> Result<Vec<QueueMessage>> {
        use crate::store::MessageTable;
        self.validator.validate_batch(payloads)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));

        let ids = self
            .messages
            .batch_insert(
                self.queue_info.id,
                payloads,
                crate::types::BatchInsertParams {
                    read_ct: 0,
                    enqueued_at: now,
                    vt,
                    producer_worker_id: Some(self.worker_info.id),
                    consumer_worker_id: None,
                },
            )
            .await?;

        // get_by_ids is strict about returning same count if possible, but map_row in MessageTable returns Vec.
        // If some IDs are missing (should not happen in transaction/atomic insert), they won't be returned.
        let queue_messages = self.messages.get_by_ids(&ids).await?;
        Ok(queue_messages)
    }

    async fn insert_message(
        &self,
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        use crate::store::MessageTable;
        use crate::types::NewMessage;

        let new_message = NewMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
            producer_worker_id: Some(self.worker_info.id),
            consumer_worker_id: None,
        };

        let message = self.messages.insert(new_message).await?;
        Ok(message.id)
    }

    async fn replay_dlq(&self, archived_msg_id: i64) -> Result<Option<QueueMessage>> {
        use crate::store::ArchiveTable; // Import trait to use method
                                        // Delegate to ArchiveTable logic which handles transaction safe replay
        self.archive.replay_message(archived_msg_id).await
    }

    fn validation_config(&self) -> &crate::validation::ValidationConfig {
        &self.config.validation_config
    }

    fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for SqliteProducer {
    fn drop(&mut self) {
        if self.worker_info.hostname.starts_with("__ephemeral__") {
            let workers = self.workers.clone();
            let worker_id = self.worker_info.id;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            }
        }
    }
}
