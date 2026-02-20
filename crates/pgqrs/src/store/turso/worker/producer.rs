use crate::error::Result;
use crate::store::turso::tables::messages::TursoMessageTable;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::WorkerTable;
use crate::types::{QueueMessage, QueueRecord, WorkerRecord, WorkerStatus};
use crate::validation::PayloadValidator;
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use turso::Database;

/// Producer interface for enqueueing messages to a specific queue.
pub struct TursoProducer {
    pub db: Arc<Database>,
    queue_info: QueueRecord,
    worker_record: WorkerRecord,
    config: crate::config::Config,
    validator: PayloadValidator,
    messages: Arc<TursoMessageTable>,
    workers: Arc<TursoWorkerTable>,
}

impl TursoProducer {
    pub async fn new(
        db: Arc<Database>,
        queue_info: &QueueRecord,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers_arc = Arc::new(TursoWorkerTable::new(db.clone()));

        let worker_record = workers_arc
            .register(Some(queue_info.id), hostname, port)
            .await?;

        tracing::debug!(
            "Registered producer worker {} ({}:{}) for queue '{}'",
            worker_record.id,
            hostname,
            port,
            queue_info.queue_name
        );
        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db: db.clone(),
            queue_info: queue_info.clone(),
            worker_record,
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers: workers_arc,
            messages,
        })
    }

    pub async fn new_ephemeral(
        db: Arc<Database>,
        queue_info: &QueueRecord,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers_arc = Arc::new(TursoWorkerTable::new(db.clone()));
        let worker_record = workers_arc.register_ephemeral(Some(queue_info.id)).await?;

        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db,
            queue_info: queue_info.clone(),
            worker_record,
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers: workers_arc,
            messages,
        })
    }

    pub fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

#[async_trait]
impl crate::store::Worker for TursoProducer {
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

#[async_trait]
impl crate::store::Producer for TursoProducer {
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
                    producer_worker_id: Some(self.worker_record.id),
                    consumer_worker_id: None,
                },
            )
            .await?;

        // get_by_ids is strict about returning same count if possible, but map_row in MessageTable returns Vec.
        // If some IDs are missing (should not happen in transaction/atomic insert), they won't be returned.
        let queue_messages = self.messages.get_by_ids(&ids).await?;
        Ok(queue_messages)
    }

    async fn enqueue_at(
        &self,
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        use crate::store::MessageTable;
        self.validator.validate(payload)?;

        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));
        let id = self.insert_message(payload, now, vt).await?;
        self.messages.get(id).await
    }

    async fn batch_enqueue_at(
        &self,
        payloads: &[serde_json::Value],
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> Result<Vec<QueueMessage>> {
        use crate::store::MessageTable;
        self.validator.validate_batch(payloads)?;

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
                    producer_worker_id: Some(self.worker_record.id),
                    consumer_worker_id: None,
                },
            )
            .await?;

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
        use crate::types::NewQueueMessage;

        let new_message = NewQueueMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
            producer_worker_id: Some(self.worker_record.id),
            consumer_worker_id: None,
        };

        let message = self.messages.insert(new_message).await?;
        Ok(message.id)
    }

    async fn replay_dlq(&self, archived_msg_id: i64) -> Result<Option<QueueMessage>> {
        use crate::store::turso::format_turso_timestamp;
        // Need manual transaction to update message to be active again
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Begin replay".into(),
            })?;

        let now_str = format_turso_timestamp(&chrono::Utc::now());

        let msg_row_opt = crate::store::turso::query(
            r#"
            UPDATE pgqrs_messages
            SET archived_at = NULL,
                read_ct = 0,
                vt = ?,
                enqueued_at = ?,
                consumer_worker_id = NULL,
                dequeued_at = NULL
            WHERE id = ? AND archived_at IS NOT NULL
            RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
            "#,
        )
        .bind(now_str.clone())
        .bind(now_str)
        .bind(archived_msg_id)
        .fetch_optional_on_connection(&conn)
        .await;

        let msg_row_opt = match msg_row_opt {
            Ok(res) => res,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        if let Some(msg_row) = msg_row_opt {
            conn.execute("COMMIT", ())
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "COMMIT".into(),
                    source: Box::new(e),
                    context: "Commit replay".into(),
                })?;

            let msg = TursoMessageTable::map_row(&msg_row)?;
            Ok(Some(msg))
        } else {
            conn.execute("ROLLBACK", ())
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "ROLLBACK".into(),
                    source: Box::new(e),
                    context: "Rollback replay (not found)".into(),
                })?;
            Ok(None)
        }
    }

    fn validation_config(&self) -> &crate::validation::ValidationConfig {
        &self.config.validation_config
    }

    fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for TursoProducer {
    fn drop(&mut self) {
        if self.worker_record.hostname.starts_with("__ephemeral__") {
            let workers = self.workers.clone();
            let worker_id = self.worker_record.id;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            }
        }
    }
}
