//! Consumer operations and consumer interface for pgqrs.
//!
//! This module defines the [`Consumer`] struct, which provides methods for dequeuing, and managing jobs in a PostgreSQL-backed queue.
//! For message production, use the [`crate::producer::Producer`] struct.

use crate::error::Result;
use crate::store::postgres::tables::{Messages, Workers};
use crate::store::{MessageTable, WorkerTable};
use crate::{QueueMessage, WorkerStatus};
use async_trait::async_trait;
use sqlx::PgPool;

/// Default visibility timeout in seconds for locked messages
const VISIBILITY_TIMEOUT: u32 = 5;

/// Consumer interface for a specific queue.
///
/// A Consumer instance provides methods for dequeuing messages, reading messages,
/// and managing message lifecycle within a specific PostgreSQL-backed queue.
/// Each Consumer corresponds to a row in the pgqrs_queues table.
///
/// For message production, use the Producer struct.
///
/// Implements the [`Worker`] trait for lifecycle management.
pub struct Consumer {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    queue_info: crate::types::QueueRecord,
    /// Configuration for the queue
    _config: crate::config::Config,
    /// Worker record for this consumer
    worker_record: crate::types::WorkerRecord,
    /// Worker lifecycle manager (Workers repository handles this now)
    workers: Workers,
    messages: Messages,
}

impl Consumer {
    pub async fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueRecord,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_record = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        tracing::debug!(
            "Registered consumer worker {} ({}:{}) for queue '{}'",
            worker_record.id,
            hostname,
            port,
            queue_info.queue_name
        );

        let messages = Messages::new(pool.clone());

        Ok(Self {
            pool: pool.clone(),
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }

    /// Create an ephemeral consumer (NULL hostname/port, auto-cleanup).
    ///
    /// Used by high-level API functions like `consume()`.
    pub async fn new_ephemeral(
        pool: PgPool,
        queue_info: &crate::types::QueueRecord,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_record = workers.register_ephemeral(Some(queue_info.id)).await?;

        tracing::debug!(
            "Registered ephemeral consumer worker {} for queue '{}'",
            worker_record.id,
            queue_info.queue_name
        );

        let messages = Messages::new(pool.clone());

        Ok(Self {
            pool: pool.clone(),
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl crate::store::Worker for Consumer {
    fn worker_record(&self) -> &crate::types::WorkerRecord {
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

    /// Suspend this consumer (transition from Ready to Suspended).
    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_record.id).await
    }

    /// Resume this consumer (transition from Suspended to Ready).
    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_record.id).await
    }

    /// Gracefully shutdown this consumer.
    async fn shutdown(&self) -> Result<()> {
        let pending_count = self
            .messages
            .count_pending_for_queue_and_worker(self.queue_info.id, self.worker_record.id)
            .await?;

        if pending_count > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending_count as u64,
                reason: "Consumer must release all messages before shutdown".to_string(),
            });
        }

        self.workers.shutdown(self.worker_record.id).await
    }
}

#[async_trait]
impl crate::store::Consumer for Consumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, VISIBILITY_TIMEOUT)
            .await
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
        let rows_affected = self
            .messages
            .extend_visibility(message_id, self.worker_record.id, additional_seconds)
            .await?;
        Ok(rows_affected > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows_affected = self
            .messages
            .delete_owned(message_id, self.worker_record.id)
            .await?;
        Ok(rows_affected > 0)
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
        let results = self
            .messages
            .release_messages_by_ids(message_ids, self.worker_record.id)
            .await?;
        // Count how many were successfully released
        Ok(results.into_iter().filter(|&released| released).count() as u64)
    }

    async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        let rows_affected = self
            .messages
            .release_with_visibility(message_id, self.worker_record.id, visible_at)
            .await?;
        Ok(rows_affected > 0)
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for Consumer {
    fn drop(&mut self) {
        // Check if this is an ephemeral worker by hostname prefix
        if self.worker_record.hostname.starts_with("__ephemeral__") {
            // Spawn a task to properly shutdown the worker
            let workers = self.workers.clone();
            let worker_id = self.worker_record.id;

            // Best-effort shutdown - check if we are in a runtime context
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    // Suspend then shutdown the worker
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            } else {
                tracing::warn!(
                    "Skipping ephemeral worker cleanup for {} - no tokio runtime available",
                    worker_id
                );
            }
        }
    }
}
