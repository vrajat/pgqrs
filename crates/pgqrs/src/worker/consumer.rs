//! Consumer operations and consumer interface for pgqrs.
//!
//! This module defines the [`Consumer`] struct, which provides methods for dequeuing, and managing jobs in a PostgreSQL-backed queue.
//! For message production, use the [`crate::producer::Producer`] struct.
//!
//! ## What
//!
//! - [`Consumer`] is the consumer interface for interacting with a queue: fetching jobs, updating visibility, archiving and deleting messages.
//! - [`crate::producer::Producer`] handles message production and is defined in the `producer` module.
//! - Implements the [`Worker`] trait for lifecycle management
//!
//! ## How
//!
//! Create a [`Consumer`] using `Consumer::register()` which handles worker registration automatically.
//! Create a [`crate::producer::Producer`] for enqueueing messages.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::consumer::Consumer;
//! use pgqrs::producer::Producer;
//! // let consumer = Consumer::register(pool, &queue_info, "localhost", 8080, &config).await?;
//! // let producer = Producer::register(pool, &queue_info, "localhost", 8081, &config).await?;
//! // producer.enqueue(...)
//! // consumer.dequeue(...)
//! ```

use crate::error::Result;
use crate::tables::Messages;
use crate::types::{ArchivedMessage, QueueMessage, WorkerStatus};
use crate::worker::{Worker, WorkerLifecycle};
use async_trait::async_trait;
use sqlx::PgPool;

/// Default visibility timeout in seconds for locked messages
const VISIBILITY_TIMEOUT: u32 = 5;

/// Delete batch of messages
const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

/// Archive single message (atomic operation)
const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_messages
        WHERE id = $1 AND consumer_worker_id = $2
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msg
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at;
"#;

/// Archive batch of messages (efficient batch operation)
const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE id = ANY($1) AND consumer_worker_id = $2
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

/// Read available messages from queue (with SKIP LOCKED)
/// Select messages for worker with lock
pub const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages t
    SET consumer_worker_id = $5, vt = NOW() + make_interval(secs => $4::double precision), read_ct = read_ct + 1, dequeued_at = NOW()
    FROM (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1 AND (vt IS NULL OR vt <= NOW()) AND consumer_worker_id IS NULL AND read_ct < $3
        ORDER BY id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    ) selected
    WHERE t.id = selected.id
    RETURNING t.id, t.queue_id, t.producer_worker_id, t.consumer_worker_id, t.payload, t.vt, t.enqueued_at, t.read_ct, t.dequeued_at;
"#;

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
    queue_info: crate::types::QueueInfo,
    /// Configuration for the queue
    config: crate::config::Config,
    /// Worker information for this consumer
    worker_info: crate::types::WorkerInfo,
    /// Worker lifecycle manager
    lifecycle: WorkerLifecycle,
}

impl Consumer {
    pub async fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let lifecycle = WorkerLifecycle::new(pool.clone());
        let worker_info = lifecycle.register(queue_info, hostname, port).await?;
        tracing::debug!(
            "Registered consumer worker {} ({}:{}) for queue '{}'",
            worker_info.id,
            hostname,
            port,
            queue_info.queue_name
        );

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info,
            config: config.clone(),
            lifecycle,
        })
    }

    /// Read up to `limit` messages from the queue, using the default visibility timeout.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    pub async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, VISIBILITY_TIMEOUT)
            .await
    }

    pub async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    /// Read up to `limit` messages from the queue, with a custom visibility timeout.
    ///
    /// # Arguments
    /// * `vt` - Visibility timeout (seconds)
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn dequeue_many_with_delay(
        &self,
        limit: usize,
        vt: u32,
    ) -> Result<Vec<QueueMessage>> {
        let result = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES)
            .bind(self.queue_info.id)
            .bind(limit as i64)
            .bind(self.config.max_read_ct)
            .bind(vt as i32)
            .bind(self.worker_info.id) // worker_id
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    pub async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<bool> {
        let messages = Messages::new(self.pool.clone());
        let rows_affected = messages
            .extend_visibility(message_id, self.worker_info.id, additional_seconds)
            .await?;
        Ok(rows_affected > 0)
    }

    /// Permanently delete a message from the queue.
    ///
    /// This method removes the message with the specified `message_id` from the `pgqrs_messages` table.
    /// Unlike [`archive`](Self::archive), which moves messages to an archive table for potential recovery or auditing,
    /// this method deletes the message permanently and it cannot be recovered.
    ///
    /// # Arguments
    ///
    /// * `message_id` - The ID of the message to delete.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if a message was deleted, or `Ok(false)` if no message with the given ID was found.
    /// Returns an error if the database operation fails.
    pub async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows_affected =
            sqlx::query("DELETE FROM pgqrs_messages WHERE id = $1 AND consumer_worker_id = $2")
                .bind(message_id)
                .bind(self.worker_info.id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::Connection {
                    message: e.to_string(),
                })?
                .rows_affected();

        Ok(rows_affected > 0)
    }

    /// Remove a batch of messages from the queue.
    ///
    /// # Arguments
    /// * `message_ids` - Vector of message IDs to delete
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input).
    pub async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        let deleted_ids: Vec<i64> = sqlx::query_scalar(DELETE_MESSAGE_BATCH)
            .bind(&message_ids)
            .bind(self.worker_info.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: e.to_string(),
            })?;

        // For each input id, true if it was deleted, false otherwise
        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = message_ids
            .into_iter()
            .map(|id| deleted_set.contains(&id))
            .collect();
        Ok(result)
    }

    /// Archive a single message (PREFERRED over delete for data retention).
    ///
    /// Moves message from active queue to archive table with tracking metadata.
    /// This is an atomic operation that deletes from the queue and inserts into archive.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to archive
    ///
    /// # Returns
    /// True if message was successfully archived, false if message was not found
    pub async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
        let result: Option<ArchivedMessage> = sqlx::query_as::<_, ArchivedMessage>(ARCHIVE_MESSAGE)
            .bind(msg_id)
            .bind(self.worker_info.id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to archive message {msg_id}: {e}"),
            })?;

        Ok(result)
    }

    /// Archive multiple messages in a single transaction.
    ///
    /// More efficient than individual archive calls. Atomically moves messages
    /// from active queue to archive table.
    ///
    /// # Arguments
    /// * `msg_ids` - Vector of message IDs to archive
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input).
    pub async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        if msg_ids.is_empty() {
            return Ok(vec![]);
        }

        let archived_ids: Vec<i64> = sqlx::query_scalar(ARCHIVE_BATCH)
            .bind(&msg_ids)
            .bind(self.worker_info.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to archive batch messages: {e}"),
            })?;

        // For each input id, true if it was archived, false otherwise
        let archived_set: std::collections::HashSet<i64> = archived_ids.into_iter().collect();
        let result = msg_ids
            .into_iter()
            .map(|id| archived_set.contains(&id))
            .collect();
        Ok(result)
    }

    /// Release messages currently held by this consumer.
    ///
    /// This makes the messages available for other consumers to pick up.
    ///
    /// # Arguments
    /// * `message_ids` - Message IDs to release. Only messages held by this consumer will be released.
    ///
    /// # Returns
    /// Number of messages released.
    pub async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
        let messages = Messages::new(self.pool.clone());
        let results = messages
            .release_messages_by_ids(message_ids, self.worker_info.id)
            .await?;
        // Count how many were successfully released
        Ok(results.into_iter().filter(|&released| released).count() as u64)
    }
}

#[async_trait]
impl Worker for Consumer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.lifecycle.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.lifecycle
            .is_healthy(self.worker_info.id, max_age)
            .await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.lifecycle.get_status(self.worker_info.id).await
    }

    /// Suspend this consumer (transition from Ready to Suspended).
    ///
    /// # Preconditions
    /// - Consumer must be in `Ready` state
    ///
    /// # Returns
    /// Ok if suspension succeeds, error if consumer is in wrong state.
    async fn suspend(&self) -> Result<()> {
        self.lifecycle.suspend(self.worker_info.id).await
    }

    /// Resume this consumer (transition from Suspended to Ready).
    async fn resume(&self) -> Result<()> {
        self.lifecycle.resume(self.worker_info.id).await
    }

    /// Gracefully shutdown this consumer.
    ///
    /// # Preconditions
    /// - Consumer must be in Suspended state
    /// - Consumer must have no pending (held) messages
    ///
    /// Use `release_messages()` to release held messages before calling `shutdown()`.
    ///
    /// # Returns
    /// Ok if shutdown completes successfully
    async fn shutdown(&self) -> Result<()> {
        // Check if consumer has pending messages
        let pending_count = self
            .lifecycle
            .count_pending_messages(self.worker_info.id)
            .await?;

        if pending_count > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending_count as u64,
                reason: "Consumer must release all messages before shutdown".to_string(),
            });
        }

        self.lifecycle.shutdown(self.worker_info.id).await
    }
}
