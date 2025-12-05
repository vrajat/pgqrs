//! Consumer operations and consumer interface for pgqrs.
//!
//! This module defines the [`Consumer`] struct, which provides methods for dequeuing, and managing jobs in a PostgreSQL-backed queue.
//! For message production, use the [`crate::producer::Producer`] struct.
//!
//! ## What
//!
//! - [`Consumer`] is the consumer interface for interacting with a queue: fetching jobs, updating visibility, archiving and deleting messages.
//! - [`crate::producer::Producer`] handles message production and is defined in the `producer` module.
//!
//! ## How
//!
//! Create a [`Consumer`] using the admin API, then use its methods to process jobs.
//! Create a [`crate::producer::Producer`] for enqueueing messages.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::consumer::Consumer;
//! use pgqrs::producer::Producer;
//! // let consumer = ...
//! // let producer = ...
//! // producer.enqueue(...)
//! // consumer.dequeue(...)
//! ```
use crate::constants::{ARCHIVE_BATCH, ARCHIVE_MESSAGE, DELETE_MESSAGE_BATCH, VISIBILITY_TIMEOUT};
use crate::error::Result;
use crate::tables::PgqrsMessages;
use crate::types::{ArchivedMessage, QueueMessage};
use crate::{admin::PgqrsAdmin, PgqrsError};
use sqlx::PgPool;

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
pub struct Consumer {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    queue_info: crate::types::QueueInfo,
    /// Configuration for the queue
    config: crate::config::Config,
    /// Worker information for this consumer
    worker_info: crate::types::WorkerInfo,
    /// Admin interface for worker management
    admin: PgqrsAdmin,
}

impl Consumer {
    /// Create a new Consumer instance for the specified queue.
    ///
    /// This method creates a Consumer instance that operates on the unified
    /// pgqrs_messages table using the provided queue_info for filtering operations.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_info` - Queue information including ID and name
    /// * `config` - Configuration for the queue
    pub async fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        worker_info: &crate::types::WorkerInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        // Validate worker is active and matches queue
        if worker_info.queue_id != queue_info.id {
            return Err(PgqrsError::ValidationFailed {
                reason: "Worker must be registered for this queue".to_string(),
            });
        }
        if !matches!(worker_info.status, crate::types::WorkerStatus::Ready) {
            return Err(PgqrsError::ValidationFailed {
                reason: "Worker must be active".to_string(),
            });
        }
        let admin = PgqrsAdmin::new(config).await?;
        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info: worker_info.clone(),
            config: config.clone(),
            admin,
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
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    pub async fn delete(&self, message_id: i64) -> Result<bool> {
        let deleted_ids: Vec<i64> = sqlx::query_scalar(DELETE_MESSAGE_BATCH)
            .bind(vec![message_id])
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(deleted_ids.contains(&message_id))
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
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
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
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
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
    pub async fn archive_batch(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        if msg_ids.is_empty() {
            return Ok(vec![]);
        }

        let archived_ids: Vec<i64> = sqlx::query_scalar(ARCHIVE_BATCH)
            .bind(&msg_ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
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

    /// Gracefully shutdown this consumer.
    ///
    /// Releases messages that are held by this consumer but haven't started processing,
    /// then transitions the worker status from Ready to ShuttingDown, then to Stopped.
    ///
    /// The shutdown process:
    /// 1. Identifies all messages currently held by this worker
    /// 2. Releases messages that are NOT in the `in_progress_ids` list (these are messages
    ///    that were dequeued but haven't started processing yet)
    /// 3. Messages in `in_progress_ids` are left alone to allow current processing to complete
    /// 4. Transitions worker status: Ready → ShuttingDown → Stopped
    ///
    /// # Arguments
    /// * `in_progress_ids` - Message IDs that are currently being processed and should NOT be released
    ///
    /// # Returns
    /// Ok if shutdown completes successfully
    pub async fn shutdown(&self, in_progress_ids: Vec<i64>) -> Result<()> {
        // Get all messages currently held by this worker
        let worker_messages = self.admin.get_worker_messages(self.worker_info.id).await?;

        // Extract IDs of messages held by this worker
        let held_message_ids: Vec<i64> = worker_messages.iter().map(|msg| msg.id).collect();

        // Filter out messages that are currently in progress
        let in_progress_set: std::collections::HashSet<i64> = in_progress_ids.into_iter().collect();
        let messages_to_release: Vec<i64> = held_message_ids
            .into_iter()
            .filter(|id| !in_progress_set.contains(id))
            .collect();

        // Release messages that haven't started processing
        if !messages_to_release.is_empty() {
            let messages = PgqrsMessages::new(self.pool.clone());
            messages
                .release_messages_by_ids(&messages_to_release, self.worker_info.id)
                .await?;
        }

        // Transition worker status
        self.admin.begin_shutdown(self.worker_info.id).await?;
        self.admin.mark_stopped(self.worker_info.id).await?;

        Ok(())
    }
}
