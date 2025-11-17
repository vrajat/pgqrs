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
use crate::constants::{
    ARCHIVE_BATCH, ARCHIVE_MESSAGE, DELETE_MESSAGE_BATCH, DEQUEUE_MESSAGES, VISIBILITY_TIMEOUT,
};
use crate::error::Result;
use crate::tables::{PgqrsMessages, Table};
use crate::types::{ArchivedMessage, QueueMessage};
use crate::WorkerInfo;
use sqlx::PgPool;

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
    /// Messages table operations
    messages: PgqrsMessages,
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
    pub fn new(pool: PgPool, queue_info: &crate::types::QueueInfo) -> Self {
        let messages = PgqrsMessages::new(pool.clone());
        Self {
            pool,
            queue_info: queue_info.clone(),
            messages,
        }
    }

    /// Retrieve a message by its ID from the queue.
    ///
    /// This is useful for consumers to fetch specific messages.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to retrieve
    ///
    /// # Returns
    /// The message if found, or an error if not found.
    pub async fn get_message_by_id(&self, msg_id: i64) -> Result<QueueMessage> {
        self.messages.get(msg_id).await
    }

    /// Get the count of pending (not locked) messages in the queue.
    ///
    /// # Returns
    /// Number of pending messages.
    pub async fn pending_count(&self) -> Result<i64> {
        self.messages.count_pending(self.queue_info.id).await
    }

    /// Read up to `limit` messages from the queue, using the default visibility timeout.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn dequeue(&self, worker: &WorkerInfo) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(worker, 1).await
    }

    pub async fn dequeue_many(
        &self,
        worker: &WorkerInfo,
        limit: usize,
    ) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(worker, limit, VISIBILITY_TIMEOUT)
            .await
    }

    pub async fn dequeue_delay(&self, worker: &WorkerInfo, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(worker, 1, vt).await
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
        worker: &WorkerInfo,
        limit: usize,
        vt: u32,
    ) -> Result<Vec<QueueMessage>> {
        let result = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES)
            .bind(self.queue_info.id)
            .bind(limit as i64)
            .bind(vt as i32)
            .bind(worker.id) // worker_id
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
}
