//! Queue operations and producer interface for pgqrs.
//!
//! This module defines the [`Queue`] struct, which provides methods for enqueuing, dequeuing, and managing jobs in a PostgreSQL-backed queue.
//!
//! ## What
//!
//! - [`Queue`] is the main interface for interacting with a queue: adding jobs, fetching jobs, updating visibility, and batch operations.
//!
//! ## How
//!
//! Create a [`Queue`] using the admin API, then use its methods to enqueue and process jobs.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::queue::Queue;
//! // let queue = ...
//! // queue.enqueue(...)
//! ```
use crate::constants::{
    ARCHIVE_BATCH, ARCHIVE_MESSAGE, DELETE_MESSAGE_BATCH, DEQUEUE_MESSAGES, INSERT_MESSAGE,
    PENDING_COUNT, SELECT_MESSAGE_BY_ID, UPDATE_MESSAGE_VT, VISIBILITY_TIMEOUT,
};
use crate::error::Result;
use crate::types::{ArchivedMessage, QueueMessage};
use crate::WorkerInfo;
use chrono::Utc;
use sqlx::PgPool;

// SQL query constants
const SELECT_MESSAGES_BY_IDS: &str = r#"
    SELECT id, queue_id, worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
    FROM pgqrs_messages
    WHERE id = ANY($1)
"#;

/// Producer and consumer interface for a specific queue.
///
/// A Queue instance provides methods for enqueuing messages, reading messages,
/// and managing message lifecycle within a specific PostgreSQL-backed queue.
/// Each Queue corresponds to a row in the pgqrs_queues table.
pub struct Queue {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    /// Database ID of the queue from pgqrs_queues table
    pub queue_id: i64,
    /// Logical name of the queue
    pub queue_name: String,
}

impl Queue {
    /// Create a new Queue instance for the specified queue ID and name.
    ///
    /// This method creates a Queue instance that operates on the unified
    /// pgqrs_messages table using the provided queue_id for filtering operations.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_id` - Database ID of the queue from pgqrs_queues table
    /// * `queue_name` - Name of the queue (for display/logging purposes)
    pub(crate) fn new(pool: PgPool, queue_id: i64, queue_name: &str) -> Self {
        Self {
            pool,
            queue_id,
            queue_name: queue_name.to_string(),
        }
    }

    /// Retrieve a message by its ID from the queue.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to retrieve
    ///
    /// # Returns
    /// The message if found, or an error if not found.
    pub async fn get_message_by_id(&self, msg_id: i64) -> Result<QueueMessage> {
        let result = sqlx::query_as::<_, QueueMessage>(SELECT_MESSAGE_BY_ID)
            .bind(msg_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    /// Add a single message to the queue.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    ///
    /// # Returns
    /// The enqueued message.
    pub async fn enqueue(&self, payload: &serde_json::Value) -> Result<QueueMessage> {
        self.enqueue_delayed(payload, 0).await
    }

    /// Schedule a message to be available for consumption after a delay.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `delay_seconds` - Seconds to delay before message becomes available
    ///
    /// # Returns
    /// The enqueued message.
    pub async fn enqueue_delayed(
        &self,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(delay_seconds as i64);
        let id = self.insert_message(payload, now, vt).await?;
        self.get_message_by_id(id).await
    }

    /// Internal method to insert a message with specific timestamps.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `now` - Current timestamp for enqueued_at field
    /// * `vt` - Visibility timeout timestamp (when message becomes available)
    ///
    /// # Returns
    /// The ID of the inserted message.
    async fn insert_message(
        &self,
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(INSERT_MESSAGE)
            .bind(self.queue_id)
            .bind(payload)
            .bind(0_i32) // read_ct
            .bind(now)
            .bind(vt)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    /// Add multiple messages to the queue in a single batch operation.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to enqueue
    ///
    /// # Returns
    /// Vector of enqueued messages.
    pub async fn batch_enqueue(&self, payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(0);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;

        let mut ids = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let id = sqlx::query_scalar::<_, i64>(INSERT_MESSAGE)
                .bind(self.queue_id)
                .bind(payload)
                .bind(0_i32) // read_ct
                .bind(now)
                .bind(vt)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            ids.push(id);
        }

        tx.commit()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;

        // Fetch all messages in a single query using WHERE msg_id = ANY($1)
        let queue_messages = sqlx::query_as::<_, QueueMessage>(SELECT_MESSAGES_BY_IDS)
            .bind(&ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(queue_messages)
    }

    /// Get the count of pending (not locked) messages in the queue.
    ///
    /// # Returns
    /// Number of pending messages.
    pub async fn pending_count(&self) -> Result<i64> {
        use chrono::Utc;
        let now = Utc::now();

        let count = sqlx::query_scalar::<_, i64>(PENDING_COUNT)
            .bind(self.queue_id)
            .bind(now)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(count)
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
            .bind(self.queue_id)
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

    /// Extend the lock time for a message, preventing it from becoming visible again.
    ///
    /// # Arguments
    /// * `message_id` - ID of the message
    /// * `additional_seconds` - Additional seconds to extend the visibility
    ///
    /// # Returns
    /// True if the message's visibility was extended, false otherwise.
    pub async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<bool> {
        let updated = sqlx::query(UPDATE_MESSAGE_VT)
            .bind(additional_seconds as i32)
            .bind(message_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(updated.rows_affected() > 0)
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
                message: format!("Failed to archive message {}: {}", msg_id, e),
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
                message: format!("Failed to archive batch messages: {}", e),
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
