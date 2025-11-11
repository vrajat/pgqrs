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
    ARCHIVE_BATCH, ARCHIVE_MESSAGE, DELETE_MESSAGE_BATCH, DEQUEUE_MESSAGES, VISIBILITY_TIMEOUT,
};
use crate::error::Result;
use crate::tables::{PgqrsMessages, Table};
use crate::types::{ArchivedMessage, QueueMessage};
use crate::validation::PayloadValidator;
use crate::WorkerInfo;
use chrono::Utc;
use sqlx::PgPool;

/// Producer and consumer interface for a specific queue.
///
/// A Queue instance provides methods for enqueuing messages, reading messages,
/// and managing message lifecycle within a specific PostgreSQL-backed queue.
/// Each Queue corresponds to a row in the pgqrs_queues table.
pub struct Queue {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    queue_info: crate::types::QueueInfo,
    /// Configuration for the queue including validation settings
    config: crate::config::Config,
    /// Payload validator for this queue
    validator: PayloadValidator,
    /// Messages table operations
    messages: PgqrsMessages,
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
    /// * `config` - Configuration including validation settings
    pub fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        config: &crate::config::Config,
    ) -> Self {
        let messages = PgqrsMessages::new(pool.clone());
        Self {
            pool,
            queue_info: queue_info.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            messages,
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
        self.messages.get(msg_id).await
    }

    /// Add a single message to the queue.
    ///
    /// This method validates the payload according to the queue's validation configuration
    /// before enqueueing. Validation includes rate limiting, size checks, structure validation,
    /// and content filtering based on the configuration.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    ///
    /// # Returns
    /// The enqueued message if validation passes.
    ///
    /// # Errors
    /// Returns validation errors if the payload fails validation rules:
    /// - `ValidationFailed` for structure/content violations
    /// - `PayloadTooLarge` if payload exceeds size limits
    /// - `RateLimited` if rate limits are exceeded
    pub async fn enqueue(&self, payload: &serde_json::Value) -> Result<QueueMessage> {
        // Use enqueue_delayed with 0 delay - it already includes validation
        self.enqueue_delayed(payload, 0).await
    }

    /// Schedule a message to be available for consumption after a delay.
    ///
    /// This method validates the payload according to the queue's validation configuration
    /// before enqueueing with a delay.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `delay_seconds` - Seconds to delay before message becomes available
    ///
    /// # Returns
    /// The enqueued message if validation passes.
    ///
    /// # Errors
    /// Returns validation errors if the payload fails validation rules.
    pub async fn enqueue_delayed(
        &self,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        // Validate the payload before enqueueing
        self.validator.validate(payload)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));
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
        use crate::tables::NewMessage;

        let new_message = NewMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
        };

        let message = self.messages.insert(new_message).await?;
        Ok(message.id)
    }

    /// Add multiple messages to the queue in a single batch operation.
    ///
    /// This method validates all payloads according to the queue's validation configuration
    /// before enqueueing any of them. Rate limiting is applied atomically to the entire batch.
    /// If any payload fails validation, the entire batch is rejected and no messages are enqueued.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to enqueue
    ///
    /// # Returns
    /// Vector of enqueued messages if all payloads pass validation.
    ///
    /// # Errors
    /// Returns validation errors if any payload fails validation rules.
    /// The database transaction is atomic - either all messages are enqueued or none are.
    /// Rate limiting is also atomic - tokens are only consumed if the entire batch succeeds.
    pub async fn batch_enqueue(&self, payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        // Validate all payloads atomically (including rate limiting)
        self.validator.validate_batch(payloads)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(0);

        // Use the batch insert method from the messages table
        let ids = self
            .messages
            .batch_insert(self.queue_info.id, payloads, 0, now, vt)
            .await?;

        // Fetch all messages in a single query
        let queue_messages = self.messages.get_by_ids(&ids).await?;

        Ok(queue_messages)
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
        let rows_affected = self
            .messages
            .extend_visibility(message_id, additional_seconds)
            .await?;
        Ok(rows_affected > 0)
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

    /// Get the validation configuration for this queue.
    ///
    /// # Returns
    /// Reference to the validation configuration used by this queue.
    pub fn validation_config(&self) -> &crate::validation::ValidationConfig {
        &self.config.validation_config
    }

    /// Get current rate limit status for debugging.
    ///
    /// # Returns
    /// Current rate limiting status if rate limiting is enabled, None otherwise.
    pub fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}
