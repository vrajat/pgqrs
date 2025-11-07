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
    ARCHIVE_BATCH, ARCHIVE_LIST, ARCHIVE_MESSAGE, ARCHIVE_SELECT_BY_ID, PENDING_COUNT,
    PGQRS_SCHEMA, QUEUE_PREFIX,
};
use crate::error::Result;
use crate::types::QueueMessage;
use chrono::Utc;
use sqlx::PgPool;

/// Producer and consumer interface for a specific queue.
///
/// A Queue instance provides methods for enqueuing messages, reading messages,
/// and managing message lifecycle within a specific PostgreSQL-backed queue.
/// Each Queue corresponds to a table in the database.
pub struct Queue {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    /// Logical name of the queue
    pub queue_name: String,
    /// Table name in the database for this queue
    pub table_name: String,
    /// SQL for inserting a message
    pub insert_sql: String,
    /// SQL for selecting a message by ID
    pub select_by_id_sql: String,
    /// SQL for reading messages
    pub read_messages_sql: String,
    /// SQL for dequeuing a message
    pub dequeue_sql: String,
    /// SQL for updating message visibility timeout
    pub update_vt_sql: String,
    /// SQL for deleting a batch of messages
    pub delete_batch_sql: String,
    /// SQL for archiving a single message
    pub archive_sql: String,
    /// SQL for archiving a batch of messages
    pub archive_batch_sql: String,
    /// SQL for listing archive messages
    pub archive_list_sql: String,
    /// SQL for selecting archived message by ID
    pub archive_select_by_id_sql: String,
}

impl Queue {
    /// Create a new Queue instance for the specified queue name.
    ///
    /// This method is internal and sets up all the SQL statements needed
    /// for queue operations by replacing template placeholders with the
    /// actual queue name and schema.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_name` - Name of the queue (will be used to form table name)
    pub(crate) fn new(pool: PgPool, queue_name: &str) -> Self {
        let table_name = format!("{}.{}_{}", PGQRS_SCHEMA, QUEUE_PREFIX, queue_name);
        let insert_sql = crate::constants::INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let select_by_id_sql = crate::constants::SELECT_MESSAGE_BY_ID
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let read_messages_sql = crate::constants::READ_MESSAGES
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let dequeue_sql = crate::constants::DEQUEUE_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let update_vt_sql = crate::constants::UPDATE_MESSAGE_VT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let delete_batch_sql = crate::constants::DELETE_MESSAGE_BATCH
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);

        // Archive SQL statements
        let archive_sql = ARCHIVE_MESSAGE
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let archive_batch_sql = ARCHIVE_BATCH
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let archive_list_sql = ARCHIVE_LIST
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{queue_name}", queue_name);
        let archive_select_by_id_sql = ARCHIVE_SELECT_BY_ID
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{queue_name}", queue_name);

        Self {
            pool,
            queue_name: queue_name.to_string(),
            table_name,
            insert_sql,
            select_by_id_sql,
            read_messages_sql,
            dequeue_sql,
            update_vt_sql,
            delete_batch_sql,
            archive_sql,
            archive_batch_sql,
            archive_list_sql,
            archive_select_by_id_sql,
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
        let result = sqlx::query_as::<_, QueueMessage>(&self.select_by_id_sql)
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
        let result = sqlx::query_scalar::<_, i64>(&self.insert_sql)
            .bind(0_i32) // read_ct
            .bind(now)
            .bind(vt)
            .bind(payload)
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
            let id = sqlx::query_scalar::<_, i64>(&self.insert_sql)
                .bind(0_i32) // read_ct
                .bind(now)
                .bind(vt)
                .bind(payload)
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
        let sql = format!(
            "SELECT msg_id, read_ct, enqueued_at, vt, message, worker_id FROM {} WHERE msg_id = ANY($1)",
            self.table_name
        );

        let queue_messages = sqlx::query_as::<_, QueueMessage>(&sql)
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
        let sql = PENDING_COUNT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &self.queue_name);

        let count = sqlx::query_scalar::<_, i64>(&sql)
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
    pub async fn read(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.read_delay(crate::constants::VISIBILITY_TIMEOUT as u32, limit)
            .await
    }

    /// Read up to `limit` messages from the queue, with a custom visibility timeout.
    ///
    /// # Arguments
    /// * `vt` - Visibility timeout (seconds)
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn read_delay(&self, vt: u32, limit: usize) -> Result<Vec<QueueMessage>> {
        let sql = self
            .read_messages_sql
            .clone()
            .replace("{vt}", &vt.to_string())
            .replace("{limit}", &limit.to_string());

        let result = sqlx::query_as::<_, QueueMessage>(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    /// Remove a message from the queue (delete it permanently).
    ///
    /// # Arguments
    /// * `message_id` - ID of the message to delete
    ///
    /// # Returns
    /// The deleted message, or an error if not found.
    pub async fn dequeue(&self, message_id: i64) -> Result<QueueMessage> {
        let sql = self
            .dequeue_sql
            .clone()
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &self.queue_name);

        let result = sqlx::query_as::<_, QueueMessage>(&sql)
            .bind(message_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(result)
    }

    /// Remove a batch of messages from the queue.
    ///
    /// # Arguments
    /// * `message_ids` - Vector of message IDs to delete
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input).
    pub async fn delete_batch(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        let sql = self.delete_batch_sql.clone();

        let deleted_ids: Vec<i64> = sqlx::query_scalar(&sql)
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
        let sql = self.update_vt_sql.clone();
        let updated = sqlx::query(&sql)
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
    pub async fn archive(&self, msg_id: i64) -> Result<bool> {
        let result: Option<bool> = sqlx::query_scalar(&self.archive_sql)
            .bind(msg_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to archive message {}: {}", msg_id, e),
            })?;

        Ok(result.unwrap_or(false))
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

        let archived_ids: Vec<i64> = sqlx::query_scalar(&self.archive_batch_sql)
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

    /// List archived messages from the queue.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to return
    /// * `offset` - Number of messages to skip
    ///
    /// # Returns
    /// Vector of archived messages
    pub async fn archive_list(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::types::ArchivedMessage>> {
        let messages: Vec<crate::types::ArchivedMessage> = sqlx::query_as(&self.archive_list_sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to list archive messages: {}", e),
            })?;

        Ok(messages)
    }

    /// Get a specific archived message by ID.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the archived message to retrieve
    ///
    /// # Returns
    /// The archived message if found, error otherwise
    pub async fn get_archived_message_by_id(
        &self,
        msg_id: i64,
    ) -> Result<crate::types::ArchivedMessage> {
        let message: crate::types::ArchivedMessage = sqlx::query_as(&self.archive_select_by_id_sql)
            .bind(msg_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to retrieve archived message {}: {}", msg_id, e),
            })?;

        Ok(message)
    }
}
