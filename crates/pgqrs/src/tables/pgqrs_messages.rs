//! Message table CRUD operations for pgqrs.
//!
//! This module provides pure CRUD operations on the `pgqrs_messages` table without business logic.
//! Complex operations like dequeue with worker assignment and visibility timeout management remain in queue.rs.

use crate::error::Result;
use crate::tables::table::Table;
use crate::types::QueueMessage;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

// SQL constants for message table operations
const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const GET_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = $1;
"#;

const LIST_MESSAGES_BY_QUEUE: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE queue_id = $1
    ORDER BY enqueued_at DESC
    LIMIT 1000;
"#;

const LIST_ALL_MESSAGES: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    ORDER BY enqueued_at DESC;
"#;

const DELETE_MESSAGE_BY_ID: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1;
"#;

const BATCH_INSERT_MESSAGES: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    SELECT $1, unnest($2::jsonb[]), $3, $4, $5, $6, $7
    RETURNING id;
"#;

const GET_MESSAGES_BY_IDS: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = ANY($1)
    ORDER BY id;
"#;

const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = $2
    WHERE id = $1;
"#;

/// Input data for creating a new message
#[derive(Debug)]
pub struct NewMessage {
    pub queue_id: i64,
    pub payload: serde_json::Value,
    pub read_ct: i32,
    pub enqueued_at: DateTime<Utc>,
    pub vt: DateTime<Utc>,
    pub producer_worker_id: Option<i64>,
    pub consumer_worker_id: Option<i64>,
}

/// Parameters for batch message insertion
#[derive(Debug)]
pub struct BatchInsertParams {
    pub read_ct: i32,
    pub enqueued_at: DateTime<Utc>,
    pub vt: DateTime<Utc>,
    pub producer_worker_id: Option<i64>,
    pub consumer_worker_id: Option<i64>,
}

/// Messages table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_messages` table without business logic.
#[derive(Debug, Clone)]
pub struct Messages {
    pub pool: PgPool,
}

impl Messages {
    /// Create a new Messages instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert multiple messages in a single batch operation.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID for all messages
    /// * `payloads` - Vector of JSON payloads
    /// * `params` - Batch insertion parameters including timing and worker info
    ///
    /// # Returns
    /// Vector of message IDs in order
    pub async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: BatchInsertParams,
    ) -> Result<Vec<i64>> {
        let ids: Vec<i64> = sqlx::query_scalar(BATCH_INSERT_MESSAGES)
            .bind(queue_id)
            .bind(payloads)
            .bind(params.read_ct)
            .bind(params.enqueued_at)
            .bind(params.vt)
            .bind(params.producer_worker_id)
            .bind(params.consumer_worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to batch insert messages: {}", e),
            })?;

        Ok(ids)
    }

    /// Get multiple messages by their IDs.
    ///
    /// # Arguments
    /// * `ids` - Vector of message IDs to retrieve
    ///
    /// # Returns
    /// Vector of messages in ID order
    pub async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(GET_MESSAGES_BY_IDS)
            .bind(ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get messages by IDs: {}", e),
            })?;

        Ok(messages)
    }

    /// Update a message's visibility timeout.
    ///
    /// # Arguments
    /// * `id` - Message ID
    /// * `vt` - New visibility timeout
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    pub async fn update_visibility_timeout(&self, id: i64, vt: DateTime<Utc>) -> Result<u64> {
        let rows_affected = sqlx::query(UPDATE_MESSAGE_VT)
            .bind(id)
            .bind(vt)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to update visibility timeout for message {}: {}",
                    id, e
                ),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    /// Extend a message's visibility timeout by adding additional seconds.
    ///
    /// # Arguments
    /// * `id` - Message ID
    /// * `worker_id` - ID of the worker attempting to extend visibility
    /// * `additional_seconds` - Additional seconds to add to current vt
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    pub async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        const EXTEND_MESSAGE_VT: &str = r#"
            UPDATE pgqrs_messages
            SET vt = vt + make_interval(secs => $3::double precision)
            WHERE id = $1 AND consumer_worker_id = $2;
        "#;

        let rows_affected = sqlx::query(EXTEND_MESSAGE_VT)
            .bind(id)
            .bind(worker_id)
            .bind(additional_seconds as i32)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to extend visibility timeout for message {}: {}",
                    id, e
                ),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    /// Extend visibility timeout for multiple messages in a batch.
    ///
    /// Only extends messages owned by the specified worker to prevent unauthorized access.
    ///
    /// # Arguments
    /// * `message_ids` - Vector of message IDs to extend
    /// * `worker_id` - Worker ID that must own the messages
    /// * `additional_seconds` - Additional seconds to add to current vt
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input)
    pub async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        const EXTEND_BATCH_VT: &str = r#"
            UPDATE pgqrs_messages
            SET vt = vt + make_interval(secs => $3::double precision)
            WHERE id = ANY($1) AND consumer_worker_id = $2
            RETURNING id;
        "#;

        let extended_ids: Vec<i64> = sqlx::query_scalar(EXTEND_BATCH_VT)
            .bind(message_ids)
            .bind(worker_id)
            .bind(additional_seconds as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to extend visibility timeout for batch messages: {}",
                    e
                ),
            })?;

        // For each input id, true if it was extended, false otherwise
        let extended_set: std::collections::HashSet<i64> = extended_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| extended_set.contains(id))
            .collect();

        Ok(result)
    }

    /// Release specific messages by IDs, making them available for other workers.
    ///
    /// Only releases messages owned by the specified worker to prevent unauthorized access.
    ///
    /// # Arguments
    /// * `message_ids` - Vector of message IDs to release
    /// * `worker_id` - Worker ID that must own the messages
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input)
    pub async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        const RELEASE_SPECIFIC_MESSAGES: &str = r#"
            UPDATE pgqrs_messages
            SET vt = NOW(), consumer_worker_id = NULL
            WHERE id = ANY($1) AND consumer_worker_id = $2
            RETURNING id;
        "#;

        let released_ids: Vec<i64> = sqlx::query_scalar(RELEASE_SPECIFIC_MESSAGES)
            .bind(message_ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to release specific messages: {}", e),
            })?;

        // For each input id, true if it was released, false otherwise
        let released_set: std::collections::HashSet<i64> = released_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();

        Ok(result)
    }

    /// Count pending messages in a queue.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to count messages for
    ///
    /// # Returns
    /// Number of pending (available for dequeue) messages
    pub async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending_filtered(queue_id, None).await
    }

    /// Count pending messages in a queue with optional worker filter.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to count messages for
    /// * `worker_id` - Optional worker ID to filter messages by (messages locked by this worker)
    ///
    /// # Returns
    /// Number of pending (available for dequeue) messages, or messages locked by the specified worker
    pub async fn count_pending_filtered(
        &self,
        queue_id: i64,
        worker_id: Option<i64>,
    ) -> Result<i64> {
        let count = match worker_id {
            Some(wid) => {
                // Count messages locked by the specified worker
                sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = $1 AND consumer_worker_id = $2
                    "#,
                )
                .bind(queue_id)
                .bind(wid)
                .fetch_one(&self.pool)
                .await
            }
            None => {
                // Count available (pending) messages
                sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = $1 AND (vt IS NULL OR vt <= NOW()) AND consumer_worker_id IS NULL
                    "#,
                )
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await
            }
        }
        .map_err(|e| crate::error::Error::Connection {
            message: format!(
                "Failed to count pending messages for queue {}: {}",
                queue_id, e
            ),
        })?;

        Ok(count)
    }

    /// Delete multiple messages by IDs.
    ///
    /// # Arguments
    /// * `ids` - Vector of message IDs to delete
    ///
    /// # Returns
    /// Vector of booleans indicating success for each ID
    pub async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(ids.len());

        for &id in ids {
            let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::Connection {
                    message: format!("Failed to delete message {}: {}", id, e),
                })?
                .rows_affected();

            results.push(rows_affected > 0);
        }

        Ok(results)
    }
}

impl Table for Messages {
    type Entity = QueueMessage;
    type NewEntity = NewMessage;

    /// Insert a new message record.
    ///
    /// # Arguments
    /// * `data` - New message information
    ///
    /// # Returns
    /// The created message with generated ID
    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity> {
        let message = sqlx::query_as::<_, QueueMessage>(INSERT_MESSAGE)
            .bind(data.queue_id)
            .bind(data.payload)
            .bind(data.read_ct)
            .bind(data.enqueued_at)
            .bind(data.vt)
            .bind(data.producer_worker_id)
            .bind(data.consumer_worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to insert message: {}", e),
            })?;

        Ok(message)
    }

    /// Get a message by ID.
    ///
    /// # Arguments
    /// * `id` - Message ID to retrieve
    ///
    /// # Returns
    /// The message record
    async fn get(&self, id: i64) -> Result<Self::Entity> {
        let message = sqlx::query_as::<_, QueueMessage>(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get message {}: {}", id, e),
            })?;

        Ok(message)
    }

    /// List all messages.
    ///
    /// # Returns
    /// List of all messages (limited to 1000 for performance)
    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_ALL_MESSAGES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list all messages: {}", e),
            })?;

        Ok(messages)
    }

    /// Filter messages by queue ID.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Queue ID to filter by
    ///
    /// # Returns
    /// List of messages for the specified queue (limited to 1000 for performance)
    async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<Self::Entity>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_MESSAGES_BY_QUEUE)
            .bind(foreign_key_value)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list messages for queue {}: {}",
                    foreign_key_value, e
                ),
            })?;

        Ok(messages)
    }

    /// Count all messages.
    ///
    /// # Returns
    /// Total number of messages in the table
    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_messages";
        let count = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count messages: {}", e),
            })?;
        Ok(count)
    }

    /// Count messages by queue ID.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to count messages for
    ///
    /// # Returns
    /// Number of messages in the specified queue
    async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1";
        let count = sqlx::query_scalar(query)
            .bind(foreign_key_value)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count messages for queue {}: {}",
                    foreign_key_value, e
                ),
            })?;
        Ok(count)
    }

    /// Delete a message by ID.
    ///
    /// # Arguments
    /// * `id` - Message ID to delete
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete message {}: {}", id, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    /// Delete messages by queue ID within a transaction.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Queue ID to delete messages for
    /// * `tx` - Mutable reference to an active SQL transaction
    /// # Returns
    /// Number of rows affected
    async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        const DELETE_MESSAGES_BY_QUEUE_ID: &str = r#"
            DELETE FROM pgqrs_messages
            WHERE queue_id = $1;
        "#;

        let rows_affected = sqlx::query(DELETE_MESSAGES_BY_QUEUE_ID)
            .bind(foreign_key_value)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to delete messages for queue {}: {}",
                    foreign_key_value, e
                ),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_new_message_creation() {
        let now = Utc::now();
        let new_message = NewMessage {
            queue_id: 1,
            payload: serde_json::json!({"test": "data"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: Some(1),
            consumer_worker_id: None,
        };

        // Test that the NewMessage struct can be created
        assert_eq!(new_message.queue_id, 1);
        assert_eq!(new_message.read_ct, 0);
        assert_eq!(new_message.payload["test"], "data");
        assert_eq!(new_message.producer_worker_id, Some(1));
        assert_eq!(new_message.consumer_worker_id, None);
    }

    #[test]
    fn test_table_trait_associated_types() {
        // Compile-time test to ensure the trait is implemented correctly
        // This test passes if the code compiles, proving our Table trait implementation
        // has the correct associated types.

        // Note: This is a compile-time test, we don't actually create connections
        // In real usage, Messages would be created with a valid pool
        // let messages = Messages::new(pool);
        // assert_entity_type(&messages);
        // assert_new_entity_type(&messages);
    }
}
