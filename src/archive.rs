//! Archive operations and management interface for pgqrs.
//!
//! This module defines the [`Archive`] struct, which provides methods for managing archived messages
//! including listing, counting, and deleting archived messages with filtering capabilities.
//!
//! ## What
//!
//! - [`Archive`] provides management interface for archived messages
//! - Supports filtering by queue and worker
//! - Provides efficient batch operations for archive management
//!
//! ## How
//!
//! Create an [`Archive`] instance to manage archived messages across all queues or for specific queues/workers.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::archive::Archive;
//! // let archive = Archive::new(pool);
//! // let messages = archive.list(None, None, 10, 0).await?;
//! ```

// SQL query constants
const ARCHIVE_LIST_WITH_WORKER: &str = r#"
SELECT id, queue_id, worker_id, payload, priority, enqueued_at, vt,
       archived_at, read_ct, original_msg_id, processing_duration
FROM pgqrs_archive
WHERE queue_id = $1 AND worker_id = $2
ORDER BY archived_at DESC
LIMIT $3 OFFSET $4
"#;

const ARCHIVE_LIST_QUEUE_ONLY: &str = r#"
SELECT id, queue_id, worker_id, payload, priority, enqueued_at, vt,
       archived_at, read_ct, original_msg_id, processing_duration
FROM pgqrs_archive
WHERE queue_id = $1
ORDER BY archived_at DESC
LIMIT $2 OFFSET $3
"#;

const ARCHIVE_COUNT_WITH_WORKER: &str = r#"
SELECT COUNT(*)
FROM pgqrs_archive
WHERE queue_id = $1 AND worker_id = $2
"#;

const ARCHIVE_COUNT_QUEUE_ONLY: &str = r#"
SELECT COUNT(*)
FROM pgqrs_archive
WHERE queue_id = $1
"#;

const ARCHIVE_DELETE_WITH_WORKER: &str = r#"
DELETE FROM pgqrs_archive
WHERE queue_id = $1 AND worker_id = $2
"#;

const ARCHIVE_DELETE_QUEUE_ONLY: &str = r#"
DELETE FROM pgqrs_archive
WHERE queue_id = $1
"#;

use crate::constants::ARCHIVE_SELECT_BY_ID;
use crate::error::Result;
use crate::types::ArchivedMessage;
use sqlx::PgPool;

/// Archive management interface for pgqrs.
///
/// An Archive instance provides methods for managing archived messages,
/// including listing, counting, and deleting with optional filtering by worker.
pub struct Archive {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    /// Database ID of the queue from pgqrs_queues table
    pub queue_id: i64,
}

impl Archive {
    /// Create a new Archive instance for a specific queue.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_id` - Database ID of the queue
    pub fn new(pool: PgPool, queue_id: i64) -> Self {
        Self { pool, queue_id }
    }

    /// List archived messages with optional filtering.
    ///
    /// # Arguments
    /// * `worker_id` - Optional worker ID filter
    /// * `limit` - Maximum number of messages to return
    /// * `offset` - Number of messages to skip
    ///
    /// # Returns
    /// Vector of archived messages
    pub async fn list(
        &self,
        worker_id: Option<i64>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let query = match worker_id {
            Some(_) => ARCHIVE_LIST_WITH_WORKER,
            None => ARCHIVE_LIST_QUEUE_ONLY,
        };

        let messages: Vec<ArchivedMessage> = match worker_id {
            Some(w) => {
                sqlx::query_as(query)
                    .bind(self.queue_id)
                    .bind(w)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&self.pool)
                    .await
            }
            None => {
                sqlx::query_as(query)
                    .bind(self.queue_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&self.pool)
                    .await
            }
        }
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: format!("Failed to list archived messages: {}", e),
        })?;

        Ok(messages)
    }

    /// Count archived messages with optional filtering.
    ///
    /// # Arguments
    /// * `worker_id` - Optional worker ID filter
    ///
    /// # Returns
    /// Count of archived messages matching the criteria
    pub async fn count(&self, worker_id: Option<i64>) -> Result<i64> {
        let query = match worker_id {
            Some(_) => ARCHIVE_COUNT_WITH_WORKER,
            None => ARCHIVE_COUNT_QUEUE_ONLY,
        };

        let count: i64 = match worker_id {
            Some(w) => {
                sqlx::query_scalar(query)
                    .bind(self.queue_id)
                    .bind(w)
                    .fetch_one(&self.pool)
                    .await
            }
            None => {
                sqlx::query_scalar(query)
                    .bind(self.queue_id)
                    .fetch_one(&self.pool)
                    .await
            }
        }
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: format!("Failed to count archived messages: {}", e),
        })?;

        Ok(count)
    }

    /// Delete archived messages with optional filtering.
    ///
    /// # Arguments
    /// * `worker_id` - Optional worker ID filter
    ///
    /// # Returns
    /// Number of archived messages deleted
    pub async fn delete(&self, worker_id: Option<i64>) -> Result<u64> {
        let query = match worker_id {
            Some(_) => ARCHIVE_DELETE_WITH_WORKER,
            None => ARCHIVE_DELETE_QUEUE_ONLY,
        };

        let result = match worker_id {
            Some(w) => {
                sqlx::query(query)
                    .bind(self.queue_id)
                    .bind(w)
                    .execute(&self.pool)
                    .await
            }
            None => {
                sqlx::query(query)
                    .bind(self.queue_id)
                    .execute(&self.pool)
                    .await
            }
        }
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: format!("Failed to delete archived messages: {}", e),
        })?;

        Ok(result.rows_affected())
    }

    /// Get a specific archived message by ID.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the archived message to retrieve
    ///
    /// # Returns
    /// The archived message if found, error otherwise
    pub async fn get_by_id(&self, msg_id: i64) -> Result<ArchivedMessage> {
        let message: ArchivedMessage = sqlx::query_as(ARCHIVE_SELECT_BY_ID)
            .bind(msg_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to retrieve archived message {}: {}", msg_id, e),
            })?;

        Ok(message)
    }

    /// Archive a single message from its ID (moves from messages table to archive table).
    ///
    /// This is used internally by Queue operations but can also be called directly.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to archive
    ///
    /// # Returns
    /// True if message was successfully archived, false if message was not found
    pub async fn archive_message(&self, msg_id: i64) -> Result<bool> {
        let result: Option<bool> = sqlx::query_scalar(crate::constants::ARCHIVE_MESSAGE)
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
    /// More efficient than individual archive calls for bulk operations.
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

        let archived_ids: Vec<i64> = sqlx::query_scalar(crate::constants::ARCHIVE_BATCH)
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
