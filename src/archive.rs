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
SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt,
       read_ct, archived_at, processing_duration
FROM pgqrs_archive
WHERE queue_id = $1 AND worker_id = $2
ORDER BY archived_at DESC
LIMIT $3 OFFSET $4
"#;

const ARCHIVE_LIST_QUEUE_ONLY: &str = r#"
SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt,
       read_ct, archived_at, processing_duration
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
        let messages: Vec<ArchivedMessage> = match worker_id {
            Some(w) => {
                sqlx::query_as(ARCHIVE_LIST_WITH_WORKER)
                    .bind(self.queue_id)
                    .bind(w)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&self.pool)
                    .await
            }
            None => {
                sqlx::query_as(ARCHIVE_LIST_QUEUE_ONLY)
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
        let count: i64 = match worker_id {
            Some(w) => {
                sqlx::query_scalar(ARCHIVE_COUNT_WITH_WORKER)
                    .bind(self.queue_id)
                    .bind(w)
                    .fetch_one(&self.pool)
                    .await
            }
            None => {
                sqlx::query_scalar(ARCHIVE_COUNT_QUEUE_ONLY)
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
        let result = match worker_id {
            Some(w) => {
                sqlx::query(ARCHIVE_DELETE_WITH_WORKER)
                    .bind(self.queue_id)
                    .bind(w)
                    .execute(&self.pool)
                    .await
            }
            None => {
                sqlx::query(ARCHIVE_DELETE_QUEUE_ONLY)
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
}
