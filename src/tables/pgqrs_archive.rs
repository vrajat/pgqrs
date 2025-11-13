//! Archive operations and management interface for pgqrs.
//!
//! This module defines the [`PgqrsArchive`] struct, which provides methods for managing archived messages
//! including listing, counting, and deleting archived messages with filtering capabilities.
//!
//! ## What
//!
//! - [`PgqrsArchive`] provides management interface for archived messages
//! - Supports filtering by queue and worker
//! - Provides efficient batch operations for archive management
//!
//! ## How
//!
//! Create a [`PgqrsArchive`] instance to manage archived messages across all queues or for specific queues/workers.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::tables::pgqrs_archive::PgqrsArchive;
//! // let archive = PgqrsArchive::new(pool);
//! // let messages = archive.list(None, None, 10, 0).await?;
//! ```

// SQL query constants
const ARCHIVE_LIST_WITH_WORKER: &str = r#"
SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt,
       read_ct, archived_at, dequeued_at
FROM pgqrs_archive
WHERE queue_id = $1 AND worker_id = $2
ORDER BY archived_at DESC
LIMIT $3 OFFSET $4
"#;

const ARCHIVE_LIST_QUEUE_ONLY: &str = r#"
SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt,
       read_ct, archived_at, dequeued_at
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

use crate::error::Result;
use crate::tables::table::Table;
use crate::types::{ArchivedMessage, NewArchivedMessage, QueueInfo};
use sqlx::PgPool;

/// Archive management interface for pgqrs.
///
/// A PgqrsArchive instance provides methods for managing archived messages,
/// including listing, counting, and deleting with optional filtering by worker.
pub struct PgqrsArchive {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    /// Database ID of the queue from pgqrs_queues table
    pub queue_info: QueueInfo,
}

impl PgqrsArchive {
    /// Create a new PgqrsArchive instance for a specific queue.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_info` - Information about the queue
    pub fn new(pool: PgPool, queue_info: &QueueInfo) -> Self {
        Self {
            pool,
            queue_info: queue_info.clone(),
        }
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
                    .bind(self.queue_info.id)
                    .bind(w)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&self.pool)
                    .await
            }
            None => {
                sqlx::query_as(ARCHIVE_LIST_QUEUE_ONLY)
                    .bind(self.queue_info.id)
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
                    .bind(self.queue_info.id)
                    .bind(w)
                    .fetch_one(&self.pool)
                    .await
            }
            None => {
                sqlx::query_scalar(ARCHIVE_COUNT_QUEUE_ONLY)
                    .bind(self.queue_info.id)
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
                    .bind(self.queue_info.id)
                    .bind(w)
                    .execute(&self.pool)
                    .await
            }
            None => {
                sqlx::query(ARCHIVE_DELETE_QUEUE_ONLY)
                    .bind(self.queue_info.id)
                    .execute(&self.pool)
                    .await
            }
        }
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: format!("Failed to delete archived messages: {}", e),
        })?;

        Ok(result.rows_affected())
    }

    /// Count all archived messages for a queue using a transaction.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to count archived messages for
    /// * `tx` - Database transaction
    ///
    /// # Returns
    /// Number of archived messages for the queue
    pub async fn count_for_queue_tx<'a, 'b: 'a>(
        queue_id: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        const COUNT_ARCHIVE_FOR_QUEUE: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_archive
            WHERE queue_id = $1
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_ARCHIVE_FOR_QUEUE)
            .bind(queue_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!(
                    "Failed to count archived messages for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(count)
    }
}

// SQL constants for Table trait implementation
const INSERT_ARCHIVE: &str = r#"
    INSERT INTO pgqrs_archive (original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id
"#;

const GET_ARCHIVE_BY_ID: &str = r#"
    SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE id = $1
"#;

const LIST_ALL_ARCHIVE: &str = r#"
    SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    ORDER BY archived_at DESC
"#;

const LIST_ARCHIVE_BY_QUEUE: &str = r#"
    SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE queue_id = $1
    ORDER BY archived_at DESC
"#;

const DELETE_ARCHIVE_BY_ID: &str = r#"
    DELETE FROM pgqrs_archive
    WHERE id = $1
"#;

/// Archive table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_archive` table without business logic.
/// This is separate from the queue-specific PgqrsArchive which provides higher-level operations.
#[derive(Debug)]
pub struct PgqrsArchiveTable {
    pub pool: PgPool,
}

impl PgqrsArchiveTable {
    /// Create a new PgqrsArchiveTable instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Table for PgqrsArchiveTable {
    type Entity = ArchivedMessage;
    type NewEntity = NewArchivedMessage;

    /// Insert a new archived message.
    ///
    /// # Arguments
    /// * `data` - New archived message data
    ///
    /// # Returns
    /// The archived message record with generated ID and archived_at timestamp
    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity> {
        use chrono::Utc;

        let archived_at = Utc::now();

        let archive_id: i64 = sqlx::query_scalar(INSERT_ARCHIVE)
            .bind(data.original_msg_id)
            .bind(data.queue_id)
            .bind(data.worker_id)
            .bind(data.payload.clone())
            .bind(data.enqueued_at)
            .bind(data.vt)
            .bind(data.read_ct)
            .bind(data.dequeued_at)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to insert archived message: {}", e),
            })?;

        Ok(ArchivedMessage {
            id: archive_id,
            original_msg_id: data.original_msg_id,
            queue_id: data.queue_id,
            worker_id: data.worker_id,
            payload: data.payload,
            enqueued_at: data.enqueued_at,
            vt: data.vt,
            read_ct: data.read_ct,
            archived_at,
            dequeued_at: data.dequeued_at,
        })
    }

    /// Get an archived message by ID.
    ///
    /// # Arguments
    /// * `id` - Archive ID to retrieve
    ///
    /// # Returns
    /// The archived message record
    async fn get(&self, id: i64) -> Result<Self::Entity> {
        let archive = sqlx::query_as::<_, ArchivedMessage>(GET_ARCHIVE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to get archived message {}: {}", id, e),
            })?;

        Ok(archive)
    }

    /// List all archived messages.
    ///
    /// # Returns
    /// List of all archived messages ordered by archived time (newest first)
    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ALL_ARCHIVE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to list archived messages: {}", e),
            })?;

        Ok(archives)
    }

    /// Filter archived messages by queue ID.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to filter by
    ///
    /// # Returns
    /// List of archived messages for the specified queue
    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<Self::Entity>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ARCHIVE_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!(
                    "Failed to filter archived messages by queue ID {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(archives)
    }

    /// Count all archived messages.
    ///
    /// # Returns
    /// Total number of archived messages in the table
    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_archive";
        let count = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to count archived messages: {}", e),
            })?;
        Ok(count)
    }

    /// Count archived messages by queue ID.
    ///
    /// # Arguments
    /// * `queue_id` - Queue ID to count archived messages for
    ///
    /// # Returns
    /// Number of archived messages in the specified queue
    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = $1";
        let count = sqlx::query_scalar(query)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!(
                    "Failed to count archived messages for queue {}: {}",
                    queue_id, e
                ),
            })?;
        Ok(count)
    }

    /// Delete an archived message by ID.
    ///
    /// # Arguments
    /// * `id` - Archive ID to delete
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_ARCHIVE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to delete archived message {}: {}", id, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_archived_message_creation() {
        use chrono::Utc;
        use serde_json::json;

        let new_archive = NewArchivedMessage {
            original_msg_id: 123,
            queue_id: 1,
            worker_id: Some(456),
            payload: json!({"test": "data"}),
            enqueued_at: Utc::now(),
            vt: Utc::now(),
            read_ct: 3,
            dequeued_at: Some(Utc::now()),
        };

        assert_eq!(new_archive.original_msg_id, 123);
        assert_eq!(new_archive.queue_id, 1);
        assert_eq!(new_archive.worker_id, Some(456));
        assert_eq!(new_archive.read_ct, 3);
    }

    #[test]
    fn test_table_trait_associated_types() {
        // This test ensures the type associations work correctly
        // We can't use dyn Table because async traits aren't object-safe,
        // but we can verify the types compile correctly
        let _type_check = |table: PgqrsArchiveTable| {
            // Verify the associated types are correct
            let _entity_check: Option<ArchivedMessage> = None;
            let _new_entity_check: Option<NewArchivedMessage> = None;
            table
        };

        // Just verify we can construct the types
        assert_eq!(
            std::mem::size_of::<PgqrsArchiveTable>(),
            std::mem::size_of::<PgPool>()
        );
    }
}
