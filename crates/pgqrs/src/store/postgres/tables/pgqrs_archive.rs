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
//! Create a [`Archive`] instance to manage archived messages across all queues or for specific queues/workers.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::tables::pgqrs_archive::Archive;
//! // let archive = Archive::new(pool);
//! // let messages = archive.list(None, None, 10, 0).await?;
//! ```

// SQL query constants
const LIST_DLQ_MESSAGES: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt,
           read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE read_ct >= $1  -- max_attempts
      AND consumer_worker_id IS NULL
      AND dequeued_at IS NULL
    ORDER BY archived_at DESC
    LIMIT $2 OFFSET $3;
"#;

const COUNT_DLQ_MESSAGES: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_archive
    WHERE read_ct >= $1  -- max_attempts
      AND consumer_worker_id IS NULL
      AND dequeued_at IS NULL;
"#;

const ARCHIVE_LIST_WITH_WORKER: &str = r#"
SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt,
       read_ct, archived_at, dequeued_at
FROM pgqrs_archive
WHERE consumer_worker_id = $1
ORDER BY archived_at DESC
LIMIT $2 OFFSET $3
"#;

const ARCHIVE_COUNT_WITH_WORKER: &str = r#"
SELECT COUNT(*)
FROM pgqrs_archive
WHERE consumer_worker_id = $1
"#;

const ARCHIVE_DELETE_WITH_WORKER: &str = r#"
DELETE FROM pgqrs_archive
WHERE consumer_worker_id = $1
"#;

use crate::error::Result;
use crate::tables::table::Table;
use crate::types::{ArchivedMessage, NewArchivedMessage, QueueMessage};
use async_trait::async_trait;
use sqlx::PgPool;

/// Archive table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_archive` table without business logic.
/// This is separate from the queue-specific Archive which provides higher-level operations.
#[derive(Debug, Clone)]
pub struct Archive {
    pub pool: PgPool,
}

impl Archive {
    /// Create a new Archive instance for a specific queue.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// SQL constants for Table trait implementation
const INSERT_ARCHIVE: &str = r#"
    INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    RETURNING id
"#;

const GET_ARCHIVE_BY_ID: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE id = $1
"#;

const LIST_ALL_ARCHIVE: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    ORDER BY archived_at DESC
"#;

const LIST_ARCHIVE_BY_QUEUE: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE queue_id = $1
    ORDER BY archived_at DESC
"#;

const DELETE_ARCHIVE_BY_ID: &str = r#"
    DELETE FROM pgqrs_archive
    WHERE id = $1
"#;

const ARCHIVE_PURGE_QUEUE: &str = r#"
    DELETE FROM pgqrs_archive
    WHERE queue_id = $1
"#;

#[async_trait]
impl crate::store::ArchiveTable for Archive {
    async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
        use chrono::Utc;

        let archived_at = Utc::now();

        let archive_id: i64 = sqlx::query_scalar(INSERT_ARCHIVE)
            .bind(data.original_msg_id)
            .bind(data.queue_id)
            .bind(data.producer_worker_id)
            .bind(data.consumer_worker_id)
            .bind(data.payload.clone())
            .bind(data.enqueued_at)
            .bind(data.vt)
            .bind(data.read_ct)
            .bind(data.dequeued_at)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to insert archived message: {}", e),
            })?;

        Ok(ArchivedMessage {
            id: archive_id,
            original_msg_id: data.original_msg_id,
            queue_id: data.queue_id,
            producer_worker_id: data.producer_worker_id,
            consumer_worker_id: data.consumer_worker_id,
            payload: data.payload,
            enqueued_at: data.enqueued_at,
            vt: data.vt,
            read_ct: data.read_ct,
            archived_at,
            dequeued_at: data.dequeued_at,
        })
    }

    async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        let archive = sqlx::query_as::<_, ArchivedMessage>(GET_ARCHIVE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get archived message {}: {}", id, e),
            })?;

        Ok(archive)
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ALL_ARCHIVE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list archived messages: {}", e),
            })?;

        Ok(archives)
    }

    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_archive";
        let count = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count archived messages: {}", e),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_ARCHIVE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete archived message {}: {}", id, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ARCHIVE_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to filter archived messages by queue ID {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(archives)
    }

    async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        let result = sqlx::query(ARCHIVE_DELETE_WITH_WORKER)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete archived messages: {}", e),
            })?;

        Ok(result.rows_affected())
    }

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let messages: Vec<ArchivedMessage> = sqlx::query_as(LIST_DLQ_MESSAGES)
            .bind(max_attempts)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list DLQ messages: {}", e),
            })?;
        Ok(messages)
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_DLQ_MESSAGES)
            .bind(max_attempts)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count DLQ messages: {}", e),
            })?;
        Ok(count)
    }

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let messages = sqlx::query_as(ARCHIVE_LIST_WITH_WORKER)
            .bind(worker_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        Ok(messages)
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(ARCHIVE_COUNT_WITH_WORKER)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count archived messages for worker {}: {}",
                    worker_id, e
                ),
            })?;
        Ok(count)
    }

    async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        // Replay: Move from archive back to messages
        let msg = sqlx::query_as::<_, QueueMessage>(r#"
            WITH archived AS (
                DELETE FROM pgqrs_archive WHERE id = $1 RETURNING *
            )
            INSERT INTO pgqrs_messages (
                queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id
            )
            SELECT
                queue_id, payload, 0, NOW(), NOW(), producer_worker_id
            FROM archived
            RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
        "#)
        .bind(msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::error::Error::Connection {
            message: format!("Failed to replay message {}: {}", msg_id, e)
        })?;

        Ok(msg)
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
            producer_worker_id: Some(456),
            consumer_worker_id: Some(789),
            payload: json!({"test": "data"}),
            enqueued_at: Utc::now(),
            vt: Utc::now(),
            read_ct: 3,
            dequeued_at: Some(Utc::now()),
        };

        assert_eq!(new_archive.original_msg_id, 123);
        assert_eq!(new_archive.queue_id, 1);
        assert_eq!(new_archive.producer_worker_id, Some(456));
        assert_eq!(new_archive.consumer_worker_id, Some(789));
        assert_eq!(new_archive.read_ct, 3);
    }

    #[test]
    fn test_table_trait_associated_types() {
        // This test ensures the type associations work correctly
        // We can't use dyn Table because async traits aren't object-safe,
        // but we can verify the types compile correctly
        let _type_check = |table: Archive| {
            // Verify the associated types are correct
            let _entity_check: Option<ArchivedMessage> = None;
            let _new_entity_check: Option<NewArchivedMessage> = None;
            table
        };

        // Just verify we can construct the types
        assert_eq!(
            std::mem::size_of::<Archive>(),
            std::mem::size_of::<PgPool>()
        );
    }
}
