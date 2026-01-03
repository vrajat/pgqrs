//! Archive operations and management interface for pgqrs.
//!
//! This module defines the [`Archive`] struct, which provides methods for managing archived messages
//! including listing, counting, and deleting archived messages with filtering capabilities.

use crate::error::Result;
use crate::types::{ArchivedMessage, NewArchivedMessage, QueueMessage};
use async_trait::async_trait;
use sqlx::PgPool;

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

// SQL constants for Table functionality
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

const DELETE_ARCHIVE_BY_QUEUE: &str = r#"
    DELETE FROM pgqrs_archive WHERE queue_id = $1
"#;

const COUNT_ARCHIVE_BY_QUEUE_TX: &str = r#"
    SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = $1
"#;

/// Archive table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_archive` table without business logic.
#[derive(Debug, Clone)]
pub struct Archive {
    pub pool: PgPool,
}

impl Archive {
    /// Create a new Archive instance for a specific queue.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_ARCHIVE".into(),
                source: e,
                context: format!("Failed to archive message {}", data.original_msg_id),
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

    pub async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        let archive = sqlx::query_as::<_, ArchivedMessage>(GET_ARCHIVE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_ARCHIVE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to get archived message {}", id),
            })?;

        Ok(archive)
    }

    pub async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ALL_ARCHIVE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_ARCHIVE".into(),
                source: e,
                context: "Failed to list all archived messages".into(),
            })?;

        Ok(archives)
    }

    pub async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_archive";
        let count = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_ARCHIVE".into(),
                source: e,
                context: "Failed to count archived messages".into(),
            })?;
        Ok(count)
    }

    pub async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_ARCHIVE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_ARCHIVE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to delete archived message {}", id),
            })?;

        Ok(result.rows_affected())
    }

    pub async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<ArchivedMessage>> {
        let archives = sqlx::query_as::<_, ArchivedMessage>(LIST_ARCHIVE_BY_QUEUE)
            .bind(foreign_key_value)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_ARCHIVE_BY_QUEUE ({})", foreign_key_value),
                source: e,
                context: format!("Failed to list archives for queue {}", foreign_key_value),
            })?;
        Ok(archives)
    }

    pub async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_ARCHIVE_BY_QUEUE_TX)
            .bind(foreign_key_value)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("COUNT_ARCHIVE_BY_QUEUE_TX ({})", foreign_key_value),
                source: e,
                context: format!("Failed to count archives for queue {}", foreign_key_value),
            })?;
        Ok(count)
    }

    pub async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_ARCHIVE_BY_QUEUE)
            .bind(foreign_key_value)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_ARCHIVE_BY_QUEUE ({})", foreign_key_value),
                source: e,
                context: format!("Failed to delete archives for queue {}", foreign_key_value),
            })?;
        Ok(result.rows_affected())
    }

    pub async fn list_dlq_messages(
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_DLQ_MESSAGES".into(),
                source: e,
                context: format!(
                    "Failed to list DLQ messages (max_attempts={})",
                    max_attempts
                ),
            })?;
        Ok(messages)
    }

    pub async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_DLQ_MESSAGES)
            .bind(max_attempts)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_DLQ_MESSAGES".into(),
                source: e,
                context: format!(
                    "Failed to count DLQ messages (max_attempts={})",
                    max_attempts
                ),
            })?;
        Ok(count)
    }

    pub async fn list_by_worker(
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
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_LIST_WITH_WORKER ({})", worker_id),
                source: e,
                context: format!("Failed to list archives for worker {}", worker_id),
            })?;

        Ok(messages)
    }

    pub async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(ARCHIVE_COUNT_WITH_WORKER)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_COUNT_WITH_WORKER ({})", worker_id),
                source: e,
                context: format!("Failed to count archives for worker {}", worker_id),
            })?;
        Ok(count)
    }

    pub async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        let result = sqlx::query(ARCHIVE_DELETE_WITH_WORKER)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ARCHIVE_DELETE_WITH_WORKER".into(),
                source: e,
                context: format!("Failed to delete archives for worker {}", worker_id),
            })?;

        Ok(result.rows_affected())
    }

    pub async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
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
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("REPLAY_MESSAGE ({})", msg_id),
            source: e,
            context: format!("Failed to replay message {}", msg_id),
        })?;

        Ok(msg)
    }
}

#[async_trait]
impl crate::store::ArchiveTable for Archive {
    async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
        self.insert(data).await
    }

    async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        self.get(id).await
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        self.list().await
    }

    async fn count(&self) -> Result<i64> {
        self.count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.delete(id).await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        self.filter_by_fk(queue_id).await
    }

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        self.list_dlq_messages(max_attempts, limit, offset).await
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        self.dlq_count(max_attempts).await
    }

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        self.list_by_worker(worker_id, limit, offset).await
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        self.count_by_worker(worker_id).await
    }

    async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        self.delete_by_worker(worker_id).await
    }

    async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        self.replay_message(msg_id).await
    }

    async fn count_for_queue(&self, queue_id: i64) -> Result<i64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = $1")
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("COUNT_ARCHIVE_BY_QUEUE ({})", queue_id),
                    source: e,
                    context: format!("Failed to count archives for queue {}", queue_id),
                })?;
        Ok(count)
    }
}
