//! Message table CRUD operations for pgqrs.
//!
//! This module provides pure CRUD operations on the `pgqrs_messages` table without business logic.
//! Complex operations like dequeue with worker assignment and visibility timeout management remain in queue.rs.

use crate::error::Result;
use crate::types::QueueMessage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

// SQL constants for message table operations
const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const GET_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
    FROM pgqrs_messages
    WHERE id = $1;
"#;

const LIST_MESSAGES_BY_QUEUE: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
    FROM pgqrs_messages
    WHERE queue_id = $1 AND archived_at IS NULL
    ORDER BY enqueued_at DESC
    LIMIT 1000;
"#;

const LIST_ALL_MESSAGES: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
    FROM pgqrs_messages
    WHERE archived_at IS NULL
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
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
    FROM pgqrs_messages
    WHERE id = ANY($1)
    ORDER BY id;
"#;

const DELETE_MESSAGES_BY_QUEUE: &str = r#"
    DELETE FROM pgqrs_messages WHERE queue_id = $1
"#;

const COUNT_MESSAGES_BY_QUEUE_TX: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1
"#;

const DELETE_MESSAGE_OWNED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1 AND consumer_worker_id = $2
"#;

const DELETE_MESSAGE_BATCH_OWNED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

const ARCHIVE_MESSAGE: &str = r#"
    UPDATE pgqrs_messages
    SET archived_at = NOW()
    WHERE id = $1 AND consumer_worker_id = $2 AND archived_at IS NULL
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const ARCHIVE_BATCH: &str = r#"
    UPDATE pgqrs_messages
    SET archived_at = NOW()
    WHERE id = ANY($1) AND consumer_worker_id = $2 AND archived_at IS NULL
    RETURNING id;
"#;

const DEQUEUE_MESSAGES_AT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = $5 + make_interval(secs => $3::double precision),
        read_ct = read_ct + 1,
        dequeued_at = COALESCE(dequeued_at, $5),
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= $5)
          AND consumer_worker_id IS NULL
          AND archived_at IS NULL
          AND read_ct < $6
        ORDER BY enqueued_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const REPLAY_FROM_DLQ: &str = r#"
    UPDATE pgqrs_messages
    SET archived_at = NULL,
        read_ct = 0,
        vt = NOW(),
        enqueued_at = NOW(),
        consumer_worker_id = NULL,
        dequeued_at = NULL
    WHERE id = $1 AND archived_at IS NOT NULL
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const UPDATE_MESSAGE_PAYLOAD: &str = r#"
    UPDATE pgqrs_messages
    SET payload = $2
    WHERE id = $1;
"#;

const EXTEND_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = vt + make_interval(secs => $3::double precision)
    WHERE id = $1 AND consumer_worker_id = $2;
"#;

const EXTEND_BATCH_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = vt + make_interval(secs => $3::double precision)
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

const RELEASE_SPECIFIC_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NOW(), consumer_worker_id = NULL
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

const RELEASE_WITH_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = $3, consumer_worker_id = NULL
    WHERE id = $1 AND consumer_worker_id = $2;
"#;

/// Messages table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_messages` table without business logic.
#[derive(Debug, Clone)]
pub struct Messages {
    pub pool: PgPool,
}

impl Messages {
    /// Create a new Messages instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_MESSAGES_BY_QUEUE_TX)
            .bind(foreign_key_value)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_MESSAGES_BY_QUEUE_TX".into(),
                source: Box::new(e),
                context: format!("Failed to count messages for queue {}", foreign_key_value),
            })?;
        Ok(count)
    }

    pub async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_MESSAGES_BY_QUEUE)
            .bind(foreign_key_value)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGES_BY_QUEUE".into(),
                source: Box::new(e),
                context: format!("Failed to delete messages for queue {}", foreign_key_value),
            })?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl crate::store::MessageTable for Messages {
    async fn insert(&self, data: crate::types::NewQueueMessage) -> Result<QueueMessage> {
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_MESSAGE".into(),
                source: Box::new(e),
                context: "Failed to insert message".into(),
            })?;

        Ok(message)
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        let message = sqlx::query_as::<_, QueueMessage>(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_MESSAGE_BY_ID".into(),
                source: Box::new(e),
                context: format!("Failed to get message {}", id),
            })?;

        Ok(message)
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_ALL_MESSAGES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_MESSAGES".into(),
                source: Box::new(e),
                context: "Failed to list all messages".into(),
            })?;

        Ok(messages)
    }

    async fn count(&self) -> Result<i64> {
        let count =
            sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "COUNT_MESSAGES".into(),
                    source: Box::new(e),
                    context: "Failed to count messages".into(),
                })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_BY_ID".into(),
                source: Box::new(e),
                context: format!("Failed to delete message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_MESSAGES_BY_QUEUE)
            .bind(foreign_key_value)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_MESSAGES_BY_QUEUE".into(),
                source: Box::new(e),
                context: format!("Failed to list messages for queue {}", foreign_key_value),
            })?;

        Ok(messages)
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BATCH_INSERT_MESSAGES".into(),
                source: Box::new(e),
                context: format!("Failed to batch insert {} messages", payloads.len()),
            })?;

        Ok(ids)
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(GET_MESSAGES_BY_IDS)
            .bind(ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_MESSAGES_BY_IDS".into(),
                source: Box::new(e),
                context: format!("Failed to get {} messages by IDs", ids.len()),
            })?;

        Ok(messages)
    }

    async fn update_payload(&self, id: i64, payload: serde_json::Value) -> Result<u64> {
        let rows_affected = sqlx::query(UPDATE_MESSAGE_PAYLOAD)
            .bind(id)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "UPDATE_MESSAGE_PAYLOAD".into(),
                source: Box::new(e),
                context: format!("Failed to update payload for message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        let rows_affected = sqlx::query(EXTEND_MESSAGE_VT)
            .bind(id)
            .bind(worker_id)
            .bind(additional_seconds as i32)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "EXTEND_MESSAGE_VT".into(),
                source: Box::new(e),
                context: format!("Failed to extend visibility for message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let extended_ids: Vec<i64> = sqlx::query_scalar(EXTEND_BATCH_VT)
            .bind(message_ids)
            .bind(worker_id)
            .bind(additional_seconds as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "EXTEND_BATCH_VT".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to batch extend visibility for {} messages",
                    message_ids.len()
                ),
            })?;

        let extended_set: std::collections::HashSet<i64> = extended_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| extended_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let released_ids: Vec<i64> = sqlx::query_scalar(RELEASE_SPECIFIC_MESSAGES)
            .bind(message_ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_SPECIFIC_MESSAGES".into(),
                source: Box::new(e),
                context: format!("Failed to release {} messages", message_ids.len()),
            })?;

        let released_set: std::collections::HashSet<i64> = released_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn release_with_visibility(
        &self,
        id: i64,
        worker_id: i64,
        vt: DateTime<Utc>,
    ) -> Result<u64> {
        let rows_affected = sqlx::query(RELEASE_WITH_VT)
            .bind(id)
            .bind(worker_id)
            .bind(vt)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_WITH_VT".into(),
                source: Box::new(e),
                context: format!("Failed to release message {} with visibility", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn count_pending_for_queue(&self, queue_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM pgqrs_messages
            WHERE queue_id = $1 AND (vt IS NULL OR vt <= NOW()) AND consumer_worker_id IS NULL AND archived_at IS NULL
            "#,
        )
        .bind(queue_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("COUNT_PENDING (queue_id={})", queue_id),
            source: Box::new(e),
            context: format!("Failed to count pending messages for queue {}", queue_id),
        })?;

        Ok(count)
    }

    async fn count_pending_for_queue_and_worker(
        &self,
        queue_id: i64,
        worker_id: i64,
    ) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM pgqrs_messages
            WHERE queue_id = $1 AND consumer_worker_id = $2 AND archived_at IS NULL
            "#,
        )
        .bind(queue_id)
        .bind(worker_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("COUNT_PENDING_FILTERED (queue_id={})", queue_id),
            source: Box::new(e),
            context: format!("Failed to count pending messages for queue {}", queue_id),
        })?;

        Ok(count)
    }

    async fn dequeue_at(
        &self,
        queue_id: i64,
        limit: usize,
        vt: u32,
        worker_id: i64,
        now: chrono::DateTime<chrono::Utc>,
        max_read_ct: i32,
    ) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES_AT)
            .bind(queue_id)
            .bind(limit as i64)
            .bind(vt as i32)
            .bind(worker_id)
            .bind(now)
            .bind(max_read_ct)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE_MESSAGES_AT".into(),
                source: Box::new(e),
                context: "Failed to dequeue messages".into(),
            })?;

        Ok(messages)
    }

    async fn archive(&self, id: i64, worker_id: i64) -> Result<Option<QueueMessage>> {
        let result: Option<QueueMessage> = sqlx::query_as::<_, QueueMessage>(ARCHIVE_MESSAGE)
            .bind(id)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_MESSAGE ({})", id),
                source: Box::new(e),
                context: format!("Failed to archive message {}", id),
            })?;

        Ok(result)
    }

    async fn archive_many(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let archived_ids: Vec<i64> = sqlx::query_scalar(ARCHIVE_BATCH)
            .bind(ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ARCHIVE_BATCH".into(),
                source: Box::new(e),
                context: "Failed to archive message batch".into(),
            })?;

        let archived_set: std::collections::HashSet<i64> = archived_ids.into_iter().collect();
        let result = ids.iter().map(|id| archived_set.contains(id)).collect();
        Ok(result)
    }

    async fn replay_dlq(&self, id: i64) -> Result<Option<QueueMessage>> {
        let msg = sqlx::query_as::<_, QueueMessage>(REPLAY_FROM_DLQ)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("REPLAY_FROM_DLQ ({})", id),
                source: Box::new(e),
                context: format!("Failed to replay message {}", id),
            })?;

        Ok(msg)
    }

    async fn delete_owned(&self, id: i64, worker_id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_OWNED)
            .bind(id)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_OWNED".into(),
                source: Box::new(e),
                context: format!("Failed to delete message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    async fn delete_many_owned(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        let deleted_ids: Vec<i64> = sqlx::query_scalar(DELETE_MESSAGE_BATCH_OWNED)
            .bind(ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_BATCH_OWNED".into(),
                source: Box::new(e),
                context: "Failed to delete message batch".into(),
            })?;

        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = ids.iter().map(|id| deleted_set.contains(id)).collect();
        Ok(result)
    }

    async fn list_archived_by_queue(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(
            r#"
            SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
            FROM pgqrs_messages
            WHERE queue_id = $1 AND archived_at IS NOT NULL
            ORDER BY archived_at DESC
            "#,
        )
        .bind(queue_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_ARCHIVED_BY_QUEUE".into(),
            source: Box::new(e),
            context: format!("Failed to list archived messages for queue {}", queue_id),
        })?;

        Ok(messages)
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1")
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "COUNT_MESSAGES_BY_QUEUE".into(),
                    source: Box::new(e),
                    context: format!("Failed to count messages for queue {}", queue_id),
                })?;
        Ok(count)
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(ids.len());

        for &id in ids {
            let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("DELETE_MESSAGE_BY_ID ({})", id),
                    source: Box::new(e),
                    context: format!("Failed to delete message {}", id),
                })?
                .rows_affected();

            results.push(rows_affected > 0);
        }

        Ok(results)
    }
}
