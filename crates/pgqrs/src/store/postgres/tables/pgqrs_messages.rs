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

const DELETE_MESSAGES_BY_QUEUE: &str = r#"
    DELETE FROM pgqrs_messages WHERE queue_id = $1
"#;

const COUNT_MESSAGES_BY_QUEUE_TX: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1
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
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, data: crate::types::NewMessage) -> Result<QueueMessage> {
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
                source: e,
                context: "Failed to insert message".into(),
            })?;

        Ok(message)
    }

    pub async fn get(&self, id: i64) -> Result<QueueMessage> {
        let message = sqlx::query_as::<_, QueueMessage>(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_MESSAGE_BY_ID".into(),
                source: e,
                context: format!("Failed to get message {}", id),
            })?;

        Ok(message)
    }

    pub async fn list(&self) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_ALL_MESSAGES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_MESSAGES".into(),
                source: e,
                context: "Failed to list all messages".into(),
            })?;

        Ok(messages)
    }

    pub async fn count(&self) -> Result<i64> {
        let count = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_messages")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_MESSAGES".into(),
                source: e,
                context: "Failed to count messages".into(),
            })?;
        Ok(count)
    }

    pub async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_BY_ID".into(),
                source: e,
                context: format!("Failed to delete message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    pub async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(LIST_MESSAGES_BY_QUEUE)
            .bind(foreign_key_value)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_MESSAGES_BY_QUEUE".into(),
                source: e,
                context: format!("Failed to list messages for queue {}", foreign_key_value),
            })?;

        Ok(messages)
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
                source: e,
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
                source: e,
                context: format!("Failed to delete messages for queue {}", foreign_key_value),
            })?;
        Ok(result.rows_affected())
    }

    // Existing inherent methods
    pub async fn batch_insert(
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
                source: e,
                context: format!("Failed to batch insert {} messages", payloads.len()),
            })?;

        Ok(ids)
    }

    pub async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        let messages = sqlx::query_as::<_, QueueMessage>(GET_MESSAGES_BY_IDS)
            .bind(ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_MESSAGES_BY_IDS".into(),
                source: e,
                context: format!("Failed to get {} messages by IDs", ids.len()),
            })?;

        Ok(messages)
    }

    pub async fn update_visibility_timeout(&self, id: i64, vt: DateTime<Utc>) -> Result<u64> {
        let rows_affected = sqlx::query(UPDATE_MESSAGE_VT)
            .bind(id)
            .bind(vt)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "UPDATE_MESSAGE_VT".into(),
                source: e,
                context: format!("Failed to update visibility timeout for message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "EXTEND_MESSAGE_VT".into(),
                source: e,
                context: format!("Failed to extend visibility for message {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "EXTEND_BATCH_VT".into(),
                source: e,
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_SPECIFIC_MESSAGES".into(),
                source: e,
                context: format!("Failed to release {} messages", message_ids.len()),
            })?;

        let released_set: std::collections::HashSet<i64> = released_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();

        Ok(result)
    }

    pub async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending_filtered(queue_id, None).await
    }

    pub async fn count_pending_filtered(
        &self,
        queue_id: i64,
        worker_id: Option<i64>,
    ) -> Result<i64> {
        let count = match worker_id {
            Some(wid) => {
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
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("COUNT_PENDING (queue_id={})", queue_id),
            source: e,
            context: format!("Failed to count pending messages for queue {}", queue_id),
        })?;

        Ok(count)
    }

    pub async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(ids.len());

        for &id in ids {
            let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("DELETE_MESSAGE_BY_ID ({})", id),
                    source: e,
                    context: format!("Failed to delete message {}", id),
                })?
                .rows_affected();

            results.push(rows_affected > 0);
        }

        Ok(results)
    }
}

#[async_trait]
impl crate::store::MessageTable for Messages {
    async fn insert(&self, data: crate::types::NewMessage) -> Result<QueueMessage> {
        self.insert(data).await
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        self.get(id).await
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        self.list().await
    }

    async fn count(&self) -> Result<i64> {
        self.count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.delete(id).await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        self.filter_by_fk(queue_id).await
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        self.batch_insert(queue_id, payloads, params).await
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        self.get_by_ids(ids).await
    }

    async fn update_visibility_timeout(&self, id: i64, vt: DateTime<Utc>) -> Result<u64> {
        self.update_visibility_timeout(id, vt).await
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        self.extend_visibility(id, worker_id, additional_seconds)
            .await
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        self.extend_visibility_batch(message_ids, worker_id, additional_seconds)
            .await
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        self.release_messages_by_ids(message_ids, worker_id).await
    }

    async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending(queue_id).await
    }

    async fn count_pending_filtered(&self, queue_id: i64, worker_id: Option<i64>) -> Result<i64> {
        self.count_pending_filtered(queue_id, worker_id).await
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        self.delete_by_ids(ids).await
    }
}
