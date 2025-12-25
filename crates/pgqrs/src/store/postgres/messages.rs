use crate::error::Result;
use crate::store::MessageStore;
use crate::types::QueueMessage;
use chrono::{Duration, Utc};
use serde_json::Value;
use sqlx::PgPool;


#[derive(Clone, Debug)]
pub struct PostgresMessageStore {
    pool: PgPool,
    max_read_ct: u32,
}

impl PostgresMessageStore {
    pub fn new(pool: PgPool, max_read_ct: u32) -> Self {
        Self { pool, max_read_ct }
    }
}

// SQL Constants
const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    VALUES ($1, $2, 0, $3, $4, $5, NULL)
    RETURNING id
"#;

const BATCH_INSERT_MESSAGES: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    SELECT $1, unnest($2::jsonb[]), 0, $3, $4, $5, NULL
    RETURNING id;
"#;

const GET_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = $1;
"#;

// From consumer.rs
const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages t
    SET consumer_worker_id = $5, vt = NOW() + make_interval(secs => $4::double precision), read_ct = read_ct + 1, dequeued_at = NOW()
    FROM (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1 AND (vt IS NULL OR vt <= NOW()) AND consumer_worker_id IS NULL AND read_ct < $3
        ORDER BY id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    ) selected
    WHERE t.id = selected.id
    RETURNING t.id, t.queue_id, t.producer_worker_id, t.consumer_worker_id, t.payload, t.vt, t.enqueued_at, t.read_ct, t.dequeued_at;
"#;

const DELETE_MESSAGE_BY_ID: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1;
"#;

const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

const EXTEND_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = vt + make_interval(secs => $3::double precision)
    WHERE id = $1 AND consumer_worker_id = $2;
"#;

const RELEASE_SPECIFIC_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NOW(), consumer_worker_id = NULL
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

#[async_trait::async_trait]
impl MessageStore for PostgresMessageStore {
    type Error = sqlx::Error;

    async fn get(&self, id: i64) -> Result<QueueMessage, Self::Error> {
        sqlx::query_as::<_, QueueMessage>(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
    }

    async fn enqueue(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> Result<i64, Self::Error> {
        let now = Utc::now();
        let vt = fast_calc_vt(now, delay_seconds);

        let id: i64 = sqlx::query_scalar(INSERT_MESSAGE)
            .bind(queue_id)
            .bind(payload)
            .bind(now)
            .bind(vt)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(id)
    }

    async fn enqueue_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> Result<Vec<i64>, Self::Error> {
        let now = Utc::now();
        let vt = now; // No delay support in batch enqueue currently (as per producer.rs implementation)

        let ids: Vec<i64> = sqlx::query_scalar(BATCH_INSERT_MESSAGES)
            .bind(queue_id)
            .bind(payloads)
            .bind(now)
            .bind(vt)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(ids)
    }

    async fn dequeue(
        &self,
        queue_id: i64,
        worker_id: i64,
        limit: usize,
        vt_seconds: u32,
    ) -> Result<Vec<QueueMessage>, Self::Error> {
        let messages = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES)
            .bind(queue_id)
            .bind(limit as i64)
            .bind(self.max_read_ct as i32)
            .bind(vt_seconds as i32)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(messages)
    }

    async fn delete(&self, id: i64) -> Result<bool, Self::Error> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(rows_affected > 0)
    }

    async fn delete_batch(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>, Self::Error> {
        let deleted_ids: Vec<i64> = sqlx::query_scalar(DELETE_MESSAGE_BATCH)
            .bind(ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;

        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = ids
            .iter()
            .map(|id| deleted_set.contains(id))
            .collect();
        Ok(result)
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<bool, Self::Error> {
        let rows_affected = sqlx::query(EXTEND_MESSAGE_VT)
            .bind(id)
            .bind(worker_id)
            .bind(additional_seconds as i32)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(rows_affected > 0)
    }

    async fn release(
        &self,
        id: i64,
        worker_id: i64,
    ) -> Result<bool, Self::Error> {
        let result = self.release_batch(&[id], worker_id).await?;
        Ok(result.first().copied().unwrap_or(false))
    }

    async fn release_batch(
        &self,
        ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>, Self::Error> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let released_ids: Vec<i64> = sqlx::query_scalar(RELEASE_SPECIFIC_MESSAGES)
            .bind(ids)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;

        let released_set: std::collections::HashSet<i64> = released_ids.into_iter().collect();
        let result = ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();
        Ok(result)
    }
}

fn fast_calc_vt(now: chrono::DateTime<Utc>, delay_seconds: Option<u32>) -> chrono::DateTime<Utc> {
    match delay_seconds {
        Some(sec) if sec > 0 => now + Duration::seconds(sec as i64),
        _ => now,
    }
}
