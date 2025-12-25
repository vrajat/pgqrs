
use crate::error::Result;
use crate::store::ArchiveStore;
use crate::types::ArchivedMessage;
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresArchiveStore {
    pool: PgPool,
}

impl PostgresArchiveStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// SQL Constants
const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_messages
        WHERE id = $1 AND consumer_worker_id = $2
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msg
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at;
"#;

const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE id = ANY($1)
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

const LIST_DLQ_MESSAGES: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
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
    SELECT COUNT(*) FROM pgqrs_archive WHERE consumer_worker_id = $1
"#;

#[async_trait::async_trait]
impl ArchiveStore for PostgresArchiveStore {
    type Error = sqlx::Error;

    async fn archive_message(
        &self,
        msg_id: i64,
        worker_id: i64,
    ) -> Result<Option<ArchivedMessage>, Self::Error> {
        let result: Option<ArchivedMessage> = sqlx::query_as::<_, ArchivedMessage>(ARCHIVE_MESSAGE)
            .bind(msg_id)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(result)
    }

    async fn archive_batch(&self, msg_ids: &[i64]) -> Result<Vec<bool>, Self::Error> {
        if msg_ids.is_empty() {
             return Ok(vec![]);
        }

        let archived_ids: Vec<i64> = sqlx::query_scalar(ARCHIVE_BATCH)
             .bind(msg_ids)
             .fetch_all(&self.pool)
             .await?;

        let archived_set: std::collections::HashSet<i64> = archived_ids.into_iter().collect();
        let result = msg_ids
             .iter()
             .map(|id| archived_set.contains(id))
             .collect();
        Ok(result)
    }

    async fn list_dlq_messages(&self, max_attempts: i32, limit: i64, offset: i64) -> Result<Vec<ArchivedMessage>, Self::Error> {
         sqlx::query_as::<_, ArchivedMessage>(LIST_DLQ_MESSAGES)
            .bind(max_attempts)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64, Self::Error> {
         sqlx::query_scalar(COUNT_DLQ_MESSAGES)
            .bind(max_attempts)
            .fetch_one(&self.pool)
            .await
    }

    async fn list_by_worker(&self, worker_id: i64, limit: i64, offset: i64) -> Result<Vec<ArchivedMessage>, Self::Error> {
         sqlx::query_as::<_, ArchivedMessage>(ARCHIVE_LIST_WITH_WORKER)
             .bind(worker_id)
             .bind(limit)
             .bind(offset)
             .fetch_all(&self.pool)
             .await
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64, Self::Error> {
         sqlx::query_scalar(ARCHIVE_COUNT_WITH_WORKER)
             .bind(worker_id)
             .fetch_one(&self.pool)
             .await
    }
}
