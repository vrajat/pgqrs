
use crate::error::{Error, Result};
use crate::store::QueueStore;
use crate::types::{NewQueue, QueueInfo};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresQueueStore {
    pool: PgPool,
}

impl PostgresQueueStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const INSERT_QUEUE: &str = r#"
    INSERT INTO pgqrs_queues (queue_name)
    VALUES ($1)
    RETURNING id, queue_name, created_at, updated_at;
"#;

const GET_QUEUE_BY_NAME: &str = r#"
    SELECT id, queue_name, created_at, updated_at
    FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

const LIST_QUEUES: &str = r#"
    SELECT id, queue_name, created_at, updated_at
    FROM pgqrs_queues
    ORDER BY created_at DESC;
"#;

const DELETE_QUEUE_BY_NAME: &str = r#"
    DELETE FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

const CHECK_QUEUE_EXISTS: &str = r#"
    SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = $1);
"#;

const LOCK_QUEUE_FOR_UPDATE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR NO KEY UPDATE;
"#;

const GET_QUEUE_METRICS: &str = r#"
    WITH queue_counts AS (
        SELECT
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE vt <= NOW()) as pending_messages,
            COUNT(*) FILTER (WHERE vt > NOW()) as locked_messages,
            MIN(enqueued_at) FILTER (WHERE vt <= NOW()) as oldest_pending_message,
            MAX(enqueued_at) as newest_message
        FROM pgqrs_messages
        WHERE queue_id = $1
    ),
    archive_counts AS (
        SELECT COUNT(*) as archived_messages
        FROM pgqrs_archive
        WHERE queue_id = $1
    )
    SELECT
        $2 as name,
        COALESCE(qc.total_messages, 0) as total_messages,
        COALESCE(qc.pending_messages, 0) as pending_messages,
        COALESCE(qc.locked_messages, 0) as locked_messages,
        COALESCE(ac.archived_messages, 0) as archived_messages,
        qc.oldest_pending_message,
        qc.newest_message
    FROM queue_counts qc
    CROSS JOIN archive_counts ac;
"#;

const GET_ALL_QUEUES_METRICS: &str = r#"
    WITH queue_counts AS (
        SELECT
            queue_id,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE vt <= NOW()) as pending_messages,
            COUNT(*) FILTER (WHERE vt > NOW()) as locked_messages,
            MIN(enqueued_at) FILTER (WHERE vt <= NOW()) as oldest_pending_message,
            MAX(enqueued_at) as newest_message
        FROM pgqrs_messages
        GROUP BY queue_id
    ),
    archive_counts AS (
        SELECT queue_id, COUNT(*) as archived_messages
        FROM pgqrs_archive
        GROUP BY queue_id
    )
    SELECT
        q.queue_name as name,
        COALESCE(qc.total_messages, 0) as total_messages,
        COALESCE(qc.pending_messages, 0) as pending_messages,
        COALESCE(qc.locked_messages, 0) as locked_messages,
        COALESCE(ac.archived_messages, 0) as archived_messages,
        qc.oldest_pending_message,
        qc.newest_message
    FROM pgqrs_queues q
    LEFT JOIN queue_counts qc ON q.id = qc.queue_id
    LEFT JOIN archive_counts ac ON q.id = ac.queue_id;
"#;

#[async_trait::async_trait]
impl QueueStore for PostgresQueueStore {
    type Error = Error;

    async fn get_by_name(&self, name: &str) -> Result<QueueInfo, Self::Error> {
        sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
    }

    async fn insert(&self, data: NewQueue) -> Result<QueueInfo, Self::Error> {
        sqlx::query_as::<_, QueueInfo>(INSERT_QUEUE)
            .bind(data.queue_name)
            .fetch_one(&self.pool)
            .await
    }

    async fn exists(&self, name: &str) -> Result<bool, Self::Error> {
        sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64, Self::Error> {
        let result = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn list(&self) -> Result<Vec<QueueInfo>, Self::Error> {
        sqlx::query_as::<_, QueueInfo>(LIST_QUEUES)
            .fetch_all(&self.pool)
            .await
    }

    async fn purge(&self, name: &str) -> Result<(), Self::Error> {
        // Start a transaction for atomic purge with row locking
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to begin transaction: {}", e),
            })?;

        // Lock the queue row for data modification and get queue_id
        let queue_id: Option<i64> = sqlx::query_scalar(LOCK_QUEUE_FOR_UPDATE)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to lock queue '{}': {}", name, e),
            })?;

        let queue_id = queue_id.ok_or_else(|| Error::QueueNotFound {
            name: name.to_string(),
        })?;

        // Execute deletions directly
        sqlx::query("DELETE FROM pgqrs_messages WHERE queue_id = $1")
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM pgqrs_archive WHERE queue_id = $1")
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM pgqrs_workers WHERE queue_id = $1")
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;

        tx.commit()
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to commit queue purge: {}", e),
            })?;

        Ok(())
    }

    async fn metrics(&self, name: &str) -> Result<crate::types::QueueMetrics, Self::Error> {
        let queue_info = self.get_by_name(name).await?;

        let metrics = sqlx::query_as::<_, crate::types::QueueMetrics>(GET_QUEUE_METRICS)
            .bind(queue_info.id)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to get metrics for queue {}: {}", name, e),
            })?;

        Ok(metrics)
    }

    async fn list_metrics(&self) -> Result<Vec<crate::types::QueueMetrics>, Self::Error> {
        let metrics = sqlx::query_as::<_, crate::types::QueueMetrics>(GET_ALL_QUEUES_METRICS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to get all queues metrics: {}", e),
            })?;

        Ok(metrics)
    }
}
