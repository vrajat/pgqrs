use crate::error::Result;
use crate::store::QueueTable;
use crate::tables::NewQueue;
use crate::types::{QueueInfo, QueueMetrics};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresQueueTable {
    pool: PgPool,
}

impl PostgresQueueTable {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const INSERT_QUEUE: &str = r#"
    INSERT INTO pgqrs_queues (queue_name)
    VALUES ($1)
    RETURNING id, queue_name, created_at;
"#;

const GET_QUEUE_BY_ID: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues
    WHERE id = $1;
"#;

const GET_QUEUE_BY_NAME: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

const LIST_QUEUES: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues
    ORDER BY created_at DESC;
"#;

const COUNT_QUEUES: &str = r#"
    SELECT COUNT(*) FROM pgqrs_queues;
"#;

const DELETE_QUEUE: &str = r#"
    DELETE FROM pgqrs_queues WHERE id = $1;
"#;

const DELETE_QUEUE_BY_NAME: &str = r#"
    DELETE FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

const CHECK_QUEUE_EXISTS: &str = r#"
    SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = $1);
"#;

const PURGE_QUEUE: &str = r#"
    DELETE FROM pgqrs_messages WHERE queue_id = $1;
"#;

#[async_trait::async_trait]
impl QueueTable for PostgresQueueTable {
    // === CRUD Operations ===

    async fn insert(&self, data: NewQueue) -> Result<QueueInfo> {
        sqlx::query_as::<_, QueueInfo>(INSERT_QUEUE)
            .bind(data.queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn get(&self, id: i64) -> Result<QueueInfo> {
        sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        sqlx::query_as::<_, QueueInfo>(LIST_QUEUES)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn count(&self) -> Result<i64> {
        sqlx::query_scalar(COUNT_QUEUES)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_QUEUE)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    // === Queue-Specific Business Operations ===

    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let result = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn get_metrics(&self, _queue_id: i64) -> Result<QueueMetrics> {
        // TODO: Implement queue metrics calculation
        todo!("Implement get_metrics()")
    }

    async fn purge(&self, queue_id: i64) -> Result<u64> {
        let result = sqlx::query(PURGE_QUEUE)
            .bind(queue_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
