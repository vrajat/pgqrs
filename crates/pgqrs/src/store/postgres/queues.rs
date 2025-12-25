
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
}
