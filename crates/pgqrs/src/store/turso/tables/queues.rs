use crate::error::Result;
use crate::store::turso::parse_turso_timestamp;
use crate::types::QueueInfo;
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

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

const LIST_ALL_QUEUES: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues
    ORDER BY created_at DESC;
"#;

const DELETE_QUEUE_BY_ID: &str = r#"
    DELETE FROM pgqrs_queues
    WHERE id = $1;
"#;

const DELETE_QUEUE_BY_NAME: &str = r#"
    DELETE FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

const CHECK_QUEUE_EXISTS: &str = r#"
    SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = $1);
"#;

#[derive(Debug, Clone)]
pub struct TursoQueueTable {
    db: Arc<Database>,
}

impl TursoQueueTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<QueueInfo> {
        let id: i64 = row.get(0)?;
        let queue_name: String = row.get(1)?;
        let created_at_str: String = row.get(2)?;
        let created_at = parse_turso_timestamp(&created_at_str)?;

        Ok(QueueInfo {
            id,
            queue_name,
            created_at,
        })
    }
}

#[async_trait]
impl crate::store::QueueTable for TursoQueueTable {
    async fn insert(&self, data: crate::types::NewQueue) -> Result<QueueInfo> {
        let row_res = crate::store::turso::query(INSERT_QUEUE)
            .bind(data.queue_name.as_str())
            .fetch_one(&self.db)
            .await;

        match row_res {
            Ok(row) => Self::map_row(&row),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("UNIQUE constraint failed") || msg.contains("constraint failed") {
                    return Err(crate::error::Error::QueueAlreadyExists {
                        name: data.queue_name.clone(),
                    });
                }
                if let crate::error::Error::NotFound { .. } = e {
                    return Err(crate::error::Error::QueueAlreadyExists {
                        name: data.queue_name.clone(),
                    });
                }
                Err(e)
            }
        }
    }

    async fn get(&self, id: i64) -> Result<QueueInfo> {
        let row = crate::store::turso::query(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.db)
            .await?;
        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        let rows = crate::store::turso::query(LIST_ALL_QUEUES)
            .fetch_all(&self.db)
            .await?;

        let mut queues = Vec::with_capacity(rows.len());
        for row in rows {
            queues.push(Self::map_row(&row)?);
        }
        Ok(queues)
    }

    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_queues";
        let count: i64 = crate::store::turso::query_scalar(query)
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query(DELETE_QUEUE_BY_ID)
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(count)
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        let row = crate::store::turso::query(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_optional(&self.db)
            .await?;

        match row {
            Some(r) => Self::map_row(&r),
            None => Err(crate::error::Error::QueueNotFound {
                name: name.to_string(),
            }),
        }
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let exists: bool = crate::store::turso::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.db)
            .await?;
        Ok(exists)
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let count = crate::store::turso::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.db)
            .await?;
        Ok(count)
    }
}
