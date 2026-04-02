use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::turso::dialect::TursoDialect;
use crate::store::turso::parse_turso_timestamp;
use crate::types::QueueRecord;
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoQueueTable {
    db: Arc<Database>,
}

impl TursoQueueTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<QueueRecord> {
        let id: i64 = row.get(0)?;
        let queue_name: String = row.get(1)?;
        let created_at_str: String = row.get(2)?;
        let created_at = parse_turso_timestamp(&created_at_str)?;

        Ok(QueueRecord {
            id,
            queue_name,
            created_at,
        })
    }
}

#[async_trait]
impl crate::store::QueueTable for TursoQueueTable {
    async fn insert(&self, data: crate::types::NewQueueRecord) -> Result<QueueRecord> {
        let row_res = crate::store::turso::query(TursoDialect::QUEUE.insert)
            .bind(data.queue_name.as_str())
            .fetch_one_once(&self.db)
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

    async fn get(&self, id: i64) -> Result<QueueRecord> {
        let row = crate::store::turso::query(TursoDialect::QUEUE.get)
            .bind(id)
            .fetch_one(&self.db)
            .await?;
        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<QueueRecord>> {
        let rows = crate::store::turso::query(TursoDialect::QUEUE.list)
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
        let count = crate::store::turso::query(TursoDialect::QUEUE.delete)
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueRecord> {
        let row = crate::store::turso::query(TursoDialect::QUEUE.get_by_name)
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
        let exists: bool = crate::store::turso::query_scalar(TursoDialect::QUEUE.exists)
            .bind(name)
            .fetch_one(&self.db)
            .await?;
        Ok(exists)
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let count = crate::store::turso::query(TursoDialect::QUEUE.delete_by_name)
            .bind(name)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }
}
