use crate::error::Result;
use crate::store::sqlite::parse_sqlite_timestamp;
use crate::types::QueueInfo;
use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

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
pub struct SqliteQueueTable {
    pool: SqlitePool,
}

impl SqliteQueueTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<QueueInfo> {
        let id: i64 = row.try_get("id")?;
        let queue_name: String = row.try_get("queue_name")?;
        let created_at_str: String = row.try_get("created_at")?;
        let created_at = parse_sqlite_timestamp(&created_at_str)?;

        Ok(QueueInfo {
            id,
            queue_name,
            created_at,
        })
    }
}

#[async_trait]
impl crate::store::QueueTable for SqliteQueueTable {
    async fn insert(&self, data: crate::types::NewQueue) -> Result<QueueInfo> {
        let row = sqlx::query(INSERT_QUEUE)
            .bind(&data.queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                if let sqlx::Error::Database(db_err) = &e {
                    // SQLite unique constraint violation code is 2067 (SQLITE_CONSTRAINT_UNIQUE)
                    // or sometimes 1555 (primary key) or 19 (constraint)
                    // sqlx might expose it via code()
                    if let Some(code) = db_err.code() {
                        if code == "2067" || code == "1555" || code == "19" {
                            return crate::error::Error::QueueAlreadyExists {
                                name: data.queue_name.clone(),
                            };
                        }
                    }
                }
                crate::error::Error::QueryFailed {
                    query: format!("INSERT_QUEUE ({})", data.queue_name),
                    source: Box::new(e),
                    context: format!("Failed to create queue '{}'", data.queue_name),
                }
            })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<QueueInfo> {
        let row = sqlx::query(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_QUEUE_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to get queue {}", id),
            })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        let rows = sqlx::query(LIST_ALL_QUEUES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_QUEUES".into(),
                source: Box::new(e),
                context: "Failed to list all queues".into(),
            })?;

        let mut queues = Vec::with_capacity(rows.len());
        for row in rows {
            queues.push(Self::map_row(row)?);
        }
        Ok(queues)
    }

    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_queues";
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_QUEUES".into(),
                source: Box::new(e),
                context: "Failed to count queues".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_QUEUE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_QUEUE_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete queue {}", id),
            })?;

        Ok(result.rows_affected())
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        let row = sqlx::query(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => crate::error::Error::QueueNotFound {
                    name: name.to_string(),
                },
                _ => crate::error::Error::QueryFailed {
                    query: format!("GET_QUEUE_BY_NAME ({})", name),
                    source: Box::new(e),
                    context: format!("Failed to get queue '{}'", name),
                },
            })?;

        Self::map_row(row)
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("CHECK_QUEUE_EXISTS ({})", name),
                source: Box::new(e),
                context: format!("Failed to check if queue '{}' exists", name),
            })?;

        Ok(exists)
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let result = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_QUEUE_BY_NAME ({})", name),
                source: Box::new(e),
                context: format!("Failed to delete queue '{}'", name),
            })?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::QueueTable;
    use crate::types::NewQueue;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("Failed to create pool");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to create queues table");

        pool
    }

    #[tokio::test]
    async fn test_queue_insert_and_get() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        let queue = table
            .insert(NewQueue {
                queue_name: "test_queue".to_string(),
            })
            .await
            .expect("Failed to insert queue");

        assert_eq!(queue.queue_name, "test_queue");
        assert!(queue.id > 0);

        let fetched = table.get(queue.id).await.expect("Failed to get queue");
        assert_eq!(fetched.queue_name, "test_queue");
        assert_eq!(fetched.id, queue.id);
    }

    #[tokio::test]
    async fn test_queue_get_by_name() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        table
            .insert(NewQueue {
                queue_name: "named_queue".to_string(),
            })
            .await
            .expect("Failed to insert queue");

        let queue = table
            .get_by_name("named_queue")
            .await
            .expect("Failed to get by name");
        assert_eq!(queue.queue_name, "named_queue");
    }

    #[tokio::test]
    async fn test_queue_list_and_count() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        table
            .insert(NewQueue {
                queue_name: "queue1".to_string(),
            })
            .await
            .expect("Failed to insert");
        table
            .insert(NewQueue {
                queue_name: "queue2".to_string(),
            })
            .await
            .expect("Failed to insert");

        let queues = table.list().await.expect("Failed to list");
        assert!(queues.len() >= 2);

        let count = table.count().await.expect("Failed to count");
        assert!(count >= 2);
    }

    #[tokio::test]
    async fn test_queue_exists() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        table
            .insert(NewQueue {
                queue_name: "exists_test".to_string(),
            })
            .await
            .expect("Failed to insert");

        let exists = table
            .exists("exists_test")
            .await
            .expect("Failed to check exists");
        assert!(exists);

        let not_exists = table
            .exists("nonexistent")
            .await
            .expect("Failed to check exists");
        assert!(!not_exists);
    }

    #[tokio::test]
    async fn test_queue_delete() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        let queue = table
            .insert(NewQueue {
                queue_name: "delete_test".to_string(),
            })
            .await
            .expect("Failed to insert");

        let deleted = table.delete(queue.id).await.expect("Failed to delete");
        assert_eq!(deleted, 1);

        let exists = table
            .exists("delete_test")
            .await
            .expect("Failed to check exists");
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_queue_delete_by_name() {
        let pool = create_test_pool().await;
        let table = SqliteQueueTable::new(pool);

        table
            .insert(NewQueue {
                queue_name: "delete_by_name_test".to_string(),
            })
            .await
            .expect("Failed to insert");

        let deleted = table
            .delete_by_name("delete_by_name_test")
            .await
            .expect("Failed to delete");
        assert_eq!(deleted, 1);
    }
}
