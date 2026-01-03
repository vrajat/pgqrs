//! Queue table CRUD operations for pgqrs.
//!
//! This module provides pure CRUD operations on the `pgqrs_queues` table without business logic.
//! Complex operations like referential integrity checks and transaction management remain in admin.rs.

use crate::error::Result;
use crate::types::QueueInfo;
use async_trait::async_trait;
use sqlx::PgPool;

// SQL constants for queue table operations
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

/// Queues table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_queues` table without business logic.
#[derive(Debug, Clone)]
pub struct Queues {
    pub pool: PgPool,
}

impl Queues {
    /// Create a new Queues instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Delete a queue by name using a transaction.
    ///
    /// # Arguments
    /// * `name` - Queue name to delete
    /// * `tx` - Database transaction
    ///
    /// # Returns
    /// * `Result<u64>` - Number of rows affected
    pub async fn delete_by_name_tx<'a, 'b: 'a>(
        name: &str,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_QUEUE_BY_NAME ({})", name),
                source: e,
                context: format!("Failed to delete queue '{}' in transaction", name),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}

#[async_trait]
impl crate::store::QueueTable for Queues {
    /// Insert a new queue record.
    ///
    /// # Arguments
    /// * `data` - New queue information
    ///
    /// # Returns
    /// The created queue with generated ID and timestamp
    async fn insert(&self, data: crate::types::NewQueue) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(INSERT_QUEUE)
            .bind(&data.queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                // Check if this is a unique constraint violation (queue already exists)
                if let sqlx::Error::Database(db_err) = &e {
                    if db_err.code().as_deref() == Some("23505") {
                        // PostgreSQL unique violation code
                        return crate::error::Error::QueueAlreadyExists {
                            name: data.queue_name.clone(),
                        };
                    }
                }
                crate::error::Error::QueryFailed {
                    query: format!("INSERT_QUEUE ({})", data.queue_name),
                    source: e,
                    context: format!("Failed to create queue '{}'", data.queue_name),
                }
            })?;

        Ok(queue)
    }

    /// Get a queue by ID.
    ///
    /// # Arguments
    /// * `id` - Queue ID to retrieve
    ///
    /// # Returns
    /// The queue record
    async fn get(&self, id: i64) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_QUEUE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to get queue {}", id),
            })?;

        Ok(queue)
    }

    /// List all queues.
    ///
    /// # Returns
    /// List of all queue records
    async fn list(&self) -> Result<Vec<QueueInfo>> {
        let queues = sqlx::query_as::<_, QueueInfo>(LIST_ALL_QUEUES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_QUEUES".into(),
                source: e,
                context: "Failed to list all queues".into(),
            })?;

        Ok(queues)
    }

    /// Count all queues.
    ///
    /// # Returns
    /// Total number of queues in the table
    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_queues";
        let count = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_QUEUES".into(),
                source: e,
                context: "Failed to count queues".into(),
            })?;
        Ok(count)
    }

    /// Delete a queue by ID.
    ///
    /// # Arguments
    /// * `id` - Queue ID to delete
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_QUEUE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to delete queue {}", id),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    /// Get a queue by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to retrieve
    ///
    /// # Returns
    /// The queue record
    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => crate::error::Error::QueueNotFound {
                    name: name.to_string(),
                },
                _ => crate::error::Error::QueryFailed {
                    query: format!("GET_QUEUE_BY_NAME ({})", name),
                    source: e,
                    context: format!("Failed to get queue '{}'", name),
                },
            })?;

        Ok(queue)
    }

    /// Check if a queue exists by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to check
    ///
    /// # Returns
    /// True if queue exists, false otherwise
    async fn exists(&self, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("CHECK_QUEUE_EXISTS ({})", name),
                source: e,
                context: format!("Failed to check if queue '{}' exists", name),
            })?;

        Ok(exists)
    }

    /// Delete a queue by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to delete
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_QUEUE_BY_NAME ({})", name),
                source: e,
                context: format!("Failed to delete queue '{}'", name),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}
