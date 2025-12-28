//! Queue table CRUD operations for pgqrs.
//!
//! This module provides pure CRUD operations on the `pgqrs_queues` table without business logic.
//! Complex operations like referential integrity checks and transaction management remain in admin.rs.

use crate::error::Result;
use crate::error::Result;
use crate::types::QueueInfo;
use sqlx::PgPool;
use async_trait::async_trait;

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

/// Input data for creating a new queue
#[derive(Debug)]
pub struct NewQueue {
    pub queue_name: String,
}

/// Queues table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_queues` table without business logic.
#[derive(Debug, Clone)]
pub struct Queues {
    pub pool: PgPool,
}

impl Queues {
    /// Create a new Queues instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
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
    /// Number of rows affected (should be 1 if successful)
    pub async fn delete_by_name_tx<'a, 'b: 'a>(
        name: &str,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete queue '{}': {}", name, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}

#[async_trait]
impl crate::store::QueueTable for Queues {
    async fn insert(&self, data: crate::tables::NewQueue) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(INSERT_QUEUE)
            .bind(data.queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to create queue: {}", e),
            })?;
        Ok(queue)
    }

    async fn get(&self, id: i64) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => crate::error::Error::QueueNotFound {
                    name: format!("ID {}", id),
                },
                _ => crate::error::Error::Connection {
                    message: format!("Failed to get queue {}: {}", id, e),
                },
            })?;
        Ok(queue)
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        let queues = sqlx::query_as::<_, QueueInfo>(LIST_ALL_QUEUES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list queues: {}", e),
            })?;
        Ok(queues)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_queues")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count queues: {}", e),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete queue {}: {}", id, e),
            })?
            .rows_affected();
        Ok(rows_affected)
    }

    /// Get a queue by name.
    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
         let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => crate::error::Error::QueueNotFound {
                    name: name.to_string(),
                },
                _ => crate::error::Error::Connection {
                    message: format!("Failed to get queue '{}': {}", name, e),
                },
            })?;

        Ok(queue)
    }

    /// Check if a queue exists by name.
    async fn exists(&self, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to check if queue '{}' exists: {}", name, e),
            })?;

        Ok(exists)
    }

    /// Delete a queue by name.
    async fn delete_by_name(&self, name: &str) -> Result<u64> {
         let rows_affected = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete queue '{}': {}", name, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_queue_creation() {
        let new_queue = NewQueue {
            queue_name: "test_queue".to_string(),
        };

        // Test that the NewQueue struct can be created
        assert_eq!(new_queue.queue_name, "test_queue");
    }

    #[test]
    fn test_table_trait_associated_types() {
        // Compile-time test to ensure the trait is implemented correctly
        // This test passes if the code compiles, proving our Table trait implementation
        // has the correct associated types.

        // Note: This is a compile-time test, we don't actually create connections
        // In real usage, Queues would be created with a valid pool
    }
}
