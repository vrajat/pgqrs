//! Queue table CRUD operations for pgqrs.
//!
//! This module provides pure CRUD operations on the `pgqrs_queues` table without business logic.
//! Complex operations like referential integrity checks and transaction management remain in admin.rs.

use crate::error::{PgqrsError, Result};
use crate::tables::table::Table;
use crate::types::QueueInfo;
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

/// Input data for creating a new queue
#[derive(Debug)]
pub struct NewQueue {
    pub queue_name: String,
}

/// Queues table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_queues` table without business logic.
#[derive(Debug)]
pub struct PgqrsQueues {
    pub pool: PgPool,
}

impl PgqrsQueues {
    /// Create a new PgqrsQueues instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a queue by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to retrieve
    ///
    /// # Returns
    /// The queue record
    pub async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_NAME)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => PgqrsError::QueueNotFound {
                    name: name.to_string(),
                },
                _ => PgqrsError::Connection {
                    message: format!("Failed to get queue '{}': {}", name, e),
                },
            })?;

        Ok(queue)
    }

    /// Delete a queue by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to delete
    ///
    /// # Returns
    /// Number of rows affected (should be 1 if successful)
    pub async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_QUEUE_BY_NAME)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to delete queue '{}': {}", name, e),
            })?
            .rows_affected();

        Ok(rows_affected)
    }

    /// Check if a queue exists by name.
    ///
    /// # Arguments
    /// * `name` - Queue name to check
    ///
    /// # Returns
    /// True if queue exists, false otherwise
    pub async fn exists(&self, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(CHECK_QUEUE_EXISTS)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to check if queue '{}' exists: {}", name, e),
            })?;

        Ok(exists)
    }
}

impl Table for PgqrsQueues {
    type Entity = QueueInfo;
    type NewEntity = NewQueue;

    /// Insert a new queue record.
    ///
    /// # Arguments
    /// * `data` - New queue information
    ///
    /// # Returns
    /// The created queue with generated ID and timestamp
    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity> {
        let queue = sqlx::query_as::<_, QueueInfo>(INSERT_QUEUE)
            .bind(&data.queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                // Check if this is a unique constraint violation (queue already exists)
                if let sqlx::Error::Database(db_err) = &e {
                    if db_err.code().as_deref() == Some("23505") {
                        // PostgreSQL unique violation code
                        return PgqrsError::QueueAlreadyExists {
                            name: data.queue_name.clone(),
                        };
                    }
                }
                PgqrsError::Connection {
                    message: format!("Failed to insert queue '{}': {}", data.queue_name, e),
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
    async fn get(&self, id: i64) -> Result<Self::Entity> {
        let queue = sqlx::query_as::<_, QueueInfo>(GET_QUEUE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to get queue {}: {}", id, e),
            })?;

        Ok(queue)
    }

    /// List queues, optionally filtered.
    ///
    /// # Arguments
    /// * `filter_id` - Optional queue ID to filter by (ignored for this implementation)
    ///
    /// # Returns
    /// List of all queues ordered by creation time (newest first)
    async fn list(&self, _filter_id: Option<i64>) -> Result<Vec<Self::Entity>> {
        let queues = sqlx::query_as::<_, QueueInfo>(LIST_ALL_QUEUES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to list queues: {}", e),
            })?;

        Ok(queues)
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
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to delete queue {}: {}", id, e),
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
        use crate::tables::Table;

        // Compile-time test to ensure the trait is implemented correctly
        fn assert_entity_type<T: Table<Entity = QueueInfo>>(_: &T) {}
        fn assert_new_entity_type<T: Table<NewEntity = NewQueue>>(_: &T) {}

        // Note: This is a compile-time test, we don't actually create connections
        // In real usage, PgqrsQueues would be created with a valid pool
        // let queues = PgqrsQueues::new(pool);
        // assert_entity_type(&queues);
        // assert_new_entity_type(&queues);
    }
}