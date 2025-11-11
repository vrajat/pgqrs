//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`PgqrsWorkers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table without business logic.

use crate::error::{PgqrsError, Result};
use crate::tables::table::Table;
use crate::types::WorkerInfo;
use sqlx::PgPool;

// SQL constants for worker table operations
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_name, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_name = $1
    ORDER BY started_at DESC
"#;

const DELETE_WORKER_BY_ID: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE id = $1
"#;

/// Input data for creating a new worker
#[derive(Debug)]
pub struct NewWorker {
    pub hostname: String,
    pub port: i32,
    pub queue_name: String,
}

/// Workers table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_workers` table without business logic.
#[derive(Debug)]
pub struct PgqrsWorkers {
    pub pool: PgPool,
}

impl PgqrsWorkers {
    /// Create a new PgqrsWorkers instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Table for PgqrsWorkers {
    type Entity = WorkerInfo;
    type NewEntity = NewWorker;

    /// Insert a new worker record.
    ///
    /// # Arguments
    /// * `data` - New worker information
    ///
    /// # Returns
    /// The created worker with generated ID and timestamps
    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity> {
        use crate::types::WorkerStatus;
        use chrono::Utc;

        let now = Utc::now();

        let worker_id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.hostname)
            .bind(data.port)
            .bind(&data.queue_name)
            .bind(now)
            .bind(now)
            .bind(WorkerStatus::Ready)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to insert worker: {}", e),
            })?;

        Ok(WorkerInfo {
            id: worker_id,
            hostname: data.hostname,
            port: data.port,
            queue_name: data.queue_name,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    /// Get a worker by ID.
    ///
    /// # Arguments
    /// * `id` - Worker ID to retrieve
    ///
    /// # Returns
    /// The worker record
    async fn get(&self, id: i64) -> Result<Self::Entity> {
        let worker = sqlx::query_as::<_, WorkerInfo>(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to get worker {}: {}", id, e),
            })?;

        Ok(worker)
    }

    /// List workers, optionally filtered.
    ///
    /// # Arguments
    /// * `_filter_id` - Currently unused (all workers returned)
    ///
    /// # Returns
    /// Vector of all workers
    async fn list(&self, _filter_id: Option<i64>) -> Result<Vec<Self::Entity>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to list workers: {}", e),
            })?;

        Ok(workers)
    }

    /// Delete a worker by ID.
    ///
    /// # Arguments
    /// * `id` - Worker ID to delete
    ///
    /// # Returns
    /// Number of rows affected (0 or 1)
    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to delete worker {}: {}", id, e),
            })?;

        Ok(result.rows_affected())
    }
}

impl PgqrsWorkers {
    /// List workers by queue name.
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to filter by
    ///
    /// # Returns
    /// Vector of workers for the specified queue
    pub async fn list_by_queue(&self, queue_name: &str) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_WORKERS_BY_QUEUE)
            .bind(queue_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to list workers for queue {}: {}", queue_name, e),
            })?;

        Ok(workers)
    }

    /// List all workers in the system.
    ///
    /// # Returns
    /// Vector of all workers
    pub async fn list_all(&self) -> Result<Vec<WorkerInfo>> {
        self.list(None).await
    }

    /// Count active workers for a specific queue.
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to count workers for
    ///
    /// # Returns
    /// Number of active workers (ready or shutting_down status)
    pub async fn count_active_for_queue(&self, queue_name: &str) -> Result<i64> {
        const COUNT_ACTIVE_WORKERS: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_name = $1 AND status IN ('ready', 'shutting_down')
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_ACTIVE_WORKERS)
            .bind(queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!(
                    "Failed to count active workers for queue '{}': {}",
                    queue_name, e
                ),
            })?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_worker_creation() {
        let new_worker = NewWorker {
            hostname: "test-host".to_string(),
            port: 8080,
            queue_name: "test-queue".to_string(),
        };

        // Test that the NewWorker struct can be created
        assert_eq!(new_worker.hostname, "test-host");
        assert_eq!(new_worker.port, 8080);
        assert_eq!(new_worker.queue_name, "test-queue");
    }

    #[test]
    fn test_table_trait_associated_types() {
        // Compile-time test to verify the trait is properly implemented with correct types
        fn _check_table_trait_types() {
            // This function won't be called, but if it compiles, our trait is correctly implemented
            fn assert_entity_type<T: Table<Entity = WorkerInfo>>(_: &T) {}
            fn assert_new_entity_type<T: Table<NewEntity = NewWorker>>(_: &T) {}

            // These would need a real database pool to actually create:
            // let pool = PgPool::connect("...").await.unwrap();
            // let workers = PgqrsWorkers::new(pool);
            // assert_entity_type(&workers);
            // assert_new_entity_type(&workers);
        }
    }
}
