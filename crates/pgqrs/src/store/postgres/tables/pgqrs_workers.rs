//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`Workers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table.

use crate::error::Result;

use crate::types::{WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::PgPool;

// SQL constants for worker table operations
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    ORDER BY started_at DESC
"#;

const DELETE_WORKER_BY_ID: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE id = $1
"#;

const DELETE_WORKERS_BY_QUEUE: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE queue_id = $1
"#;

const LIST_WORKERS_BY_QUEUE_AND_STATE: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1 AND status = $2
    ORDER BY started_at DESC
"#;

const LIST_ZOMBIE_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    AND status IN ('ready', 'suspended')
    AND heartbeat_at < NOW() - $2
    ORDER BY heartbeat_at ASC
"#;

// --- Lifecycle SQL from lifecycle.rs ---

const FIND_WORKER_BY_HOST_PORT: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE hostname = $1 AND port = $2
"#;

const RESET_WORKER_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready', queue_id = $2, started_at = NOW(), heartbeat_at = NOW(), shutdown_at = NULL
    WHERE id = $1 AND status = 'stopped'
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

const TRANSITION_READY_TO_SUSPENDED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'suspended'
    WHERE id = $1 AND status = 'ready'
    RETURNING id
"#;

const TRANSITION_SUSPENDED_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready'
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

const TRANSITION_SUSPENDED_TO_STOPPED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped', shutdown_at = $2
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

const GET_WORKER_STATUS: &str = r#"
    SELECT status FROM pgqrs_workers WHERE id = $1
"#;

const UPDATE_HEARTBEAT: &str = r#"
    UPDATE pgqrs_workers
    SET heartbeat_at = $1
    WHERE id = $2
"#;

/// Input data for creating a new worker
#[derive(Debug)]
pub struct NewWorker {
    pub hostname: String,
    pub port: i32,
    /// Queue ID (None for Admin workers)
    pub queue_id: Option<i64>,
}

/// Workers table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_workers` table.
#[derive(Debug, Clone)]
pub struct Workers {
    pub pool: PgPool,
}

impl Workers {
    /// Create a new Workers instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::WorkerTable for Workers {
    async fn insert(&self, data: crate::tables::NewWorker) -> Result<WorkerInfo> {
        let now = Utc::now();

        let worker_id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.hostname)
            .bind(data.port)
            .bind(data.queue_id)
            .bind(now)
            .bind(now)
            .bind(WorkerStatus::Ready)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to insert worker: {}", e),
            })?;

        Ok(WorkerInfo {
            id: worker_id,
            hostname: data.hostname,
            port: data.port,
            queue_id: data.queue_id,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    async fn get(&self, id: i64) -> Result<WorkerInfo> {
        let worker = sqlx::query_as::<_, WorkerInfo>(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get worker {}: {}", id, e),
            })?;

        Ok(worker)
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list workers: {}", e),
            })?;

        Ok(workers)
    }

    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_workers";
        let row = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count workers: {}", e),
            })?;
        Ok(row)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete worker {}: {}", id, e),
            })?;

        Ok(result.rows_affected())
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_WORKERS_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to filter workers by queue ID {}: {}", queue_id, e),
            })?;

        Ok(workers)
    }

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<i64> {
        const COUNT_WORKERS_BY_STATE: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_id = $1 AND status = $2
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_WORKERS_BY_STATE)
            .bind(queue_id)
            .bind(state)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count workers for queue {} with state: {}",
                    queue_id, e
                ),
            })?;

        Ok(count)
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        const COUNT_ZOMBIE_WORKERS: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_id = $1
            AND status IN ('ready', 'suspended')
            AND heartbeat_at < NOW() - $2
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count zombie workers for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(count)
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_WORKERS_BY_QUEUE_AND_STATE)
            .bind(queue_id)
            .bind(state)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list workers for queue {} with state: {}",
                    queue_id, e
                ),
            })?;

        Ok(workers)
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list zombie workers for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(workers)
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
            queue_id: Some(1),
        };

        // Test that the NewWorker struct can be created
        assert_eq!(new_worker.hostname, "test-host");
        assert_eq!(new_worker.port, 8080);
        assert_eq!(new_worker.queue_id, Some(1));
    }

    #[test]
    fn test_new_admin_worker_creation() {
        let new_worker = NewWorker {
            hostname: "admin-host".to_string(),
            port: 9090,
            queue_id: None, // Admin workers have no queue
        };

        assert_eq!(new_worker.hostname, "admin-host");
        assert_eq!(new_worker.port, 9090);
        assert_eq!(new_worker.queue_id, None);
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
            // let workers = Workers::new(pool);
            // assert_entity_type(&workers);
            // assert_new_entity_type(&workers);
        }
    }
}
