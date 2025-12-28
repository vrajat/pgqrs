//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`Workers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table.

use crate::error::Result;
use crate::tables::table::Table;
use crate::types::{WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::PgPool;

// SQL constants for worker table operations
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, 'ready')
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

    /// Insert a new worker record.
    ///
    /// # Arguments
    /// * `data` - New worker information
    ///
    /// # Returns
    /// The created worker with generated ID and timestamps
    async fn insert(&self, data: NewWorker) -> Result<WorkerInfo> {
        let now = Utc::now();

        let worker_id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.hostname)
            .bind(data.port)
            .bind(data.queue_id)
            .bind(now)
            .bind(now)
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

    /// Get a worker by ID.
    ///
    /// # Arguments
    /// * `id` - Worker ID to retrieve
    ///
    /// # Returns
    /// The worker record
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

    /// List all workers.
    ///
    /// # Returns
    /// Vector of all workers
    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list workers: {}", e),
            })?;

        Ok(workers)
    }

    /// Filter workers by queue ID.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Queue ID to filter by
    ///
    /// # Returns
    /// Filter workers by queue ID.
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

    /// Count all workers.
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

    /// Count workers by queue ID.
    async fn count_for_fk<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = $1";
        let row = sqlx::query_scalar(query)
            .bind(queue_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count workers for queue {}: {}", queue_id, e),
            })?;
        Ok(row)
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
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete worker {}: {}", id, e),
            })?;

        Ok(result.rows_affected())
    }

    /// Delete workers by queue ID within a transaction.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Queue ID to filter by
    /// * `tx` - Mutable reference to an active SQL transaction
    /// # Returns
    /// Number of rows affected
    async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let rows_affected = sqlx::query(DELETE_WORKERS_BY_QUEUE)
            .bind(queue_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete messages for queue {}: {}", queue_id, e),
            })?
            .rows_affected();
        Ok(rows_affected)
    }

    /// Count workers for a queue in a specific state
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue
    /// * `state` - Worker state to filter by
    ///
    /// # Returns
    /// Count of workers matching the criteria
    pub async fn count_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
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

    /// Count potentially dead "zombie" workers.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue to check
    /// * `older_than` - Duration threshold for last heartbeat
    ///
    /// # Returns
    /// Count of zombie workers
    pub async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: Duration,
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

    /// List workers for a queue in a specific state.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue
    /// * `state` - Worker state to filter by
    ///
    /// # Returns
    /// List of workers matching the criteria
    pub async fn list_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
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

    /// List potentially dead "zombie" workers.
    ///
    /// Finds workers that are in Ready or Suspended state but haven't
    /// sent a heartbeat within the specified duration.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue to check
    /// * `older_than` - Duration threshold for last heartbeat
    ///
    /// # Returns
    /// List of zombie workers
    pub async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: Duration,
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

    /// List potentially dead "zombie" workers within a transaction.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue to check
    /// * `older_than` - Duration threshold for last heartbeat
    /// * `tx` - Mutable reference to an active SQL transaction
    ///
    /// # Returns
    /// List of zombie workers
    pub async fn list_zombies_for_queue_tx<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        older_than: Duration,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list zombie workers for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(workers)
    }

    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        let status: WorkerStatus = sqlx::query_scalar(GET_WORKER_STATUS)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get worker {} status: {}", worker_id, e),
            })?;

        Ok(status)
    }

    async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();
        sqlx::query(UPDATE_HEARTBEAT)
            .bind(now)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to update heartbeat for worker {}: {}", worker_id, e),
            })?;

        Ok(())
    }

    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool> {
        let threshold = Utc::now() - max_age;

        // Query returns true if heartbeat_at >= threshold (i.e., within max_age)
        let is_healthy: bool =
            sqlx::query_scalar("SELECT heartbeat_at >= $2 FROM pgqrs_workers WHERE id = $1")
                .bind(worker_id)
                .bind(threshold)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::Connection {
                    message: format!("Failed to check health for worker {}: {}", worker_id, e),
                })?;

        Ok(is_healthy)
    }

    async fn suspend(&self, worker_id: i64) -> Result<()> {
       let result: Option<i64> = sqlx::query_scalar(TRANSITION_READY_TO_SUSPENDED)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to suspend worker {}: {}", worker_id, e),
            })?;

        match result {
            Some(_) => Ok(()),
            None => {
                let current_status = self.get_status(worker_id).await?;
                Err(crate::error::Error::InvalidStateTransition {
                    from: current_status.to_string(),
                    to: "suspended".to_string(),
                    reason: "Worker must be in Ready state to suspend".to_string(),
                })
            }
        }
    }

    async fn resume(&self, worker_id: i64) -> Result<()> {
        let result: Option<i64> = sqlx::query_scalar(TRANSITION_SUSPENDED_TO_READY)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to resume worker {}: {}", worker_id, e),
            })?;

        match result {
            Some(_) => Ok(()),
            None => {
                let current_status = self.get_status(worker_id).await?;
                Err(crate::error::Error::InvalidStateTransition {
                    from: current_status.to_string(),
                    to: "ready".to_string(),
                    reason: "Worker must be in Suspended state to resume".to_string(),
                })
            }
        }
    }

    async fn shutdown(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();
        let result: Option<i64> = sqlx::query_scalar(TRANSITION_SUSPENDED_TO_STOPPED)
            .bind(worker_id)
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to shutdown worker {}: {}", worker_id, e),
            })?;

        match result {
            Some(_) => Ok(()),
            None => {
                let current_status = self.get_status(worker_id).await?;
                Err(crate::error::Error::InvalidStateTransition {
                    from: current_status.to_string(),
                    to: "stopped".to_string(),
                    reason: "Worker must be in Suspended state to shutdown".to_string(),
                })
            }
        }
    }
}

impl Table for Workers {
    type Entity = WorkerInfo;
    type NewEntity = NewWorker;

    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity> {
        Workers::insert(self, data).await
    }

    async fn get(&self, id: i64) -> Result<Self::Entity> {
        Workers::get(self, id).await
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        Workers::list(self).await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerInfo>> {
        Workers::filter_by_fk(self, queue_id).await
    }

    async fn count(&self) -> Result<i64> {
        Workers::count(self).await
    }

    async fn count_for_fk<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        Workers::count_for_fk(self, queue_id, tx).await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        Workers::delete(self, id).await
    }

    async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        Workers::delete_by_fk(self, queue_id, tx).await
    }
}

#[async_trait]
impl crate::store::WorkerTable for Workers {
    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo> {
        // Try to find existing worker by hostname+port
        let existing_worker: Option<WorkerInfo> = sqlx::query_as(FIND_WORKER_BY_HOST_PORT)
            .bind(hostname)
            .bind(port)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to find worker: {}", e),
            })?;

        match existing_worker {
            Some(worker) => {
                match worker.status {
                    WorkerStatus::Stopped => {
                        // Reset stopped worker to ready
                        let worker_info = sqlx::query_as::<_, WorkerInfo>(RESET_WORKER_TO_READY)
                            .bind(worker.id)
                            .bind(queue_id)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|e| crate::error::Error::Connection {
                                message: format!("Failed to reset worker: {}", e),
                            })?;
                         Ok(worker_info)
                    }
                    WorkerStatus::Ready => {
                        Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {}:{} is already active as worker {}. Cannot register duplicate.",
                                hostname, port, worker.id
                            ),
                        })
                    }
                    WorkerStatus::Suspended => {
                        Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {}:{} is suspended (worker {}). Use resume() to reactivate.",
                                hostname, port, worker.id
                            ),
                        })
                    }
                }
            }
            None => {
                // Create new worker
                let new_worker = NewWorker {
                    hostname: hostname.to_string(),
                    port,
                    queue_id,
                };
                self.insert(new_worker).await
            }
        }
    }

    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        Workers::get_status(self, worker_id).await
    }

    async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        Workers::heartbeat(self, worker_id).await
    }

    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool> {
        Workers::is_healthy(self, worker_id, max_age).await
    }

    async fn suspend(&self, worker_id: i64) -> Result<()> {
        Workers::suspend(self, worker_id).await
    }

    async fn resume(&self, worker_id: i64) -> Result<()> {
        Workers::resume(self, worker_id).await
    }

    async fn shutdown(&self, worker_id: i64) -> Result<()> {
        Workers::shutdown(self, worker_id).await
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
