//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`Workers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table.

use crate::error::Result;
use crate::types::{WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
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
    DELETE FROM pgqrs_workers WHERE queue_id = $1
"#;

const COUNT_WORKERS_BY_QUEUE_TX: &str = r#"
    SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = $1
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

/// SQL to find existing worker by hostname and port
const FIND_WORKER_BY_HOST_PORT: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE hostname = $1 AND port = $2
"#;

/// SQL to reset a stopped worker back to ready state
const RESET_WORKER_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready', queue_id = $2, started_at = NOW(), heartbeat_at = NOW(), shutdown_at = NULL
    WHERE id = $1 AND status = 'stopped'
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

/// SQL to insert a new ephemeral worker (unique hostname with UUID, port -1)
const INSERT_EPHEMERAL_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, status)
    VALUES ($1, -1, $2, 'ready')
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

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

    pub async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_WORKERS_BY_QUEUE_TX)
            .bind(foreign_key_value)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count workers for queue {}: {}",
                    foreign_key_value, e
                ),
            })?;
        Ok(count)
    }

    pub async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKERS_BY_QUEUE)
            .bind(foreign_key_value)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to delete workers for queue {}: {}",
                    foreign_key_value, e
                ),
            })?;
        Ok(result.rows_affected())
    }

    pub async fn list_zombies_for_queue_tx<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list zombie workers for queue {} in tx: {}",
                    queue_id, e
                ),
            })?;
        Ok(workers)
    }

    /// Get the current status of a worker.
    pub async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        const GET_WORKER_STATUS: &str = "SELECT status FROM pgqrs_workers WHERE id = $1";
        let status: WorkerStatus = sqlx::query_scalar(GET_WORKER_STATUS)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get worker {} status: {}", worker_id, e),
            })?;

        Ok(status)
    }

    /// Update worker heartbeat timestamp.
    pub async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        const UPDATE_HEARTBEAT: &str = r#"
            UPDATE pgqrs_workers
            SET heartbeat_at = $1
            WHERE id = $2
        "#;
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

    /// Check if this worker is healthy based on heartbeat age
    pub async fn is_healthy(&self, worker_id: i64, max_age: chrono::Duration) -> Result<bool> {
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

    /// Transition worker from Ready to Suspended.
    pub async fn suspend(&self, worker_id: i64) -> Result<()> {
        const TRANSITION_READY_TO_SUSPENDED: &str = r#"
            UPDATE pgqrs_workers
            SET status = 'suspended'
            WHERE id = $1 AND status = 'ready'
            RETURNING id
        "#;
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

    /// Transition worker from Suspended to Ready.
    pub async fn resume(&self, worker_id: i64) -> Result<()> {
        const TRANSITION_SUSPENDED_TO_READY: &str = r#"
            UPDATE pgqrs_workers
            SET status = 'ready'
            WHERE id = $1 AND status = 'suspended'
            RETURNING id
        "#;
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

    /// Shutdown worker: transition from Suspended to Stopped.
    pub async fn shutdown(&self, worker_id: i64) -> Result<()> {
        const TRANSITION_SUSPENDED_TO_STOPPED: &str = r#"
            UPDATE pgqrs_workers
            SET status = 'stopped', shutdown_at = $2
            WHERE id = $1 AND status = 'suspended'
            RETURNING id
        "#;
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

// Implement the public WorkerTable trait by delegating to inherent methods
#[async_trait]
impl crate::store::WorkerTable for Workers {
    async fn insert(&self, data: crate::types::NewWorker) -> Result<WorkerInfo> {
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

        let worker_info = match existing_worker {
            Some(worker) => {
                match worker.status {
                    WorkerStatus::Stopped => {
                        // Reset stopped worker to ready
                        sqlx::query_as::<_, WorkerInfo>(RESET_WORKER_TO_READY)
                            .bind(worker.id)
                            .bind(queue_id)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|e| crate::error::Error::Connection {
                                message: format!("Failed to reset worker: {}", e),
                            })?
                    }
                    WorkerStatus::Ready => {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {}:{} is already active. Cannot register duplicate.",
                                hostname, port
                            ),
                        });
                    }
                    WorkerStatus::Suspended => {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {}:{} is suspended. Use resume() to reactivate.",
                                hostname, port
                            ),
                        });
                    }
                }
            }
            None => {
                // Create new worker
                let now = Utc::now();
                // We use INSERT_WORKER constant defined at top of file, but adapt it to handle Option<queue_id>
                let inserted_id: i64 = sqlx::query_scalar(INSERT_WORKER)
                    .bind(hostname)
                    .bind(port)
                    .bind(queue_id)
                    .bind(now)
                    .bind(now)
                    .bind(WorkerStatus::Ready)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::Connection {
                        message: format!("Failed to create worker: {}", e),
                    })?;

                WorkerInfo {
                    id: inserted_id,
                    hostname: hostname.to_string(),
                    port,
                    queue_id,
                    started_at: now,
                    heartbeat_at: now,
                    shutdown_at: None,
                    status: WorkerStatus::Ready,
                }
            }
        };
        Ok(worker_info)
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerInfo> {
        // Generate a unique hostname using UUID
        let hostname = format!("__ephemeral__{}", uuid::Uuid::new_v4());

        // Create new ephemeral worker (always creates new, never reuses)
        let worker_info = sqlx::query_as::<_, WorkerInfo>(INSERT_EPHEMERAL_WORKER)
            .bind(&hostname)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to create ephemeral worker: {}", e),
            })?;

        Ok(worker_info)
    }
}
