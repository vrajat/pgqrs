//! Worker lifecycle management with atomic state transitions.
//!
//! This module provides the [`WorkerLifecycle`] struct that handles all worker
//! state transitions using atomic SQL operations with row-level locking.

use crate::error::Result;
use crate::types::WorkerStatus;
use crate::WorkerInfo;
use chrono::{Duration, Utc};
use sqlx::PgPool;

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

/// SQL to insert a new worker
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, status)
    VALUES ($1, $2, $3, 'ready')
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

/// SQL for atomic state transition from Ready to Suspended
const TRANSITION_READY_TO_SUSPENDED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'suspended'
    WHERE id = $1 AND status = 'ready'
    RETURNING id
"#;

/// SQL for atomic state transition from Suspended to Ready
const TRANSITION_SUSPENDED_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready'
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

/// SQL for atomic state transition from Suspended to Stopped
const TRANSITION_SUSPENDED_TO_STOPPED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped', shutdown_at = $2
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

/// SQL to get current worker status
const GET_WORKER_STATUS: &str = r#"
    SELECT status FROM pgqrs_workers WHERE id = $1
"#;

/// SQL to count messages held by a worker
const COUNT_WORKER_MESSAGES: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id = $1
"#;

/// SQL to update worker heartbeat
const UPDATE_HEARTBEAT: &str = r#"
    UPDATE pgqrs_workers
    SET heartbeat_at = $1
    WHERE id = $2
"#;

/// Worker lifecycle manager providing atomic state transitions.
///
/// This struct encapsulates all worker state transition logic, ensuring
/// that transitions are atomic and follow the correct state machine.
///
/// ## State Machine
///
/// ```text
/// Ready <-> Suspended -> Stopped
/// ```
#[derive(Debug, Clone)]
pub struct WorkerLifecycle {
    pool: PgPool,
}

impl WorkerLifecycle {
    /// Create a new WorkerLifecycle instance.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Register and create a new WorkerInfo instance for the specified queue.
    ///
    /// This method handles worker registration automatically:
    /// - If a worker with the hostname+port exists and is Stopped → resets to Ready
    /// - If no worker exists → creates a new one
    /// - If worker exists but is Ready or Suspended → returns error
    ///
    /// # Arguments
    /// * `queue_info` - Queue information including ID and name
    /// * `hostname` - Hostname identifier for this worker
    /// * `port` - Port identifier for this worker
    ///
    /// # Errors
    /// - Returns error if worker exists and is in Ready state (already active)
    /// - Returns error if worker exists and is in Suspended state (needs explicit resume)
    pub async fn register(
        &self,
        queue_info: &crate::types::QueueInfo,
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
                            .bind(queue_info.id)
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
                sqlx::query_as::<_, WorkerInfo>(INSERT_WORKER)
                    .bind(hostname)
                    .bind(port)
                    .bind(queue_info.id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::Connection {
                        message: format!("Failed to create worker: {}", e),
                    })?
            }
        };
        Ok(worker_info)
    }

    /// Get the current status of a worker.
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to check
    ///
    /// # Returns
    /// The current [`WorkerStatus`] of the worker.
    pub async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        let status: WorkerStatus = sqlx::query_scalar(GET_WORKER_STATUS)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get worker {} status: {}", worker_id, e),
            })?;

        Ok(status)
    }

    /// Count messages currently held by a worker.
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to check
    ///
    /// # Returns
    /// Number of messages held by the worker.
    pub async fn count_pending_messages(&self, worker_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_WORKER_MESSAGES)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count messages for worker {}: {}", worker_id, e),
            })?;

        Ok(count)
    }

    /// Transition worker from Ready to Suspended.
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to suspend
    ///
    /// # Returns
    /// Ok if transition succeeds, error if worker is not in Ready state.
    pub async fn suspend(&self, worker_id: i64) -> Result<()> {
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
                // Get current status for better error message
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
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to resume
    ///
    /// # Returns
    /// Ok if transition succeeds, error if worker is not in Suspended state.
    pub async fn resume(&self, worker_id: i64) -> Result<()> {
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
    ///
    /// # Preconditions
    /// - Worker must be in Suspended state
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to shutdown
    ///
    /// # Returns
    /// Ok if shutdown succeeds, error if worker is not in Suspended state.
    pub async fn shutdown(&self, worker_id: i64) -> Result<()> {
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

    /// Update worker heartbeat timestamp.
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to update
    ///
    /// # Returns
    /// Ok if heartbeat update succeeds.
    pub async fn heartbeat(&self, worker_id: i64) -> Result<()> {
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
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to check
    /// * `max_age` - Maximum allowed age for the last heartbeat
    ///
    /// # Returns
    /// `true` if the worker's last heartbeat is within the max_age threshold
    pub async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool> {
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
}
