//! Worker lifecycle management for pgqrs.
//!
//! This module provides the `WorkerHandle` for managing generic worker state
//! transitions (suspend, resume, shutdown) in a PostgreSQL-backed job queue system.
//!
//! ## Worker Lifecycle
//!
//! Workers follow a strict state machine:
//!
//! ```text
//! Ready <-> Suspended -> Stopped
//! ```
//!
//! - `Ready`: Worker is active and can process work
//! - `Suspended`: Worker is paused, not accepting new work, but can be resumed or shut down
//! - `Stopped`: Worker has completed shutdown (terminal state)
//!
//! ## Key Rules
//!
//! 1. Workers can only transition to `Stopped` from `Suspended` state
//! 2. Consumers can only `suspend()` if they have no pending messages
//! 3. All state transitions are atomic database operations with row-level locking
//!
//! ## Using WorkerHandle
//!
//! `WorkerHandle` allows you to manage any worker (Producer, Consumer, Admin) using just its ID.
//!
//! ```rust,ignore
//! use pgqrs::worker::WorkerHandle;
//! use pgqrs::store::Worker;
//!
//! // Get a handle to any worker by ID
//! let handle = WorkerHandle::new(pool.clone(), worker_id);
//!
//! // Call Worker trait methods
//! let status = handle.status().await?;
//! handle.suspend().await?;
//! handle.shutdown().await?;
//! ```

mod lifecycle;

pub use lifecycle::WorkerLifecycle;

use crate::error::Result;
use crate::store::Worker;
use crate::types::WorkerStatus;
use async_trait::async_trait;
use sqlx::PgPool;

/// A generic handle to any worker that implements the [`Worker`] trait.
///
/// This provides a way to manage worker lifecycle (status, modify state)
/// given just a worker ID, without needing the concrete worker instance.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    worker_id: i64,
    lifecycle: WorkerLifecycle,
}

impl WorkerHandle {
    /// Create a new WorkerHandle for the given worker ID.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `worker_id` - ID of the worker to manage
    ///
    /// # Returns
    /// A new `WorkerHandle` instance
    pub fn new(pool: PgPool, worker_id: i64) -> Self {
        Self {
            worker_id,
            lifecycle: WorkerLifecycle::new(pool),
        }
    }
}

#[async_trait]
impl Worker for WorkerHandle {
    fn worker_id(&self) -> i64 {
        self.worker_id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.lifecycle.get_status(self.worker_id).await
    }

    async fn heartbeat(&self) -> Result<()> {
        self.lifecycle.heartbeat(self.worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.lifecycle.is_healthy(self.worker_id, max_age).await
    }

    async fn suspend(&self) -> Result<()> {
        self.lifecycle.suspend(self.worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        self.lifecycle.resume(self.worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.lifecycle.shutdown(self.worker_id).await
    }
}
