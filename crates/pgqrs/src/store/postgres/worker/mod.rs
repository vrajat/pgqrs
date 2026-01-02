//! Worker lifecycle management for pgqrs.
//!
//! This module provides the [`Worker`] trait and lifecycle management functions
//! for managing worker state transitions in a PostgreSQL-backed job queue system.
//!
//! ## What
//!
//! - [`Worker`] trait defines the interface for all worker types (Producer, Consumer, Admin)
//! - [`WorkerHandle`] is a generic worker reference that implements [`Worker`] for any worker ID
//! - [`Admin`] provides administrative functions for managing queues and workers
//! - [`Producer`] handles message production (enqueueing)
//! - [`Consumer`] handles message consumption (dequeueing)
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
//! ## How
//!
//! Implement the [`Worker`] trait for your worker types.
//!
//! ### Example: Using WorkerHandle for generic worker operations
//!
//! ```rust,ignore
//! use pgqrs::worker::{Worker, WorkerHandle};
//!
//! // Get a handle to any worker by ID
//! let handle = WorkerHandle::new(pool.clone(), worker_id);
//!
//! // Call Worker trait methods
//! let status = handle.status().await?;
//! handle.suspend().await?;
//! handle.shutdown().await?;
//! ```
//!
//! ### Example: Graceful shutdown
//!
//! ```rust
//! # use pgqrs::Worker;
//! async fn graceful_shutdown(worker: &impl Worker) -> pgqrs::Result<()> {
//!     // First suspend the worker
//!     worker.suspend().await?;
//!     // Then shut it down
//!     worker.shutdown().await?;
//!     Ok(())
//! }
//! ```

pub mod admin;
pub mod consumer;
pub mod producer;

pub use admin::Admin;
pub use consumer::Consumer;
pub use producer::Producer;

use crate::store::postgres::tables::pgqrs_workers::Workers;
use crate::store::Worker;

use crate::error::Result;
use crate::types::WorkerStatus;
use async_trait::async_trait;
use sqlx::PgPool;

/// A generic handle to any worker that implements the [`Worker`] trait.
///
/// This is the Rust equivalent of a "base class" - it allows you to call
/// Worker trait methods on any worker given just its ID, without needing
/// to know the concrete type (Producer, Consumer, or Admin).
///
/// ## Example
///
/// ```rust,ignore
/// use pgqrs::worker::{Worker, WorkerHandle};
///
/// // Given just a worker ID, create a handle and call trait methods
/// let handle = WorkerHandle::new(pool.clone(), worker_id);
///
/// // Check status
/// let status = handle.status().await?;
///
/// // Suspend and shutdown
/// handle.suspend().await?;
/// handle.shutdown().await?;
/// ```
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    worker_id: i64,
    workers: Workers,
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
            workers: Workers::new(pool),
        }
    }
}

#[async_trait]
impl Worker for WorkerHandle {
    fn worker_id(&self) -> i64 {
        self.worker_id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_id).await
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers.is_healthy(self.worker_id, max_age).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_id).await
    }
}
