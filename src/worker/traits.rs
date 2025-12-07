//! Worker trait definition for pgqrs.
//!
//! This module defines the [`Worker`] trait that all worker types must implement.

use crate::error::Result;
use crate::types::WorkerStatus;
use async_trait::async_trait;

/// Trait defining the interface for all worker types.
///
/// All workers (Producer, Consumer, Admin) implement this trait to provide
/// consistent lifecycle management and monitoring capabilities.
///
/// ## Lifecycle States
///
/// - `Ready`: Worker is active and can process work
/// - `Suspended`: Worker is paused, can be resumed or shut down
/// - `Stopped`: Worker has completed shutdown (terminal)
///
/// ## State Transitions
///
/// ```text
/// Ready -> Suspended -> Stopped
///   ^         |
///   |_________|  (resume)
/// ```
#[async_trait]
pub trait Worker: Send + Sync {
    /// Get the unique identifier for this worker.
    fn worker_id(&self) -> i64;

    /// Get the current status of this worker from the database.
    ///
    /// # Returns
    /// The current [`WorkerStatus`] of the worker.
    async fn status(&self) -> Result<WorkerStatus>;

    /// Suspend this worker (transition from Ready to Suspended).
    ///
    /// A suspended worker does not accept new work but can be resumed or shut down.
    ///
    /// # Preconditions
    /// - Worker must be in `Ready` state
    /// - For Consumers: must have no pending (held) messages
    ///
    /// # Returns
    /// Ok if suspension succeeds, error if preconditions not met or state transition fails.
    async fn suspend(&self) -> Result<()>;

    /// Resume this worker (transition from Suspended to Ready).
    ///
    /// # Preconditions
    /// - Worker must be in `Suspended` state
    ///
    /// # Returns
    /// Ok if resume succeeds, error if not suspended or state transition fails.
    async fn resume(&self) -> Result<()>;

    /// Initiate shutdown (transition from Suspended to Stopped).
    ///
    /// # Preconditions
    /// - Worker must be in `Suspended` state
    ///
    /// # Returns
    /// Ok if shutdown completes, error if not suspended or state transition fails.
    async fn shutdown(&self) -> Result<()>;

    /// Update the heartbeat timestamp for this worker.
    ///
    /// # Returns
    /// Ok if heartbeat update succeeds, error otherwise.
    async fn heartbeat(&self) -> Result<()>;

    /// Check if this worker is healthy based on heartbeat age.
    ///
    /// # Arguments
    /// * `max_age` - Maximum allowed age for the last heartbeat
    ///
    /// # Returns
    /// `true` if the worker's last heartbeat is within the max_age threshold
    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool>;
}
