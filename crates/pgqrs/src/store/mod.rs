//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repository traits,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

pub mod any;
pub mod postgres;

use crate::error::Result;
use crate::tables::{NewMessage, NewQueue, NewWorker, NewWorkflow, WorkflowRecord};
use crate::types::{
    ArchivedMessage, QueueInfo, QueueMessage, QueueMetrics, SystemStats, WorkerHealthStats,
    WorkerInfo, WorkerPurgeResult, WorkerStatus,
};
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

// Re-export AnyStore for convenience
pub use any::AnyStore;

// ============================================================================
// Concurrency Model
// ============================================================================

/// Concurrency model supported by a database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyModel {
    /// Single process only. Multiple processes will cause data corruption.
    /// Used by: SQLite, Turso
    SingleProcess,

    /// Multiple processes supported with proper locking.
    /// Used by: PostgreSQL
    MultiProcess,
}

// ============================================================================
// Repository Traits (extend base Table trait)
// ============================================================================

/// Repository for managing queues.
///
/// Provides both CRUD operations and queue-specific business logic.
#[async_trait]
pub trait QueueTable: Send + Sync {
    // === CRUD Operations ===

    /// Insert a new queue
    async fn insert(&self, data: NewQueue) -> Result<QueueInfo>;

    /// Get a queue by ID
    async fn get(&self, id: i64) -> Result<QueueInfo>;

    /// List all queues
    async fn list(&self) -> Result<Vec<QueueInfo>>;

    /// Count all queues
    async fn count(&self) -> Result<i64>;

    /// Delete a queue by ID
    async fn delete(&self, id: i64) -> Result<u64>;

    // === Queue-Specific Business Operations ===

    /// Get queue information by name
    async fn get_by_name(&self, name: &str) -> Result<QueueInfo>;

    /// Check if a queue exists
    async fn exists(&self, name: &str) -> Result<bool>;

    /// Delete a queue by name (returns number of deleted rows)
    async fn delete_by_name(&self, name: &str) -> Result<u64>;

    /// Get queue metrics (message counts, etc.)
    async fn get_metrics(&self, queue_id: i64) -> Result<QueueMetrics>;

    /// Purge all messages from a queue (returns count purged)
    async fn purge(&self, queue_id: i64) -> Result<u64>;
}

/// Repository for managing messages.
///
/// Extends the base `Table` trait with message-specific business operations.
#[async_trait]
pub trait MessageTable: Send + Sync {
    // === CRUD Operations ===

    /// Insert a new message
    async fn insert(&self, data: NewMessage) -> Result<QueueMessage>;

    /// Get a message by ID
    async fn get(&self, id: i64) -> Result<QueueMessage>;

    /// List all messages
    async fn list(&self) -> Result<Vec<QueueMessage>>;

    /// Count all messages
    async fn count(&self) -> Result<i64>;

    /// Delete a message by ID (returns true if deleted)
    async fn delete(&self, id: i64) -> Result<bool>;

    // === Message-Specific Business Operations ===

    /// Enqueue a single message
    async fn enqueue(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> Result<i64>;

    /// Enqueue multiple messages
    async fn enqueue_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> Result<Vec<i64>>;

    /// Dequeue messages atomically.
    ///
    /// Implementation notes:
    /// - PostgreSQL: Uses `FOR UPDATE SKIP LOCKED`
    /// - SQLite: Uses claiming pattern with explicit transaction
    /// - Turso: Uses claiming pattern
    async fn dequeue(
        &self,
        queue_id: i64,
        worker_id: i64,
        limit: usize,
        vt_seconds: u32,
    ) -> Result<Vec<QueueMessage>>;

    /// Delete a message by ID

    /// Delete multiple messages
    async fn delete_batch(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>>;

    /// Extend visibility timeout for a message
    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<bool>;

    /// Release a message (return to queue)
    async fn release(&self, id: i64, worker_id: i64) -> Result<bool>;

    /// Release multiple messages
    async fn release_batch(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>>;

    /// Count active messages by worker
    async fn count_by_worker(&self, worker_id: i64) -> Result<i64>;
}

/// Repository for managing workers.
///
/// Extends the base `Table` trait with worker lifecycle operations.
#[async_trait]
pub trait WorkerTable: Send + Sync {
    // === CRUD Operations ===

    /// Insert a new worker
    async fn insert(&self, data: NewWorker) -> Result<WorkerInfo>;

    /// Get a worker by ID
    async fn get(&self, id: i64) -> Result<WorkerInfo>;

    /// List all workers
    async fn list(&self) -> Result<Vec<WorkerInfo>>;

    /// Count all workers
    async fn count(&self) -> Result<i64>;

    /// Delete a worker by ID
    async fn delete(&self, id: i64) -> Result<u64>;

    // === Worker-Specific Business Operations ===

    /// Register a new worker
    async fn register(
        &self,
        queue_id: Option<i64>, // None for admin
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo>;

    /// Get status of a worker
    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus>;

    /// Send a heartbeat for a worker
    async fn heartbeat(&self, worker_id: i64) -> Result<()>;

    /// Check if a worker is healthy (heartbeat within timeout)
    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool>;

    // Lifecycle Transitions
    /// Suspend a worker (atomic state transition: Ready -> Suspended)
    async fn suspend(&self, worker_id: i64) -> Result<()>;

    /// Resume a worker (atomic state transition: Suspended -> Ready)
    async fn resume(&self, worker_id: i64) -> Result<()>;

    /// Shutdown a worker (atomic state transition: * -> Stopped)
    async fn shutdown(&self, worker_id: i64) -> Result<()>;

    /// Get health statistics for workers
    async fn health_stats(&self, timeout: Duration) -> Result<WorkerHealthStats>;

    /// Count pending messages for a worker
    async fn count_pending_messages(&self, worker_id: i64) -> Result<i64>;
}

/// Repository for managing archived messages.
///
/// Provides archive and DLQ operations.
#[async_trait]
pub trait ArchiveTable: Send + Sync {
    // === CRUD Operations ===

    /// Get an archived message by ID
    async fn get(&self, id: i64) -> Result<ArchivedMessage>;

    /// List archived messages
    async fn list(&self) -> Result<Vec<ArchivedMessage>>;

    /// Count archived messages
    async fn count(&self) -> Result<i64>;

    /// Delete an archived message by ID
    async fn delete(&self, id: i64) -> Result<u64>;

    // === Archive-Specific Business Operations ===

    /// Archive a specific message atomically (delete from messages + insert here)
    /// Returns None if message not found or not owned by worker
    async fn archive_message(&self, msg_id: i64, worker_id: i64)
        -> Result<Option<ArchivedMessage>>;

    /// Archive a batch of messages
    async fn archive_batch(&self, msg_ids: &[i64]) -> Result<Vec<bool>>;

    // Read methods
    /// List DLQ messages (messages exceeding max attempts)
    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>>;

    /// Count DLQ messages
    async fn dlq_count(&self, max_attempts: i32) -> Result<i64>;

    /// List archived messages by worker
    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>>;

    /// Count archived messages by worker
    async fn count_by_worker(&self, worker_id: i64) -> Result<i64>;
}

/// Repository for managing workflows.
///
/// Provides workflow-specific operations.
#[async_trait]
pub trait WorkflowTable: Send + Sync {
    // === CRUD Operations ===

    /// Insert a new workflow
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord>;

    /// Get a workflow by ID
    async fn get(&self, id: i64) -> Result<WorkflowRecord>;

    /// List all workflows
    async fn list(&self) -> Result<Vec<WorkflowRecord>>;

    /// Count workflows
    async fn count(&self) -> Result<i64>;

    /// Delete a workflow by ID
    async fn delete(&self, id: i64) -> Result<u64>;

    // Workflow-specific operations can be added here as needed
}

// ============================================================================
// Main Store Trait
// ============================================================================

/// Main store trait that provides access to entity-specific repositories
/// and cross-table business operations.
///
/// This trait uses trait objects (`&dyn *Table`) to enable runtime polymorphism
/// without exposing generics in the public API.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    // === Table Accessors ===

    /// Get access to the queue repository
    fn queues(&self) -> &dyn QueueTable;

    /// Get access to the message repository
    fn messages(&self) -> &dyn MessageTable;

    /// Get access to the worker repository
    fn workers(&self) -> &dyn WorkerTable;

    /// Get access to the archive repository
    fn archive(&self) -> &dyn ArchiveTable;

    /// Get access to the workflow repository
    fn workflows(&self) -> &dyn WorkflowTable;

    // === Lifecycle Operations ===

    /// Install/migrate database schema
    async fn install(&self) -> Result<()>;

    /// Verify database integrity
    ///
    /// Checks:
    /// - All required tables exist
    /// - Referential integrity (no orphaned records)
    ///
    /// Runs in a read-only transaction.
    async fn verify(&self) -> Result<()>;

    // === Cross-Table Business Operations ===

    /// Move messages exceeding max_attempts to archive (DLQ).
    ///
    /// Implementation:
    /// - PostgreSQL: Single CTE (DELETE...INSERT...RETURNING)
    /// - SQLite: Explicit transaction (SELECT, INSERT, DELETE)
    ///
    /// Returns IDs of archived messages.
    async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>>;

    /// Replay a message from DLQ back to active queue.
    ///
    /// Atomically moves message from archive to messages table.
    /// Returns the new message if successful.
    async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>>;

    /// Release messages held by dead/stale workers.
    ///
    /// Finds workers with no heartbeat within timeout,
    /// releases their messages back to queue.
    /// Returns count of released messages.
    async fn release_zombie_messages(&self, worker_timeout: Duration) -> Result<u64>;

    /// Purge stale workers and release their messages.
    ///
    /// Returns statistics about purged workers and released messages.
    async fn purge_stale_workers(&self, timeout: Duration) -> Result<WorkerPurgeResult>;

    /// Get system-wide statistics.
    ///
    /// Aggregates data from all tables:
    /// - Queue count
    /// - Active worker count
    /// - Pending message count
    /// - DLQ count
    /// - Archive count
    async fn get_system_stats(&self) -> Result<SystemStats>;

    // === Capabilities ===

    /// Returns the concurrency model supported by this backend.
    fn concurrency_model(&self) -> ConcurrencyModel;

    /// Returns the backend name (e.g., "postgres", "sqlite", "turso")
    fn backend_name(&self) -> &'static str;
}
