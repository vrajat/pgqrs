//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repositories,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

use crate::rate_limit::RateLimitStatus;
use crate::types::{
    ArchivedMessage, NewArchivedMessage, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus,
};
use crate::types::{NewQueue, NewWorkflow, WorkflowRecord};
use crate::validation::ValidationConfig;
use crate::Config;
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

/// Concurrency model supported by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyModel {
    /// Backend supports multiple processes accessing the store concurrently.
    MultiProcess,
    /// Backend supports only a single process accessing the store.
    SingleProcess,
}

pub mod any;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use any::AnyStore;

/// Trait defining the interface for all worker types.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Get the unique identifier for this worker.
    fn worker_id(&self) -> i64;
    async fn status(&self) -> crate::error::Result<WorkerStatus>;
    async fn suspend(&self) -> crate::error::Result<()>;
    async fn resume(&self) -> crate::error::Result<()>;
    async fn shutdown(&self) -> crate::error::Result<()>;
    async fn heartbeat(&self) -> crate::error::Result<()>;
    async fn is_healthy(&self, max_age: Duration) -> crate::error::Result<bool>;
}

/// Admin interface for managing pgqrs infrastructure.
///
/// This trait provides a unified interface for all administrative operations.
/// Methods are organized into logical groups:
/// - **Schema management**: `install()`, `verify()`
/// - **Queue operations**: `create_queue()`, `get_queue()`, `delete_queue()`, `purge_queue()`, `dlq()`
/// - **Worker management**: `delete_worker()`, `list_workers()`, `release_worker_messages()`, `purge_old_workers()`
/// - **Metrics**: `queue_metrics()`, `all_queues_metrics()`, `system_stats()`, `worker_stats()`, `worker_health_stats()`
#[async_trait]
pub trait Admin: Worker {
    // ===== Schema Management =====

    /// Install the pgqrs schema in the database.
    ///
    /// This creates all necessary tables, indexes, and functions required for pgqrs to operate.
    async fn install(&self) -> crate::error::Result<()>;

    /// Verify the pgqrs schema is correctly installed.
    ///
    /// Checks that all required tables, indexes, and constraints exist and are valid.
    async fn verify(&self) -> crate::error::Result<()>;

    /// Register this admin worker (internal use).
    async fn register(&mut self, hostname: String, port: i32) -> crate::error::Result<WorkerInfo>;

    // ===== Queue Operations =====

    /// Create a new queue.
    ///
    /// # Arguments
    /// * `name` - Unique name for the queue
    async fn create_queue(&self, name: &str) -> crate::error::Result<QueueInfo>;

    /// Get queue information by name.
    async fn get_queue(&self, name: &str) -> crate::error::Result<QueueInfo>;

    /// Delete a queue.
    ///
    /// The queue must be empty (no messages or workers) before deletion.
    async fn delete_queue(&self, queue_info: &QueueInfo) -> crate::error::Result<()>;

    /// Purge all messages and workers from a queue.
    ///
    /// This removes all pending messages, releases locked messages, and removes all workers.
    /// Use with caution as this operation cannot be undone.
    async fn purge_queue(&self, name: &str) -> crate::error::Result<()>;

    /// Get IDs of messages in the dead letter queue.
    ///
    /// Returns message IDs that have exceeded the maximum retry attempts.
    async fn dlq(&self) -> crate::error::Result<Vec<i64>>;

    // ===== Metrics =====

    /// Get metrics for a specific queue.
    ///
    /// Returns counts of total, pending, locked, and archived messages.
    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::types::QueueMetrics>;

    /// Get metrics for all queues.
    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::types::QueueMetrics>>;

    /// Get system-wide statistics.
    ///
    /// Returns aggregate statistics across all queues, workers, and messages.
    async fn system_stats(&self) -> crate::error::Result<crate::types::SystemStats>;

    /// Get worker health statistics.
    ///
    /// # Arguments
    /// * `heartbeat_timeout` - Duration after which a worker is considered stale
    /// * `group_by_queue` - If true, returns per-queue stats; if false, returns global stats
    async fn worker_health_stats(
        &self,
        heartbeat_timeout: Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::types::WorkerHealthStats>>;

    /// Get worker statistics for a queue.
    ///
    /// Returns counts of workers by status and average messages per worker.
    async fn worker_stats(
        &self,
        queue_name: &str,
    ) -> crate::error::Result<crate::types::WorkerStats>;

    // ===== Worker Management =====

    /// Delete a worker by ID.
    ///
    /// The worker must not have any associated messages or archives.
    async fn delete_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    /// List all workers across all queues.
    async fn list_workers(&self) -> crate::error::Result<Vec<WorkerInfo>>;

    /// Get messages currently held by a worker.
    async fn get_worker_messages(&self, worker_id: i64) -> crate::error::Result<Vec<QueueMessage>>;

    /// Reclaim messages that have exceeded their visibility timeout.
    ///
    /// # Arguments
    /// * `queue_id` - Queue to reclaim messages from
    /// * `older_than` - Optional duration; messages with VT older than this are reclaimed
    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<Duration>,
    ) -> crate::error::Result<u64>;

    /// Purge workers that haven't sent a heartbeat recently.
    ///
    /// Only removes workers in Stopped status with old heartbeats.
    async fn purge_old_workers(&self, older_than: chrono::Duration) -> crate::error::Result<u64>;

    /// Release all messages held by a worker.
    ///
    /// Makes the messages available for other workers to process.
    async fn release_worker_messages(&self, worker_id: i64) -> crate::error::Result<u64>;
}

/// Producer interface for enqueueing messages.
#[async_trait]
pub trait Producer: Worker {
    async fn get_message_by_id(&self, msg_id: i64) -> crate::error::Result<QueueMessage>;
    async fn enqueue(&self, payload: &Value) -> crate::error::Result<QueueMessage>;
    async fn enqueue_delayed(
        &self,
        payload: &Value,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage>;
    async fn batch_enqueue(&self, payloads: &[Value]) -> crate::error::Result<Vec<QueueMessage>>;
    async fn batch_enqueue_delayed(
        &self,
        payloads: &[serde_json::Value],
        delay_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    // Internal but public method in source
    async fn insert_message(
        &self,
        payload: &Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<i64>;

    async fn replay_dlq(&self, archived_msg_id: i64) -> crate::error::Result<Option<QueueMessage>>;

    fn validation_config(&self) -> &ValidationConfig;
    fn rate_limit_status(&self) -> Option<RateLimitStatus>;
}

/// Consumer interface for processing messages.
#[async_trait]
pub trait Consumer: Worker {
    async fn dequeue(&self) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_many(&self, limit: usize) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_delay(&self, vt: u32) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_many_with_delay(
        &self,
        limit: usize,
        vt: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<bool>;

    async fn delete(&self, message_id: i64) -> crate::error::Result<bool>;
    async fn delete_many(&self, message_ids: Vec<i64>) -> crate::error::Result<Vec<bool>>;

    async fn archive(&self, msg_id: i64) -> crate::error::Result<Option<ArchivedMessage>>;
    async fn archive_many(&self, msg_ids: Vec<i64>) -> crate::error::Result<Vec<bool>>;

    async fn release_messages(&self, message_ids: &[i64]) -> crate::error::Result<u64>;
}

/// Interface for a durable workflow execution handle.
#[async_trait]
pub trait Workflow: Send + Sync {
    /// Get the workflow ID.
    fn id(&self) -> i64;

    /// Start the workflow execution.
    async fn start(&mut self) -> crate::error::Result<()>;

    /// Complete the workflow successfully with a value.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;

    /// Fail the workflow with an error value.
    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()>;

    /// Acquire a step lock for this workflow.
    ///
    /// This is used internally by macros to acquire steps within a workflow execution context.
    async fn acquire_step(
        &self,
        step_id: &str,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>>;
}

/// Extension trait for Workflow to provide generic convenience methods.
#[async_trait]
pub trait WorkflowExt: Workflow {
    /// Complete the workflow successfully with a serializable output.
    async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    /// Fail the workflow with a serializable error.
    async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value).await
    }
}
// Automatically implement Extension on anything that implements Workflow
impl<T: ?Sized + Workflow> WorkflowExt for T {}

/// A guard for a workflow step execution.
#[async_trait]
pub trait StepGuard: Send + Sync {
    /// Complete the step successfully.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;

    /// Fail the step.
    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()>;
}

/// Extension trait for StepGuard to provide generic convenience methods.
#[async_trait]
pub trait StepGuardExt: StepGuard {
    /// Complete the step successfully with a serializable output.
    async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    /// Fail the step with a serializable error.
    async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value).await
    }
}
impl<T: ?Sized + StepGuard> StepGuardExt for T {}

/// The result of attempting to start a step.
pub enum StepResult<T> {
    /// The step needs to be executed. The returned guard MUST be used to report success or failure.
    Execute(Box<dyn StepGuard>),
    /// The step was already completed successfully in a previous run. Contains the cached output.
    Skipped(T),
}

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Database backend type for implementations.
    type Db: sqlx::Database;

    /// Execute raw SQL without parameters.
    ///
    /// This method is intended for test setup/cleanup and administrative operations.
    /// For production queries, prefer using the repository interfaces.
    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()>;

    /// Execute raw SQL with a single i64 parameter.
    ///
    /// This method is intended for test setup/cleanup and administrative operations.
    /// The parameter is bound as $1 in the SQL string.
    /// For production queries, prefer using the repository interfaces.
    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()>;

    /// Execute raw SQL with two i64 parameters.
    ///
    /// This method is intended for test setup/cleanup and administrative operations.
    /// The parameters are bound as $1 and $2 in the SQL string.
    /// For production queries, prefer using the repository interfaces.
    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()>;

    /// Query a single i64 value using raw SQL.
    async fn query_int(&self, sql: &str) -> crate::error::Result<i64>;

    /// Query a single string value using raw SQL.
    async fn query_string(&self, sql: &str) -> crate::error::Result<String>;

    /// Query a single boolean value using raw SQL.
    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool>;

    /// Get the configuration for this store
    fn config(&self) -> &Config;

    /// Get access to the queue repository.
    fn queues(&self) -> &dyn QueueTable;
    /// Get access to the message repository.
    fn messages(&self) -> &dyn MessageTable;
    /// Get access to the worker repository.
    fn workers(&self) -> &dyn WorkerTable;
    /// Get access to the archive repository.
    fn archive(&self) -> &dyn ArchiveTable;
    /// Get access to the workflow repository.
    fn workflows(&self) -> &dyn WorkflowTable;

    /// Create a new workflow.
    ///
    /// This is the low-level API. Use `pgqrs::workflow()` builder for convenience.
    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> crate::error::Result<Box<dyn Workflow>>;

    /// Attempt to acquire a step lock.
    ///
    /// This is the low-level API. Use `pgqrs::step()` builder for convenience.
    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> crate::error::Result<StepResult<serde_json::Value>>;

    /// Get an admin worker interface.
    async fn admin(&self, config: &Config) -> crate::error::Result<Box<dyn Admin>>;

    /// Get a producer interface for a specific queue with worker identity.
    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>>;

    /// Get a consumer interface for a specific queue with worker identity.
    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>>;

    /// Get a workflow handle.
    fn workflow(&self, id: i64) -> Box<dyn Workflow>;

    /// Get a generic worker handle by ID.
    fn worker(&self, id: i64) -> Box<dyn Worker>;

    /// Returns the concurrency model supported by this backend.
    fn concurrency_model(&self) -> ConcurrencyModel;

    /// Returns the backend name (e.g., "postgres", "sqlite", "turso")
    fn backend_name(&self) -> &'static str;

    /// Create an ephemeral producer (NULL hostname/port, auto-cleanup).
    /// Used by high-level API functions like `produce()`.
    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>>;

    /// Create an ephemeral consumer (NULL hostname/port, auto-cleanup).
    /// Used by high-level API functions like `consume()`.
    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>>;
}

/// Repository for managing queues.
#[async_trait]
pub trait QueueTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewQueue) -> crate::error::Result<QueueInfo>;
    async fn get(&self, id: i64) -> crate::error::Result<QueueInfo>;
    async fn list(&self) -> crate::error::Result<Vec<QueueInfo>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;

    // Queue-specific methods from src/tables/pgqrs_queues.rs
    async fn get_by_name(&self, name: &str) -> crate::error::Result<QueueInfo>;
    async fn exists(&self, name: &str) -> crate::error::Result<bool>;
    async fn delete_by_name(&self, name: &str) -> crate::error::Result<u64>;
}

/// Repository for managing messages.
#[async_trait]
pub trait MessageTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: crate::types::NewMessage) -> crate::error::Result<QueueMessage>;
    async fn get(&self, id: i64) -> crate::error::Result<QueueMessage>;
    async fn list(&self) -> crate::error::Result<Vec<QueueMessage>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<QueueMessage>>;

    // Message-specific methods from src/tables/pgqrs_messages.rs
    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> crate::error::Result<Vec<i64>>;

    async fn get_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<QueueMessage>>;

    async fn update_visibility_timeout(
        &self,
        id: i64,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<u64>;

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<u64>;

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<Vec<bool>>;

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> crate::error::Result<Vec<bool>>;

    async fn count_pending(&self, queue_id: i64) -> crate::error::Result<i64>;

    async fn count_pending_filtered(
        &self,
        queue_id: i64,
        worker_id: Option<i64>,
    ) -> crate::error::Result<i64>;

    async fn delete_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<bool>>;
}

/// Repository for managing workers.
#[async_trait]
pub trait WorkerTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: crate::types::NewWorker) -> crate::error::Result<WorkerInfo>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkerInfo>;
    async fn list(&self) -> crate::error::Result<Vec<WorkerInfo>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<WorkerInfo>>;

    // Worker-specific methods from src/tables/pgqrs_workers.rs
    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> crate::error::Result<i64>;

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> crate::error::Result<i64>;

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> crate::error::Result<Vec<WorkerInfo>>;

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> crate::error::Result<Vec<WorkerInfo>>;

    /// Register a worker with state machine handling.
    ///
    /// - Reuses stopped workers (resets to Ready)
    /// - Creates new worker if doesn't exist
    /// - Errors if worker is already active
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue (None for admin workers)
    /// * `hostname` - Hostname for the worker
    /// * `port` - Port for the worker
    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> crate::error::Result<WorkerInfo>;

    /// Register an ephemeral worker (one-off, short-lived).
    ///
    /// Ephemeral workers use an auto-generated unique hostname and port -1.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue (None for admin workers)
    async fn register_ephemeral(&self, queue_id: Option<i64>) -> crate::error::Result<WorkerInfo>;
}

/// Repository for managing archived messages.
#[async_trait]
pub trait ArchiveTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewArchivedMessage) -> crate::error::Result<ArchivedMessage>;
    async fn get(&self, id: i64) -> crate::error::Result<ArchivedMessage>;
    async fn list(&self) -> crate::error::Result<Vec<ArchivedMessage>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<ArchivedMessage>>;

    // Archive-specific methods from src/tables/pgqrs_archive.rs
    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    async fn dlq_count(&self, max_attempts: i32) -> crate::error::Result<i64>;

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    async fn count_by_worker(&self, worker_id: i64) -> crate::error::Result<i64>;

    async fn delete_by_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    async fn replay_message(&self, msg_id: i64) -> crate::error::Result<Option<QueueMessage>>;

    /// Count archived messages for a specific queue
    async fn count_for_queue(&self, queue_id: i64) -> crate::error::Result<i64>;
}

/// Repository for managing workflows.
#[async_trait]
pub trait WorkflowTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewWorkflow) -> crate::error::Result<WorkflowRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkflowRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkflowRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}
