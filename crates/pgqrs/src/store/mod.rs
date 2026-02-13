//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repositories,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

use crate::rate_limit::RateLimitStatus;
use crate::types::{
    ArchivedMessage, NewArchivedMessage, NewRunRecord, NewStepRecord, QueueMessage, QueueRecord,
    RunRecord, StepRecord, WorkerRecord, WorkerStatus,
};
use crate::types::{NewQueueRecord, NewWorkflowRecord, WorkflowRecord};
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
#[cfg(feature = "turso")]
pub mod turso;

pub use any::AnyStore;

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) mod sqlite_utils {
    use crate::error::{Error, Result};
    use chrono::{DateTime, Utc};

    /// Parse SQLite/Turso TEXT timestamp to DateTime<Utc>
    pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
        // SQLite datetime() returns "YYYY-MM-DD HH:MM:SS" format
        // We append +0000 to parse it as UTC
        DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| Error::Internal {
                message: format!("Invalid timestamp: {}", e),
            })
    }

    /// Format DateTime<Utc> for SQLite/Turso TEXT storage
    pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

/// Trait defining the interface for all worker types.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Get the unique identifier for this worker.
    fn worker_record(&self) -> &WorkerRecord;

    /// Get the unique identifier for this worker.
    fn worker_id(&self) -> i64 {
        self.worker_record().id
    }

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
/// - **Schema management**: `verify()`
/// - **Queue operations**: `create_queue()`, `get_queue()`, `delete_queue()`, `purge_queue()`, `dlq()`
/// - **Worker management**: `delete_worker()`, `list_workers()`, `release_worker_messages()`, `purge_old_workers()`
/// - **Metrics**: `queue_metrics()`, `all_queues_metrics()`, `system_stats()`, `worker_stats()`, `worker_health_stats()`
#[async_trait]
pub trait Admin: Worker {
    // ===== Schema Management =====

    /// Verify the pgqrs schema is correctly installed.
    ///
    /// Checks that all required tables, indexes, and constraints exist and are valid.
    async fn verify(&self) -> crate::error::Result<()>;

    // ===== Queue Operations =====

    /// Create a new queue.
    ///
    /// # Arguments
    /// * `name` - Unique name for the queue
    async fn create_queue(&self, name: &str) -> crate::error::Result<QueueRecord>;

    /// Get queue information by name.
    async fn get_queue(&self, name: &str) -> crate::error::Result<QueueRecord>;

    /// Delete a queue.
    ///
    /// The queue must be empty (no messages or workers) before deletion.
    async fn delete_queue(&self, queue_info: &QueueRecord) -> crate::error::Result<()>;

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
    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::stats::QueueMetrics>;

    /// Get metrics for all queues.
    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::stats::QueueMetrics>>;

    /// Get system-wide statistics.
    ///
    /// Returns aggregate statistics across all queues, workers, and messages.
    async fn system_stats(&self) -> crate::error::Result<crate::stats::SystemStats>;

    /// Get worker health statistics.
    ///
    /// # Arguments
    /// * `heartbeat_timeout` - Duration after which a worker is considered stale
    /// * `group_by_queue` - If true, returns per-queue stats; if false, returns global stats
    async fn worker_health_stats(
        &self,
        heartbeat_timeout: Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::stats::WorkerHealthStats>>;

    /// Get worker statistics for a queue.
    ///
    /// Returns counts of workers by status and average messages per worker.
    async fn worker_stats(
        &self,
        queue_name: &str,
    ) -> crate::error::Result<crate::stats::WorkerStats>;

    // ===== Worker Management =====

    /// Delete a worker by ID.
    ///
    /// The worker must not have any associated messages or archives.
    async fn delete_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    /// List all workers across all queues.
    async fn list_workers(&self) -> crate::error::Result<Vec<WorkerRecord>>;

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

    /// Enqueue a message at a specific time (for testing/time mocking).
    ///
    /// This allows tests to control the enqueue time for deterministic behavior.
    async fn enqueue_at(
        &self,
        payload: &Value,
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage>;

    /// Batch enqueue messages at a specific time (for testing/time mocking).
    async fn batch_enqueue_at(
        &self,
        payloads: &[Value],
        now: chrono::DateTime<chrono::Utc>,
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

/// Interface for a workflow definition.
#[async_trait]
pub trait Workflow: Send + Sync {
    fn name(&self) -> &str;
}

/// Interface for a workflow execution run.
#[async_trait]
pub trait Run: Send + Sync {
    /// Get the run ID.
    fn id(&self) -> i64;

    /// Start the run execution.
    async fn start(&mut self) -> crate::error::Result<()>;

    /// Complete the run successfully with a value.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;

    /// Fail the run with an error value.
    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()>;

    /// Acquire a step lock for this run.
    ///
    /// This is used by the `#[pgqrs_step]` macro to provide step idempotency.
    async fn acquire_step(
        &self,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>>;
}

/// Extension trait for Run to provide generic convenience methods.
#[async_trait]
pub trait RunExt: Run {
    /// Complete the run successfully with a serializable output.
    async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    /// Fail the run with a serializable error.
    async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value).await
    }
}
// Automatically implement Extension on anything that implements Run
impl<T: ?Sized + Run> RunExt for T {}

// Allow extension methods (and callers) to work with boxed run trait objects.
#[async_trait]
impl<T: ?Sized + Run> Run for Box<T> {
    fn id(&self) -> i64 {
        (**self).id()
    }

    async fn start(&mut self) -> crate::error::Result<()> {
        (**self).start().await
    }

    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        (**self).complete(output).await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()> {
        (**self).fail_with_json(error).await
    }

    async fn acquire_step(
        &self,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        (**self).acquire_step(step_id, current_time).await
    }
}

/// A guard for a workflow step execution.
#[async_trait]
pub trait StepGuard: Send + Sync {
    /// Complete the step successfully.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;

    /// Fail the step.
    async fn fail_with_json(
        &mut self,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()>;
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
        self.fail_with_json(value, chrono::Utc::now()).await
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
    /// Get access to the workflow run repository.
    fn workflow_runs(&self) -> &dyn RunRecordTable;
    /// Get access to the workflow step repository.
    fn workflow_steps(&self) -> &dyn StepRecordTable;

    /// Attempt to acquire a step lock.
    ///
    /// This is the low-level API. Use `pgqrs::step()` builder for convenience.
    async fn acquire_step(
        &self,
        run_id: i64,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<StepResult<serde_json::Value>>;

    /// Initialize the pgqrs schema in the database.
    ///
    /// This creates all necessary tables, indexes, and functions required for pgqrs to operate.
    async fn bootstrap(&self) -> crate::error::Result<()>;

    /// Get an admin worker interface.
    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Admin>>;

    /// Get an ephemeral admin worker interface.
    async fn admin_ephemeral(&self, config: &Config) -> crate::error::Result<Box<dyn Admin>>;

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

    /// Get a workflow definition handle.
    fn workflow(&self, name: &str) -> crate::error::Result<Box<dyn Workflow>>;

    /// Create a workflow definition (template) idempotently.
    ///
    /// This should ensure the backing queue exists and create the workflow record if missing.
    async fn create_workflow(&self, name: &str) -> crate::error::Result<WorkflowRecord>;

    /// Trigger a workflow run.
    ///
    /// This should create a run record and enqueue a message with payload `{ "run_id": ... }`.
    async fn trigger_workflow(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> crate::error::Result<RunRecord>;

    /// Create a local run handle from an existing run id.
    ///
    /// Backends may use this to allow in-process execution after triggering.
    async fn run(&self, run_id: i64) -> crate::error::Result<Box<dyn Run>>;

    /// Get a generic worker handle by ID.
    async fn worker(&self, id: i64) -> crate::error::Result<Box<dyn Worker>>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl BackendType {
    const POSTGRES_PREFIXES: &'static [&'static str] =
        &["postgres://", "postgresql://", "postgres", "pg"];
    const SQLITE_PREFIXES: &'static [&'static str] = &["sqlite://", "sqlite:", "sqlite"];
    const TURSO_PREFIXES: &'static [&'static str] = &["turso://", "turso:", "turso"];

    pub fn detect(dsn: &str) -> crate::error::Result<Self> {
        // Check Postgres
        if Self::POSTGRES_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "postgres")]
            return Ok(Self::Postgres);

            #[cfg(not(feature = "postgres"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Postgres backend is not enabled".to_string(),
            });
        }

        // Check SQLite
        if Self::SQLITE_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "sqlite")]
            return Ok(Self::Sqlite);

            #[cfg(not(feature = "sqlite"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Sqlite backend is not enabled".to_string(),
            });
        }

        // Check Turso
        if Self::TURSO_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "turso")]
            return Ok(Self::Turso);

            #[cfg(not(feature = "turso"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Turso backend is not enabled".to_string(),
            });
        }

        Err(crate::error::Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Unsupported DSN format: {}", dsn),
        })
    }
}

/// Repository for managing queues.
#[async_trait]
pub trait QueueTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewQueueRecord) -> crate::error::Result<QueueRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<QueueRecord>;
    async fn list(&self) -> crate::error::Result<Vec<QueueRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;

    // Queue-specific methods from src/tables/pgqrs_queues.rs
    async fn get_by_name(&self, name: &str) -> crate::error::Result<QueueRecord>;
    async fn exists(&self, name: &str) -> crate::error::Result<bool>;
    async fn delete_by_name(&self, name: &str) -> crate::error::Result<u64>;
}

/// Repository for managing messages.
#[async_trait]
pub trait MessageTable: Send + Sync {
    // Methods from Table
    async fn insert(
        &self,
        data: crate::types::NewQueueMessage,
    ) -> crate::error::Result<QueueMessage>;
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
    async fn insert(
        &self,
        data: crate::types::NewWorkerRecord,
    ) -> crate::error::Result<WorkerRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkerRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkerRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<WorkerRecord>>;

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
    ) -> crate::error::Result<Vec<WorkerRecord>>;

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> crate::error::Result<Vec<WorkerRecord>>;

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
    ) -> crate::error::Result<WorkerRecord>;

    /// Register an ephemeral worker (one-off, short-lived).
    ///
    /// Ephemeral workers use an auto-generated unique hostname and port -1.
    ///
    /// # Arguments
    /// * `queue_id` - ID of the queue (None for admin workers)
    async fn register_ephemeral(&self, queue_id: Option<i64>)
        -> crate::error::Result<WorkerRecord>;
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
    async fn insert(&self, data: NewWorkflowRecord) -> crate::error::Result<WorkflowRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkflowRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkflowRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}

/// Repository for managing workflow runs.
#[async_trait]
pub trait RunRecordTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewRunRecord) -> crate::error::Result<RunRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<RunRecord>;
    async fn list(&self) -> crate::error::Result<Vec<RunRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}

/// Repository for managing workflow steps.
#[async_trait]
pub trait StepRecordTable: Send + Sync {
    // Methods from Table
    async fn insert(&self, data: NewStepRecord) -> crate::error::Result<StepRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<StepRecord>;
    async fn list(&self) -> crate::error::Result<Vec<StepRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}
