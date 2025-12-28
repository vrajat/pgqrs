//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repositories,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

use crate::rate_limit::RateLimitStatus;
use crate::tables::{NewQueue, NewWorkflow, WorkflowRecord};
use crate::types::{ArchivedMessage, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus};
use crate::validation::ValidationConfig;
use crate::Config;
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

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
#[async_trait]
pub trait Admin: Worker {
    async fn install(&self) -> crate::error::Result<()>;
    async fn verify(&self) -> crate::error::Result<()>;
    async fn register(&mut self, hostname: String, port: i32) -> crate::error::Result<WorkerInfo>;
    async fn create_queue(&self, name: &str) -> crate::error::Result<QueueInfo>;
    async fn get_queue(&self, name: &str) -> crate::error::Result<QueueInfo>;
    // Matching the exact signature from src/worker/admin.rs in v4
    async fn delete_queue(&self, queue_info: &QueueInfo) -> crate::error::Result<()>;

    // Metrics and management
    async fn purge_queue(&self, name: &str) -> crate::error::Result<()>;
    async fn dlq(&self) -> crate::error::Result<Vec<i64>>;
    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::types::QueueMetrics>;
    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::types::QueueMetrics>>;
    async fn system_stats(&self) -> crate::error::Result<crate::types::SystemStats>;
    async fn worker_health_stats(
        &self,
        heartbeat_timeout: Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::types::WorkerHealthStats>>;
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
    fn id(&self) -> i64;
    async fn start(&self) -> crate::error::Result<()>;
    async fn success<T: serde::Serialize + Send + Sync>(&self, output: T) -> crate::error::Result<()>;
    async fn fail<T: serde::Serialize + Send + Sync>(&self, error: T) -> crate::error::Result<()>;
}

/// Interface for a workflow step execution guard.
#[async_trait]
pub trait StepGuard: Send + Sync {
    async fn success<T: serde::Serialize + Send + Sync>(self, output: T) -> crate::error::Result<()>;
    async fn fail<T: serde::Serialize + Send + Sync>(self, error: T) -> crate::error::Result<()>;
}

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    /// The repository type for queue operations.
    type QueueTable: QueueTable;
    /// The repository type for message operations.
    type MessageTable: MessageTable;
    /// The repository type for worker operations.
    type WorkerTable: WorkerTable;
    /// The repository type for archive operations.
    type ArchiveTable: ArchiveTable;
    /// The repository type for workflow operations.
    type WorkflowTable: WorkflowTable;

    /// The type implementing the Admin interface.
    type Admin: Admin;
    /// The type implementing the Producer interface.
    type Producer: Producer;
    /// The type implementing the Consumer interface.
    type Consumer: Consumer;
    /// The type implementing the Workflow interface.
    type Workflow: Workflow;
    /// The type implementing the StepGuard interface.
    type StepGuard: StepGuard;

    /// Get access to the queue repository.
    fn queues(&self) -> &Self::QueueTable;
    /// Get access to the message repository.
    fn messages(&self) -> &Self::MessageTable;
    /// Get access to the worker repository.
    fn workers(&self) -> &Self::WorkerTable;
    /// Get access to the archive repository.
    fn archive(&self) -> &Self::ArchiveTable;
    /// Get access to the workflow repository.
    fn workflows(&self) -> &Self::WorkflowTable;

    /// Get an admin worker interface.
    async fn admin(&self, config: &Config) -> crate::error::Result<Self::Admin>;

    /// Get a producer interface for a specific queue with worker identity.
    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Self::Producer>;

    /// Get a consumer interface for a specific queue with worker identity.
    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Self::Consumer>;

    /// Get a workflow handle.
    fn workflow(&self, id: i64) -> Self::Workflow;

    /// Acquire a step execution guard.
    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> crate::error::Result<Option<Self::StepGuard>>;

    /// Create a new workflow execution.
    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> crate::error::Result<Self::Workflow>;
}

/// Repository for managing queues.
#[async_trait]
pub trait QueueTable: Send + Sync {
    /// Get queue information by name
    async fn get_by_name(&self, name: &str) -> crate::error::Result<QueueInfo>;
    /// Insert a new queue
    async fn insert(&self, data: NewQueue) -> crate::error::Result<QueueInfo>;
    /// Check if a queue exists
    async fn exists(&self, name: &str) -> crate::error::Result<bool>;
    /// Delete a queue by name
    async fn delete_by_name(&self, name: &str) -> crate::error::Result<u64>;
    /// List all queues
    async fn list(&self) -> crate::error::Result<Vec<QueueInfo>>;
}

/// Repository for managing messages.
#[async_trait]
pub trait MessageTable: Send + Sync {
    /// Enqueue a single message
    async fn enqueue(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> crate::error::Result<i64>;

    /// Enqueue multiple messages
    async fn enqueue_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> crate::error::Result<Vec<i64>>;

    /// Dequeue messages
    async fn dequeue(
        &self,
        queue_id: i64,
        worker_id: i64,
        limit: usize,
        vt_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    /// Get a message by ID
    async fn get(&self, id: i64) -> crate::error::Result<QueueMessage>;

    /// Delete a message by ID
    async fn delete(&self, id: i64) -> crate::error::Result<bool>;

    /// Delete multiple messages
    async fn delete_batch(&self, ids: &[i64], worker_id: i64) -> crate::error::Result<Vec<bool>>;

    /// Extend visibility timeout
    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<bool>;

    /// Release a message (return to queue)
    async fn release(&self, id: i64, worker_id: i64) -> crate::error::Result<bool>;

    /// Release multiple messages
    async fn release_batch(&self, ids: &[i64], worker_id: i64) -> crate::error::Result<Vec<bool>>;
}

/// Repository for managing workers.
#[async_trait]
pub trait WorkerTable: Send + Sync {
    /// Register a new worker
    async fn register(
        &self,
        queue_id: Option<i64>, // None for admin
        hostname: &str,
        port: i32,
    ) -> crate::error::Result<WorkerInfo>;

    /// Get status of a worker
    async fn get_status(&self, worker_id: i64) -> crate::error::Result<WorkerStatus>;
    /// Send a heartbeat for a worker
    async fn heartbeat(&self, worker_id: i64) -> crate::error::Result<()>;
    /// Check if a worker is healthy
    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> crate::error::Result<bool>;

    // CRUD methods
    async fn get(&self, id: i64) -> crate::error::Result<WorkerInfo>;
    async fn list(&self) -> crate::error::Result<Vec<WorkerInfo>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;

    // Lifecycle Transitions
    /// Suspend a worker
    async fn suspend(&self, worker_id: i64) -> crate::error::Result<()>;
    /// Resume a worker
    async fn resume(&self, worker_id: i64) -> crate::error::Result<()>;
    /// Shutdown a worker
    async fn shutdown(&self, worker_id: i64) -> crate::error::Result<()>;
}

/// Repository for managing archived messages.
#[async_trait]
pub trait ArchiveTable: Send + Sync {
    /// Archive a specific message
    async fn archive_message(
        &self,
        msg_id: i64,
        worker_id: i64,
    ) -> crate::error::Result<Option<ArchivedMessage>>;

    /// Archive a batch of messages
    async fn archive_batch(&self, msg_ids: &[i64]) -> crate::error::Result<Vec<bool>>;

    // Read methods
    /// List DLQ messages
    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    /// Count DLQ messages
    async fn dlq_count(&self, max_attempts: i32) -> crate::error::Result<i64>;

    /// List archived messages by worker
    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    /// Count archived messages by worker
    async fn count_by_worker(&self, worker_id: i64) -> crate::error::Result<i64>;
}

/// Repository for managing workflows.
#[async_trait]
pub trait WorkflowTable: Send + Sync {
    /// Insert a new workflow
    async fn insert(&self, data: NewWorkflow) -> crate::error::Result<WorkflowRecord>;
    /// Get a workflow by ID
    async fn get(&self, id: i64) -> crate::error::Result<WorkflowRecord>;
    /// List all workflows
    async fn list(&self) -> crate::error::Result<Vec<WorkflowRecord>>;
    /// Count workflows
    async fn count(&self) -> crate::error::Result<i64>;
    /// Delete a workflow by ID
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}
