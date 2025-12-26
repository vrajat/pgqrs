//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repositories,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

use crate::tables::{NewQueue, NewWorkflow, WorkflowRecord};
use crate::types::{ArchivedMessage, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    /// The error type returned by store operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Initialize the storage backend (e.g. run migrations)
    async fn install(&self) -> std::result::Result<(), Self::Error>;

    /// Verify the storage backend health and schema integrity
    async fn verify(&self) -> std::result::Result<(), Self::Error>;

    /// The repository type for queue operations.
    type QueueStore: QueueStore<Error = Self::Error>;
    /// The repository type for message operations.
    type MessageStore: MessageStore<Error = Self::Error>;
    /// The repository type for worker operations.
    type WorkerStore: WorkerStore<Error = Self::Error>;
    /// The repository type for archive operations.
    type ArchiveStore: ArchiveStore<Error = Self::Error>;
    /// The repository type for workflow operations.
    type WorkflowStore: WorkflowStore<Error = Self::Error>;

    /// Get access to the queue repository.
    fn queues(&self) -> &Self::QueueStore;
    /// Get access to the message repository.
    fn messages(&self) -> &Self::MessageStore;
    /// Get access to the worker repository.
    fn workers(&self) -> &Self::WorkerStore;
    /// Get access to the archive repository.
    fn archive(&self) -> &Self::ArchiveStore;
    /// Get access to the workflow repository.
    fn workflows(&self) -> &Self::WorkflowStore;
}

/// Repository for managing queues.
#[async_trait]
pub trait QueueStore: Send + Sync {
    /// The error type returned by queue operations.
    type Error;

    /// Get queue information by name
    async fn get_by_name(&self, name: &str) -> std::result::Result<QueueInfo, Self::Error>;
    /// Insert a new queue
    async fn insert(&self, data: NewQueue) -> std::result::Result<QueueInfo, Self::Error>;
    /// Check if a queue exists
    async fn exists(&self, name: &str) -> std::result::Result<bool, Self::Error>;
    /// Delete a queue by name
    async fn delete_by_name(&self, name: &str) -> std::result::Result<u64, Self::Error>;
    /// List all queues
    async fn list(&self) -> std::result::Result<Vec<QueueInfo>, Self::Error>;

    /// Purge all messages from a queue by name
    async fn purge(&self, name: &str) -> std::result::Result<(), Self::Error>;

    /// Get metrics for a specific queue
    async fn metrics(
        &self,
        name: &str,
    ) -> std::result::Result<crate::types::QueueMetrics, Self::Error>;

    /// Get metrics for all queues
    async fn list_metrics(
        &self,
    ) -> std::result::Result<Vec<crate::types::QueueMetrics>, Self::Error>;
}

/// Repository for managing messages.
#[async_trait]
pub trait MessageStore: Send + Sync {
    /// The error type returned by message operations.
    type Error;

    /// Enqueue a single message
    async fn enqueue(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> std::result::Result<i64, Self::Error>;

    /// Enqueue multiple messages
    async fn enqueue_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> std::result::Result<Vec<i64>, Self::Error>;

    /// Dequeue messages
    async fn dequeue(
        &self,
        queue_id: i64,
        worker_id: i64,
        limit: usize,
        vt_seconds: u32,
    ) -> std::result::Result<Vec<QueueMessage>, Self::Error>;

    /// Get a message by ID
    async fn get(&self, id: i64) -> std::result::Result<QueueMessage, Self::Error>;

    /// Delete a message by ID
    async fn delete(&self, id: i64) -> std::result::Result<bool, Self::Error>;

    /// Delete multiple messages
    async fn delete_batch(
        &self,
        ids: &[i64],
        worker_id: i64,
    ) -> std::result::Result<Vec<bool>, Self::Error>;

    /// Extend visibility timeout
    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> std::result::Result<bool, Self::Error>;

    /// Release a message (return to queue)
    async fn release(&self, id: i64, worker_id: i64) -> std::result::Result<bool, Self::Error>;

    /// Release multiple messages
    async fn release_batch(
        &self,
        ids: &[i64],
        worker_id: i64,
    ) -> std::result::Result<Vec<bool>, Self::Error>;

    /// Count active messages held by a specific worker
    async fn count_active_by_worker(&self, worker_id: i64)
        -> std::result::Result<i64, Self::Error>;

    /// Move messages exceeding max read count to DLQ (archive)
    async fn move_to_dlq(&self, max_read_ct: u32) -> std::result::Result<Vec<i64>, Self::Error>;
}

/// Repository for managing workers.
#[async_trait]
pub trait WorkerStore: Send + Sync {
    /// The error type returned by worker operations.
    type Error;

    /// Register a new worker
    async fn register(
        &self,
        queue_id: Option<i64>, // None for admin
        hostname: &str,
        port: i32,
    ) -> std::result::Result<WorkerInfo, Self::Error>;

    /// Get status of a worker
    async fn get_status(&self, worker_id: i64) -> std::result::Result<WorkerStatus, Self::Error>;
    /// Send a heartbeat for a worker
    async fn heartbeat(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    /// Check if a worker is healthy
    async fn is_healthy(
        &self,
        worker_id: i64,
        max_age: Duration,
    ) -> std::result::Result<bool, Self::Error>;

    // Lifecycle Transitions
    /// Suspend a worker
    async fn suspend(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    /// Resume a worker
    async fn resume(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    /// Shutdown a worker
    async fn shutdown(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;

    /// Get health statistics for workers
    async fn health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> std::result::Result<Vec<crate::types::WorkerHealthStats>, Self::Error>;

    /// Purge stale stopped workers
    async fn purge_stale(
        &self,
        heartbeat_timeout: chrono::Duration,
    ) -> std::result::Result<u64, Self::Error>;
}

/// Repository for managing archived messages.
#[async_trait]
pub trait ArchiveStore: Send + Sync {
    /// The error type returned by archive operations.
    type Error;

    /// Archive a specific message
    async fn archive_message(
        &self,
        msg_id: i64,
        worker_id: i64,
    ) -> std::result::Result<Option<ArchivedMessage>, Self::Error>;

    /// Archive a batch of messages
    async fn archive_batch(&self, msg_ids: &[i64]) -> std::result::Result<Vec<bool>, Self::Error>;

    // Read methods
    /// List DLQ messages
    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> std::result::Result<Vec<ArchivedMessage>, Self::Error>;

    /// Count DLQ messages
    async fn dlq_count(&self, max_attempts: i32) -> std::result::Result<i64, Self::Error>;

    /// List archived messages by worker
    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> std::result::Result<Vec<ArchivedMessage>, Self::Error>;

    /// Count archived messages by worker
    async fn count_by_worker(&self, worker_id: i64) -> std::result::Result<i64, Self::Error>;

    /// Replay an archived message back to the active queue (DLQ replay)
    async fn replay_message(
        &self,
        archived_id: i64,
    ) -> std::result::Result<Option<crate::types::QueueMessage>, Self::Error>;
}

/// Repository for managing workflows.
#[async_trait]
pub trait WorkflowStore: Send + Sync {
    /// The error type returned by workflow operations.
    type Error;

    /// Insert a new workflow
    async fn insert(&self, data: NewWorkflow) -> std::result::Result<WorkflowRecord, Self::Error>;
    /// Get a workflow by ID
    async fn get(&self, id: i64) -> std::result::Result<WorkflowRecord, Self::Error>;
    /// List all workflows
    async fn list(&self) -> std::result::Result<Vec<WorkflowRecord>, Self::Error>;
    /// Count workflows
    async fn count(&self) -> std::result::Result<i64, Self::Error>;
    /// Delete a workflow by ID
    async fn delete(&self, id: i64) -> std::result::Result<u64, Self::Error>;
}
