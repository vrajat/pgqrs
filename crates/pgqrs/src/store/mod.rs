//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait and its associated repositories,
//! enabling pgqrs to support multiple database backends (Postgres, SQLite, Turso).

use crate::tables::{NewQueue, NewWorkflow, WorkflowRecord};
use crate::types::{ArchivedMessage, MessageID, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    type QueueStore: QueueStore<Error = Self::Error>;
    type MessageStore: MessageStore<Error = Self::Error>;
    type WorkerStore: WorkerStore<Error = Self::Error>;
    type ArchiveStore: ArchiveStore<Error = Self::Error>;
    type WorkflowStore: WorkflowStore<Error = Self::Error>;

    fn queues(&self) -> &Self::QueueStore;
    fn messages(&self) -> &Self::MessageStore;
    fn workers(&self) -> &Self::WorkerStore;
    fn archive(&self) -> &Self::ArchiveStore;
    fn workflows(&self) -> &Self::WorkflowStore;

    // TODO: Transaction management to be added when needed
    // async fn begin(&self) -> Result<Box<dyn Transaction<Error = Self::Error>>>;
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    type Error;

    async fn get_by_name(&self, name: &str) -> std::result::Result<QueueInfo, Self::Error>;
    async fn insert(&self, data: NewQueue) -> std::result::Result<QueueInfo, Self::Error>;
    async fn exists(&self, name: &str) -> std::result::Result<bool, Self::Error>;
    async fn delete_by_name(&self, name: &str) -> std::result::Result<u64, Self::Error>;
    async fn list(&self) -> std::result::Result<Vec<QueueInfo>, Self::Error>;
}

#[async_trait]
pub trait MessageStore: Send + Sync {
    type Error;

    /// Enqueue a single message
    async fn enqueue(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> std::result::Result<MessageID, Self::Error>;

    /// Enqueue multiple messages
    async fn enqueue_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> std::result::Result<Vec<MessageID>, Self::Error>;

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
}

#[async_trait]
pub trait WorkerStore: Send + Sync {
    type Error;

    async fn register(
        &self,
        queue_id: Option<i64>, // None for admin
        hostname: &str,
        port: i32,
    ) -> std::result::Result<WorkerInfo, Self::Error>;

    async fn get_status(&self, worker_id: i64) -> std::result::Result<WorkerStatus, Self::Error>;
    async fn heartbeat(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    async fn is_healthy(
        &self,
        worker_id: i64,
        max_age: Duration,
    ) -> std::result::Result<bool, Self::Error>;

    // Lifecycle Transitions
    async fn suspend(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    async fn resume(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
    async fn shutdown(&self, worker_id: i64) -> std::result::Result<(), Self::Error>;
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    type Error;

    async fn archive_message(
        &self,
        msg_id: i64,
    ) -> std::result::Result<Option<ArchivedMessage>, Self::Error>;
    async fn archive_batch(&self, msg_ids: &[i64]) -> std::result::Result<Vec<bool>, Self::Error>;

    // Read methods
    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> std::result::Result<Vec<ArchivedMessage>, Self::Error>;
    async fn dlq_count(&self, max_attempts: i32) -> std::result::Result<i64, Self::Error>;
    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> std::result::Result<Vec<ArchivedMessage>, Self::Error>;
    async fn count_by_worker(&self, worker_id: i64) -> std::result::Result<i64, Self::Error>;
}

#[async_trait]
pub trait WorkflowStore: Send + Sync {
    type Error;

    async fn insert(&self, data: NewWorkflow) -> std::result::Result<WorkflowRecord, Self::Error>;
    async fn get(&self, id: i64) -> std::result::Result<WorkflowRecord, Self::Error>;
    async fn list(&self) -> std::result::Result<Vec<WorkflowRecord>, Self::Error>;
    async fn count(&self) -> std::result::Result<i64, Self::Error>;
    async fn delete(&self, id: i64) -> std::result::Result<u64, Self::Error>;
}
