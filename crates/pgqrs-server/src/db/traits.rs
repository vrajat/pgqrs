use super::error::PgqrsError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::types::JsonValue;

/// Metadata for a queue, as stored in the meta table.
#[derive(Debug, Clone)]
pub struct Queue {
    /// Name of the queue
    pub queue_name: String,
    /// Timestamp when the queue was created
    pub created_at: DateTime<Utc>,
    /// Whether the queue is unlogged (faster, less durable)
    pub unlogged: bool,
}

/// A message in a queue.
#[derive(Debug, Clone)]
pub struct QueueMessage {
    /// Message ID
    pub id: i64,
    /// JSON payload
    pub payload: JsonValue,
    /// When the message was enqueued
    pub enqueued_at: DateTime<Utc>,
    /// Visibility timeout (when message becomes visible)
    pub vt: DateTime<Utc>,
    /// Number of times the message has been read
    pub read_ct: i32,
}

/// Statistics for a queue.
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    /// Number of pending messages
    pub pending: i64,
    /// Number of messages currently in flight
    pub in_flight: i64,
    /// Number of messages dead-lettered
    pub dead_lettered: i64,
}

/// Repository trait for queue management (CRUD operations).
#[async_trait]
pub trait QueueRepo {
    /// Create a new queue and return its metadata.
    async fn create_queue(&self, name: &str, unlogged: bool) -> Result<Queue, PgqrsError>;
    /// Delete a queue and all its messages.
    async fn delete_queue(&self, name: &str) -> Result<(), PgqrsError>;
    /// List all queues and their metadata.
    async fn list_queues(&self) -> Result<Vec<Queue>, PgqrsError>;
    /// Purge all messages from a queue.
    async fn purge_queue(&self, name: &str) -> Result<(), PgqrsError>;
    /// Get metadata for a specific queue.
    async fn get_queue(&self, name: &str) -> Result<Queue, PgqrsError>;
}

/// Repository trait for message operations in a queue.
#[async_trait]
pub trait MessageRepo {
    /// Enqueue a new message into a queue.
    async fn enqueue(&self, queue: &str, payload: &JsonValue) -> Result<QueueMessage, PgqrsError>;
    /// Enqueue a message with a delay before it becomes visible.
    async fn enqueue_delayed(
        &self,
        queue: &str,
        payload: &JsonValue,
        delay_seconds: u32,
    ) -> Result<QueueMessage, PgqrsError>;
    /// Enqueue multiple messages in a batch.
    async fn batch_enqueue(
        &self,
        queue: &str,
        payloads: &[JsonValue],
    ) -> Result<Vec<QueueMessage>, PgqrsError>;
    /// Dequeue (remove) a message from a queue.
    async fn dequeue(&self, queue: &str, message_id: i64) -> Result<QueueMessage, PgqrsError>;
    /// Acknowledge a message (mark as processed).
    async fn ack(&self, queue: &str, message_id: i64) -> Result<(), PgqrsError>;
    /// Negative-acknowledge a message (mark as failed).
    async fn nack(&self, queue: &str, message_id: i64) -> Result<(), PgqrsError>;
    /// Peek at messages in a queue without removing them.
    async fn peek(&self, queue: &str, limit: usize) -> Result<Vec<QueueMessage>, PgqrsError>;
    /// Get statistics for a queue.
    async fn stats(&self, queue: &str) -> Result<QueueStats, PgqrsError>;
    /// Get a message by its ID.
    async fn get_message_by_id(
        &self,
        queue: &str,
        message_id: i64,
    ) -> Result<QueueMessage, PgqrsError>;

    /// Extend the visibility timeout for a message (heartbeat).
    async fn heartbeat(
        &self,
        queue: &str,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<(), PgqrsError>;
}
