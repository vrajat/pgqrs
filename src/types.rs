//! Core types for pgqrs: queue messages, metrics, and metadata.
//!
//! This module defines the main data structures used for queue operations, metrics, and metadata.
//!
//! ## What
//!
//! - [`QueueMessage`] represents a job/message in the queue.
//! - [`QueueMetrics`] provides statistics about a queue.
//! - [`MetaResult`] describes queue metadata from the database.
//!
//! ## How
//!
//! Use these types for interacting with queue data, inspecting metrics, and reading metadata.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::types::QueueMessage;
//! fn print_message(msg: QueueMessage) {
//!     println!("{}", msg);
//! }
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{self};
use tabled::Tabled;

/// A message in the queue
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, Tabled)]
pub struct QueueMessage {
    /// Unique message ID
    pub msg_id: i64,
    /// Number of times this message has been read
    pub read_ct: i32,
    /// Timestamp when the message was enqueued
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Visibility timeout (when the message becomes available again)
    pub vt: chrono::DateTime<chrono::Utc>,
    /// The actual message payload (JSON)
    pub message: serde_json::Value,
    /// Worker ID that has this message assigned (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub worker_id: Option<i64>,
}

impl fmt::Display for QueueMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueueMessage {{ msg_id: {}, read_ct: {}, enqueued_at: {}, vt: {}, message: {} }}",
            self.msg_id, self.read_ct, self.enqueued_at, self.vt, self.message
        )
    }
}

/// Queue metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    /// Name of the queue
    pub name: String,
    /// Total number of messages ever enqueued
    pub total_messages: i64,
    /// Number of messages currently pending
    pub pending_messages: i64,
    /// Number of messages currently locked (being processed)
    pub locked_messages: i64,
    /// Number of messages archived
    pub archived_messages: i64,
    /// Timestamp of the oldest pending message
    pub oldest_pending_message: Option<DateTime<Utc>>,
    /// Timestamp of the newest message
    pub newest_message: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Tabled)]
pub struct MetaResult {
    /// Name of the queue
    pub queue_name: String,
    /// Timestamp when the queue was created
    pub created_at: DateTime<Utc>,
    /// Whether the queue is unlogged (PostgreSQL optimization)
    pub unlogged: bool,
}

impl fmt::Display for MetaResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MetaResult {{ queue_name: {}, created_at: {}, unlogged: {} }}",
            self.queue_name, self.created_at, self.unlogged
        )
    }
}

/// An archived message with additional tracking information
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, Tabled)]
pub struct ArchivedMessage {
    /// Unique message ID
    pub msg_id: i64,
    /// Number of times this message has been read
    pub read_ct: i32,
    /// Timestamp when the message was enqueued
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Visibility timeout (when the message becomes available again)
    pub vt: chrono::DateTime<chrono::Utc>,
    /// The actual message payload (JSON)
    pub message: serde_json::Value,
    /// Timestamp when the message was archived
    #[tabled(skip)]
    pub archived_at: Option<chrono::DateTime<chrono::Utc>>,
    /// How long the message was being processed before archiving (in milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub processing_duration: Option<i64>,
}

impl fmt::Display for ArchivedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArchivedMessage {{ msg_id: {}, read_ct: {}, enqueued_at: {}, archived_at: {:?} }}",
            self.msg_id, self.read_ct, self.enqueued_at, self.archived_at
        )
    }
}

impl ArchivedMessage {
    /// Get the processing duration as a `std::time::Duration`
    pub fn get_processing_duration(&self) -> Option<std::time::Duration> {
        self.processing_duration
            .map(|millis| std::time::Duration::from_millis(millis as u64))
    }

    /// Set the processing duration from a `std::time::Duration`
    pub fn set_processing_duration(&mut self, duration: std::time::Duration) {
        self.processing_duration = Some(duration.as_millis() as i64);
    }
}

/// A worker instance that processes messages from queues
#[derive(Debug, Clone, Serialize, Deserialize, Tabled)]
pub struct Worker {
    /// Unique worker ID
    pub id: i64,
    /// Hostname where the worker is running
    pub hostname: String,
    /// Port number for the worker
    pub port: i32,
    /// Queue ID this worker is processing
    pub queue_id: String,
    /// Timestamp when the worker started
    pub started_at: DateTime<Utc>,
    /// Last heartbeat timestamp
    pub heartbeat_at: DateTime<Utc>,
    /// Timestamp when shutdown was initiated (if any)
    #[tabled(skip)]
    pub shutdown_at: Option<DateTime<Utc>>,
    /// Current status of the worker
    pub status: WorkerStatus,
}

// Custom deserialization for Worker from database rows
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WorkerRow {
    pub id: i64,
    pub hostname: String,
    pub port: i32,
    pub queue_id: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub shutdown_at: Option<DateTime<Utc>>,
    pub status: String,
}

impl From<WorkerRow> for Worker {
    fn from(row: WorkerRow) -> Self {
        let status = match row.status.as_str() {
            "ready" => WorkerStatus::Ready,
            "shutting_down" => WorkerStatus::ShuttingDown,
            "stopped" => WorkerStatus::Stopped,
            unknown => {
                tracing::warn!(
                    "Unknown worker status '{}' for worker {}, defaulting to 'ready'",
                    unknown,
                    row.id
                );
                WorkerStatus::Ready // Default fallback with logging
            }
        };

        Worker {
            id: row.id,
            hostname: row.hostname,
            port: row.port,
            queue_id: row.queue_id,
            started_at: row.started_at,
            heartbeat_at: row.heartbeat_at,
            shutdown_at: row.shutdown_at,
            status,
        }
    }
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Worker {{ id: {}, hostname: {}, port: {}, queue_id: {}, status: {:?} }}",
            self.id, self.hostname, self.port, self.queue_id, self.status
        )
    }
}

/// Worker status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkerStatus {
    /// Worker is ready to process messages
    Ready,
    /// Worker is shutting down gracefully
    ShuttingDown,
    /// Worker has stopped
    Stopped,
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerStatus::Ready => write!(f, "ready"),
            WorkerStatus::ShuttingDown => write!(f, "shutting_down"),
            WorkerStatus::Stopped => write!(f, "stopped"),
        }
    }
}

/// Worker statistics for monitoring and management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStats {
    /// Total number of workers
    pub total_workers: u32,
    /// Number of ready workers
    pub ready_workers: u32,
    /// Number of workers shutting down
    pub shutting_down_workers: u32,
    /// Number of stopped workers
    pub stopped_workers: u32,
    /// Average messages per worker
    pub average_messages_per_worker: f64,
    /// Age of the oldest worker
    pub oldest_worker_age: std::time::Duration,
    /// Age of the newest heartbeat
    pub newest_heartbeat_age: std::time::Duration,
}

impl fmt::Display for WorkerStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkerStats {{ total: {}, ready: {}, shutting_down: {}, stopped: {}, avg_messages: {:.2} }}",
            self.total_workers, self.ready_workers, self.shutting_down_workers,
            self.stopped_workers, self.average_messages_per_worker
        )
    }
}
