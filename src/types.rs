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
    pub id: i64,
    /// Queue ID this message belongs to
    pub queue_id: i64,
    /// Worker ID that has this message assigned (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub worker_id: Option<i64>,
    /// The actual message payload (JSON)
    pub payload: serde_json::Value,
    /// Visibility timeout (when the message becomes available again)
    pub vt: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the message was created
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Number of times this message has been read
    pub read_ct: i32,
    /// Timestamp when the message was dequeued (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub dequeued_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl fmt::Display for QueueMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueueMessage {{ id: {}, queue_id: {}, read_ct: {}, enqueued_at: {}, vt: {}, payload: {} }}",
            self.id, self.queue_id, self.read_ct, self.enqueued_at, self.vt, self.payload
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

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::FromRow, Tabled)]
pub struct QueueInfo {
    /// Queue ID (primary key)
    pub id: i64,
    /// Name of the queue
    pub queue_name: String,
    /// Timestamp when the queue was created
    pub created_at: DateTime<Utc>,
}

impl fmt::Display for QueueInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueueInfo {{ id: {}, queue_name: {}, created_at: {} }}",
            self.id, self.queue_name, self.created_at
        )
    }
}

/// An archived message with additional tracking information
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, Tabled)]
pub struct ArchivedMessage {
    /// Unique archive entry ID
    pub id: i64,
    /// Original message ID from pgqrs_messages table
    pub original_msg_id: i64,
    /// Queue ID this message belonged to
    pub queue_id: i64,
    /// Worker ID that processed this message (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub worker_id: Option<i64>,
    /// The actual message payload (JSON)
    pub payload: serde_json::Value,
    /// Timestamp when the message was originally created
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Visibility timeout when the message was archived
    pub vt: chrono::DateTime<chrono::Utc>,
    /// Number of times this message was read before archiving
    pub read_ct: i32,
    /// Timestamp when the message was archived
    pub archived_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the message was dequeued from the queue (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub dequeued_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl fmt::Display for ArchivedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArchivedMessage {{ id: {}, original_msg_id: {}, queue_id: {}, enqueued_at: {}, archived_at: {} }}",
            self.id, self.original_msg_id, self.queue_id, self.enqueued_at, self.archived_at
        )
    }
}

impl ArchivedMessage {
    /// Calculate processing duration if both enqueued_at and dequeued_at are available
    pub fn get_processing_duration(&self) -> Option<std::time::Duration> {
        self.dequeued_at.map(|dequeued| {
            let duration = dequeued - self.enqueued_at;
            duration.to_std().unwrap_or_default()
        })
    }
}

/// A worker instance that processes messages from queues
#[derive(Debug, Clone, Serialize, Deserialize, Tabled, sqlx::FromRow)]
pub struct WorkerInfo {
    /// Unique worker ID
    pub id: i64,
    /// Hostname where the worker is running
    pub hostname: String,
    /// Port number for the worker
    pub port: i32,
    /// Queue ID this worker is processing
    pub queue_name: String,
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

impl fmt::Display for WorkerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkerInfo {{ id: {}, hostname: {}, port: {}, queue_name: {}, status: {:?} }}",
            self.id, self.hostname, self.port, self.queue_name, self.status
        )
    }
}

/// Worker status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "worker_status", rename_all = "snake_case")]
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
