//! Core types for pgqrs: queue messages, metrics, and metadata.
//!
//! This module defines the main data structures used for queue operations, metrics, and metadata.
//!
//! ## What
//!
//! - [`QueueMessage`] represents a job/message in the queue.
//! - [`QueueMetrics`] provides statistics about a queue.
//!
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
    /// Worker ID that produced this message
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub producer_worker_id: Option<i64>,
    /// Worker ID that consumed this message
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub consumer_worker_id: Option<i64>,
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
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct QueueMetrics {
    /// Name of the queue
    pub name: String,
    /// Total number of messages currently in the queue (active messages only)
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

impl Tabled for QueueMetrics {
    const LENGTH: usize = 7;

    fn fields(&self) -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            self.name.clone().into(),
            self.total_messages.to_string().into(),
            self.pending_messages.to_string().into(),
            self.locked_messages.to_string().into(),
            self.archived_messages.to_string().into(),
            display_option_datetime(&self.oldest_pending_message).into(),
            display_option_datetime(&self.newest_message).into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "name",
            "total_messages",
            "pending_messages",
            "locked_messages",
            "archived_messages",
            "oldest_pending_message",
            "newest_message",
        ]
        .into_iter()
        .map(|s| s.into())
        .collect()
    }
}

/// Helper function to format Option<DateTime<Utc>> for Tabled
pub fn display_option_datetime(o: &Option<DateTime<Utc>>) -> String {
    match o {
        Some(dt) => dt.to_rfc3339(),
        None => "N/A".to_string(),
    }
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

/// System-wide statistics
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SystemStats {
    /// Total number of queues
    pub total_queues: i64,
    /// Total number of workers (all statuses)
    pub total_workers: i64,
    /// Number of active workers (Ready/Running)
    pub active_workers: i64,
    /// Total messages across all queues (active only)
    pub total_messages: i64,
    /// Total pending messages across all queues
    pub pending_messages: i64,
    /// Total locked messages across all queues
    pub locked_messages: i64,
    /// Total archived messages across all queues
    pub archived_messages: i64,
    /// Schema version
    pub schema_version: String,
}

impl Tabled for SystemStats {
    const LENGTH: usize = 8;

    fn fields(&self) -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            self.total_queues.to_string().into(),
            self.total_workers.to_string().into(),
            self.active_workers.to_string().into(),
            self.total_messages.to_string().into(),
            self.pending_messages.to_string().into(),
            self.locked_messages.to_string().into(),
            self.archived_messages.to_string().into(),
            self.schema_version.clone().into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "total_queues",
            "total_workers",
            "active_workers",
            "total_messages",
            "pending_messages",
            "locked_messages",
            "archived_messages",
            "schema_version",
        ]
        .into_iter()
        .map(|s| s.into())
        .collect()
    }
}

/// Worker health statistics
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkerHealthStats {
    /// Queue name (or "Global" for global stats)
    pub queue_name: String,
    /// Total workers
    pub total_workers: i64,
    /// Active (Ready) workers
    pub ready_workers: i64,
    /// Suspended workers
    pub suspended_workers: i64,
    /// Stopped workers
    pub stopped_workers: i64,
    /// Workers with expired heartbeats
    pub stale_workers: i64,
}

impl Tabled for WorkerHealthStats {
    const LENGTH: usize = 6;

    fn fields(&self) -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            self.queue_name.clone().into(),
            self.total_workers.to_string().into(),
            self.ready_workers.to_string().into(),
            self.suspended_workers.to_string().into(),
            self.stopped_workers.to_string().into(),
            self.stale_workers.to_string().into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "queue_name",
            "total_workers",
            "ready_workers",
            "suspended_workers",
            "stopped_workers",
            "stale_workers",
        ]
        .into_iter()
        .map(|s| s.into())
        .collect()
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
    /// Worker ID that produced this message
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub producer_worker_id: Option<i64>,
    /// Worker ID that consumed this message
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub consumer_worker_id: Option<i64>,
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

/// Input data for creating a new archived message
#[derive(Debug)]
pub struct NewArchivedMessage {
    /// Original message ID from pgqrs_messages table
    pub original_msg_id: i64,
    /// Queue ID this message belonged to
    pub queue_id: i64,
    /// Worker ID that produced this message
    pub producer_worker_id: Option<i64>,
    /// Worker ID that consumed this message
    pub consumer_worker_id: Option<i64>,
    /// The actual message payload (JSON)
    pub payload: serde_json::Value,
    /// Timestamp when the message was originally created
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Visibility timeout when the message was archived
    pub vt: chrono::DateTime<chrono::Utc>,
    /// Number of times this message was read before archiving
    pub read_ct: i32,
    /// Timestamp when the message was dequeued from the queue (if any)
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
    pub fn get_processing_duration(&self) -> Option<chrono::Duration> {
        self.dequeued_at.map(|dequeued| dequeued - self.enqueued_at)
    }
}

/// A worker instance that processes messages from queues
#[derive(Debug, Clone, Serialize, Deserialize, Tabled, sqlx::FromRow)]
pub struct WorkerInfo {
    /// Unique worker ID
    pub id: i64,
    /// Hostname where the worker is running ("__ephemeral__" for ephemeral workers)
    pub hostname: String,
    /// Port number for the worker (-1 for ephemeral workers)
    pub port: i32,
    /// Queue ID this worker is processing (None for Admin workers)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub queue_id: Option<i64>,
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
        match self.queue_id {
            Some(queue_id) => write!(
                f,
                "WorkerInfo {{ id: {}, hostname: {}, port: {}, queue_id: {}, status: {:?} }}",
                self.id, self.hostname, self.port, queue_id, self.status
            ),
            None => write!(
                f,
                "WorkerInfo {{ id: {}, hostname: {}, port: {}, queue_id: None, status: {:?} }}",
                self.id, self.hostname, self.port, self.status
            ),
        }
    }
}

/// Worker status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "worker_status", rename_all = "snake_case")]
pub enum WorkerStatus {
    /// Worker is ready to process messages
    Ready,
    /// Worker is suspended (not accepting new work, can be resumed or shut down)
    Suspended,
    /// Worker has stopped
    Stopped,
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerStatus::Ready => write!(f, "ready"),
            WorkerStatus::Suspended => write!(f, "suspended"),
            WorkerStatus::Stopped => write!(f, "stopped"),
        }
    }
}

impl std::str::FromStr for WorkerStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ready" => Ok(WorkerStatus::Ready),
            "suspended" => Ok(WorkerStatus::Suspended),
            "stopped" => Ok(WorkerStatus::Stopped),
            _ => Err(format!("Invalid worker status: {}", s)),
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
    /// Number of suspended workers
    pub suspended_workers: u32,
    /// Number of stopped workers
    pub stopped_workers: u32,
    /// Average messages per worker
    pub average_messages_per_worker: f64,
    /// Age of the oldest worker
    pub oldest_worker_age: chrono::Duration,
    /// Age of the newest heartbeat
    pub newest_heartbeat_age: chrono::Duration,
}

impl fmt::Display for WorkerStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkerStats {{ total: {}, ready: {}, suspended: {}, stopped: {}, avg_messages: {:.2} }}",
            self.total_workers, self.ready_workers, self.suspended_workers,
            self.stopped_workers, self.average_messages_per_worker
        )
    }
}

// Moved from src/tables/pgqrs_queues.rs
/// Input data for creating a new queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewQueue {
    pub queue_name: String,
}

// Moved from src/tables/pgqrs_workers.rs
/// Input data for creating a new worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewWorker {
    pub hostname: String,
    pub port: i32,
    /// Queue ID (None for Admin workers)
    pub queue_id: Option<i64>,
}

// Moved from src/tables/pgqrs_messages.rs
/// Input data for creating a new message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewMessage {
    pub queue_id: i64,
    pub payload: serde_json::Value,
    pub read_ct: i32,
    pub enqueued_at: DateTime<Utc>,
    pub vt: DateTime<Utc>,
    pub producer_worker_id: Option<i64>,
    pub consumer_worker_id: Option<i64>,
}

/// Parameters for batch message insertion
#[derive(Debug, Clone)]
pub struct BatchInsertParams {
    pub read_ct: i32,
    pub enqueued_at: DateTime<Utc>,
    pub vt: DateTime<Utc>,
    pub producer_worker_id: Option<i64>,
    pub consumer_worker_id: Option<i64>,
}

// Moved from src/workflow/mod.rs
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, sqlx::Type)]
#[sqlx(
    type_name = "pgqrs_workflow_status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum WorkflowStatus {
    Pending,
    Running,
    Success,
    Error,
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowStatus::Pending => write!(f, "PENDING"),
            WorkflowStatus::Running => write!(f, "RUNNING"),
            WorkflowStatus::Success => write!(f, "SUCCESS"),
            WorkflowStatus::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for WorkflowStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(WorkflowStatus::Pending),
            "RUNNING" => Ok(WorkflowStatus::Running),
            "SUCCESS" => Ok(WorkflowStatus::Success),
            "ERROR" => Ok(WorkflowStatus::Error),
            _ => Err(format!("Invalid workflow status: {}", s)),
        }
    }
}

// Moved from src/tables/pgqrs_workflows.rs
#[derive(Debug, sqlx::FromRow, Clone)]
pub struct WorkflowRecord {
    pub workflow_id: i64,
    pub name: String,
    pub status: WorkflowStatus,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub executor_id: Option<String>,
}

pub struct NewWorkflow {
    pub name: String,
    pub input: Option<serde_json::Value>,
}
