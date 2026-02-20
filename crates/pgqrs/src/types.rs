//! Core types for pgqrs: queue messages, metrics, and metadata.
//!
//! This module defines the main data structures used for queue operations, metrics, and metadata.
//!
//! ## What
//!
//! - [`QueueMessage`] represents a job/message in the queue.
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
#[derive(Debug, Clone, Serialize, Deserialize, Tabled)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
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
    /// Timestamp when the message was archived (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub archived_at: Option<chrono::DateTime<chrono::Utc>>,
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

impl QueueMessage {
    /// Calculate processing duration if both enqueued_at and dequeued_at are available
    pub fn get_processing_duration(&self) -> Option<chrono::Duration> {
        self.dequeued_at.map(|dequeued| dequeued - self.enqueued_at)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Tabled)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct QueueRecord {
    /// Queue ID (primary key)
    pub id: i64,
    /// Name of the queue
    pub queue_name: String,
    /// Timestamp when the queue was created
    pub created_at: DateTime<Utc>,
}

impl fmt::Display for QueueRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueueRecord {{ id: {}, queue_name: {}, created_at: {} }}",
            self.id, self.queue_name, self.created_at
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Tabled)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct WorkerRecord {
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

impl fmt::Display for WorkerRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.queue_id {
            Some(queue_id) => write!(
                f,
                "WorkerRecord {{ id: {}, hostname: {}, port: {}, queue_id: {}, status: {:?} }}",
                self.id, self.hostname, self.port, queue_id, self.status
            ),
            None => write!(
                f,
                "WorkerRecord {{ id: {}, hostname: {}, port: {}, queue_id: None, status: {:?} }}",
                self.id, self.hostname, self.port, self.status
            ),
        }
    }
}

/// Worker status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "worker_status", rename_all = "snake_case")
)]
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

// Moved from src/tables/pgqrs_queues.rs
/// Input data for creating a new queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewQueueRecord {
    pub queue_name: String,
}

// Moved from src/tables/pgqrs_workers.rs
/// Input data for creating a new worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewWorkerRecord {
    pub hostname: String,
    pub port: i32,
    /// Queue ID (None for Admin workers)
    pub queue_id: Option<i64>,
}

// Moved from src/tables/pgqrs_messages.rs
/// Input data for creating a new message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewQueueMessage {
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
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(
        type_name = "pgqrs_workflow_status",
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
pub enum WorkflowStatus {
    Queued,
    Running,
    Paused,
    Success,
    Error,
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowStatus::Queued => write!(f, "QUEUED"),
            WorkflowStatus::Running => write!(f, "RUNNING"),
            WorkflowStatus::Paused => write!(f, "PAUSED"),
            WorkflowStatus::Success => write!(f, "SUCCESS"),
            WorkflowStatus::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for WorkflowStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "QUEUED" => Ok(WorkflowStatus::Queued),
            "RUNNING" => Ok(WorkflowStatus::Running),
            "PAUSED" => Ok(WorkflowStatus::Paused),
            "SUCCESS" => Ok(WorkflowStatus::Success),
            "ERROR" => Ok(WorkflowStatus::Error),
            _ => Err(format!("Invalid workflow status: {}", s)),
        }
    }
}

// Moved from src/tables/pgqrs_workflows.rs
#[derive(Debug, Clone)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct WorkflowRecord {
    pub id: i64,
    pub name: String,
    pub queue_id: i64,
    pub created_at: DateTime<Utc>,
}

pub struct NewWorkflowRecord {
    pub name: String,
    pub queue_id: i64,
}

/// A workflow execution run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct RunRecord {
    pub id: i64,
    pub workflow_id: i64,
    pub status: WorkflowStatus,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Input data for creating a new workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewRunRecord {
    pub workflow_id: i64,
    pub input: Option<serde_json::Value>,
}

/// A step within a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct StepRecord {
    pub id: i64,
    pub run_id: i64,
    pub step_name: String,
    pub status: WorkflowStatus,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub retry_count: i32,
}

/// Input data for creating a new workflow step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewStepRecord {
    pub run_id: i64,
    pub step_name: String,
    pub input: Option<serde_json::Value>,
}
