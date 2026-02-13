use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use tabled::Tabled;

/// Queue metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
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

/// System-wide statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
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
