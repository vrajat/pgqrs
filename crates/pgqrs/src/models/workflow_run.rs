use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Status of a workflow run execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "run_status", rename_all = "SCREAMING_SNAKE_CASE")
)]
pub enum RunStatus {
    /// Workflow run is queued and waiting to start
    Pending,
    /// Workflow run is currently executing
    Running,
    /// Workflow run has been paused
    Paused,
    /// Workflow run completed successfully
    Success,
    /// Workflow run failed with an error
    Error,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunStatus::Pending => write!(f, "PENDING"),
            RunStatus::Running => write!(f, "RUNNING"),
            RunStatus::Paused => write!(f, "PAUSED"),
            RunStatus::Success => write!(f, "SUCCESS"),
            RunStatus::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for RunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(RunStatus::Pending),
            "RUNNING" => Ok(RunStatus::Running),
            "PAUSED" => Ok(RunStatus::Paused),
            "SUCCESS" => Ok(RunStatus::Success),
            "ERROR" => Ok(RunStatus::Error),
            _ => Err(format!("Invalid run status: {}", s)),
        }
    }
}

/// Represents an execution instance of a workflow.
///
/// Tracks the lifecycle, state, and results of a single workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct WorkflowRun {
    /// Unique identifier for the run
    pub run_id: i64,
    /// ID of the workflow definition being executed
    pub workflow_id: i64,
    /// Current status of the run
    pub status: RunStatus,
    /// Input parameters for the run
    pub input: Option<serde_json::Value>,
    /// Output result of the run (if successful)
    pub output: Option<serde_json::Value>,
    /// Error details (if failed)
    pub error: Option<serde_json::Value>,
    /// Timestamp when the run was created
    pub created_at: DateTime<Utc>,
    /// Timestamp when execution started
    pub started_at: Option<DateTime<Utc>>,
    /// Timestamp when execution was paused
    pub paused_at: Option<DateTime<Utc>>,
    /// Timestamp when execution completed (success or error)
    pub completed_at: Option<DateTime<Utc>>,
    /// ID of the worker currently processing this run
    pub worker_id: Option<i64>,
}
