use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Status of a workflow step execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "step_status", rename_all = "SCREAMING_SNAKE_CASE")
)]
pub enum StepStatus {
    /// Step is waiting to be executed
    Pending,
    /// Step is currently executing
    Running,
    /// Step execution has been paused
    Paused,
    /// Step completed successfully
    Success,
    /// Step failed with an error
    Error,
}

impl fmt::Display for StepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StepStatus::Pending => write!(f, "PENDING"),
            StepStatus::Running => write!(f, "RUNNING"),
            StepStatus::Paused => write!(f, "PAUSED"),
            StepStatus::Success => write!(f, "SUCCESS"),
            StepStatus::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for StepStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(StepStatus::Pending),
            "RUNNING" => Ok(StepStatus::Running),
            "PAUSED" => Ok(StepStatus::Paused),
            "SUCCESS" => Ok(StepStatus::Success),
            "ERROR" => Ok(StepStatus::Error),
            _ => Err(format!("Invalid step status: {}", s)),
        }
    }
}

/// Represents the state of a single step within a workflow run.
///
/// Used for tracking progress and crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct WorkflowStep {
    /// ID of the workflow run this step belongs to
    pub run_id: i64,
    /// Unique identifier for the step within the workflow
    pub step_id: String,
    /// Current status of the step
    pub status: StepStatus,
    /// Input parameters for the step
    pub input: Option<serde_json::Value>,
    /// Output result of the step (if successful)
    pub output: Option<serde_json::Value>,
    /// Error details (if failed)
    pub error: Option<serde_json::Value>,
    /// Timestamp when the step started execution
    pub started_at: Option<DateTime<Utc>>,
    /// Timestamp when the step completed execution
    pub completed_at: Option<DateTime<Utc>>,
}
