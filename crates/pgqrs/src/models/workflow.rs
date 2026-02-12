use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a workflow definition in the system.
///
/// A workflow is a template for execution, associated with a specific queue.
/// It defines the structure and configuration for workflow runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct Workflow {
    /// Unique identifier for the workflow
    pub workflow_id: i64,
    /// Unique name of the workflow
    pub name: String,
    /// ID of the queue associated with this workflow
    pub queue_id: i64,
    /// Timestamp when the workflow was created
    pub created_at: DateTime<Utc>,
}
