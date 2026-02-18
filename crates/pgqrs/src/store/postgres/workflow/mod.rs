pub mod guard;
pub mod handle;

pub use guard::StepGuard;
pub use handle::Workflow;

use serde::{Deserialize, Serialize};

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
