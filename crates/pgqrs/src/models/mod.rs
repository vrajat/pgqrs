//! Database models for the workflow system.
//!
//! This module contains the struct definitions for the database tables:
//! - `Workflow`: Workflow definitions (`pgqrs_workflows`)
//! - `WorkflowRun`: Workflow execution instances (`pgqrs_workflow_runs`)
//! - `WorkflowStep`: Step execution state (`pgqrs_workflow_steps`)

pub mod workflow;
pub mod workflow_run;
pub mod workflow_step;

pub use workflow::Workflow;
pub use workflow_run::{RunStatus, WorkflowRun};
pub use workflow_step::{StepStatus, WorkflowStep};
