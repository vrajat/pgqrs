//! Table operations for pgqrs unified architecture.
//!
//! This module contains CRUD operations for each artifact/table in the pgqrs system.
//! Each table module provides focused operations on a specific table without heavy business logic.

pub mod pgqrs_archive;
pub mod pgqrs_messages;
pub mod pgqrs_queues;
pub mod pgqrs_workers;
pub mod pgqrs_workflow_runs;
pub mod pgqrs_workflow_steps;
pub mod pgqrs_workflows;

pub use pgqrs_archive::Archive;
pub use pgqrs_messages::Messages;
pub use pgqrs_queues::Queues;
pub use pgqrs_workers::Workers;
pub use pgqrs_workflow_runs::WorkflowRuns;
pub use pgqrs_workflow_steps::WorkflowSteps;
pub use pgqrs_workflows::Workflows;
