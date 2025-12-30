//! Table operations for pgqrs unified architecture.
//!
//! This module contains CRUD operations for each artifact/table in the pgqrs system.
//! Each table module provides focused operations on a specific table without heavy business logic.

pub mod pgqrs_archive;
pub mod pgqrs_messages;
pub mod pgqrs_queues;
pub mod pgqrs_workers;
pub mod pgqrs_workflows;

pub use pgqrs_archive::Archive;
pub use pgqrs_messages::Messages;
pub use pgqrs_queues::Queues;
pub use pgqrs_workers::Workers;
pub use pgqrs_workflows::{WorkflowRecord, Workflows}; // WorkflowRecord might be in types too?
                                                      // workflow::WorkflowRecord is in types. pgqrs_workflows might re-export it.
                                                      // Checked pgqrs_workflows.rs: "pub use crate::types::{NewWorkflow, WorkflowRecord};"
                                                      // So it IS re-exported there.
                                                      // Keep WorkflowRecord for now if used via tables.
