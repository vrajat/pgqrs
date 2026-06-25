//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait for the Postgres-backed runtime.

/// Concurrency model supported by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyModel {
    /// Backend supports multiple processes accessing the store concurrently.
    MultiProcess,
    /// Backend supports only a single process accessing the store.
    SingleProcess,
}

pub mod postgres;

pub use crate::workers::*;
pub use postgres::tables::{
    db_state::DbState, pgqrs_messages::Messages, pgqrs_queues::Queues, pgqrs_workers::Workers,
    pgqrs_workflow_runs::RunRecords, pgqrs_workflow_steps::StepRecords, pgqrs_workflows::Workflows,
};
pub use postgres::Store;
