//! pgqrs is a postgres-native, library-only durable execution engine.
//!
//! Built in Rust with Python bindings, pgqrs runs in-process and persists workflow state in your
//! database. It supports PostgreSQL for production and SQLite/Turso for embedded or test setups.
//!
//! ## Key Properties
//! - Postgres-native execution using SKIP LOCKED and ACID transactions
//! - Library-only runtime that runs alongside your application
//! - Multi-backend support: Postgres, SQLite, and Turso
//! - Exactly-once step execution with durable workflow state
//!
//! ## Quick Start (Queue)
//! ```rust,no_run
//! use pgqrs;
//! use pgqrs::store::Store;
//! use serde_json::json;

//!
//! async fn app_workflow(
//!     run: pgqrs::Run,
//!     input: serde_json::Value,
//! ) -> Result<serde_json::Value, pgqrs::Error> {
//!     let files = pgqrs::workflow_step(&run, "list_files", || async {
//!         Ok::<_, pgqrs::Error>(vec![input["path"].as_str().unwrap().to_string()])
//!     })
//!     .await?;
//!
//!     let archive = pgqrs::workflow_step(&run, "create_archive", || async {
//!         Ok::<_, pgqrs::Error>(format!("{}.zip", files[0]))
//!     })
//!     .await?;
//!
//!     Ok(json!({"archive": archive}))
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = pgqrs::connect("postgresql://localhost/mydb").await?;
//! pgqrs::admin(&store).install().await?;
//!
//! pgqrs::workflow()
//!     .name("archive_files")
//!     .store(&store)
//!     .create()
//!     .await?;
//!
//! let consumer = pgqrs::consumer("worker-1", 8080, "archive_files")
//!     .create(&store)
//!     .await?;
//!
//! let handler = pgqrs::workflow_handler(store.clone(), move |run, input| async move {
//!     app_workflow(run, input).await
//! });
//! let handler = { let handler = handler.clone(); move |msg| (handler)(msg) };
//!
//! pgqrs::workflow()
//!     .name("archive_files")
//!     .store(&store)
//!     .trigger(&json!({"path": "/tmp/report.csv"}))?
//!     .execute()
//!     .await?;
//!
//! pgqrs::dequeue()
//!     .worker(&consumer)
//!     .handle(handler)
//!     .execute(&store)
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Learn more in the documentation: <https://pgqrs.vrajat.com>

pub mod config;
pub mod error;
pub mod policy;
mod rate_limit;
pub mod stats;
pub mod store;
pub mod tables;
pub mod types;
pub mod validation;
pub mod workers;
pub mod workflow;

pub mod builders {
    pub mod admin;
    pub mod consumer;
    pub mod dequeue;
    pub mod enqueue;
    pub mod producer;
    pub mod run;
    pub mod step;
    pub mod tables;
    pub mod workflow;
}

pub use crate::builders::admin::admin;
pub use crate::builders::consumer::consumer;
pub use crate::builders::dequeue::dequeue;
pub use crate::builders::enqueue::enqueue;
pub use crate::builders::producer::producer;
pub use crate::builders::tables::tables;

pub use crate::store::Store;
pub use crate::tables::{
    MessageTable, QueueTable, RunRecordTable, StepRecordTable, WorkerTable, WorkflowTable,
};
pub use crate::workers::{Admin, Consumer, Producer, Run, Step, Worker};
#[cfg(any(test, feature = "test-utils"))]
pub use crate::workflow::workflow_handler_with_time;
pub use crate::workflow::{pause_error, workflow_handler, workflow_step};

pub use crate::config::Config;
pub use crate::error::{Error, Result, TransientStepError};
pub use crate::policy::{BackoffStrategy, StepRetryPolicy, WorkflowConfig};
pub use crate::rate_limit::RateLimitStatus;
pub use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats, WorkerStats};

pub use crate::types::{
    NewQueueMessage, NewQueueRecord, NewRunRecord, NewStepRecord, NewWorkerRecord,
    NewWorkflowRecord, QueueMessage, QueueRecord, RunRecord, StepRecord, WorkerRecord,
    WorkerStatus, WorkflowRecord, WorkflowStatus,
};

pub use crate::validation::ValidationConfig;
pub use pgqrs_macros::{pgqrs_step, pgqrs_workflow};

pub use crate::builders::run::RunBuilder;
pub use crate::builders::step::StepBuilder;
pub use crate::builders::workflow::WorkflowBuilder;

/// Entry point for creating or getting a workflow.
pub fn workflow() -> WorkflowBuilder<'static, crate::store::AnyStore> {
    WorkflowBuilder::new()
}

/// Entry point for creating a run handle from a message.
pub fn run() -> RunBuilder<'static, crate::store::AnyStore> {
    RunBuilder::new()
}

/// Entry point for acquiring a step.
pub fn step() -> StepBuilder<'static> {
    StepBuilder::new()
}

/// Connect to a database using a DSN string.
pub async fn connect(dsn: &str) -> crate::error::Result<crate::store::AnyStore> {
    crate::store::any::AnyStore::connect_with_dsn(dsn).await
}

/// Connect to a database using a custom configuration.
pub async fn connect_with_config(
    config: &crate::config::Config,
) -> crate::error::Result<crate::store::AnyStore> {
    crate::store::any::AnyStore::connect(config).await
}
