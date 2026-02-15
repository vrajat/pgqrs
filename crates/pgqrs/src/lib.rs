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
    ArchiveTable, MessageTable, QueueTable, RunRecordTable, StepRecordTable, WorkerTable,
    WorkflowTable,
};
pub use crate::workers::{Admin, Consumer, Producer, Run, RunExt, StepGuard, StepGuardExt, Worker};
pub use crate::workflow::{pause_error, workflow_handler, workflow_step};

pub use crate::config::Config;
pub use crate::error::{Error, Result, TransientStepError};
pub use crate::policy::{BackoffStrategy, StepRetryPolicy, WorkflowConfig};
pub use crate::rate_limit::RateLimitStatus;
pub use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats, WorkerStats};

pub use crate::types::{
    ArchivedMessage, NewArchivedMessage, NewQueueMessage, NewQueueRecord, NewRunRecord,
    NewStepRecord, NewWorkerRecord, NewWorkflowRecord, QueueMessage, QueueRecord, RunRecord,
    StepRecord, WorkerRecord, WorkerStatus, WorkflowRecord, WorkflowStatus,
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
