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
//!     .name(archive_files)
//!     .create(&store)
//!     .await?;
//!
//! let consumer = pgqrs::consumer("worker-1", 8080, archive_files.name())
//!     .create(&store)
//!     .await?;
//!
//! let handler = pgqrs::workflow_handler(store.clone(), move |run, input| async move {
//!     app_workflow(run, input).await
//! });
//! let handler = { let handler = handler.clone(); move |msg| (handler)(msg) };
//!
//! pgqrs::workflow()
//!     .name(archive_files)
//!     .trigger(&json!({"path": "/tmp/report.csv"}))?
//!     .execute(&store)
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
/// Retry policies and workflow backoff strategies.
pub mod policy;
mod rate_limit;
/// Metrics and system statistics.
pub mod stats;
pub mod store;
pub mod tables;
pub mod types;
pub mod validation;
pub mod workers;
pub mod workflow;

/// High-level builder APIs for queues and workflows.
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

pub use crate::store::Store;
pub use crate::tables::{
    MessageTable, QueueTable, RunRecordTable, StepRecordTable, WorkerTable, WorkflowTable,
};
pub use crate::workers::{Admin, Consumer, Producer, Run, Step, Worker};
#[cfg(any(test, feature = "test-utils"))]
pub use crate::workflow::workflow_handler_with_time;
pub use crate::workflow::{
    pause_error, workflow_handler, workflow_step, WorkflowDef, WorkflowFuture,
};

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

/// Create a managed admin builder.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// pgqrs::admin(&store).install().await?;
/// # Ok(()) }
/// ```
pub fn admin<S: Store>(store: &S) -> builders::admin::AdminBuilder<'_, S> {
    builders::admin::AdminBuilder::new(store)
}

/// Create a managed producer worker.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let producer = pgqrs::producer("localhost", 3000, "orders").create(&store).await?;
/// # Ok(()) }
/// ```
pub fn producer<'a>(
    hostname: &'a str,
    port: i32,
    queue: &'a str,
) -> builders::producer::ProducerBuilder<'a> {
    builders::producer::ProducerBuilder::new(hostname, port, queue)
}

/// Create a managed consumer worker.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let consumer = pgqrs::consumer("localhost", 3000, "orders").create(&store).await?;
/// # Ok(()) }
/// ```
pub fn consumer<'a>(
    hostname: &'a str,
    port: i32,
    queue: &'a str,
) -> builders::consumer::ConsumerBuilder<'a> {
    builders::consumer::ConsumerBuilder::new(hostname, port, queue)
}

/// Start an enqueue operation.
///
/// ```rust,no_run
/// # use pgqrs;
/// # use serde_json::json;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let ids = pgqrs::enqueue()
///     .message(&json!({"task": "send_email"}))
///     .to("tasks")
///     .execute(&store)
///     .await?;
/// # Ok(()) }
/// ```
pub fn enqueue() -> builders::enqueue::EnqueueBuilder<'static, ()> {
    builders::enqueue::EnqueueBuilder::new()
}

/// Start a dequeue operation.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let messages = pgqrs::dequeue()
///     .from("tasks")
///     .batch(5)
///     .fetch_all(&store)
///     .await?;
/// # Ok(()) }
/// ```
pub fn dequeue() -> builders::dequeue::DequeueBuilder<'static> {
    builders::dequeue::DequeueBuilder::new()
}

/// Start a tables builder.
///
/// ```rust,no_run
/// # use pgqrs::store::AnyStore;
/// # async fn example(store: AnyStore) -> pgqrs::error::Result<()> {
/// let workers = pgqrs::tables(&store).workers().list().await?;
/// # Ok(()) }
/// ```
pub fn tables<S: Store>(store: &S) -> builders::tables::TablesBuilder<'_, S> {
    builders::tables::TablesBuilder::new(store)
}

/// Entry point for creating or getting a workflow.
///
/// ```rust,no_run
/// # use pgqrs;
/// # use serde_json::json;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// pgqrs::workflow()
///     .name(archive_files)
///     .create(&store)
///     .await?;
/// let message = pgqrs::workflow()
///     .name(archive_files)
///     .trigger(&json!({"path": "/tmp/report.csv"}))?
///     .execute(&store)
///     .await?;
/// # Ok(()) }
/// ```
pub fn workflow<TInput, TOutput>() -> builders::workflow::WorkflowBuilder<TInput, TOutput>
where
    TInput: serde::de::DeserializeOwned + Send + 'static,
    TOutput: serde::Serialize + Send + 'static,
{
    builders::workflow::WorkflowBuilder::new()
}

/// Entry point for creating a run handle from a message.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// # #[pgqrs::pgqrs_workflow(name = "archive_files")]
/// # async fn archive_files(_run: &pgqrs::Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> { Ok(input) }
/// let message = pgqrs::workflow()
///     .name(archive_files)
///     .trigger(&serde_json::json!({"path": "/tmp/report.csv"}))?
///     .execute(&store)
///     .await?;
/// let run = pgqrs::run()
///     .message(message)
///     .store(&store)
///     .execute()
///     .await?;
/// # Ok(()) }
/// ```
pub fn run() -> builders::run::RunBuilder<'static, crate::store::AnyStore> {
    builders::run::RunBuilder::new()
}

/// Entry point for acquiring a step.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// # #[pgqrs::pgqrs_workflow(name = "archive_files")]
/// # async fn archive_files(_run: &pgqrs::Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> { Ok(input) }
/// let message = pgqrs::workflow()
///     .name(archive_files)
///     .trigger(&serde_json::json!({"path": "/tmp/report.csv"}))?
///     .execute(&store)
///     .await?;
/// let run = pgqrs::run()
///     .message(message)
///     .store(&store)
///     .execute()
///     .await?;
/// let step = pgqrs::step()
///     .run(&run)
///     .name("list_files")
///     .execute()
///     .await?;
/// # Ok(()) }
/// ```
pub fn step() -> builders::step::StepBuilder<'static> {
    builders::step::StepBuilder::new()
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
