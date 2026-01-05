//! # pgqrs
//!
//! **pgqrs** is a PostgreSQL-backed durable workflow engine and job queue. Written in Rust with Python bindings.
//!
//! ## Features
//!
//! ### Core
//! - **Library-only**: No servers to operate. Use directly in your Rust or Python applications.
//! - **Connection Pooler Compatible**: Works with [pgBouncer](https://www.pgbouncer.org) and [pgcat](https://github.com/postgresml/pgcat) for connection scaling.
//!
//! ### Job Queue
//! - **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
//! - **Exactly-once Delivery**: Guarantees within visibility timeout window.
//! - **Message Archiving**: Built-in audit trails and historical data retention.
//!
//! ### Durable Workflows
//! - **Crash Recovery**: Resume from the last completed step after failures.
//! - **Exactly-once Steps**: Completed steps are never re-executed.
//! - **Persistent State**: All workflow progress stored in PostgreSQL.
//!
//! ## Quick Start
//!
//! ### Job Queue
//!
//! Simple, reliable message queue for background processing:
//!
//! ```rust
//! use pgqrs;
//! use serde_json::json;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to PostgreSQL
//! let store = pgqrs::connect("postgresql://localhost/mydb").await?;
//!
//! // Setup (run once)
//! pgqrs::admin(&store).install().await?;
//! pgqrs::admin(&store).create_queue("tasks").await?;
//!
//! // Producer: enqueue a job
//! let ids = pgqrs::enqueue()
//!     .message(&json!({"task": "send_email", "to": "user@example.com"}))
//!     .to("tasks")
//!     .execute(&store)
//!     .await?;
//!
//! // Consumer: process jobs
//! pgqrs::dequeue()
//!     .from("tasks")
//!     .handle(|msg| async move {
//!         println!("Processing: {:?}", msg.payload);
//!         Ok(())
//!     })
//!     .execute(&store)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Durable Workflows
//!
//! Orchestrate multi-step processes that survive crashes:
//!
//! ```rust
//! use pgqrs;
//! use pgqrs_macros::{pgqrs_workflow, pgqrs_step};
//!
//! #[pgqrs_step]
//! async fn fetch_data(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
//!     // Fetch data - only executes once per workflow run
//!     Ok("data".to_string())
//! }
//!
//! #[pgqrs_step]
//! async fn process_data(ctx: &pgqrs::Workflow, data: String) -> Result<i32, anyhow::Error> {
//!     Ok(data.len() as i32)
//! }
//!
//! #[pgqrs_workflow]
//! async fn data_pipeline(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
//!     let data = fetch_data(ctx, url).await?;
//!     let count = process_data(ctx, data).await?;
//!     Ok(format!("Processed {} bytes", count))
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = pgqrs::connect("postgresql://localhost/mydb").await?;
//! pgqrs::admin(&store).install().await?;
//!
//! let url = "https://example.com/data";
//! let workflow = pgqrs::admin(&store)
//!     .create_workflow("data_pipeline", &url)
//!     .await?;
//!
//! let result = data_pipeline(&workflow, url).await?;
//! println!("Result: {}", result);
//! # Ok(())
//! # }
//! ```
//!
//! For more details, see the [documentation](https://vrajat.github.io/pgqrs/) and [examples](https://github.com/vrajat/pgqrs/tree/main/crates/pgqrs/examples).

pub mod config;
pub mod error;
mod rate_limit;
pub mod store;
pub mod types;
mod validation;

// Tier 1: High-level API (builders module)
pub mod builders;

// Re-export Tier 1 high-level functions at crate root
pub use builders::{
    admin, connect, connect_with_config, consumer, dequeue, enqueue, producer, tables,
    worker_handle,
};

// Re-export worker types and modules at crate root for convenience

pub use crate::store::{
    Admin, ArchiveTable, Consumer, MessageTable, Producer, QueueTable, StepGuard, StepGuardExt,
    StepResult, Store, Worker, WorkerTable, Workflow, WorkflowExt, WorkflowTable,
};

pub use crate::config::Config;
pub use crate::error::{Error, Result};
pub use crate::rate_limit::RateLimitStatus;

pub use crate::types::{
    ArchivedMessage, NewArchivedMessage, NewMessage, NewQueue, NewWorker, NewWorkflow, QueueInfo,
    QueueMessage, QueueMetrics, SystemStats, WorkerHealthStats, WorkerInfo, WorkerStats,
    WorkerStatus, WorkflowRecord, WorkflowStatus,
};

pub use crate::validation::ValidationConfig;
pub use pgqrs_macros::{pgqrs_step, pgqrs_workflow};

pub use builders::workflow::{step, workflow};
