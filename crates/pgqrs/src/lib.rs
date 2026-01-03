//! # pgqrs
//!
//! `pgqrs` is a library-only PostgreSQL-backed job queue for Rust applications.
//!
//! ## Features
//! - **Lightweight**: No servers to operate. Directly use `pgqrs` as a library in your Rust applications.
//! - **Compatible with Connection Poolers**: Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
//! - **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
//! - **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by time limit.
//! - **Message Archiving**: Built-in archiving system for audit trails and historical data retention.
//!
//! ## Example
//!
//!
//! ### Producer
//! ```rust
//! # use pgqrs::{enqueue, Config, Store};
//! # use pgqrs::store::AnyStore;
//! # use serde_json::json;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let config = Config::from_dsn("postgres://localhost/mydb");
//! # let store = AnyStore::connect(&config).await?;
//! // Enqueue a single message to a queue
//! let ids = pgqrs::enqueue()
//!     .message(&json!({"foo": "bar"}))
//!     .to("my_queue")
//!     .execute(&store)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Consumer
//! ```rust
//! # use pgqrs::{dequeue, Config, Store};
//! # use pgqrs::store::AnyStore;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let config = Config::from_dsn("postgres://localhost/mydb");
//! # let store = AnyStore::connect(&config).await?;
//! // Dequeue and handle a message with automatic lifecycle management
//! pgqrs::dequeue()
//!     .from("my_queue")
//!     .handle(|msg| async move {
//!         println!("Received: {:?}", msg);
//!         Ok(())
//!     })
//!     .execute(&store)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! For more details and advanced usage, see the [README](https://github.com/vrajat/pgqrs/blob/main/README.md) and [examples](https://github.com/vrajat/pgqrs/tree/main/examples).

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
