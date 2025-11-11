//! # pgqrs
//!
//! **pgqrs** is a PostgreSQL-backed job queue for Rust applications, providing both a type-safe async library API and a CLI for queue administration.
//!
//! ## What is pgqrs?
//!
//! pgqrs enables you to reliably enqueue, dequeue, and manage jobs using PostgreSQL as the backend. It leverages advanced database features like `SKIP LOCKED` for efficient concurrent job processing, and provides exactly-once delivery semantics within a visibility timeout window.
//!
//! ## How to use pgqrs
//!
//! - **Library API**: Integrate with your Rust application to enqueue and process jobs. See [`Queue`] and [`QueueMessage`](crate::types::QueueMessage).
//! - **CLI Tools**: Use the CLI to administer, debug, and inspect queues. See the README for CLI usage.
//!
//! ## Features
//!
//! - **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching
//! - **Type-Safe**: Rust types for message payloads
//! - **Visibility Timeout**: Exactly-once delivery within a lock period
//! - **CLI Tools**: Administer and debug queues from the command line
//!
//! ## Example
//!
//! ```no_run
//! use pgqrs::admin::PgqrsAdmin;
//! use pgqrs::config::Config;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::from_dsn("postgresql://user:pass@localhost/db");
//!     let admin = PgqrsAdmin::new(&config).await?;
//!     admin.install().await?;
//!     admin.create_queue("jobs").await?;
//!     Ok(())
//! }
//! ```
//!
//! For more details and advanced usage, see the [README](https://github.com/vrajat/pgqrs/blob/main/README.md) and [examples](https://github.com/vrajat/pgqrs/tree/main/examples).

pub mod admin;
pub mod archive;
pub mod config;
pub mod error;
pub mod queue;
mod rate_limit;
pub mod types;
mod validation;

mod constants;

pub use crate::admin::PgqrsAdmin;
pub use crate::error::{PgqrsError, Result};
pub use crate::queue::Queue;
pub use crate::types::{WorkerInfo, WorkerStats, WorkerStatus};
