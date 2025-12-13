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
//! ### Producer
//! ```rust
//! use pgqrs::Producer;
//! use serde_json::Value;
//!
//! /// Enqueue a payload to the queue
//! async fn enqueue_job(producer: &Producer, payload: Value) -> Result<i64, Box<dyn std::error::Error>> {
//!     let message = producer.enqueue(&payload).await?;
//!     Ok(message.id)
//! }
//! ```
//!
//! ### Consumer
//! ```rust
//! use pgqrs::Consumer;
//! use std::time::Duration;
//!
//! /// Poll for jobs from the queue and print them as they arrive
//! async fn poll_and_print_jobs(consumer: &Consumer) -> Result<(), Box<dyn std::error::Error>> {
//!     loop {
//!         let messages = consumer.dequeue().await?;
//!         if messages.is_empty() {
//!             // No job found, wait before polling again
//!             tokio::time::sleep(Duration::from_secs(2)).await;
//!         } else {
//!             for message in messages {
//!                 println!("Dequeued job: {}", message.payload);
//!                 // Optionally archive or delete the message after processing
//!                 consumer.archive(message.id).await?;
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! For more details and advanced usage, see the [README](https://github.com/vrajat/pgqrs/blob/main/README.md) and [examples](https://github.com/vrajat/pgqrs/tree/main/examples).

pub mod config;
pub mod error;
mod rate_limit;
pub mod tables;
pub mod types;
mod validation;
pub mod worker;

// Re-export worker types at crate root for convenience
pub use crate::worker::admin;
pub use crate::worker::consumer;
pub use crate::worker::producer;
pub use crate::worker::{Admin, Consumer, Producer, Worker, WorkerHandle};

pub use crate::config::Config;
pub use crate::error::{Error, Result};
pub use crate::rate_limit::RateLimitStatus;
pub use crate::tables::{Archive, NewWorker, Table, Workers};
pub use crate::types::{WorkerInfo, WorkerStats, WorkerStatus};
pub use crate::validation::ValidationConfig;
