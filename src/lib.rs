/**
 # pgqrs

A PostgreSQL-backed job queue for Rust applications, with a CLI for administration and a type-safe async library API.

## Features

- **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching
- **Type-Safe**: Rust types for message payloads
- **Visibility Timeout**: Exactly-once delivery within a lock period
- **CLI Tools**: Administer and debug queues from the command line
*/


pub mod admin;
pub mod config;
pub mod error;
pub mod queue;
pub mod types;

mod constants;
mod schema;

pub use crate::admin::PgqrsAdmin;
pub use crate::error::{PgqrsError, Result};
pub use crate::queue::Queue;