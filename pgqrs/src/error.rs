//! Error types and result handling for pgqrs.
//!
//! This module defines the core error type [`PgqrsError`] used throughout the crate, as well as the [`Result`] alias for fallible operations.
//!
//! ## What
//!
//! - [`PgqrsError`] enumerates all error cases that can occur in pgqrs, including database, pool, serialization, configuration, and queue-specific errors.
//! - [`Result<T>`] is a convenient alias for `Result<T, PgqrsError>`.
//!
//! ## How
//!
//! Use [`PgqrsError`] for error handling in your application code and when matching on error cases. Most crate APIs return [`Result<T>`].
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::error::{PgqrsError, Result};
//!
//! fn do_something() -> Result<()> {
//!     // ...
//!     Err(PgqrsError::QueueNotFound { name: "jobs".to_string() })
//! }
//! ```
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgqrsClientError {
    #[error("Connection error: {0}")]
    Connection(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Timeout error")]
    Timeout,

    #[error("Backoff exhausted after retries")]
    BackoffExhausted,
}

pub type Result<T> = std::result::Result<T, PgqrsClientError>;

// CLI-specific errors
#[derive(Error, Debug)]
pub enum CliError {
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Client error: {0}")]
    Client(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("File error: {0}")]
    File(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("pgqrs client error: {0}")]
    PgqrsClient(#[from] PgqrsClientError),
}
