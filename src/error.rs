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

/// Result type for pgqrs operations
pub type Result<T> = std::result::Result<T, PgqrsError>;

/// Error types for pgqrs operations
#[derive(Error, Debug)]
pub enum PgqrsError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Queue '{name}' not found")]
    QueueNotFound { name: String },

    #[error("Queue '{name}' already exists")]
    QueueAlreadyExists { name: String },

    #[error("Message with id '{id}' not found")]
    MessageNotFound { id: uuid::Uuid },

    #[error("Invalid message format: {message}")]
    InvalidMessage { message: String },

    #[error("Schema validation failed: {message}")]
    SchemaValidation { message: String },

    #[error("Operation timeout: {operation}")]
    Timeout { operation: String },

    #[error("Connection error: {message}")]
    Connection { message: String },

    #[error("Internal error: {message}")]
    Internal { message: String },
}
