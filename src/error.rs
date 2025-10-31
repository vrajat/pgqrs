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

/// Error types for pgqrs operations.
///
/// This enum covers all error cases that can occur when using pgqrs,
/// including database connectivity, configuration, serialization, and
/// queue-specific operations.
#[derive(Error, Debug)]
pub enum PgqrsError {
    /// Database operation failed (SQLx errors)
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// JSON serialization/deserialization failed
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Configuration loading or parsing failed
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    /// Required configuration field is missing
    #[error("Missing required configuration: {field}")]
    MissingConfig { field: String },

    /// Configuration field has an invalid value
    #[error("Invalid configuration value for {field}: {message}")]
    InvalidConfig { field: String, message: String },

    /// Attempted to access a queue that doesn't exist
    #[error("Queue '{name}' not found")]
    QueueNotFound { name: String },

    /// Attempted to create a queue that already exists
    #[error("Queue '{name}' already exists")]
    QueueAlreadyExists { name: String },

    /// Attempted to access a message that doesn't exist
    #[error("Message with id '{id}' not found")]
    MessageNotFound { id: uuid::Uuid },

    /// Message payload has invalid format or structure
    #[error("Invalid message format: {message}")]
    InvalidMessage { message: String },

    /// Database schema validation failed
    #[error("Schema validation failed: {message}")]
    SchemaValidation { message: String },

    /// Operation exceeded timeout limit
    #[error("Operation timeout: {operation}")]
    Timeout { operation: String },

    /// Database connection failed or was lost
    #[error("Connection error: {message}")]
    Connection { message: String },

    /// Unexpected internal error occurred
    #[error("Internal error: {message}")]
    Internal { message: String },
}
