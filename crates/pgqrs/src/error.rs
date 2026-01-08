//! Error types and result handling for pgqrs.
//!
//! This module defines the core error type [`Error`] used throughout the crate, as well as the [`Result`] alias for fallible operations.
//!
//! ## What
//!
//! - [`Error`] enumerates all error cases that can occur in pgqrs, including database, pool, serialization, configuration, and queue-specific errors.
//! - [`Result<T>`] is a convenient alias for `Result<T, Error>`.
//!
//! ## How
//!
//! Use [`Error`] for error handling in your application code and when matching on error cases. Most crate APIs return [`Result<T>`].
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::error::{Error, Result};
//!
//! fn do_something() -> Result<()> {
//!     // ...
//!     Err(Error::QueueNotFound { name: "jobs".to_string() })
//! }
//! ```
use thiserror::Error;

/// Result type for pgqrs operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for pgqrs operations.
///
/// This enum covers all error cases that can occur when using pgqrs,
/// including database connectivity, configuration, serialization, and
/// queue-specific operations.
#[derive(Error, Debug)]
pub enum Error {
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
    #[error("Database connection failed: {source}. Context: {context}")]
    ConnectionFailed {
        #[source]
        source: sqlx::Error,
        context: String,
    },

    /// SQL query failed
    #[error("Database query failed: {query}. Context: {context}. Source: {source}")]
    QueryFailed {
        #[source]
        source: sqlx::Error,
        query: String,
        context: String,
    },

    /// Database transaction operation failed
    #[error("Database transaction failed: {source}. Context: {context}")]
    TransactionFailed {
        #[source]
        source: sqlx::Error,
        context: String,
    },

    /// Database connection pool is exhausted
    #[error("Database connection pool exhausted: {source}. Context: {context}")]
    PoolExhausted {
        #[source]
        source: sqlx::Error,
        context: String,
    },

    /// Database migration failed
    #[error("Database migration failed: {0}")]
    MigrationFailed(#[from] sqlx::migrate::MigrateError),

    /// Unexpected internal error occurred
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Payload validation failed
    #[error("Validation failed: {reason}")]
    ValidationFailed { reason: String },

    /// Rate limit exceeded
    #[error("Rate limit exceeded, retry after {retry_after:?}")]
    RateLimited { retry_after: std::time::Duration },

    /// Payload size exceeded limit
    #[error("Payload size {actual_bytes} exceeds limit {max_bytes}")]
    PayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },

    /// Invalid worker state transition
    #[error("Invalid state transition from {from} to {to}: {reason}")]
    InvalidStateTransition {
        from: String,
        to: String,
        reason: String,
    },

    /// Worker has pending messages that prevent state transition
    #[error("Worker has {count} pending messages: {reason}")]
    WorkerHasPendingMessages { count: u64, reason: String },

    /// Invalid worker type for operation
    #[error("Invalid worker type: {message}")]
    InvalidWorkerType { message: String },

    /// Worker not found
    #[error("Worker with id '{id}' not found")]
    WorkerNotFound { id: i64 },

    /// Worker not registered - methods requiring registration called before register()
    #[error("Worker not registered: {message}")]
    WorkerNotRegistered { message: String },

    /// DEPRECATED: Database connection failed or was lost. Use ConnectionFailed, QueryFailed etc. instead.
    #[deprecated(
        since = "0.6.0",
        note = "Use ConnectionFailed, QueryFailed, TransactionFailed, or PoolExhausted instead"
    )]
    #[error("Connection error: {message}")]
    Connection { message: String },
}
