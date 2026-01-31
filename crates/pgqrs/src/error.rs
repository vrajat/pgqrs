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

/// Boxed error type for heterogeneous error sources
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Represents a transient error that should trigger automatic step retry.
///
/// Use this type to indicate that a workflow step failed due to a temporary issue
/// (network timeout, rate limit, temporary resource unavailability) and should be
/// retried automatically according to the configured retry policy.
///
/// ## What
///
/// `TransientStepError` wraps transient failures with metadata to support automatic retry:
/// - `code`: Error classification (e.g., "TIMEOUT", "RATE_LIMITED")
/// - `message`: Human-readable error description
/// - `source`: Optional original error for debugging
/// - `retry_after`: Optional custom delay before retry
///
/// ## How
///
/// Create using the builder pattern:
///
/// ```rust
/// use pgqrs::error::TransientStepError;
/// use std::time::Duration;
///
/// // Basic transient error
/// let err = TransientStepError::new("TIMEOUT", "Connection timeout");
///
/// // With custom retry delay (e.g., from Retry-After header)
/// let err = TransientStepError::new("RATE_LIMITED", "Too many requests")
///     .with_delay(Duration::from_secs(60));
/// ```
#[derive(Debug, Clone)]
pub struct TransientStepError {
    /// Error code for classification (e.g., "TIMEOUT", "RATE_LIMITED", "CONNECTION_FAILED")
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Original error source (preserved for debugging)
    pub source: Option<String>,
    /// Custom delay before retry (e.g., from Retry-After header)
    pub retry_after: Option<std::time::Duration>,
}

impl TransientStepError {
    /// Create a new transient error with code and message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            source: None,
            retry_after: None,
        }
    }

    /// Attach the source error for debugging.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(source.to_string());
        self
    }

    /// Set a custom retry delay (e.g., from Retry-After header).
    pub fn with_delay(mut self, delay: std::time::Duration) -> Self {
        self.retry_after = Some(delay);
        self
    }
}

impl std::fmt::Display for TransientStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for TransientStepError {}

/// Error types for pgqrs operations.
///
/// This enum covers all error cases that can occur when using pgqrs,
/// including database connectivity, configuration, serialization, and
/// queue-specific operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Database operation failed (SQLx errors)
    #[cfg(feature = "sqlx")]
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Turso database error
    #[cfg(feature = "turso")]
    #[error("Turso error: {0}")]
    Turso(#[from] turso::Error),

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
    ConnectionFailed { source: BoxError, context: String },

    /// SQL query failed
    #[error("Database query failed: {query}. Context: {context}. Source: {source}")]
    QueryFailed {
        source: BoxError,
        query: String,
        context: String,
    },

    /// Database transaction operation failed
    #[error("Database transaction failed: {source}. Context: {context}")]
    TransactionFailed { source: BoxError, context: String },

    /// Database connection pool is exhausted
    #[error("Database connection pool exhausted: {source}. Context: {context}")]
    PoolExhausted { source: BoxError, context: String },

    /// Database migration failed
    #[cfg(feature = "sqlx")]
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

    /// Entity not found
    #[error("{entity} with id '{id}' not found")]
    NotFound { entity: String, id: String },

    /// Transient error that can be retried
    #[error("Transient error ({code}): {message}")]
    Transient {
        code: String,
        message: String,
        retry_after: Option<std::time::Duration>,
    },

    /// Step retries exhausted
    #[error("Step failed after {attempts} attempts: {error}")]
    RetriesExhausted {
        error: serde_json::Value,
        attempts: u32,
    },

    /// Step not ready for execution (retry scheduled)
    ///
    /// # Worker Behavior
    ///
    /// When a worker receives `StepNotReady`, it should:
    /// 1. **Not retry immediately** - the step is scheduled for future retry
    /// 2. Move on to other work (poll other workflows/steps)
    /// 3. Come back after `retry_at` has passed
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use pgqrs::error::Error;
    /// # use pgqrs::store::{StepResult, Store};
    /// # async fn example(store: &impl Store, workflow_id: i64, step_id: &str) -> pgqrs::error::Result<()> {
    /// let now = chrono::Utc::now();
    /// match store.acquire_step(workflow_id, step_id, now).await {
    ///     Err(Error::StepNotReady { retry_at, .. }) => {
    ///         // Don't sleep! Do other work and poll again after retry_at
    ///         tracing::info!("Step scheduled for retry at {}", retry_at);
    ///         return Ok(()); // Move to next workflow
    ///     }
    ///     Ok(StepResult::Execute(guard)) => {
    ///         // Execute the step
    ///     }
    ///     Ok(StepResult::Skipped(output)) => {
    ///         // Step already completed
    ///     }
    ///     Err(e) => return Err(e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[error("Step not ready for execution (retry scheduled for {retry_at})")]
    StepNotReady {
        retry_at: chrono::DateTime<chrono::Utc>,
        retry_count: u32,
    },
}
