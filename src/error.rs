use thiserror::Error;

/// Result type for pgqrs operations
pub type Result<T> = std::result::Result<T, PgqrsError>;

/// Error types for pgqrs operations
#[derive(Error, Debug)]
pub enum PgqrsError {
    #[error("Connection pool error: {0}")]
    Pool(#[from] r2d2::Error),

    #[error("Database error: {0}")]
    Database(#[from] diesel::result::Error),

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

    #[error("Migration error: {message}")]
    Migration { message: String },
}
