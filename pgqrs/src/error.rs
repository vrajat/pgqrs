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
