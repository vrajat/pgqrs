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