use thiserror::Error;

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
}
