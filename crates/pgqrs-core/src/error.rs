use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgqrsError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Other error: {0}")]
    Other(String),
}
