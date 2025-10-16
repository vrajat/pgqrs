use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use crate::config::Config;
use crate::error::PgqrsError;

pub async fn create_pool(config: &Config) -> Result<PgPool, PgqrsError> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .map_err(PgqrsError::from)
}
