use crate::db::constants::{PGQRS_SCHEMA, UNINSTALL_STATEMENT};

use super::error::PgqrsError;
use sqlx::{postgres::PgPoolOptions, PgPool};

/// Initialize the pgqrs database schema
///
/// This function runs the necessary SQL migrations to set up the pgqrs schema
/// and meta table in the PostgreSQL database.
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
///
/// # Returns
/// * `Ok(())` if initialization succeeds
/// * `Err(PgqrsError)` if initialization fails
pub async fn init_db(pool: &PgPool) -> Result<(), PgqrsError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(PgqrsError::from)?;

    Ok(())
}

/// Uninstall pgqrs schema and remove all state from the database.
///
/// # Returns
/// Ok if uninstall (or validation) succeeds, error otherwise.
pub fn uninstall(dsn: &String) -> Result<(), PgqrsError> {
    let uninstall_statement = UNINSTALL_STATEMENT.replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1) // Small pool per test
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(dsn)
            .await
            .expect("Failed to connect to Postgres");

        sqlx::query(&uninstall_statement)
            .execute(&pool)
            .await
            .map_err(PgqrsError::from)?;
        Ok::<(), PgqrsError>(())
    })
}
