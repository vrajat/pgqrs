use crate::db::constants::{PGQRS_SCHEMA, UNINSTALL_STATEMENT};

use super::error::PgqrsError;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

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

/// Check if the pgqrs schema is already initialized
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
///
/// # Returns
/// * `Ok(true)` if schema exists and is properly initialized
/// * `Ok(false)` if schema needs to be initialized
/// * `Err(PgqrsError)` if check fails
pub async fn is_db_initialized(pool: &PgPool) -> Result<bool, PgqrsError> {
    let exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata
            WHERE schema_name = 'pgqrs'
        ) AND EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pgqrs' AND table_name = 'meta'
        ) as schema_exists
    "#;

    let row = sqlx::query(exists_query)
        .fetch_one(pool)
        .await
        .map_err(PgqrsError::from)?;

    let exists: bool = row.get("schema_exists");
    Ok(exists)
}

/// Initialize database if not already initialized
///
/// This is a convenience function that checks if the database is initialized
/// and runs the initialization if needed.
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
///
/// # Returns
/// * `Ok(())` if database is ready (either already initialized or successfully initialized)
/// * `Err(PgqrsError)` if initialization fails
pub async fn ensure_db_initialized(pool: &PgPool) -> Result<(), PgqrsError> {
    if !is_db_initialized(pool).await? {
        init_db(pool).await?;
    }
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
