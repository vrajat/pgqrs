use super::error::PgqrsError;
use sqlx::{PgPool, Row};

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