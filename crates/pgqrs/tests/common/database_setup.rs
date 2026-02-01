//! Shared database setup and cleanup functionality for test containers

use super::constants::*;
use sqlx::postgres::PgPoolOptions;

/// Common database setup function that can be used by all container types
pub async fn setup_database_common(
    dsn: String,
    schema: &str,
    connection_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Global Setup Strategy:
    // Schemas are provisioned by `setup_test_schemas` (via cargo nextest setup-script).
    // We assume they exist.
    // We just verify connection.

    let pool = PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .acquire_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT_SECS))
        .connect(&dsn)
        .await?;

    let _val: i32 = sqlx::query_scalar(VERIFICATION_QUERY)
        .fetch_one(&pool)
        .await?;

    println!(
        "{} connected to '{}' (Setup: Global)",
        connection_type, schema
    );

    Ok(())
}

/// Common database cleanup function that can be used by all container types
pub async fn cleanup_database_common(
    _dsn: String,
    _schema: &str,
    _connection_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // No-op cleanup. We leave the data/schema for inspection or subsequent tests
    // Real cleanup happens at the "test-teardown" target level (dropping schemas) or via TRUNCATE at start of next test.
    Ok(())
}
