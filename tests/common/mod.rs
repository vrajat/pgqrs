pub mod constants;
pub mod container;
pub mod pgbouncer;
pub mod postgres;

use ctor::dtor;

/// Get a PostgreSQL DSN for testing.
///
/// This function handles external databases (via PGQRS_TEST_DSN env var),
/// PgBouncer setup (via PGQRS_TEST_USE_PGBOUNCER env var), and regular
/// PostgreSQL testcontainers. The database schema is automatically
/// installed and cleaned up when tests complete.
///
/// # Returns
/// The database DSN string that can be used for tests
pub async fn get_postgres_dsn() -> String {
    container::get_database_dsn().await
}

/// Get a PostgreSQL DSN for testing with a specific schema for isolation.
///
/// This creates a separate schema within the same database for test isolation.
/// The schema will be created if it doesn't exist.
///
/// # Arguments
/// * `schema` - The schema name to use for isolation
///
/// # Returns
/// The database DSN string that can be used for tests with the specified schema
pub async fn get_database_dsn_with_schema(schema: &str) -> String {
    container::get_database_dsn_with_schema(schema).await
}

#[dtor]
fn drop_database() {
    // Create a simple runtime for cleanup
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        if let Err(e) = container::cleanup_database().await {
            eprintln!("Error during database cleanup: {}", e);
        }
    });
}
