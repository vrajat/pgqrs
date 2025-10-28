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
/// A string reference to the database DSN that can be used for tests
pub async fn get_postgres_dsn() -> &'static str {
    container::get_database_dsn().await
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
