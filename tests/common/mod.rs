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
#[allow(dead_code)] // Used by multiple test modules, but Rust doesn't detect cross-module usage
pub async fn get_postgres_dsn(schema: Option<&str>) -> String {
    container::get_postgres_dsn(schema).await
}

#[allow(dead_code)] // Used by multiple test modules, but Rust doesn't detect cross-module usage
pub async fn get_pgbouncer_dsn(schema: Option<&str>) -> String {
    container::get_pgbouncer_dsn(schema).await
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
