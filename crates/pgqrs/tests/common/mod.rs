pub mod backend;
pub mod constants;
pub mod container;
pub mod database_setup;
pub mod pgbouncer;
pub mod postgres;

pub use backend::TestBackend;

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

/// Get DSN for the selected test backend from environment variables.
///
/// This function handles environment variable resolution for backend-specific DSNs.
/// For Postgres, if no env var is set, returns None (caller should use testcontainers).
#[allow(dead_code)]
pub fn get_dsn_from_env(backend: TestBackend, _schema: Option<&str>) -> Option<String> {
    match backend {
        TestBackend::Postgres => {
            // Try backend-specific env var first, then fall back to generic
            std::env::var("PGQRS_TEST_POSTGRES_DSN")
                .or_else(|_| std::env::var("PGQRS_TEST_DSN"))
                .ok()
        }
        TestBackend::Sqlite => std::env::var("PGQRS_TEST_SQLITE_DSN").ok(),
        TestBackend::Turso => std::env::var("PGQRS_TEST_TURSO_DSN").ok(),
    }
}

/// Create a store for the currently selected test backend.
///
/// This function respects the `PGQRS_TEST_BACKEND` environment variable
/// and creates the appropriate backend (Postgres, SQLite, or Turso).
///
/// # Arguments
/// * `schema` - Schema name for isolation (used differently per backend)
///
/// # Returns
/// An `AnyStore` instance connected to the selected backend
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    let backend = TestBackend::from_env();
    create_store_for_backend(backend, schema).await
}

/// Create a store for a specific backend.
///
/// # Arguments
/// * `backend` - The specific backend to use
/// * `schema` - Schema name for isolation
///
/// # Returns
/// An `AnyStore` instance connected to the specified backend
#[allow(dead_code)]
pub async fn create_store_for_backend(
    backend: TestBackend,
    schema: &str,
) -> pgqrs::store::AnyStore {
    let dsn = if let Some(env_dsn) = get_dsn_from_env(backend, Some(schema)) {
        env_dsn
    } else {
        // Fall back to container-based setup (currently only Postgres supported)
        match backend {
            TestBackend::Postgres => container::get_postgres_dsn(Some(schema)).await,
            TestBackend::Sqlite => {
                // Default to in-memory database if no DSN provided
                "sqlite::memory:".to_string()
            }
            TestBackend::Turso => panic!("PGQRS_TEST_TURSO_DSN must be set for Turso backend"),
        }
    };

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");

    pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to create store")
}

/// Get the current test backend (for skip logic).
#[allow(dead_code)]
pub fn current_backend() -> TestBackend {
    TestBackend::from_env()
}

/// Skip test if not running on specified backend.
#[macro_export]
macro_rules! skip_unless_backend {
    ($backend:expr) => {
        if common::current_backend() != $backend {
            eprintln!("Skipping test: requires {:?} backend", $backend);
            return;
        }
    };
}

/// Skip test if running on specified backend.
#[macro_export]
macro_rules! skip_on_backend {
    ($backend:expr) => {
        if common::current_backend() == $backend {
            eprintln!("Skipping test: not supported on {:?} backend", $backend);
            return;
        }
    };
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
