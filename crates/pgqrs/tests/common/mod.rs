pub mod backend;
pub mod constants;
#[cfg(feature = "postgres")]
pub mod container;
#[cfg(feature = "postgres")]
pub mod database_setup; // database_setup also seems to use postgres logic? checked? assume yes or check.
#[cfg(feature = "postgres")]
pub mod pgbouncer;
#[cfg(feature = "postgres")]
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
#[cfg(feature = "postgres")]
pub async fn get_postgres_dsn(schema: Option<&str>) -> String {
    container::get_postgres_dsn(schema).await
}

#[allow(dead_code)] // Used by multiple test modules, but Rust doesn't detect cross-module usage
#[cfg(feature = "postgres")]
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
        #[cfg(feature = "postgres")]
        if backend == TestBackend::Postgres {
            crate::common::database_setup::setup_database_common(
                env_dsn.clone(),
                schema,
                "External Env Postgres",
            )
            .await
            .expect("Failed to setup env postgres");
        }
        env_dsn
    } else {
        // Fall back to container-based setup (currently only Postgres supported)
        match backend {
            #[cfg(feature = "postgres")]
            TestBackend::Postgres => container::get_postgres_dsn(Some(schema)).await,
            #[cfg(not(feature = "postgres"))]
            TestBackend::Postgres => panic!("Postgres feature is disabled"),
            TestBackend::Sqlite => {
                // Use a unique in-memory database per store creation to ensure isolation
                // The ?cache=shared allows pool connections to verify consistent state
                format!(
                    "sqlite:file:{}?mode=memory&cache=shared",
                    uuid::Uuid::new_v4()
                )
            }
            TestBackend::Turso => {
                // Default to a unique file-based Turso database if no env override is provided
                format!(
                    "file:{}",
                    std::env::temp_dir()
                        .join(format!("pgqrs_turso_test_{}.db", uuid::Uuid::new_v4()))
                        .display()
                )
            }
        }
    };

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");

    let store = pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to create store");

    store
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
        #[cfg(feature = "postgres")]
        if let Err(e) = container::cleanup_database().await {
            eprintln!("Error during database cleanup: {}", e);
        }
    });
}
