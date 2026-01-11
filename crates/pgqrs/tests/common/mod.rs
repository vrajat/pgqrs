pub mod constants;
#[cfg(feature = "postgres")]
pub mod container;
#[cfg(feature = "postgres")]
pub mod database_setup;
#[cfg(feature = "postgres")]
pub mod pgbouncer;
#[cfg(feature = "postgres")]
pub mod postgres;

use ctor::dtor;
use pgqrs::store::BackendType; // Use the core enum

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

/// Get the current test backend.
///
/// Logic:
/// 1. Check `PGQRS_TEST_BACKEND`. If set, parse it.
/// 2. If not set, check `PGQRS_TEST_DSN`. If set, detect backend from DSN.
/// 3. Default to Postgres.
#[allow(dead_code)]
pub fn current_backend() -> BackendType {
    if let Ok(backend_str) = std::env::var("PGQRS_TEST_BACKEND") {
        return BackendType::detect(&backend_str.to_lowercase())
            .unwrap_or_else(|e| panic!("{}", e));
    }

    if let Ok(dsn) = std::env::var("PGQRS_TEST_DSN") {
        if let Ok(backend) = BackendType::detect(&dsn) {
            return backend;
        }
    }

    // Default
    #[cfg(feature = "postgres")]
    {
        BackendType::Postgres
    }
    #[cfg(not(feature = "postgres"))]
    {
        panic!("No backend specified and postgres feature is disabled. Set PGQRS_TEST_BACKEND.");
    }
}

/// Get DSN for the selected test backend from environment variables.
///
/// Implements strict validation:
/// - Checks `PGQRS_TEST_{BACKEND}_DSN`.
/// - Errors if a mismatched DSN variable is present when strict mode is required?
///   The user said "Cant allow BACKEND=turso and SQLITE_DSN".
///   We will check if `PGQRS_TEST_BACKEND` is set explicitly, and if so,
///   ensure that we aren't confusingly extracting DSNs from other backends.
///   ensure that we aren't confusingly extracting DSNs from other backends.
#[allow(dead_code)]
pub fn get_dsn_from_env(backend: BackendType) -> Option<String> {
    // Validate: If we are asking for Turso, ensure we don't have SQLITE_DSN set to avoid ambiguity?
    // Actually, simply prioritizing the correct var is usually enough, but let's be strict if they exist.

    match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            // Check for ambiguity
            if std::env::var("PGQRS_TEST_SQLITE_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_SQLITE_DSN is set but backend is Postgres"
                );
            }
            if std::env::var("PGQRS_TEST_TURSO_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_TURSO_DSN is set but backend is Postgres"
                );
            }

            std::env::var("PGQRS_TEST_POSTGRES_DSN")
                .or_else(|_| std::env::var("PGQRS_TEST_DSN"))
                .ok()
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_POSTGRES_DSN is set but backend is Sqlite"
                );
            }
            if std::env::var("PGQRS_TEST_TURSO_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_TURSO_DSN is set but backend is Sqlite"
                );
            }
            std::env::var("PGQRS_TEST_SQLITE_DSN").ok()
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_POSTGRES_DSN is set but backend is Turso"
                );
            }
            if std::env::var("PGQRS_TEST_SQLITE_DSN").is_ok() {
                panic!(
                    "Ambiguous configuration: PGQRS_TEST_SQLITE_DSN is set but backend is Turso"
                );
            }
            std::env::var("PGQRS_TEST_TURSO_DSN").ok()
        }
    }
}

/// Create a store for the currently selected test backend.
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    let dsn = get_test_dsn(schema).await;

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");

    pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to create store")
}

/// Get DSN for the current test backend.
#[allow(dead_code)]
pub async fn get_test_dsn(schema: &str) -> String {
    let backend = current_backend();
    if let Some(env_dsn) = get_dsn_from_env(backend) {
        #[cfg(feature = "postgres")]
        if backend == BackendType::Postgres {
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
        match backend {
            #[cfg(feature = "postgres")]
            BackendType::Postgres => container::get_postgres_dsn(Some(schema)).await,
            #[cfg(feature = "sqlite")]
            BackendType::Sqlite => {
                let dir = std::env::current_dir()
                    .expect("Failed to get CWD")
                    .join("target")
                    .join("test_dbs");
                std::fs::create_dir_all(&dir).expect("Failed to create test db dir");
                format!(
                    "sqlite://{}/sqlite_{}_{}.db?mode=rwc",
                    dir.to_string_lossy(),
                    schema,
                    uuid::Uuid::new_v4()
                )
            }
            #[cfg(feature = "turso")]
            BackendType::Turso => {
                format!(
                    "turso://{}",
                    std::env::temp_dir()
                        .join(format!("{}_{}.db", schema, uuid::Uuid::new_v4()))
                        .display()
                )
            }
        }
    }
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
