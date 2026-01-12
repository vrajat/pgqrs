pub mod constants;

#[cfg(feature = "postgres")]
pub mod database_setup;
#[cfg(feature = "postgres")]
pub mod pgbouncer;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod resource;

use ctor::dtor;
use pgqrs::store::BackendType;
use resource::{ResourceManager, TestResource, RESOURCE_MANAGER};

/// Get a PostgreSQL DSN for testing.
#[allow(dead_code)]
#[cfg(feature = "postgres")]
pub async fn get_postgres_dsn(schema: Option<&str>) -> String {
    initialize_global_resource(BackendType::Postgres, false).await;
    let guard = RESOURCE_MANAGER.read().unwrap();
    guard.as_ref().unwrap().resource.get_dsn(schema).await
}

#[allow(dead_code)]
#[cfg(feature = "postgres")]
pub async fn get_pgbouncer_dsn(schema: Option<&str>) -> String {
    initialize_global_resource(BackendType::Postgres, true).await;
    let guard = RESOURCE_MANAGER.read().unwrap();
    guard.as_ref().unwrap().resource.get_dsn(schema).await
}

/// Get the current test backend.
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

#[allow(dead_code)]
pub fn get_dsn_from_env(backend: BackendType) -> Option<String> {
    match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            if std::env::var("PGQRS_TEST_SQLITE_DSN").is_ok()
                || std::env::var("PGQRS_TEST_TURSO_DSN").is_ok()
            {
                panic!("Ambiguous configuration: DSN mismatch for Postgres");
            }
            std::env::var("PGQRS_TEST_POSTGRES_DSN")
                .or_else(|_| std::env::var("PGQRS_TEST_DSN"))
                .ok()
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!("Ambiguous configuration: DSN mismatch for Sqlite");
            }
            std::env::var("PGQRS_TEST_SQLITE_DSN").ok()
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!("Ambiguous configuration: DSN mismatch for Turso");
            }
            std::env::var("PGQRS_TEST_TURSO_DSN").ok()
        }
    }
}

// Global initialization logic
async fn initialize_global_resource(backend: BackendType, use_pgbouncer: bool) {
    {
        let guard = RESOURCE_MANAGER.read().unwrap();
        if guard.is_some() {
            return;
        }
    }

    let mut guard = RESOURCE_MANAGER.write().unwrap();
    if guard.is_some() {
        return;
    }

    let external_dsn = get_dsn_from_env(backend);

    let resource: Box<dyn TestResource> = match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            if let Some(dsn) = external_dsn {
                if use_pgbouncer {
                    Box::new(pgbouncer::ExternalPgBouncerResource::new(dsn))
                } else {
                    Box::new(postgres::ExternalPostgresResource::new(dsn))
                }
            } else {
                if use_pgbouncer {
                    let r = pgbouncer::PgBouncerResource::new();
                    r.initialize().await.expect("Failed to init pgbouncer");
                    Box::new(r)
                } else {
                    let r = postgres::PostgresResource::new();
                    r.initialize().await.expect("Failed to init postgres");
                    Box::new(r)
                }
            }
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            let r = resource::FileResource::new("sqlite://".to_string());
            r.initialize()
                .await
                .expect("Failed to init sqlite resource");
            Box::new(r)
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            let r = resource::FileResource::new("turso://".to_string());
            r.initialize().await.expect("Failed to init turso resource");
            Box::new(r)
        }
    };

    *guard = Some(ResourceManager::new(resource));
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

    // 1. Check Env Overrides (External) for non-Postgres (which uses Manager)
    #[cfg(feature = "postgres")]
    let is_postgres_backend = backend == BackendType::Postgres;
    #[cfg(not(feature = "postgres"))]
    let is_postgres_backend = false;

    if !is_postgres_backend {
        if let Some(env_dsn) = get_dsn_from_env(backend) {
            return env_dsn;
        }
    }

    // 2. Use Managed Resource
    match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => get_postgres_dsn(Some(schema)).await,
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            initialize_global_resource(BackendType::Sqlite, false).await;
            let guard = RESOURCE_MANAGER.read().unwrap();
            guard.as_ref().unwrap().resource.get_dsn(Some(schema)).await
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            initialize_global_resource(BackendType::Turso, false).await;
            let guard = RESOURCE_MANAGER.read().unwrap();
            guard.as_ref().unwrap().resource.get_dsn(Some(schema)).await
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
    // Cleanup global resource
    let mut guard = match RESOURCE_MANAGER.write() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    };

    if let Some(manager) = guard.take() {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create cleanup runtime");
        rt.block_on(async {
            if let Err(e) = manager.resource.cleanup().await {
                eprintln!("Error during resource cleanup: {}", e);
            }
        });
    }
}
