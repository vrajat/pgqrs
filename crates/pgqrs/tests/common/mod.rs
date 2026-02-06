#![allow(clippy::await_holding_lock)]
pub mod constants;

#[cfg(feature = "postgres")]
pub mod database_setup;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod resource;

use ctor::dtor;
use pgqrs::store::BackendType;
use resource::{ResourceManager, TestResource, RESOURCE_MANAGER};

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
pub fn get_dsn_from_env(backend: BackendType) -> Option<Box<dyn TestResource>> {
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
                .map(|dsn| {
                    Box::new(postgres::ExternalPostgresResource::new(dsn)) as Box<dyn TestResource>
                })
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!("Ambiguous configuration: DSN mismatch for Sqlite");
            }
            std::env::var("PGQRS_TEST_SQLITE_DSN").ok().map(|dsn| {
                Box::new(resource::ExternalFileResource::new(dsn)) as Box<dyn TestResource>
            })
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok() {
                panic!("Ambiguous configuration: DSN mismatch for Turso");
            }
            std::env::var("PGQRS_TEST_TURSO_DSN").ok().map(|dsn| {
                Box::new(resource::ExternalFileResource::new(dsn)) as Box<dyn TestResource>
            })
        }
    }
}

/// Create a store for the currently selected test backend.
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    let dsn = get_test_dsn(schema).await;

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");

    let store = pgqrs::connect_with_config(&config)
        .await
        .unwrap_or_else(|e| panic!("Failed to create store with DSN: {}. Error: {:?}", dsn, e));

    // Install schema based on backend:
    // - Postgres: Uses global setup (setup_test_schemas binary), skip install
    // - SQLite/Turso: No global setup, install per-test
    match current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            // Schema already provisioned by setup_test_schemas binary
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            pgqrs::admin(&store)
                .install()
                .await
                .expect("Failed to install SQLite schema");
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            pgqrs::admin(&store)
                .install()
                .await
                .expect("Failed to install Turso schema");
        }
    }

    store
}

/// Get DSN for the current test backend.
#[allow(dead_code)]
pub async fn get_test_dsn(schema: &str) -> String {
    let backend = current_backend();

    {
        let guard = RESOURCE_MANAGER.read().unwrap();
        if let Some(manager) = guard.as_ref() {
            return manager.resource.get_dsn(Some(schema)).await;
        }
    }

    let mut guard = RESOURCE_MANAGER.write().unwrap();
    if let Some(manager) = guard.as_ref() {
        return manager.resource.get_dsn(Some(schema)).await;
    }

    let resource: Box<dyn TestResource> = if let Some(r) = get_dsn_from_env(backend) {
        r
    } else {
        match backend {
            #[cfg(feature = "postgres")]
            BackendType::Postgres => {
                // Postgres requires external database (from CI services or local Docker)
                // If PGQRS_TEST_DSN is not set, tests will fail with connection error
                panic!(
                    "Postgres backend requires PGQRS_TEST_DSN environment variable. \
                     Run 'make test-postgres' or set PGQRS_TEST_DSN manually."
                );
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
        }
    };

    *guard = Some(ResourceManager::new(resource));
    guard.as_ref().unwrap().resource.get_dsn(Some(schema)).await
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
