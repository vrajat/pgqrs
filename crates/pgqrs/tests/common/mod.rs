#![allow(clippy::await_holding_lock)]
pub mod resource;

use ctor::dtor;
use pgqrs::store::{BackendType, Store};
use resource::{ResourceManager, TestResource, RESOURCE_MANAGER};
use std::path::PathBuf;

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

    // Default to the first local-capable backend available so test binaries
    // remain runnable without extra environment setup.
    #[cfg(feature = "sqlite")]
    {
        return BackendType::Sqlite;
    }
    #[cfg(all(not(feature = "sqlite"), feature = "turso"))]
    {
        return BackendType::Turso;
    }
    #[cfg(all(not(feature = "sqlite"), not(feature = "turso"), feature = "postgres"))]
    {
        return BackendType::Postgres;
    }
    #[cfg(all(
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "postgres"),
        feature = "s3"
    ))]
    {
        return BackendType::S3;
    }
    #[cfg(not(any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "s3"
    )))]
    {
        panic!("No supported backend feature enabled. Set PGQRS_TEST_BACKEND.");
    }
}

/// Create a store for the currently selected test backend.
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    create_store_with_config(schema, |_: &mut pgqrs::config::Config| {}).await
}

/// Create a store for the currently selected test backend with config customization.
#[allow(dead_code)]
pub async fn create_store_with_config(
    schema: &str,
    mutator: impl FnOnce(&mut pgqrs::config::Config),
) -> pgqrs::store::AnyStore {
    let dsn = get_test_dsn(schema).await;

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");
    let mut config = config;
    mutator(&mut config);

    let store = pgqrs::connect_with_config(&config)
        .await
        .unwrap_or_else(|e| panic!("Failed to create store with DSN: {}. Error: {:?}", dsn, e));

    #[cfg(feature = "s3")]
    if current_backend() == BackendType::S3 {
        assert!(
            matches!(store, pgqrs::store::AnyStore::S3(_)),
            "Expected AnyStore::S3 when PGQRS_TEST_BACKEND=s3"
        );
    }

    // Install schema based on backend:
    // - Postgres: Uses global setup (setup_test_schemas binary), skip install
    // - SQLite/Turso: No global setup, install per-test
    #[allow(unreachable_patterns)]
    match current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            // Schema already provisioned by setup_test_schemas binary
        }
        _ => {
            store.bootstrap().await.expect("Failed to bootstrap schema");
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

    #[cfg(any(feature = "postgres", feature = "s3"))]
    let env_non_empty = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());
    let resource: Box<dyn TestResource> = match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            if let Some(dsn) =
                env_non_empty("PGQRS_TEST_POSTGRES_DSN").or_else(|| env_non_empty("PGQRS_TEST_DSN"))
            {
                Box::new(resource::ExternalResource::new(dsn))
            } else {
                panic!(
                    "Postgres backend requires PGQRS_TEST_DSN environment variable. \
                     Run 'make test-postgres' or set PGQRS_TEST_DSN manually."
                );
            }
        }
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => {
            if let Ok(dsn) = std::env::var("PGQRS_TEST_SQLITE_DSN") {
                Box::new(resource::ExternalFileResource::new(dsn))
            } else {
                let r = resource::FileResource::new("sqlite://".to_string());
                r.initialize()
                    .await
                    .expect("Failed to init sqlite resource");
                Box::new(r)
            }
        }
        #[cfg(feature = "s3")]
        BackendType::S3 => {
            if let Some(dsn) =
                env_non_empty("PGQRS_TEST_S3_DSN").or_else(|| env_non_empty("PGQRS_TEST_DSN"))
            {
                Box::new(resource::ExternalS3FileResource::new(dsn))
            } else {
                let bucket = std::env::var("PGQRS_S3_BUCKET")
                    .unwrap_or_else(|_| "pgqrs-test-bucket".to_string());
                Box::new(resource::S3FileResource::new_generated(bucket))
            }
        }
        #[cfg(feature = "turso")]
        BackendType::Turso => {
            if let Ok(dsn) = std::env::var("PGQRS_TEST_TURSO_DSN") {
                Box::new(resource::ExternalFileResource::new(dsn))
            } else {
                let r = resource::FileResource::new("turso://".to_string());
                r.initialize().await.expect("Failed to init turso resource");
                Box::new(r)
            }
        }
    };

    *guard = Some(ResourceManager::new(resource));
    guard.as_ref().unwrap().resource.get_dsn(Some(schema)).await
}

/// Resolve the CLI binary path used by integration tests.
#[allow(dead_code)]
pub fn pgqrs_cli_bin() -> PathBuf {
    std::env::var_os("PGQRS_CLI_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(assert_cmd::cargo_bin!("pgqrs")))
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
    if keep_test_data() {
        return;
    }

    // Cleanup global resource. This dtor runs during process teardown; avoid panics and run
    // async cleanup on a fresh thread so TLS-dependent SDKs still work reliably.
    let manager = {
        let mut guard = match RESOURCE_MANAGER.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        guard.take()
    };

    if let Some(manager) = manager {
        let cleanup_thread = std::thread::Builder::new()
            .name("pgqrs-test-cleanup".to_string())
            .spawn(move || match tokio::runtime::Runtime::new() {
                Ok(rt) => rt.block_on(async {
                    if let Err(e) = manager.resource.cleanup().await {
                        eprintln!("Error during resource cleanup: {}", e);
                    }
                }),
                Err(e) => eprintln!("Failed to create cleanup runtime: {}", e),
            });

        match cleanup_thread {
            Ok(handle) => {
                if handle.join().is_err() {
                    eprintln!("Cleanup thread panicked");
                }
            }
            Err(e) => eprintln!("Failed to spawn cleanup thread: {}", e),
        }
    }
}

fn keep_test_data() -> bool {
    std::env::var("PGQRS_KEEP_TEST_DATA")
        .map(|v| {
            let lowered = v.trim().to_ascii_lowercase();
            !(lowered.is_empty() || lowered == "0" || lowered == "false" || lowered == "no")
        })
        .unwrap_or(false)
}
