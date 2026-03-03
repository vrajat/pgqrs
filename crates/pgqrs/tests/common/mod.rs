#![allow(clippy::await_holding_lock)]
pub mod resource;

use ctor::dtor;
use pgqrs::store::{BackendType, Store};
use resource::{ResourceManager, TestResource, RESOURCE_MANAGER};
#[cfg(feature = "s3")]
use std::time::Duration;

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

/// Create a store for the currently selected test backend.
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    let dsn = get_test_dsn(schema).await;

    let config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");

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

    #[cfg(feature = "s3")]
    if current_backend() == BackendType::S3 {
        assert_s3_object_created_for_dsn(&dsn)
            .await
            .expect("S3 backend selected, but sqlite state object was not found in S3");
    }

    store
}

#[cfg(feature = "s3")]
fn parse_s3_dsn_for_assertion(dsn: &str) -> Option<(String, String)> {
    let full = dsn
        .strip_prefix("s3://")
        .or_else(|| dsn.strip_prefix("s3:"))?;
    let mut parts = full.splitn(2, '/');
    let bucket = parts.next()?.trim();
    let key = parts.next()?.trim();
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

#[cfg(feature = "s3")]
fn is_local_s3_endpoint(endpoint: &str) -> bool {
    let ep = endpoint.to_ascii_lowercase();
    ep.contains("localhost") || ep.contains("127.0.0.1") || ep.contains("localstack")
}

#[cfg(feature = "s3")]
async fn assert_s3_object_created_for_dsn(dsn: &str) -> Result<(), String> {
    let endpoint = std::env::var("PGQRS_S3_ENDPOINT")
        .map_err(|_| "PGQRS_S3_ENDPOINT must be set for S3 tests".to_string())?;
    if !is_local_s3_endpoint(&endpoint) {
        // Only enforce this hard assertion for local/localstack endpoints.
        return Ok(());
    }

    let (bucket, key) = parse_s3_dsn_for_assertion(dsn)
        .ok_or_else(|| format!("Invalid S3 DSN for assertion: {dsn}"))?;

    let region = std::env::var("PGQRS_S3_REGION")
        .map_err(|_| "PGQRS_S3_REGION must be set for S3 tests".to_string())?;
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let client = pgqrs::store::s3::client::build_aws_s3_client(
        pgqrs::store::s3::client::AwsS3ClientConfig {
            region,
            endpoint: Some(endpoint),
            access_key,
            secret_key,
            force_path_style: true,
            credentials_provider_name: "pgqrs-test-s3-assert",
        },
    )
    .await;

    // S3 visibility is effectively immediate for LocalStack, but allow a short retry window.
    let mut last_err = String::new();
    for _ in 0..10 {
        match client.head_object().bucket(&bucket).key(&key).send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(format!(
        "Object s3://{bucket}/{key} not visible in endpoint after bootstrap: {last_err}"
    ))
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
