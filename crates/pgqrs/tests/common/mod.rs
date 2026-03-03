#![allow(clippy::await_holding_lock)]
pub mod resource;

use ctor::dtor;
use pgqrs::store::{BackendType, Store};
use resource::{ResourceManager, TestResource, RESOURCE_MANAGER};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
#[cfg(feature = "s3")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "s3")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "s3")]
static TEST_KEY_SEQ: AtomicU64 = AtomicU64::new(1);

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
    #[cfg(any(feature = "postgres", feature = "s3"))]
    let env_non_empty = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());

    match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => {
            if std::env::var("PGQRS_TEST_SQLITE_DSN").is_ok()
                || std::env::var("PGQRS_TEST_TURSO_DSN").is_ok()
            {
                panic!("Ambiguous configuration: DSN mismatch for Postgres");
            }
            env_non_empty("PGQRS_TEST_POSTGRES_DSN")
                .or_else(|| env_non_empty("PGQRS_TEST_DSN"))
                .map(|dsn| Box::new(resource::ExternalResource::new(dsn)) as Box<dyn TestResource>)
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
        #[cfg(feature = "s3")]
        BackendType::S3 => {
            if std::env::var("PGQRS_TEST_POSTGRES_DSN").is_ok()
                || std::env::var("PGQRS_TEST_SQLITE_DSN").is_ok()
            {
                panic!("Ambiguous configuration: DSN mismatch for S3");
            }
            env_non_empty("PGQRS_TEST_S3_DSN")
                .or_else(|| env_non_empty("PGQRS_TEST_DSN"))
                .map(|dsn| {
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
            #[cfg(feature = "s3")]
            BackendType::S3 => {
                let bucket = std::env::var("PGQRS_S3_BUCKET")
                    .unwrap_or_else(|_| "pgqrs-test-bucket".to_string());
                let key = build_human_s3_key(schema);
                Box::new(resource::ExternalFileResource::new(format!(
                    "s3://{}/{}",
                    bucket, key
                )))
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
    if keep_test_data() {
        return;
    }

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

    cleanup_local_test_artifacts();
}

fn keep_test_data() -> bool {
    std::env::var("PGQRS_KEEP_TEST_DATA")
        .map(|v| {
            let lowered = v.trim().to_ascii_lowercase();
            !(lowered.is_empty() || lowered == "0" || lowered == "false" || lowered == "no")
        })
        .unwrap_or(false)
}

fn candidate_temp_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(v) = std::env::var("CARGO_TARGET_TMPDIR") {
        dirs.push(PathBuf::from(v));
    }
    if let Ok(cwd) = std::env::current_dir() {
        dirs.push(cwd.join("target").join("tmp"));
    }
    dirs.push(std::env::temp_dir());

    let mut seen = HashSet::new();
    dirs.into_iter()
        .filter(|p| seen.insert(p.clone()))
        .collect()
}

fn remove_file_if_exists(path: &Path) {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
}

fn cleanup_local_test_artifacts() {
    let Some(backend) = detect_backend_for_cleanup() else {
        return;
    };
    for dir in candidate_temp_dirs() {
        if !dir.exists() {
            continue;
        }

        #[cfg(feature = "s3")]
        if backend == BackendType::S3 {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with("pgqrs_s3_state_") {
                        if path.exists() {
                            let _ = std::fs::remove_dir_all(path);
                        }
                    }
                }
            }
        }

        #[cfg(feature = "sqlite")]
        if backend == BackendType::Sqlite {
            cleanup_sqlite_or_turso_files(&dir);
        }

        #[cfg(feature = "turso")]
        if backend == BackendType::Turso {
            cleanup_sqlite_or_turso_files(&dir);
        }
    }
}

fn detect_backend_for_cleanup() -> Option<BackendType> {
    if let Ok(backend_str) = std::env::var("PGQRS_TEST_BACKEND") {
        if let Ok(backend) = BackendType::detect(&backend_str.to_lowercase()) {
            return Some(backend);
        }
    }
    if let Ok(dsn) = std::env::var("PGQRS_TEST_DSN") {
        if let Ok(backend) = BackendType::detect(&dsn) {
            return Some(backend);
        }
    }
    None
}

fn cleanup_sqlite_or_turso_files(dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_test_db = name.starts_with("sqlite_")
                || name.starts_with("turso_")
                || name.starts_with("test_turso_")
                || name.starts_with("test_default_schema_turso_");
            if !is_test_db {
                continue;
            }

            if name.ends_with(".db") {
                remove_file_if_exists(&path);
                remove_file_if_exists(
                    PathBuf::from(format!("{}-wal", path.to_string_lossy())).as_path(),
                );
                remove_file_if_exists(
                    PathBuf::from(format!("{}-shm", path.to_string_lossy())).as_path(),
                );
                continue;
            }

            if name.contains(".db-wal") || name.contains(".db-shm") {
                remove_file_if_exists(&path);
            }
        }
    }
}

#[cfg(feature = "s3")]
fn build_human_s3_key(schema: &str) -> String {
    let suite = sanitize_component(schema);
    let test = sanitize_component(
        std::thread::current()
            .name()
            .unwrap_or("unknown_test")
            .rsplit("::")
            .next()
            .unwrap_or("unknown_test"),
    );
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let pid = std::process::id();
    let seq = TEST_KEY_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{suite}-{test}-{ts}-{pid}-{seq}.sqlite")
}

#[cfg(feature = "s3")]
fn sanitize_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = false;
    for c in s.chars() {
        let is_valid = c.is_ascii_alphanumeric();
        if is_valid {
            out.push(c.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed
    }
}
