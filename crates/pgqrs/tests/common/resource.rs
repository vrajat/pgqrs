use async_trait::async_trait;
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "s3")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "s3")]
use object_store::ObjectStore;
use once_cell::sync::Lazy;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
use pgqrs::store::BackendType;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
use std::path::PathBuf;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
use std::sync::Mutex;
use std::sync::RwLock;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(feature = "sqlite", feature = "turso"))]
static FILE_RESOURCE_SEQ: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "s3")]
static S3_RESOURCE_SEQ: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "s3")]
const S3_TEST_CACHE_DIR_NAME: &str = "pgqrs_s3_cache";
#[cfg(feature = "s3")]
static S3_CREATED_CACHE_DIRS: Lazy<Mutex<Vec<PathBuf>>> = Lazy::new(|| Mutex::new(Vec::new()));

#[cfg(feature = "s3")]
fn s3_cache_base_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = std::env::var("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .map_err(|_| "CARGO_TARGET_TMPDIR must be set for S3 test resources")?
        .join(S3_TEST_CACHE_DIR_NAME);
    std::fs::create_dir_all(&dir)?;
    Ok(dir.canonicalize().unwrap_or(dir))
}

#[cfg(feature = "s3")]
pub fn allocate_s3_cache_dir(schema: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let base = s3_cache_base_dir()?;
    let cache_dir = base.join(format!(
        "{}-{}-{}",
        sanitize_component(schema),
        std::process::id(),
        S3_RESOURCE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&cache_dir)?;
    S3_CREATED_CACHE_DIRS
        .lock()
        .unwrap()
        .push(cache_dir.clone());
    Ok(cache_dir)
}

/// Trait for managing test backend resources (Tests Containers or Files)
#[async_trait]
pub trait TestResource: Send + Sync {
    /// Initialize the resource (start container or prepare directory)
    #[allow(dead_code)]
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Get a DSN for the resource, optionally specifying a schema/filename suffix
    async fn get_dsn(&self, schema: Option<&str>) -> String;

    /// Cleanup the resource (stop container or delete files)
    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Resource Manager that holds the active singleton resource
pub struct ResourceManager {
    pub resource: Box<dyn TestResource>,
}

impl ResourceManager {
    pub fn new(resource: Box<dyn TestResource>) -> Self {
        Self { resource }
    }
}

/// Global resource manager
pub static RESOURCE_MANAGER: Lazy<RwLock<Option<ResourceManager>>> =
    Lazy::new(|| RwLock::new(None));

// --- File Resource Implementation (SQLite/Turso) ---

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub struct FileResource {
    created_files: Mutex<Vec<PathBuf>>,
    backend_type: BackendType,
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
impl FileResource {
    pub fn new(backend_prefix: String) -> Self {
        let backend_type = BackendType::detect(&backend_prefix)
            .expect("FileResource requires a supported backend DSN prefix");
        Self {
            created_files: Mutex::new(Vec::new()),
            backend_type,
        }
    }

    fn get_temp_dir() -> PathBuf {
        std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .or_else(|_| std::env::current_dir().map(|cwd| cwd.join("target").join("tmp")))
            .unwrap_or_else(|_| std::env::temp_dir())
    }

    fn backend_name(&self) -> &str {
        match self.backend_type {
            #[cfg(feature = "sqlite")]
            BackendType::Sqlite => "sqlite",
            #[cfg(feature = "turso")]
            BackendType::Turso => "turso",
            #[cfg(feature = "postgres")]
            BackendType::Postgres => "postgres",
            #[cfg(feature = "s3")]
            BackendType::S3 => "s3",
        }
    }

    fn backend_temp_dir(&self) -> PathBuf {
        Self::get_temp_dir().join(self.backend_name())
    }

    fn configure_backend_temp_dir(&self, _dir: &PathBuf) {
        #[cfg(feature = "turso")]
        if self.backend_type == BackendType::Turso {
            // Turso/libsql creates spill files via std::env::temp_dir(), so force them into
            // a backend-owned directory that the test resource cleanup manages.
            unsafe {
                std::env::set_var("TMPDIR", _dir);
                std::env::set_var("TMP", _dir);
                std::env::set_var("TEMP", _dir);
            }
        }
    }

    fn track_file(&self, path: PathBuf) {
        let mut files = self.created_files.lock().unwrap();
        files.push(path);
    }
}

#[async_trait]
#[cfg(any(feature = "sqlite", feature = "turso"))]
impl TestResource for FileResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = self.backend_temp_dir();
        std::fs::create_dir_all(&dir)?;
        self.configure_backend_temp_dir(&dir);
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let dir = self.backend_temp_dir();
        // Ensure dir exists (init might have happened once, but good to be safe)
        let _ = std::fs::create_dir_all(&dir);
        self.configure_backend_temp_dir(&dir);
        // Canonicalize to ensure absolute path for libs/drivers
        let dir = dir.canonicalize().unwrap_or(dir);

        let schema_part = schema.unwrap_or("default");
        let backend = sanitize_component(self.backend_name());
        let suite = sanitize_component(schema_part);
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
        let seq = FILE_RESOURCE_SEQ.fetch_add(1, Ordering::Relaxed);
        let filename = format!("{backend}-{suite}-{test}-{ts}-{pid}-{seq}.db");
        let path = dir.join(filename);

        // Track for cleanup
        self.track_file(path.clone());

        // Explicitly create the file
        if !path.exists() {
            match std::fs::File::create(&path) {
                Ok(_) => eprintln!("Created SQLite DB file: {:?}", path),
                Err(e) => eprintln!("Failed to pre-create DB file {:?}: {}", path, e),
            };
        } else {
            eprintln!("Using existing SQLite DB file: {:?}", path);
        };

        match self.backend_type {
            #[cfg(feature = "sqlite")]
            BackendType::Sqlite => {
                // For sqlx specifically
                format!("sqlite://{}?mode=rwc", path.to_string_lossy())
            }
            #[cfg(feature = "turso")]
            BackendType::Turso => format!("turso://{}", path.to_string_lossy()),
            #[allow(unreachable_patterns)]
            _ => unreachable!("FileResource only supports sqlite/turso backends"),
        }
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = self.backend_temp_dir();
        let mut files = self.created_files.lock().unwrap();
        for path in files.iter() {
            if path.exists() {
                if let Err(e) = std::fs::remove_file(path) {
                    eprintln!("Failed to remove test file {:?}: {}", path, e);
                }
                // Cleanup WAL/SHM
                let path_str = path.to_string_lossy();
                let wal = PathBuf::from(format!("{}-wal", path_str));
                let shm = PathBuf::from(format!("{}-shm", path_str));
                if wal.exists() {
                    let _ = std::fs::remove_file(wal);
                }
                if shm.exists() {
                    let _ = std::fs::remove_file(shm);
                }
            }
        }
        files.clear();

        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with("tursodb-ephemeral-") {
                        let _ = std::fs::remove_file(path);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
fn sanitize_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = false;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
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

// --- S3 File Resource ---
#[cfg(feature = "s3")]
pub struct S3FileResource {
    bucket: String,
    issued_dsns: Mutex<Vec<String>>,
    delete_remote: bool,
}

#[cfg(feature = "s3")]
impl S3FileResource {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            issued_dsns: Mutex::new(Vec::new()),
            delete_remote: true,
        }
    }

    fn track_dsn(&self, dsn: String) {
        let mut dsns = self.issued_dsns.lock().unwrap();
        dsns.push(dsn);
    }

    async fn delete_remote_objects(dsns: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        let mut by_bucket: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for dsn in dsns {
            let (bucket, key) = pgqrs::store::s3::parse_s3_bucket_and_key(dsn)?;
            by_bucket.entry(bucket).or_default().push(key);
        }
        if by_bucket.is_empty() {
            return Ok(());
        }

        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();

        for (bucket, keys) in by_bucket {
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_region(region.clone())
                .with_virtual_hosted_style_request(false);
            if let Some(ep) = endpoint.clone().filter(|v| !v.trim().is_empty()) {
                if ep.starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
                builder = builder.with_endpoint(ep);
            }
            let store = builder.build()?;
            for key in keys {
                let _ = store.delete(&ObjectPath::from(key.as_str())).await;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl TestResource for S3FileResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let bucket = self.bucket.clone();
        let suite = sanitize_component(schema.unwrap_or("default"));
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
        let seq = S3_RESOURCE_SEQ.fetch_add(1, Ordering::Relaxed);
        let dsn = format!("s3://{bucket}/{suite}-{test}-{ts}-{pid}-{seq}.sqlite");
        self.track_dsn(dsn.clone());
        dsn
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = {
            let mut dsns = self.issued_dsns.lock().unwrap();
            let snapshot = dsns.clone();
            dsns.clear();
            snapshot
        };
        let created_dirs = {
            let mut dirs = S3_CREATED_CACHE_DIRS.lock().unwrap();
            std::mem::take(&mut *dirs)
        };

        if self.delete_remote {
            // Best effort remote cleanup.
            let _ = Self::delete_remote_objects(&snapshot).await;
        }
        for dir in created_dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
        Ok(())
    }
}

#[cfg(feature = "s3")]
pub struct ExternalS3FileResource {
    dsn: String,
    inner: S3FileResource,
}

#[cfg(feature = "s3")]
impl ExternalS3FileResource {
    pub fn new(dsn: String) -> Self {
        Self {
            dsn,
            inner: S3FileResource {
                bucket: String::new(),
                issued_dsns: Mutex::new(Vec::new()),
                delete_remote: false,
            },
        }
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl TestResource for ExternalS3FileResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn get_dsn(&self, _schema: Option<&str>) -> String {
        self.inner.track_dsn(self.dsn.clone());
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.cleanup().await
    }
}

// --- External Resource Implementation ---

/// Generic external resource that just returns a DSN
/// Used for external databases or files managed outside the test suite
#[cfg(feature = "postgres")]
pub struct ExternalResource {
    dsn: String,
}

#[cfg(feature = "postgres")]
impl ExternalResource {
    pub fn new(dsn: String) -> Self {
        Self { dsn }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl TestResource for ExternalResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn get_dsn(&self, _schema: Option<&str>) -> String {
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

// --- External File Resource Implementation (Deprecated - use ExternalResource) ---

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub struct ExternalFileResource {
    dsn: String,
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
impl ExternalFileResource {
    pub fn new(dsn: String) -> Self {
        Self { dsn }
    }
}

#[async_trait]
#[cfg(any(feature = "sqlite", feature = "turso"))]
impl TestResource for ExternalFileResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn get_dsn(&self, _schema: Option<&str>) -> String {
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
