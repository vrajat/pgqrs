use async_trait::async_trait;
use once_cell::sync::Lazy;
#[cfg(any(feature = "sqlite", feature = "turso"))]
use std::path::PathBuf;
#[cfg(any(feature = "sqlite", feature = "turso"))]
use std::sync::Mutex;
use std::sync::RwLock;

/// Trait for managing test backend resources (Tests Containers or Files)
#[async_trait]
pub trait TestResource: Send + Sync {
    /// Initialize the resource (start container or prepare directory)
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
    backend_prefix: String,
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
impl FileResource {
    pub fn new(backend_prefix: String) -> Self {
        Self {
            created_files: Mutex::new(Vec::new()),
            backend_prefix,
        }
    }

    fn get_temp_dir() -> PathBuf {
        std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .or_else(|_| std::env::current_dir().map(|cwd| cwd.join("target").join("tmp")))
            .unwrap_or_else(|_| std::env::temp_dir())
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
        let dir = Self::get_temp_dir();
        std::fs::create_dir_all(&dir)?;
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let dir = Self::get_temp_dir();
        // Ensure dir exists (init might have happened once, but good to be safe)
        let _ = std::fs::create_dir_all(&dir);

        let schema_part = schema.unwrap_or("default");
        let filename = format!(
            "{}_{}_{}.db",
            self.backend_prefix.replace("://", ""),
            schema_part,
            uuid::Uuid::new_v4()
        );
        let path = dir.join(filename);

        // Track for cleanup
        self.track_file(path.clone());

        if self.backend_prefix.starts_with("sqlite") {
            format!("sqlite://{}?mode=rwc", path.to_string_lossy())
        } else {
            format!("{}{}", self.backend_prefix, path.to_string_lossy())
        }
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
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
        Ok(())
    }
}

// --- External File Resource Implementation ---

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
