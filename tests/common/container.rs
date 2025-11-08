use async_trait::async_trait;
use once_cell::sync::Lazy;
use std::sync::RwLock;

/// Trait for managing test database containers
#[async_trait]
pub trait DatabaseContainer: Send + Sync {
    /// Get the connection string for this database
    async fn get_dsn(&self) -> String;

    /// Get the container ID for cleanup purposes
    fn get_container_id(&self) -> Option<String>;

    /// Setup the database (install schema, etc.)
    async fn setup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>>;

    /// Cleanup the database (uninstall schema, etc.)
    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>>;

    /// Stop the container gracefully
    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Container manager that handles the lifecycle of test database containers
pub struct ContainerManager {
    container: Box<dyn DatabaseContainer>,
    dsn: Option<String>,
}

impl ContainerManager {
    pub fn new(container: Box<dyn DatabaseContainer>) -> Self {
        Self {
            container,
            dsn: None,
        }
    }

    pub async fn initialize(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        if self.dsn.is_none() {
            let dsn = self.container.get_dsn().await;
            self.container.setup_database(dsn.clone()).await?;
            self.dsn = Some(dsn.clone());
            println!("Database initialized with DSN: {}", dsn);
        }
        Ok(self.dsn.as_ref().unwrap().clone())
    }

    pub async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(dsn) = &self.dsn {
            // Always cleanup schema first
            if let Err(e) = self.container.cleanup_database(dsn.clone()).await {
                eprintln!("Warning: Failed to cleanup database schema: {}", e);
            }

            // Stop container if it exists
            if self.container.get_container_id().is_some() {
                if let Err(e) = self.container.stop_container().await {
                    eprintln!("Warning: Failed to stop container: {}", e);
                }
            }
        }
        Ok(())
    }

    pub fn get_dsn(&self) -> Option<&String> {
        self.dsn.as_ref()
    }
}

/// Global container manager
static CONTAINER_MANAGER: Lazy<RwLock<Option<ContainerManager>>> = Lazy::new(|| RwLock::new(None));

/// Initialize the global container manager with the appropriate database type
pub async fn initialize_database(schema: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    // First, check if we already have an initialized manager (read lock only)
    {
        let manager_guard = CONTAINER_MANAGER.read().unwrap();
        if let Some(manager) = manager_guard.as_ref() {
            return Ok(manager.get_dsn().unwrap().clone());
        }
    } // Release read lock immediately

    // Try to acquire write lock - only initialize if we successfully get it AND there's no existing manager
    let mut manager_guard = CONTAINER_MANAGER.write().unwrap();

    // Double-check: ensure no other thread initialized while we were waiting for write lock
    if let Some(manager) = manager_guard.as_ref() {
        return Ok(manager.get_dsn().unwrap().clone());
    }

    // We have the write lock and no existing manager - do initialization
    let dsn = if let Some(external_dsn) = std::env::var("PGQRS_TEST_DSN").ok() {
        // For external containers, we must do initialization while holding the lock
        // to prevent concurrent schema installation
        let container: Box<dyn DatabaseContainer> = Box::new(
            crate::common::postgres::ExternalPostgresContainer::new(external_dsn, schema),
        );
        let mut manager = ContainerManager::new(container);
        let dsn = manager.initialize().await?;

        // Store the initialized manager
        *manager_guard = Some(manager);
        dsn
    } else {
        // For TestContainers, we can safely release the lock and do initialization normally
        let container: Box<dyn DatabaseContainer> =
            if std::env::var("PGQRS_TEST_USE_PGBOUNCER").is_ok() {
                Box::new(crate::common::pgbouncer::PgBouncerContainer::new().await?)
            } else {
                Box::new(crate::common::postgres::PostgresContainer::new(schema).await?)
            };

        let mut manager = ContainerManager::new(container);
        let dsn = manager.initialize().await?;

        // Store the initialized manager
        *manager_guard = Some(manager);
        dsn
    };

    Ok(dsn)
}

/// Get the DSN for the initialized database
pub async fn get_database_dsn(schema: Option<&str>) -> String {
    initialize_database(schema)
        .await
        .expect("Failed to initialize database")
}

/// Cleanup function called by dtor
pub async fn cleanup_database() -> Result<(), Box<dyn std::error::Error>> {
    let manager_guard = CONTAINER_MANAGER.read().unwrap();
    if let Some(manager) = manager_guard.as_ref() {
        manager.cleanup().await?;
    }
    Ok(())
}
