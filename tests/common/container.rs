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
    async fn setup_database(&self, dsn: &str) -> Result<(), Box<dyn std::error::Error>>;

    /// Cleanup the database (uninstall schema, etc.)
    async fn cleanup_database(&self, dsn: &str) -> Result<(), Box<dyn std::error::Error>>;

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
            self.container.setup_database(&dsn).await?;
            self.dsn = Some(dsn.clone());
            println!("Database initialized with DSN: {}", dsn);
        }
        Ok(self.dsn.as_ref().unwrap().clone())
    }

    pub async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(dsn) = &self.dsn {
            // Always cleanup schema first
            let _ = self.container.cleanup_database(dsn).await;

            // Stop container if it exists
            if self.container.get_container_id().is_some() {
                let _ = self.container.stop_container().await;
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
pub async fn initialize_database() -> Result<String, Box<dyn std::error::Error>> {
    // NOTE: Holding a write lock across async operations is generally not good practice
    // as it can block other threads. However, for test infrastructure where we need
    // to ensure only one container setup happens globally, this is acceptable.
    let mut manager_guard = CONTAINER_MANAGER.write().unwrap();

    if manager_guard.is_none() {
        let container: Box<dyn DatabaseContainer> =
            if std::env::var("PGQRS_TEST_USE_PGBOUNCER").is_ok() {
                Box::new(crate::common::pgbouncer::PgBouncerContainer::new().await?)
            } else if let Some(dsn) = std::env::var("PGQRS_TEST_DSN").ok() {
                Box::new(crate::common::postgres::ExternalPostgresContainer::new(dsn))
            } else {
                Box::new(crate::common::postgres::PostgresContainer::new().await?)
            };

        let mut manager = ContainerManager::new(container);
        let dsn = manager.initialize().await?;
        *manager_guard = Some(manager);
        Ok(dsn)
    } else {
        let manager = manager_guard.as_ref().unwrap();
        Ok(manager.get_dsn().unwrap().clone())
    }
}

/// Get the DSN for the initialized database
pub async fn get_database_dsn() -> String {
    initialize_database()
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
