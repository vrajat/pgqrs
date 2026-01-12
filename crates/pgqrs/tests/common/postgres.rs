use async_trait::async_trait;
use std::sync::RwLock;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

use super::constants::*;
use super::resource::TestResource;

/// PostgreSQL testcontainer implementation wrapper
pub struct PostgresResource {
    container: RwLock<Option<PostgresContainer>>,
}

impl PostgresResource {
    pub fn new() -> Self {
        Self {
            container: RwLock::new(None),
        }
    }
}

pub struct PostgresContainer {
    container: ContainerAsync<Postgres>,
    dsn: String,
}

impl PostgresContainer {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        println!("Starting PostgreSQL testcontainer...");

        let postgres_image = Postgres::default()
            .with_db_name(TEST_DB_NAME)
            .with_user(TEST_DB_USER)
            .with_password(TEST_DB_PASSWORD);

        let container = postgres_image.start().await?;

        let dsn = format!(
            "postgres://{}:{}@{}:{}/{}",
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            container.get_host().await?,
            container.get_host_port_ipv4(POSTGRES_PORT).await?,
            TEST_DB_NAME
        );

        println!("PostgreSQL container started");
        println!("Database URL: {}", dsn);

        Ok(Self { container, dsn })
    }
}

#[async_trait]
impl TestResource for PostgresResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        let container = PostgresContainer::new().await?;
        let mut guard = self.container.write().unwrap();
        *guard = Some(container);
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let dsn = {
            let guard = self.container.read().unwrap();
            guard
                .as_ref()
                .expect("PostgresResource not initialized")
                .dsn
                .clone()
        };

        if let Some(s) = schema {
            super::database_setup::setup_database_common(dsn.clone(), s, "PostgreSQL")
                .await
                .expect("Failed to setup database schema");
        }
        dsn
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let container_opt = {
            let mut guard = self.container.write().unwrap();
            guard.take()
        };

        if let Some(c) = container_opt {
            println!("Stopping PostgreSQL container...");
            let _ = c.container.stop().await;
            println!("Stopped.");
        }
        Ok(())
    }
}

/// External PostgreSQL database implementation
pub struct ExternalPostgresResource {
    dsn: String,
}

impl ExternalPostgresResource {
    pub fn new(dsn: String) -> Self {
        println!("Using external PostgreSQL database: {}", dsn);
        Self { dsn }
    }
}

#[async_trait]
impl TestResource for ExternalPostgresResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        // No-op for external DB
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        if let Some(s) = schema {
            super::database_setup::setup_database_common(
                self.dsn.clone(),
                s,
                "External PostgreSQL",
            )
            .await
            .expect("Failed to setup external database schema");
        }
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("External PostgreSQL database, not stopping container");
        Ok(())
    }
}
