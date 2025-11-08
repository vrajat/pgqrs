use async_trait::async_trait;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

use super::constants::*;
use super::container::DatabaseContainer;

/// PostgreSQL testcontainer implementation
pub struct PostgresContainer {
    container: ContainerAsync<Postgres>,
    dsn: String,
    schema: String,
}

impl PostgresContainer {
    pub async fn new(schema: Option<&str>) -> Result<Self, Box<dyn std::error::Error>> {
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

        let schema_name = schema.unwrap_or("public").to_string();

        println!("PostgreSQL container started");
        println!("Database URL: {}", dsn);
        println!("Schema: {}", schema_name);

        Ok(Self {
            container,
            dsn,
            schema: schema_name,
        })
    }
}

#[async_trait]
impl DatabaseContainer for PostgresContainer {
    async fn get_dsn(&self) -> String {
        self.dsn.clone()
    }

    fn get_container_id(&self) -> Option<String> {
        Some(self.container.id().to_string())
    }

    async fn setup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        super::database_setup::setup_database_common(dsn, &self.schema, "PostgreSQL").await
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        super::database_setup::cleanup_database_common(dsn, &self.schema, "PostgreSQL").await
    }

    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>> {
        let id = self.container.id();
        println!("Stopping PostgreSQL container with ID: {}", id);

        // Try graceful stop first
        match self.container.stop().await {
            Ok(_) => {
                println!("PostgreSQL container stopped gracefully");
                Ok(())
            }
            Err(e) => {
                println!("Failed to stop container: {}", e);
                // Container will be stopped automatically on drop
                Ok(())
            }
        }
    }
}

/// External PostgreSQL database implementation
pub struct ExternalPostgresContainer {
    dsn: String,
    schema: String,
}

impl ExternalPostgresContainer {
    pub fn new(dsn: String, schema: Option<&str>) -> Self {
        let schema_name = schema.unwrap_or("public").to_string();
        println!(
            "Using external PostgreSQL database: {} with schema: {}",
            dsn, schema_name
        );
        Self {
            dsn,
            schema: schema_name,
        }
    }
}

#[async_trait]
impl DatabaseContainer for ExternalPostgresContainer {
    async fn get_dsn(&self) -> String {
        self.dsn.clone()
    }

    fn get_container_id(&self) -> Option<String> {
        None // External database, no container to manage
    }

    async fn setup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        super::database_setup::setup_database_common(dsn, &self.schema, "External PostgreSQL").await
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        super::database_setup::cleanup_database_common(dsn, &self.schema, "External PostgreSQL")
            .await
    }

    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("External PostgreSQL database, not stopping container");
        Ok(())
    }
}
