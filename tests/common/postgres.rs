use async_trait::async_trait;
use pgqrs::admin::PgqrsAdmin;
use sqlx::postgres::PgPoolOptions;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

use super::container::DatabaseContainer;

/// PostgreSQL testcontainer implementation
pub struct PostgresContainer {
    container: ContainerAsync<Postgres>,
    dsn: String,
}

impl PostgresContainer {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        println!("Starting PostgreSQL testcontainer...");

        let postgres_image = Postgres::default()
            .with_db_name("test_db")
            .with_user("test_user")
            .with_password("test_password");

        let container = postgres_image.start().await?;

        let dsn = format!(
            "postgres://test_user:test_password@{}:{}/test_db",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?
        );

        println!("PostgreSQL container started");
        println!("Database URL: {}", dsn);

        Ok(Self { container, dsn })
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
        // Test the connection
        {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(&dsn)
                .await?;

            let _val: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&pool).await?;
            println!("PostgreSQL connection verified");
        }

        // Install schema
        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.install().await?;
        println!("PostgreSQL schema installed");

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.uninstall().await?;
        println!("PostgreSQL schema uninstalled");
        Ok(())
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
}

impl ExternalPostgresContainer {
    pub fn new(dsn: String) -> Self {
        println!("Using external PostgreSQL database: {}", dsn);
        Self { dsn }
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
        // Test the connection
        {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(&dsn)
                .await?;

            let _val: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&pool).await?;
            println!("External PostgreSQL connection verified");
        }

        // Install schema
        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.install().await?;
        println!("External PostgreSQL schema installed");

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.uninstall().await?;
        println!("External PostgreSQL schema uninstalled");
        Ok(())
    }

    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("External PostgreSQL database, not stopping container");
        Ok(())
    }
}
