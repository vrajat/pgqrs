use async_trait::async_trait;
use pgqrs::admin::PgqrsAdmin;
use sqlx::postgres::PgPoolOptions;
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
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_schema(None).await
    }

    pub async fn new_with_schema(schema: Option<&str>) -> Result<Self, Box<dyn std::error::Error>> {
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
        // Test the connection
        {
            let pool = PgPoolOptions::new()
                .max_connections(MAX_CONNECTIONS)
                .acquire_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT_SECS))
                .connect(&dsn)
                .await?;

            let _val: i32 = sqlx::query_scalar(VERIFICATION_QUERY)
                .fetch_one(&pool)
                .await?;
            println!("PostgreSQL connection verified");

            // Create custom schema if not using 'public'
            if self.schema != "public" {
                let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
                sqlx::query(&create_schema_sql).execute(&pool).await?;
                println!("Schema '{}' created", self.schema);
            }
        }

        // Install schema using the configured schema
        let config = if self.schema == "public" {
            pgqrs::config::Config::from_dsn(dsn)
        } else {
            pgqrs::config::Config::from_dsn_with_schema(dsn, &self.schema)?
        };
        let admin = PgqrsAdmin::new(&config).await?;
        admin.install().await?;
        println!("PostgreSQL schema installed in '{}'", self.schema);

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        let admin = PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(dsn)).await?;
        admin.uninstall(&self.schema).await?;
        println!("PostgreSQL schema uninstalled from '{}'", self.schema);
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
    schema: String,
}

impl ExternalPostgresContainer {
    pub fn new(dsn: String) -> Self {
        Self::new_with_schema(dsn, None)
    }

    pub fn new_with_schema(dsn: String, schema: Option<&str>) -> Self {
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
        // Test the connection
        {
            let pool = PgPoolOptions::new()
                .max_connections(MAX_CONNECTIONS)
                .acquire_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT_SECS))
                .connect(&dsn)
                .await?;

            let _val: i32 = sqlx::query_scalar(VERIFICATION_QUERY)
                .fetch_one(&pool)
                .await?;
            println!("External PostgreSQL connection verified");

            // Create custom schema if not using 'public'
            if self.schema != "public" {
                let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
                sqlx::query(&create_schema_sql).execute(&pool).await?;
                println!("Schema '{}' created", self.schema);
            }
        }

        // Install schema using the configured schema
        let config = if self.schema == "public" {
            pgqrs::config::Config::from_dsn(dsn)
        } else {
            pgqrs::config::Config::from_dsn_with_schema(dsn, &self.schema)?
        };
        let admin = PgqrsAdmin::new(&config).await?;
        admin.install().await?;
        println!("External PostgreSQL schema installed in '{}'", self.schema);

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        let admin = PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(dsn)).await?;
        admin.uninstall(&self.schema).await?;
        println!(
            "External PostgreSQL schema uninstalled from '{}'",
            self.schema
        );
        Ok(())
    }

    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("External PostgreSQL database, not stopping container");
        Ok(())
    }
}
