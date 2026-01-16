use async_trait::async_trait;
use sqlx;
use std::sync::{Mutex, RwLock};
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

use super::constants::*;
use super::resource::TestResource;

/// PgBouncer + PostgreSQL resource wrapper
pub struct PgBouncerResource {
    container: RwLock<Option<PgBouncerContainer>>,
    schema: Mutex<Option<String>>,
}

impl PgBouncerResource {
    pub fn new() -> Self {
        Self {
            container: RwLock::new(None),
            schema: Mutex::new(None),
        }
    }
}

pub struct PgBouncerContainer {
    postgres_container: ContainerAsync<Postgres>,
    pgbouncer_container: ContainerAsync<GenericImage>,
    dsn: String,
}

impl PgBouncerContainer {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // First start PostgreSQL
        println!("Starting PostgreSQL container for PgBouncer...");
        let postgres_image = Postgres::default()
            .with_db_name(TEST_DB_NAME)
            .with_user(TEST_DB_USER)
            .with_password(TEST_DB_PASSWORD);

        let postgres_container = postgres_image.start().await?;

        let postgres_dsn = format!(
            "postgres://{}:{}@{}:{}/{}",
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            postgres_container.get_host().await?,
            postgres_container.get_host_port_ipv4(POSTGRES_PORT).await?,
            TEST_DB_NAME
        );

        println!("PostgreSQL container started for PgBouncer backend");
        println!("PostgreSQL URL: {}", postgres_dsn);

        // Test PostgreSQL connection first
        println!("Testing PostgreSQL connection...");
        let pool = sqlx::PgPool::connect(&postgres_dsn).await?;
        let _: i32 = sqlx::query_scalar(VERIFICATION_QUERY)
            .fetch_one(&pool)
            .await?;
        pool.close().await;
        println!("PostgreSQL connection verified");

        // Start PgBouncer container
        println!("Starting PgBouncer container...");

        // Get the host IP that Docker containers can reach
        let host_ip = std::process::Command::new("docker")
            .args([
                "network",
                "inspect",
                "bridge",
                "--format",
                "{{(index .IPAM.Config 0).Gateway}}",
            ])
            .output()
            .map_err(|e| format!("Failed to execute docker command: {}", e))?
            .stdout;

        let host_ip = String::from_utf8(host_ip)
            .map_err(|e| format!("Invalid UTF-8 in docker output: {}", e))?
            .trim()
            .to_string();

        if host_ip.is_empty() {
            return Err("Docker bridge gateway IP is empty".into());
        }

        println!("Using host IP for PgBouncer: {}", host_ip);
        let database_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            host_ip,
            postgres_container.get_host_port_ipv4(POSTGRES_PORT).await?,
            TEST_DB_NAME
        );

        let pgbouncer_image = GenericImage::new(PGBOUNCER_IMAGE, PGBOUNCER_VERSION)
            .with_env_var("DATABASE_URL", &database_url)
            .with_env_var("POOL_MODE", PGBOUNCER_POOL_MODE)
            .with_env_var("AUTH_TYPE", PGBOUNCER_AUTH_TYPE)
            .with_env_var("MAX_CLIENT_CONN", PGBOUNCER_MAX_CLIENT_CONN)
            .with_env_var("DEFAULT_POOL_SIZE", PGBOUNCER_DEFAULT_POOL_SIZE)
            .with_env_var("ADMIN_USERS", TEST_DB_USER)
            .with_env_var("STATS_USERS", TEST_DB_USER);

        let pgbouncer_container = pgbouncer_image.start().await?;

        let dsn = format!(
            "postgres://{}:{}@{}:{}/{}",
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            pgbouncer_container.get_host().await?,
            pgbouncer_container
                .get_host_port_ipv4(POSTGRES_PORT)
                .await?,
            TEST_DB_NAME
        );

        println!("PgBouncer container started");
        println!("PgBouncer URL: {}", dsn);

        // Wait for PgBouncer to be ready and test connection
        println!("Waiting for PgBouncer to be ready...");
        for attempt in 1..=PGBOUNCER_READY_MAX_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_secs(PGBOUNCER_RETRY_DELAY_SECS)).await;

            match sqlx::PgPool::connect(&dsn).await {
                Ok(pool) => {
                    match sqlx::query_scalar::<_, i32>(VERIFICATION_QUERY)
                        .fetch_one(&pool)
                        .await
                    {
                        Ok(_) => {
                            pool.close().await;
                            println!("PgBouncer connection verified on attempt {}", attempt);
                            break;
                        }
                        Err(e) => {
                            pool.close().await;
                            println!("PgBouncer query failed on attempt {}: {}", attempt, e);
                            if attempt == PGBOUNCER_READY_MAX_ATTEMPTS {
                                return Err(format!(
                                    "PgBouncer connection failed after {} attempts: {}",
                                    PGBOUNCER_READY_MAX_ATTEMPTS, e
                                )
                                .into());
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("PgBouncer connection failed on attempt {}: {}", attempt, e);
                    if attempt == PGBOUNCER_READY_MAX_ATTEMPTS {
                        return Err(format!(
                            "PgBouncer connection failed after {} attempts: {}",
                            PGBOUNCER_READY_MAX_ATTEMPTS, e
                        )
                        .into());
                    }
                }
            }
        }

        Ok(Self {
            postgres_container,
            pgbouncer_container,
            dsn,
        })
    }
}

#[async_trait]
impl TestResource for PgBouncerResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        let container = PgBouncerContainer::new().await?;
        let mut guard = self.container.write().unwrap();
        *guard = Some(container);
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let dsn = {
            let guard = self.container.read().unwrap();
            guard
                .as_ref()
                .expect("PgBouncerResource not initialized")
                .dsn
                .clone()
        };

        if let Some(s) = schema {
            super::database_setup::setup_database_common(dsn.clone(), s, "PgBouncer")
                .await
                .expect("Failed to setup database schema");

            if s != "public" {
                let mut guard = self.schema.lock().unwrap();
                *guard = Some(s.to_string());
            }
        }
        dsn
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let container_opt = {
            let mut guard = self.container.write().unwrap();
            guard.take()
        };

        if let Some(c) = container_opt {
            // Cleanup schema
            let schema_opt = {
                let mut guard = self.schema.lock().unwrap();
                guard.take()
            };

            if let Some(schema) = schema_opt {
                let _ = super::database_setup::cleanup_database_common(
                    c.dsn.clone(),
                    &schema,
                    "PgBouncer",
                )
                .await;
            }

            println!("Stopping PgBouncer container...");
            let _ = c.pgbouncer_container.stop().await;
            println!("Stopping PostgreSQL container...");
            let _ = c.postgres_container.stop().await;
            println!("Stopped.");
        }
        Ok(())
    }
}

pub struct ExternalPgBouncerResource {
    dsn: String,
    schema: Mutex<Option<String>>,
}

impl ExternalPgBouncerResource {
    pub fn new(dsn: String) -> Self {
        println!("Using external PgBouncer database: {}", dsn);
        Self {
            dsn,
            schema: Mutex::new(None),
        }
    }
}

#[async_trait]
impl TestResource for ExternalPgBouncerResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        // No-op for external
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        if let Some(s) = schema {
            crate::common::database_setup::setup_database_common(
                self.dsn.clone(),
                s,
                "External PgBouncer",
            )
            .await
            .expect("Failed to setup external database schema");

            if s != "public" {
                let mut guard = self.schema.lock().unwrap();
                *guard = Some(s.to_string());
            }
        }
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let schema_opt = {
            let mut guard = self.schema.lock().unwrap();
            guard.take()
        };

        if let Some(schema) = schema_opt {
            let _ = super::database_setup::cleanup_database_common(
                self.dsn.clone(),
                &schema,
                "External PgBouncer",
            )
            .await;
        }
        Ok(())
    }
}
