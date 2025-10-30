use async_trait::async_trait;
use pgqrs::admin::PgqrsAdmin;
use sqlx;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

use super::container::DatabaseContainer;

// PgBouncer container configuration
const PGBOUNCER_IMAGE: &str = "edoburu/pgbouncer";
const PGBOUNCER_VERSION: &str = "1.20.1";

/// PgBouncer + PostgreSQL container setup
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
            .with_db_name("test_db")
            .with_user("test_user")
            .with_password("test_password");

        let postgres_container = postgres_image.start().await?;
        let postgres_port = postgres_container.get_host_port_ipv4(5432).await?;

        let postgres_dsn = format!(
            "postgresql://test_user:test_password@127.0.0.1:{}/test_db",
            postgres_port
        );

        println!("PostgreSQL container started for PgBouncer backend");
        println!("PostgreSQL URL: {}", postgres_dsn);

        // Wait for PostgreSQL to be ready
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Test PostgreSQL connection first
        println!("Testing PostgreSQL connection...");
        let pool = sqlx::PgPool::connect(&postgres_dsn).await?;
        let _: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&pool).await?;
        pool.close().await;
        println!("PostgreSQL connection verified");

        // Start PgBouncer container
        println!("Starting PgBouncer container...");
        // Try to get the host IP that Docker containers can reach
        let host_ip = std::process::Command::new("docker")
            .args(&[
                "network",
                "inspect",
                "bridge",
                "--format",
                "{{(index .IPAM.Config 0).Gateway}}",
            ])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .unwrap_or_else(|_| "172.17.0.1".to_string()); // fallback to common Docker bridge IP

        println!("Using host IP for PgBouncer: {}", host_ip);
        let database_url = format!(
            "postgres://test_user:test_password@{}:{}/test_db",
            host_ip, postgres_port
        );

        let pgbouncer_image = GenericImage::new(PGBOUNCER_IMAGE, PGBOUNCER_VERSION)
            .with_env_var("DATABASE_URL", &database_url)
            .with_env_var("POOL_MODE", "session") // Use session mode for better compatibility
            .with_env_var("AUTH_TYPE", "md5") // Use md5 auth like PostgreSQL
            .with_env_var("MAX_CLIENT_CONN", "100")
            .with_env_var("DEFAULT_POOL_SIZE", "20")
            .with_env_var("ADMIN_USERS", "test_user")
            .with_env_var("STATS_USERS", "test_user");

        let pgbouncer_container = pgbouncer_image.start().await?;
        let pgbouncer_port = pgbouncer_container.get_host_port_ipv4(5432).await?;

        let dsn = format!(
            "postgresql://test_user:test_password@127.0.0.1:{}/test_db",
            pgbouncer_port
        );

        println!("PgBouncer container started");
        println!("PgBouncer URL: {}", dsn);

        // Wait for PgBouncer to be ready and test connection
        println!("Waiting for PgBouncer to be ready...");
        for attempt in 1..=10 {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            match sqlx::PgPool::connect(&dsn).await {
                Ok(pool) => {
                    match sqlx::query_scalar::<_, i32>("SELECT 1")
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
                            if attempt == 10 {
                                return Err(format!(
                                    "PgBouncer connection failed after 10 attempts: {}",
                                    e
                                )
                                .into());
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("PgBouncer connection failed on attempt {}: {}", attempt, e);
                    if attempt == 10 {
                        return Err(format!(
                            "PgBouncer connection failed after 10 attempts: {}",
                            e
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
impl DatabaseContainer for PgBouncerContainer {
    async fn get_dsn(&self) -> String {
        self.dsn.clone()
    }

    fn get_container_id(&self) -> Option<String> {
        // Return the PgBouncer container ID since that's the main connection point
        Some(self.pgbouncer_container.id().to_string())
    }

    async fn setup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        println!("Setting up admin schema via PgBouncer...");

        // Install schema via PgBouncer
        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.install().await?;
        println!("Admin schema setup complete via PgBouncer");

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        println!("Cleaning up admin schema via PgBouncer...");

        let admin = PgqrsAdmin::new(&pgqrs::config::Config {
            dsn,
            ..Default::default()
        })
        .await?;
        admin.uninstall().await?;
        println!("Admin schema cleanup complete via PgBouncer");
        Ok(())
    }

    async fn stop_container(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Stopping PgBouncer container...");

        // Stop PgBouncer container first
        if let Err(e) = self.pgbouncer_container.stop().await {
            eprintln!(
                "Error stopping PgBouncer container via testcontainers: {}",
                e
            );
        }

        println!("Stopping PostgreSQL container (PgBouncer backend)...");

        // Stop PostgreSQL container
        if let Err(e) = self.postgres_container.stop().await {
            eprintln!(
                "Error stopping PostgreSQL container via testcontainers: {}",
                e
            );
        }

        println!("PgBouncer and PostgreSQL containers stopped");
        Ok(())
    }
}
