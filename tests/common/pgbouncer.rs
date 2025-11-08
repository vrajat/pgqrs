use async_trait::async_trait;
use pgqrs::admin::PgqrsAdmin;
use sqlx;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

use super::constants::*;
use super::container::DatabaseContainer;

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
            .with_env_var("POOL_MODE", PGBOUNCER_POOL_MODE) // Use session mode for better compatibility
            .with_env_var("AUTH_TYPE", PGBOUNCER_AUTH_TYPE) // Use md5 auth like PostgreSQL
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
        let admin = PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(dsn)).await?;
        admin.install().await?;
        println!("Admin schema setup complete via PgBouncer");

        Ok(())
    }

    async fn cleanup_database(&self, dsn: String) -> Result<(), Box<dyn std::error::Error>> {
        println!("Cleaning up admin schema via PgBouncer...");

        let admin = PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(dsn)).await?;
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
