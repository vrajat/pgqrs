use clap::{Parser, Subcommand};

mod cli {
    pub mod admin;
    pub mod sql_worker;
}

#[derive(Parser, Debug)]
#[command(name = "pgqrs")]
#[command(about = "pgqrs command line utility", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the pgqrs-admin coordinator daemon
    Admin {
        /// Database DSN (can also be set via PGQRS_DSN or DATABASE_URL env vars)
        #[arg(long)]
        dsn: Option<String>,

        /// Database schema namespace
        #[arg(long, default_value = "public", env = "PGQRS_SCHEMA")]
        schema: String,

        /// Check interval in milliseconds for schedules and sweeps
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,

        /// Stale worker heartbeat timeout in seconds
        #[arg(long, default_value_t = 30)]
        heartbeat_timeout_secs: i64,

        /// Workflow run execution timeout in seconds
        #[arg(long, default_value_t = 3600)]
        workflow_timeout_secs: i64,
    },

    /// Start the pgqrs-sql-worker external SQL executor daemon
    SqlWorker {
        /// Database DSN (can also be set via PGQRS_DSN or DATABASE_URL env vars)
        #[arg(long)]
        dsn: Option<String>,

        /// Database schema namespace
        #[arg(long, default_value = "public", env = "PGQRS_SCHEMA")]
        schema: String,

        /// Comma-separated list of queues to poll
        #[arg(long, value_delimiter = ',', required = true)]
        queues: Vec<String>,

        /// Polling interval in milliseconds
        #[arg(long, default_value_t = 250)]
        interval_ms: u64,

        /// Unique name for this worker (defaults to auto-generated UUID)
        #[arg(long)]
        worker_name: Option<String>,
    },

    /// Setup test schemas for integration tests
    SetupTestSchemas {
        /// Database DSN (can also be set via PGQRS_TEST_DSN env var)
        #[arg(long)]
        dsn: Option<String>,

        /// Clean up (drop) all test schemas instead of provisioning them
        #[arg(long)]
        cleanup: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Admin {
            dsn,
            schema,
            interval_ms,
            heartbeat_timeout_secs,
            workflow_timeout_secs,
        } => {
            let dsn = dsn
                .or_else(|| std::env::var("PGQRS_DSN").ok())
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or("Database DSN is required. Set via --dsn argument, PGQRS_DSN env var, or DATABASE_URL env var.")?;

            cli::admin::run(
                dsn,
                schema,
                interval_ms,
                heartbeat_timeout_secs,
                workflow_timeout_secs,
            )
            .await?;
        }
        Commands::SqlWorker {
            dsn,
            schema,
            queues,
            interval_ms,
            worker_name,
        } => {
            let dsn = dsn
                .or_else(|| std::env::var("PGQRS_DSN").ok())
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or("Database DSN is required. Set via --dsn argument, PGQRS_DSN env var, or DATABASE_URL env var.")?;

            let worker_name =
                worker_name.unwrap_or_else(|| format!("sql-worker-{}", uuid::Uuid::new_v4()));

            cli::sql_worker::run(dsn, schema, queues, interval_ms, worker_name).await?;
        }
        Commands::SetupTestSchemas { dsn, cleanup } => {
            #[cfg(any(test, feature = "test-utils"))]
            {
                let dsn = dsn
                    .or_else(|| std::env::var("PGQRS_TEST_DSN").ok())
                    .unwrap_or_else(|| {
                        "postgres://postgres:postgres@localhost:5432/postgres".to_string()
                    });

                pgqrs::test_utils::run_postgres_schema_setup(&dsn, cleanup).await?;
            }
            #[cfg(not(any(test, feature = "test-utils")))]
            {
                let _dsn = dsn;
                let _cleanup = cleanup;
                return Err(
                    "setup-test-schemas subcommand requires the 'test-utils' feature to be enabled"
                        .into(),
                );
            }
        }
    }
    Ok(())
}
