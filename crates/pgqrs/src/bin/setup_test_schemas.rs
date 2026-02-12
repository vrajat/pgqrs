use sqlx::postgres::PgPoolOptions;
use std::env;

const TEST_DB_DSN_ENV: &str = "PGQRS_TEST_DSN";
const DEFAULT_TEST_DSN: &str = "postgres://postgres:postgres@localhost:5432/postgres";

// List of all schemas used in tests (derived from grep analysis)
const TEST_SCHEMAS: &[&str] = &[
    "pgqrs_builder_test",
    "pgqrs_builder_ergonomics_test",
    "pgqrs_concurrent_test",
    "pgqrs_error_test",
    "pgqrs_anystore_test",
    "pgqrs_anystore_dsn_test",
    "pgqrs_lib_test",
    "pgqrs_zombie_tests",
    "pgqrs_lib_stat_test",
    "pgqrs_pgbouncer_test",
    "pgqrs_cli_test",
    "pgqrs_worker_test",
    "pgqrs_workflow_test",
    "pgqrs_workflow_creation_test",
    "pgqrs_workflow_retry_test",
    // Workflow retry integration test schemas
    "workflow_retry_not_ready",
    "workflow_retry_becomes_ready",
    "workflow_retry_exhaust",
    "workflow_retry_non_transient",
    "workflow_retry_at_future",
    "workflow_retry_count",
    "workflow_retry_custom_delay",
    "workflow_retry_running_state",
    "workflow_retry_wrapping",
    "workflow_retry_concurrent",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let cleanup_mode = args.get(1).map(|s| s.as_str()) == Some("--cleanup");

    let dsn = env::var(TEST_DB_DSN_ENV).unwrap_or_else(|_| DEFAULT_TEST_DSN.to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await?;

    if cleanup_mode {
        println!("Cleaning up test schemas using DSN: {}", dsn);

        for schema in TEST_SCHEMAS {
            println!("Dropping schema: {}", schema);
            let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
            sqlx::query(&drop_sql).execute(&pool).await?;
        }

        println!("All test schemas cleaned up successfully!");
    } else {
        println!("Setting up test databases using DSN: {}", dsn);
        println!("Connected to database.");

        for schema in TEST_SCHEMAS {
            println!("Provisioning schema: {}", schema);

            // 1. Drop and Recreate Schema (Clean Slate for Suite)
            let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
            sqlx::query(&drop_sql).execute(&pool).await?;

            let create_sql = format!("CREATE SCHEMA \"{}\"", schema);
            sqlx::query(&create_sql).execute(&pool).await?;

            // 2. Install Migration
            // We rely on search_path to install tables into the new schema
            let config = pgqrs::config::Config::from_dsn_with_schema(&dsn, *schema)?;
            let store = pgqrs::connect_with_config(&config).await?;

            pgqrs::admin(&store).install().await?;
            println!("  -> Installed pgqrs tables.");
        }

        println!("All test schemas provisioned successfully!");
    }

    Ok(())
}
