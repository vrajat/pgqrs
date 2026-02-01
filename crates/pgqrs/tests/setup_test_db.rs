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
    "pgqrs_workflow_test", // Added as it was likely missing from grep but implicit
    "pgqrs_workflow_retry_test", // Added as it was likely missing from grep but implicit
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = env::var(TEST_DB_DSN_ENV).unwrap_or_else(|_| DEFAULT_TEST_DSN.to_string());
    println!("Setting up test databases using DSN: {}", dsn);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await?;

    println!("Connected to database.");

    for schema in TEST_SCHEMAS {
        println!("Provisioning schema: {}", schema);

        // 1. Drop and Recreate Schema (Clean Slate for Suite)
        let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
        sqlx::query(&drop_sql).execute(&pool).await?;

        let create_sql = format!("CREATE SCHEMA \"{}\"", schema);
        sqlx::query(&create_sql).execute(&pool).await?;

        // 2. Install Migration
        // We need to connect to the specific schema to install invalid logic if any
        // But pgqrs::admin::install uses the pool, so we need a config that points to this schema
        // Or we can rely on search_path.

        // Let's use the explicit Config approach as used in tests
        let config = pgqrs::config::Config::from_dsn_with_schema(&dsn, *schema)?;
        let store = pgqrs::connect_with_config(&config).await?;

        pgqrs::admin(&store).install().await?;
        println!("  -> Installed pgqrs tables.");
    }

    println!("All test schemas provisioned successfully!");
    Ok(())
}
