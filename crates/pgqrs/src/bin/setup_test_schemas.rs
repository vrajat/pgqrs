use std::env;

#[cfg(feature = "s3")]
use futures_util::TryStreamExt;
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "s3")]
use object_store::ObjectStore;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;

#[cfg(feature = "postgres")]
const TEST_DB_DSN_ENV: &str = "PGQRS_TEST_DSN";
#[cfg(feature = "postgres")]
const DEFAULT_TEST_DSN: &str = "postgres://postgres:postgres@localhost:5432/postgres";
#[cfg(feature = "s3")]
const DEFAULT_S3_ENDPOINT: &str = "http://localhost:4566";
#[cfg(feature = "s3")]
const DEFAULT_S3_REGION: &str = "us-east-1";
#[cfg(feature = "s3")]
const DEFAULT_S3_BUCKET: &str = "pgqrs-test-bucket";

// List of all schemas used in tests (derived from grep analysis)
#[cfg(feature = "postgres")]
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
    "macro_test_creation",
    "macro_test_success",
    "macro_test_idempotency",
    "macro_test_step_failure",
    "macro_test_workflow_failure",
    "macro_test_run_metadata",
    "workflow_tests",
    "workflow_get_tests",
    "workflow_retrieval_tests",
    "workflow_polling_tests",
    "workflow_error_polling_tests",
    "workflow_fk_tests",
    "workflow_retry_integration_tests",
    "guide_tests",
];

#[cfg(feature = "postgres")]
async fn run_postgres_schema_setup(cleanup_mode: bool) -> Result<(), Box<dyn std::error::Error>> {
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

#[cfg(feature = "s3")]
async fn run_list_s3_sqlite_objects() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| DEFAULT_S3_ENDPOINT.to_string());
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| DEFAULT_S3_REGION.to_string());
    let bucket = env::var("PGQRS_S3_BUCKET").unwrap_or_else(|_| DEFAULT_S3_BUCKET.to_string());
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region)
        .with_virtual_hosted_style_request(false);
    if endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    if !endpoint.trim().is_empty() {
        builder = builder.with_endpoint(endpoint);
    }
    let store = builder.build()?;

    println!("Listing sqlite objects in bucket '{}'...", bucket);

    let mut sqlite_keys: Vec<String> = Vec::new();
    let mut stream = store.list(None);
    while let Some(meta) = stream.try_next().await? {
        let key = meta.location.to_string();
        if key.ends_with(".sqlite") {
            sqlite_keys.push(key);
        }
    }

    sqlite_keys.sort();

    if sqlite_keys.is_empty() {
        println!("No .sqlite objects found.");
        return Ok(());
    }

    println!("Found {} sqlite object(s):", sqlite_keys.len());
    for key in sqlite_keys {
        println!(" - {}", key);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let list_s3_sqlite = args.iter().any(|a| a == "--list-s3-sqlite");
    let cleanup_mode = args.get(1).map(|s| s.as_str()) == Some("--cleanup");

    if list_s3_sqlite {
        #[cfg(feature = "s3")]
        {
            return run_list_s3_sqlite_objects().await;
        }
        #[cfg(not(feature = "s3"))]
        {
            return Err("setup_test_schemas --list-s3-sqlite requires feature 's3'".into());
        }
    }

    #[cfg(feature = "postgres")]
    {
        return run_postgres_schema_setup(cleanup_mode).await;
    }

    #[cfg(not(feature = "postgres"))]
    {
        let _ = cleanup_mode;
        return Err("setup_test_schemas schema mode requires feature 'postgres'".into());
    }
}
