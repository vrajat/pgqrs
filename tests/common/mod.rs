use ctor::dtor;
use once_cell::sync::Lazy;
use pgqrs::admin::PgqrsAdmin;
use sqlx::postgres::PgPoolOptions;
use std::sync::RwLock;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

static POSTGRES_CONTAINER: Lazy<RwLock<Option<ContainerAsync<Postgres>>>> = Lazy::new(|| {
    RwLock::new(None) // Initialize with None
});

static DSN: Lazy<RwLock<Option<String>>> = Lazy::new(|| RwLock::new(None));

/// Setup database connection, verify it works, and install schema
async fn setup_database(dsn: &str) {
    // Test the connection and setup schema
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(dsn)
        .await
        .expect("Failed to connect to Postgres");

    let _val: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("SELECT 1 failed");
    println!("Database connection verified");

    let admin = PgqrsAdmin::new(&pgqrs::config::Config {
        dsn: dsn.to_string(),
        ..Default::default()
    })
    .await
    .expect("Failed to create PgqrsAdmin");
    admin.install().await.expect("Failed to install schema");
}

async fn init_postgres() {
    let mut instance = POSTGRES_CONTAINER.write().unwrap();

    if instance.is_none() {
        // Check for external DSN first
        if let Some(dsn) = std::env::var("PGQRS_TEST_DSN").ok() {
            println!("Using external database: {}", dsn);
            setup_database(&dsn).await;
            *DSN.write().unwrap() = Some(dsn);
        } else {
            // Start container and get DSN
            let (container, database_url) = start_postgres_container().await;
            setup_database(&database_url).await;
            *DSN.write().unwrap() = Some(database_url);
            *instance = Some(container);
        }
    }
}

#[dtor]
fn drop_postgres() {
    // Always cleanup schema first if we have a DSN
    if let Some(dsn) = DSN.read().unwrap().as_ref() {
        // Create a simple runtime for cleanup
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            if let Ok(admin) = PgqrsAdmin::new(&pgqrs::config::Config {
                dsn: dsn.clone(),
                ..Default::default()
            })
            .await
            {
                let _ = admin.uninstall().await;
                println!("Schema uninstalled");
            }
        });
    }

    let container = POSTGRES_CONTAINER.read().unwrap();

    if let Some(container_ref) = container.as_ref() {
        let id = container_ref.id();
        println!("Stopping container with ID: {}", id);

        // Try to stop the container gracefully first
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let stop_result = rt.block_on(async { container_ref.stop().await });

        if stop_result.is_ok() {
            println!("Container stopped gracefully");
        } else {
            println!("Graceful stop failed, using docker commands");

            std::process::Command::new("docker")
                .arg("kill")
                .arg(id)
                .output()
                .expect("failed to kill container");

            std::process::Command::new("docker")
                .arg("rm")
                .arg(id)
                .output()
                .expect("failed to remove container");

            println!("Container stopped and removed via docker commands");
        }
    } else {
        println!("External DB used, not stopping container");
    }
}

/// Get a PostgreSQL DSN for testing.
///
/// This function handles both external database connections (via PGQRS_TEST_DSN env var)
/// and automatically managed testcontainer databases. The database schema is automatically
/// installed and cleaned up when tests complete.
///
/// # Returns
/// A string reference to the database DSN that can be used for tests
pub async fn get_postgres_dsn() -> &'static str {
    init_postgres().await;

    let dsn = DSN.read().unwrap();
    let dsn_ref = dsn.as_ref().unwrap();

    // We need to leak the string to get a static reference
    Box::leak(dsn_ref.clone().into_boxed_str())
}

/// Create a PostgreSQL testcontainer and return the container handle and DSN
///
/// # Returns
/// A tuple of (container_handle, database_url) for the PostgreSQL container
async fn start_postgres_container() -> (ContainerAsync<Postgres>, String) {
    println!("Starting PostgreSQL testcontainer...");

    let postgres_image = Postgres::default()
        .with_db_name("test_db")
        .with_user("test_user")
        .with_password("test_password");

    let container = postgres_image
        .start()
        .await
        .expect("Failed to start postgres");

    let database_url = format!(
        "postgresql://test_user:test_password@127.0.0.1:{}/test_db",
        container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port")
    );

    println!("PostgreSQL container started");
    println!("Database URL: {}", database_url);

    // Wait for postgres to be ready
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    (container, database_url)
}
