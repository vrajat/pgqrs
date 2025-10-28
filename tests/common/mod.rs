use pgqrs::admin::PgqrsAdmin;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

static CLEANUP_GUARD: OnceCell<CleanupGuard> = OnceCell::const_new();

#[derive(Debug)]
enum DbResource {
    Owned(Arc<ContainerAsync<Postgres>>),
    External,
}

#[derive(Debug)]
struct CleanupGuard {
    dsn: String,
    resource: DbResource,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        match &self.resource {
            DbResource::Owned(container) => {
                // Extract the async cleanup logic to avoid duplication
                let cleanup_future = async {
                    let admin = PgqrsAdmin::new(&pgqrs::config::Config {
                        dsn: self.dsn.clone(),
                        ..Default::default()
                    })
                    .await
                    .expect("Failed to create PgqrsAdmin for uninstall");
                    admin.uninstall().await.expect("Uninstall schema failed");
                    // Explicitly stop the container to ensure it is killed
                    let container = Arc::clone(container);
                    let _ = container.stop().await;
                };

                // Use try_current() with fallback to new runtime
                let _cleanup_result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.block_on(cleanup_future)
                } else {
                    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
                    rt.block_on(cleanup_future)
                };
                tracing::info!("Testcontainer stopped");
            }
            DbResource::External => {
                tracing::info!("External DB used, not stopping container");
            }
        }
    }
}

/// Get a PgqrsAdmin client connected to a PostgreSQL test database.
///
/// This function handles both external database connections (via PGQRS_TEST_DSN env var)
/// and automatically managed testcontainer databases. The database schema is automatically
/// installed and cleaned up when tests complete.
///
/// # Returns
/// A static reference to the PgqrsAdmin client that can be used for tests
pub async fn get_postgres_dsn() -> &'static String {
    let guard_ref = CLEANUP_GUARD
        .get_or_init(|| async {
            // Check for external DSN or start container
            let (database_url, resource) = if let Some(dsn) = std::env::var("PGQRS_TEST_DSN").ok() {
                println!("Using external database: {}", dsn);
                (dsn, DbResource::External)
            } else {
                let (database_url, container) = start_postgres_container().await;
                println!("Database URL: {}", database_url);
                // Wait for postgres to be ready
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                (database_url, DbResource::Owned(Arc::new(container)))
            };
            // Create connection pool
            let pool = PgPoolOptions::new()
                .max_connections(1) // Small pool per test
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(&database_url)
                .await
                .expect("Failed to connect to Postgres");
            // Test the connection
            {
                let _val: i32 = sqlx::query_scalar("SELECT 1")
                    .fetch_one(&pool)
                    .await
                    .expect("SELECT 1 failed");
                println!("Database connection verified");
            }

            let admin = PgqrsAdmin::new(&pgqrs::config::Config {
                dsn: database_url.clone(),
                ..Default::default()
            })
            .await
            .expect("Failed to create PgqrsAdmin");
            admin.install().await.expect("Failed to install schema");
            CleanupGuard {
                dsn: database_url.clone(),
                resource,
            }
        })
        .await;
    &guard_ref.dsn
}

/// Create a PostgreSQL testcontainer and return the database URL
///
/// This is a simpler alternative to get_pgqrs_client() when you just need
/// a database URL without the pgqrs schema installation.
///
/// # Returns
/// A tuple of (database_url, container_handle) for manual management
async fn start_postgres_container() -> (String, ContainerAsync<Postgres>) {
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

    println!("PostgreSQL container started: {}", database_url);

    // Wait for postgres to be ready
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    (database_url, container)
}
