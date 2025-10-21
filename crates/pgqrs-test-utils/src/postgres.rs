use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::RunQueryDsl;
use pgqrs::admin::PgqrsAdmin;
use std::sync::Arc;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

static CLEANUP_GUARD: OnceCell<CleanupGuard> = OnceCell::const_new();
static ADMIN: OnceCell<PgqrsAdmin> = OnceCell::const_new();

#[derive(Debug)]
enum DbResource {
    Owned(Arc<ContainerAsync<Postgres>>),
    External,
}

#[derive(Debug)]
struct CleanupGuard {
    resource: DbResource,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let admin = ADMIN.get().expect("Admin not initialized");
        admin.uninstall(false).expect("Failed to uninstall schema");
        match &self.resource {
            DbResource::Owned(container) => {
                // Explicitly stop the container to ensure it is killed
                // Note: ContainerAsync::stop is async, but Drop cannot be async.
                // So we spawn a blocking task to stop the container.
                let container = Arc::clone(container);
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
                    rt.block_on(async {
                        let _ = container.stop().await;
                    });
                });
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
pub async fn get_pgqrs_client() -> &'static PgqrsAdmin {
    ADMIN
        .get_or_init(|| async {
            // Check for external DSN
            let dsn = std::env::var("PGQRS_TEST_DSN").ok();
            if let Some(database_url) = dsn {
                println!("Using external database: {}", database_url);
                // Create connection pool
                let manager = ConnectionManager::<PgConnection>::new(&database_url);
                let pool = Pool::builder()
                    .max_size(10)
                    .build(manager)
                    .expect("Failed to create connection pool");
                // Test the connection
                {
                    let mut conn = pool.get().expect("Failed to get connection from pool");
                    diesel::sql_query("SELECT 1")
                        .execute(&mut conn)
                        .expect("Failed to execute test query");
                    println!("Database connection verified");
                }
                // Create Admin and install schema
                let mut config = pgqrs::config::Config::default();
                config.dsn = database_url.clone();
                let admin = PgqrsAdmin::new(&config);
                admin.install(false).expect("Failed to install schema");
                // Store the cleanup guard (external)
                let guard = CleanupGuard {
                    resource: DbResource::External,
                };
                CLEANUP_GUARD.set(guard).unwrap();
                admin
            } else {
                println!("Starting test database container...");
                let postgres_image = Postgres::default()
                    .with_db_name("test_db")
                    .with_user("test_user")
                    .with_password("test_password");
                let container = Arc::new(
                    postgres_image
                        .start()
                        .await
                        .expect("Failed to start postgres"),
                );
                let database_url = format!(
                    "postgresql://test_user:test_password@127.0.0.1:{}/test_db",
                    container
                        .get_host_port_ipv4(5432)
                        .await
                        .expect("Failed to get port")
                );
                println!("Database URL: {}", database_url);
                // Wait for postgres to be ready
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                // Create connection pool
                let manager = ConnectionManager::<PgConnection>::new(&database_url);
                let pool = Pool::builder()
                    .max_size(10)
                    .build(manager)
                    .expect("Failed to create connection pool");
                // Test the connection
                {
                    let mut conn = pool.get().expect("Failed to get connection from pool");
                    diesel::sql_query("SELECT 1")
                        .execute(&mut conn)
                        .expect("Failed to execute test query");
                    println!("Database connection verified");
                }
                // Create Admin and install schema
                let mut config = pgqrs::config::Config::default();
                config.dsn = database_url.clone();
                let admin = PgqrsAdmin::new(&config);
                admin.install(false).expect("Failed to install schema");
                // Store the cleanup guard (owned)
                let guard = CleanupGuard {
                    resource: DbResource::Owned(container),
                };
                CLEANUP_GUARD.set(guard).unwrap();
                admin
            }
        })
        .await
}

/// Create a PostgreSQL testcontainer and return the database URL
///
/// This is a simpler alternative to get_pgqrs_client() when you just need
/// a database URL without the pgqrs schema installation.
///
/// # Returns
/// A tuple of (database_url, container_handle) for manual management
pub async fn start_postgres_container() -> (String, ContainerAsync<Postgres>) {
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
