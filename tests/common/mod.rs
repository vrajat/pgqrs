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

        // Try to use current runtime handle first, fallback to creating new runtime
        let uninstall_result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(admin.uninstall(false))
        } else {
            // Only create a new runtime if we're not in a tokio context
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(admin.uninstall(false))
        };

        if let Err(e) = uninstall_result {
            eprintln!("Failed to uninstall schema during cleanup: {}", e);
        }

        match &self.resource {
            DbResource::Owned(container) => {
                // Explicitly stop the container to ensure it is killed
                // Note: ContainerAsync::stop is async, but Drop cannot be async.
                let container = Arc::clone(container);
                std::thread::spawn(move || {
                    // Try to use current runtime handle first, fallback to creating new runtime
                    let stop_result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        handle.block_on(container.stop())
                    } else {
                        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
                        rt.block_on(container.stop())
                    };

                    if let Err(e) = stop_result {
                        eprintln!("Failed to stop container during cleanup: {}", e);
                    }
                });
                tracing::info!("Testcontainer stopped");
            }
            DbResource::External => {
                tracing::info!("External DB used, not stopping container");
            }
        }
    }
}

pub async fn get_pgqrs_client() -> &'static PgqrsAdmin {
    ADMIN
        .get_or_init(|| async {
            // Check for external DSN
            let dsn = std::env::var("PGQRS_TEST_DSN").ok();
            if let Some(database_url) = dsn {
                println!("Using external database: {}", database_url);
                // Create Admin and install schema
                let mut config = pgqrs::config::Config::default();
                config.dsn = database_url.clone();
                let admin = PgqrsAdmin::new(&config)
                    .await
                    .expect("Failed to create admin");

                // Test the connection
                sqlx::query("SELECT 1")
                    .execute(&admin.pool)
                    .await
                    .expect("Failed to execute test query");
                println!("Database connection verified");

                admin
                    .install(false)
                    .await
                    .expect("Failed to install schema");
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

                // Create Admin and install schema
                let mut config = pgqrs::config::Config::default();
                config.dsn = database_url.clone();
                let admin = PgqrsAdmin::new(&config)
                    .await
                    .expect("Failed to create admin");

                // Test the connection
                sqlx::query("SELECT 1")
                    .execute(&admin.pool)
                    .await
                    .expect("Failed to execute test query");
                println!("Database connection verified");

                admin
                    .install(false)
                    .await
                    .expect("Failed to install schema");
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
