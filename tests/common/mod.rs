use diesel::RunQueryDsl;
use pgqrs::admin::Admin;
use tokio::sync::OnceCell;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::pg::PgConnection;

static CLEANUP_GUARD: OnceCell<CleanupGuard> = OnceCell::const_new();
static ADMIN: OnceCell<Admin> = OnceCell::const_new();

#[derive(Debug)]
struct CleanupGuard {
    _container: ContainerAsync<Postgres>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let admin = ADMIN.get().expect("Admin not initialized");
        admin.uninstall(false).expect("Failed to uninstall schema");
    }
}

pub async fn get_pgqrs_client() -> &'static Admin {
    ADMIN.get_or_init(|| async {
        println!("Starting test database container...");

        let postgres_image = Postgres::default()
            .with_db_name("test_db")
            .with_user("test_user")
            .with_password("test_password");

        let container = postgres_image.start().await.expect("Failed to start postgres");

        let database_url = format!(
            "postgresql://test_user:test_password@127.0.0.1:{}/test_db",
            container.get_host_port_ipv4(5432).await.expect("Failed to get port")
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
            diesel::sql_query("SELECT 1").execute(&mut conn)
                .expect("Failed to execute test query");
            println!("Database connection verified");
        }

        // Create Admin and install schema
        let mut config = pgqrs::Config::default();
        config.dsn = database_url;
        let admin = Admin::new(&config);
        admin.install(false).expect("Failed to install schema");
        // Store the cleanup guard
        let guard = CleanupGuard { _container: container };
        CLEANUP_GUARD.set(guard).unwrap();
        admin
    }).await
}