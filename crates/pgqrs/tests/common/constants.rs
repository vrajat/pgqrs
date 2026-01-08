// Test configuration constants

// Database configuration
#[cfg(feature = "postgres")]
pub const TEST_DB_NAME: &str = "test_db";
#[cfg(feature = "postgres")]
pub const TEST_DB_USER: &str = "test_user";
#[cfg(feature = "postgres")]
pub const TEST_DB_PASSWORD: &str = "test_password";
#[cfg(feature = "postgres")]
pub const POSTGRES_PORT: u16 = 5432;

// Connection configuration
#[cfg(feature = "postgres")]
pub const MAX_CONNECTIONS: u32 = 1;
#[cfg(feature = "postgres")]
pub const CONNECTION_TIMEOUT_SECS: u64 = 5;
#[cfg(feature = "postgres")]
pub const VERIFICATION_QUERY: &str = "SELECT 1";

// PgBouncer configuration
#[cfg(feature = "postgres")]
pub const PGBOUNCER_IMAGE: &str = "edoburu/pgbouncer";
#[cfg(feature = "postgres")]
pub const PGBOUNCER_VERSION: &str = "latest";
#[cfg(feature = "postgres")]
pub const PGBOUNCER_POOL_MODE: &str = "session";
#[cfg(feature = "postgres")]
pub const PGBOUNCER_AUTH_TYPE: &str = "md5";
#[cfg(feature = "postgres")]
pub const PGBOUNCER_MAX_CLIENT_CONN: &str = "100";
#[cfg(feature = "postgres")]
pub const PGBOUNCER_DEFAULT_POOL_SIZE: &str = "20";

// Retry configuration
#[cfg(feature = "postgres")]
pub const PGBOUNCER_READY_MAX_ATTEMPTS: u32 = 10;
#[cfg(feature = "postgres")]
pub const PGBOUNCER_RETRY_DELAY_SECS: u64 = 2;
