//! Connection builder for creating store instances.

use crate::store::AnyStore;
use crate::Config;

/// Connect to a database using a DSN string.
///
/// This is a convenience function that wraps `AnyStore::connect_with_dsn()`.
/// For advanced configuration (custom schema, connection pool size, etc.),
/// use `pgqrs::connect_with_config(&config)` instead.
///
/// # Arguments
/// * `dsn` - Database connection string (e.g., "postgresql://localhost/mydb")
///
/// # Example
/// ```no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(dsn: &str) -> crate::error::Result<AnyStore> {
    AnyStore::connect_with_dsn(dsn).await
}

/// Connect to a database using a configuration object.
///
/// This is the primary way to connect to a database with custom configuration.
/// Use this when you need custom configuration like schema, pool size, etc.
///
/// # Arguments
/// * `config` - Configuration object
///
/// # Example
/// ```no_run
/// # use pgqrs::{self, Config};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config::from_dsn("postgresql://localhost/mydb")
///     .with_schema("my_schema")
///     .with_max_connections(20);
/// let store = pgqrs::connect_with_config(&config).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_config(config: &Config) -> crate::error::Result<AnyStore> {
    AnyStore::connect(config).await
}
