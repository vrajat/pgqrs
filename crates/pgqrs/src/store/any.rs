//! AnyStore enum for runtime backend selection.
//!
//! This module provides the `AnyStore` enum which wraps different database backend
//! implementations and provides a unified interface via the `Store` trait.

use super::*;
use crate::config::Config;
use crate::store::postgres::PostgresStore;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Runtime-selectable database backend.
///
/// `AnyStore` wraps different database implementations and provides
/// a unified interface via the `Store` trait. This allows backend
/// selection at runtime based on DSN or configuration.
#[derive(Clone)]
pub enum AnyStore {
    /// PostgreSQL backend
    Postgres(PostgresStore),

    #[cfg(feature = "sqlite")]
    /// SQLite backend (requires "sqlite" feature)
    Sqlite(SqliteStore),

    #[cfg(feature = "turso")]
    /// Turso backend (requires "turso" feature)
    Turso(TursoStore),
}

impl AnyStore {
    /// Connect to a database using a DSN string.
    ///
    /// The DSN format determines which backend is used:
    /// - `postgres://` or `postgresql://` → PostgreSQL
    /// - `sqlite://` → SQLite (requires "sqlite" feature)
    /// - `libsql://` → Turso (requires "turso" feature)
    ///
    /// # Arguments
    /// * `dsn` - Database connection string
    ///
    /// # Example
    /// ```no_run
    /// # use pgqrs::store::AnyStore;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = AnyStore::connect("postgresql://localhost/mydb").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(dsn: &str) -> Result<Self> {
        if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            let pool = PgPool::connect(dsn).await?;
            Ok(AnyStore::Postgres(PostgresStore::new(pool, 5)))
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: format!("Unsupported DSN format: {}", dsn),
            })
        }
    }

    /// Connect to a database using a configuration object.
    ///
    /// This method applies all configuration settings including:
    /// - Schema search path
    /// - Connection pool size
    /// - Max read count
    /// - Connection timeout
    ///
    /// # Arguments
    /// * `config` - Configuration object
    ///
    /// # Example
    /// ```no_run
    /// # use pgqrs::{config::Config, store::AnyStore};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = Config::from_dsn("postgresql://localhost/mydb");
    /// let store = AnyStore::connect_with_config(&config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(config: &Config) -> Result<Self> {
        if config.dsn.starts_with("postgres://") || config.dsn.starts_with("postgresql://") {
            // Create search_path SQL for schema
            let search_path_sql = format!("SET search_path = \"{}\"", config.schema);

            let pool = PgPoolOptions::new()
                .max_connections(config.max_connections)
                .after_connect(move |conn, _meta| {
                    let sql = search_path_sql.clone();
                    Box::pin(async move {
                        sqlx::query(&sql).execute(conn).await?;
                        Ok(())
                    })
                })
                .connect(&config.dsn)
                .await?;

            Ok(AnyStore::Postgres(PostgresStore::new(pool, config.max_read_ct as u32)))
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: format!("Unsupported DSN format: {}", config.dsn),
            })
        }
    }
}

// Implement Store trait for AnyStore via delegation
#[async_trait]
impl Store for AnyStore {
    fn queues(&self) -> &dyn QueueTable {
        match self {
            AnyStore::Postgres(s) => s.queues(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.queues(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.queues(),
        }
    }

    fn messages(&self) -> &dyn MessageTable {
        match self {
            AnyStore::Postgres(s) => s.messages(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.messages(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.messages(),
        }
    }

    fn workers(&self) -> &dyn WorkerTable {
        match self {
            AnyStore::Postgres(s) => s.workers(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workers(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workers(),
        }
    }

    fn archive(&self) -> &dyn ArchiveTable {
        match self {
            AnyStore::Postgres(s) => s.archive(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.archive(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.archive(),
        }
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        match self {
            AnyStore::Postgres(s) => s.workflows(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflows(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workflows(),
        }
    }

    async fn install(&self) -> Result<()> {
        match self {
            AnyStore::Postgres(s) => s.install().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.install().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.install().await,
        }
    }

    async fn verify(&self) -> Result<()> {
        match self {
            AnyStore::Postgres(s) => s.verify().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.verify().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.verify().await,
        }
    }

    async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>> {
        match self {
            AnyStore::Postgres(s) => s.dlq_batch(max_attempts).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.dlq_batch(max_attempts).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.dlq_batch(max_attempts).await,
        }
    }

    async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>> {
        match self {
            AnyStore::Postgres(s) => s.replay_from_dlq(archived_id).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.replay_from_dlq(archived_id).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.replay_from_dlq(archived_id).await,
        }
    }

    async fn release_zombie_messages(&self, worker_timeout: Duration) -> Result<u64> {
        match self {
            AnyStore::Postgres(s) => s.release_zombie_messages(worker_timeout).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.release_zombie_messages(worker_timeout).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.release_zombie_messages(worker_timeout).await,
        }
    }

    async fn purge_stale_workers(&self, timeout: Duration) -> Result<WorkerPurgeResult> {
        match self {
            AnyStore::Postgres(s) => s.purge_stale_workers(timeout).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.purge_stale_workers(timeout).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.purge_stale_workers(timeout).await,
        }
    }

    async fn get_system_stats(&self) -> Result<SystemStats> {
        match self {
            AnyStore::Postgres(s) => s.get_system_stats().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.get_system_stats().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.get_system_stats().await,
        }
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        match self {
            AnyStore::Postgres(s) => s.concurrency_model(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.concurrency_model(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.concurrency_model(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            AnyStore::Postgres(s) => s.backend_name(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.backend_name(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.backend_name(),
        }
    }
}
