//! AnyStore enum for runtime backend selection.
//!
//! This module provides the `AnyStore` enum which wraps different database backend
//! implementations and provides a unified interface via the `Store` trait.

use super::*;
use crate::config::Config;
#[cfg(feature = "postgres")]
use crate::store::postgres::PostgresStore;
#[cfg(feature = "sqlite")]
use crate::store::sqlite::SqliteStore;
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Runtime-selectable database backend.
///
/// `AnyStore` wraps different database implementations and provides
/// a unified interface via the `Store` trait. This allows backend
/// selection at runtime based on DSN or configuration.
#[derive(Clone, Debug)]
pub enum AnyStore {
    /// PostgreSQL backend
    #[cfg(feature = "postgres")]
    Postgres(PostgresStore),
    /// SQLite backend
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteStore),
}

impl AnyStore {
    /// Connect to a database using a configuration object.
    ///
    /// This is the primary connection method that applies all configuration settings including:
    /// - Schema search path
    /// - Connection pool size
    /// - Max read count
    /// - Connection timeout
    ///
    /// # Arguments
    /// * `config` - Configuration object
    ///
    /// # Note
    /// This method is primarily used internally by `pgqrs::connect()`.
    /// Users should prefer the high-level `pgqrs::connect()` function.
    pub(crate) async fn connect(config: &Config) -> crate::error::Result<Self> {
        if config.dsn.starts_with("postgres://") || config.dsn.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                // Create search_path SQL for schema
                let search_path_sql = format!("SET search_path = \"{}\"", config.schema);

                let pool = PgPoolOptions::new()
                    .max_connections(config.max_connections)
                    .after_connect(move |conn, _meta| {
                        let sql = search_path_sql.clone();
                        Box::pin(async move {
                            sqlx::query(&sql).execute(&mut *conn).await?;
                            Ok(())
                        })
                    })
                    .connect(&config.dsn)
                    .await
                    .map_err(|e| crate::error::Error::ConnectionFailed {
                        source: e,
                        context: "Failed to connect to postgres".into(),
                    })?;

                Ok(AnyStore::Postgres(PostgresStore::new(pool, config)))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(crate::error::Error::InvalidConfig {
                    field: "dsn".to_string(),
                    message: "Postgres backend is not enabled".to_string(),
                })
            }
        } else if config.dsn.starts_with("sqlite://") || config.dsn.starts_with("sqlite:") {
            #[cfg(feature = "sqlite")]
            {
                let store = SqliteStore::new(&config.dsn, config).await?;
                Ok(AnyStore::Sqlite(store))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(crate::error::Error::InvalidConfig {
                    field: "dsn".to_string(),
                    message: "SQLite backend is not enabled".to_string(),
                })
            }
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Unsupported DSN format (must start with postgres://, postgresql://, or sqlite://)"
                    .to_string(),
            })
        }
    }

    /// Connect to a database using just a DSN string (simple connection).
    ///
    /// This method uses default configuration and the "public" schema.
    /// For custom schemas or advanced configuration, use [`connect`](Self::connect) instead.
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
    /// # use pgqrs::store::any::AnyStore;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = AnyStore::connect_with_dsn("postgresql://localhost/mydb").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_dsn(dsn: &str) -> crate::error::Result<Self> {
        if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                let pool = PgPool::connect(dsn).await.map_err(|e| {
                    crate::error::Error::ConnectionFailed {
                        source: e,
                        context: "Failed to connect to postgres".into(),
                    }
                })?;
                // Default config will be applied when new() creates the store
                Ok(AnyStore::Postgres(PostgresStore::new(
                    pool,
                    &Config::from_dsn(dsn),
                )))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(crate::error::Error::InvalidConfig {
                    field: "dsn".to_string(),
                    message: "Postgres backend is not enabled".to_string(),
                })
            }
        } else if dsn.starts_with("sqlite://") || dsn.starts_with("sqlite:") {
            #[cfg(feature = "sqlite")]
            {
                let config = Config::from_dsn(dsn);
                let store = SqliteStore::new(dsn, &config).await?;
                Ok(AnyStore::Sqlite(store))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(crate::error::Error::InvalidConfig {
                    field: "dsn".to_string(),
                    message: "SQLite backend is not enabled".to_string(),
                })
            }
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Unsupported DSN format: {}".to_string(),
            })
        }
    }
}

#[async_trait]
impl Store for AnyStore {
    #[cfg(feature = "postgres")]
    type Db = sqlx::Postgres;

    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    type Db = sqlx::Sqlite;

    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.execute_raw(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.execute_raw(sql).await,
        }
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.execute_raw_with_i64(sql, param).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.execute_raw_with_i64(sql, param).await,
        }
    }

    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.execute_raw_with_two_i64(sql, param1, param2).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.execute_raw_with_two_i64(sql, param1, param2).await,
        }
    }

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_int(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_int(sql).await,
        }
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_string(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_string(sql).await,
        }
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_bool(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_bool(sql).await,
        }
    }

    fn config(&self) -> &Config {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.config(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.config(),
        }
    }

    fn queues(&self) -> &dyn QueueTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.queues(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.queues(),
        }
    }

    fn messages(&self) -> &dyn MessageTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.messages(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.messages(),
        }
    }

    fn workers(&self) -> &dyn WorkerTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workers(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workers(),
        }
    }

    fn archive(&self) -> &dyn ArchiveTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.archive(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.archive(),
        }
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflows(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflows(),
        }
    }

    async fn admin(&self, config: &Config) -> crate::error::Result<Box<dyn Admin>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.admin(config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.admin(config).await,
        }
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.producer(queue, hostname, port, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.producer(queue, hostname, port, config).await,
        }
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.consumer(queue, hostname, port, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.consumer(queue, hostname, port, config).await,
        }
    }

    fn workflow(&self, id: i64) -> Box<dyn Workflow> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflow(id),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflow(id),
        }
    }

    fn worker(&self, id: i64) -> Box<dyn Worker> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.worker(id),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.worker(id),
        }
    }

    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.acquire_step(workflow_id, step_id).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.acquire_step(workflow_id, step_id).await,
        }
    }

    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> crate::error::Result<Box<dyn Workflow>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.create_workflow(name, input).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.create_workflow(name, input).await,
        }
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.concurrency_model(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.concurrency_model(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.backend_name(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.backend_name(),
        }
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.producer_ephemeral(queue, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.producer_ephemeral(queue, config).await,
        }
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.consumer_ephemeral(queue, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.consumer_ephemeral(queue, config).await,
        }
    }
}
