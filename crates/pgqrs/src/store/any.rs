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
#[derive(Clone, Debug)]
pub enum AnyStore {
    /// PostgreSQL backend
    Postgres(PostgresStore),
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
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Unsupported DSN format (must start with postgres:// or postgresql://)"
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
            let pool =
                PgPool::connect(dsn)
                    .await
                    .map_err(|e| crate::error::Error::ConnectionFailed {
                        source: e,
                        context: "Failed to connect to postgres".into(),
                    })?;
            // Default config will be applied when new() creates the store
            Ok(AnyStore::Postgres(PostgresStore::new(
                pool,
                &Config::from_dsn(dsn),
            )))
        } else {
            Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: format!("Unsupported DSN format: {}", dsn),
            })
        }
    }
}

#[async_trait]
impl Store for AnyStore {
    type Db = sqlx::Postgres;

    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        match self {
            AnyStore::Postgres(s) => s.execute_raw(sql).await,
        }
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        match self {
            AnyStore::Postgres(s) => s.execute_raw_with_i64(sql, param).await,
        }
    }

    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        match self {
            AnyStore::Postgres(s) => s.execute_raw_with_two_i64(sql, param1, param2).await,
        }
    }

    async fn query_scalar_raw<T>(&self, sql: &str) -> crate::error::Result<T>
    where
        T: 'static
            + Send
            + Unpin
            + for<'r> sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>,
    {
        match self {
            AnyStore::Postgres(s) => s.query_scalar_raw(sql).await,
        }
    }

    fn config(&self) -> &Config {
        match self {
            AnyStore::Postgres(s) => s.config(),
        }
    }

    fn queues(&self) -> &dyn QueueTable {
        match self {
            AnyStore::Postgres(s) => s.queues(),
        }
    }

    fn messages(&self) -> &dyn MessageTable {
        match self {
            AnyStore::Postgres(s) => s.messages(),
        }
    }

    fn workers(&self) -> &dyn WorkerTable {
        match self {
            AnyStore::Postgres(s) => s.workers(),
        }
    }

    fn archive(&self) -> &dyn ArchiveTable {
        match self {
            AnyStore::Postgres(s) => s.archive(),
        }
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        match self {
            AnyStore::Postgres(s) => s.workflows(),
        }
    }

    async fn admin(&self, config: &Config) -> crate::error::Result<Box<dyn Admin>> {
        match self {
            AnyStore::Postgres(s) => s.admin(config).await,
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
            AnyStore::Postgres(s) => s.producer(queue, hostname, port, config).await,
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
            AnyStore::Postgres(s) => s.consumer(queue, hostname, port, config).await,
        }
    }

    fn workflow(&self, id: i64) -> Box<dyn Workflow> {
        match self {
            AnyStore::Postgres(s) => s.workflow(id),
        }
    }

    fn worker(&self, id: i64) -> Box<dyn Worker> {
        match self {
            AnyStore::Postgres(s) => s.worker(id),
        }
    }

    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        match self {
            AnyStore::Postgres(s) => s.acquire_step(workflow_id, step_id).await,
        }
    }

    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> crate::error::Result<Box<dyn Workflow>> {
        match self {
            AnyStore::Postgres(s) => s.create_workflow(name, input).await,
        }
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        match self {
            AnyStore::Postgres(s) => s.concurrency_model(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            AnyStore::Postgres(s) => s.backend_name(),
        }
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>> {
        match self {
            AnyStore::Postgres(s) => s.producer_ephemeral(queue, config).await,
        }
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>> {
        match self {
            AnyStore::Postgres(s) => s.consumer_ephemeral(queue, config).await,
        }
    }
}
