//! AnyStore enum for runtime backend selection.
//!
//! This module provides the `AnyStore` enum which wraps different database backend
//! implementations and provides a unified interface via the `Store` trait.

use super::*;
use crate::config::Config;
#[cfg(feature = "postgres")]
use crate::store::postgres::PostgresStore;
#[cfg(feature = "s3")]
use crate::store::s3::S3Store;
#[cfg(feature = "sqlite")]
use crate::store::sqlite::SqliteStore;
#[cfg(feature = "turso")]
use crate::store::turso::TursoStore;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;

/// Runtime-selectable database backend.
///
/// `AnyStore` wraps different database implementations and provides
/// a unified interface via the `Store` trait. This allows backend
/// selection at runtime based on DSN or configuration.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum AnyStore {
    /// PostgreSQL backend
    #[cfg(feature = "postgres")]
    Postgres(PostgresStore),
    /// SQLite backend
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteStore),
    /// S3-backed SQLite backend
    #[cfg(feature = "s3")]
    S3(S3Store),
    /// Turso backend (Rust-native SQLite rewrite)
    #[cfg(feature = "turso")]
    Turso(TursoStore),
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
        let backend = BackendType::detect(&config.dsn)?;

        match backend {
            #[cfg(feature = "postgres")]
            BackendType::Postgres => {
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
                        source: Box::new(e),
                        context: "Failed to connect to postgres".into(),
                    })?;

                Ok(AnyStore::Postgres(PostgresStore::new(pool, config)))
            }
            #[cfg(feature = "s3")]
            BackendType::S3 => {
                let store = S3Store::new(config).await?;
                Ok(AnyStore::S3(store))
            }
            #[cfg(feature = "sqlite")]
            BackendType::Sqlite => {
                let store = SqliteStore::new(&config.dsn, config).await?;
                Ok(AnyStore::Sqlite(store))
            }
            #[cfg(feature = "turso")]
            BackendType::Turso => {
                let store = TursoStore::new(&config.dsn, config).await?;
                Ok(AnyStore::Turso(store))
            }
        }
    }

    /// Connect to a database using just a DSN string (simple connection).
    ///
    /// This method uses default configuration and the "public" schema.
    /// For custom schemas or advanced configuration, use `pgqrs::connect()` instead.
    ///
    /// The DSN format determines which backend is used:
    /// - `postgres://` or `postgresql://` → PostgreSQL
    /// - `sqlite://` → SQLite (requires "sqlite" feature)
    /// - `s3://` → SQLite local cache path (requires "s3" feature)
    /// - `turso://` → Turso (requires "turso" feature)
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
        let config = Config::from_dsn(dsn);
        Self::connect(&config).await
    }
}

#[async_trait]
impl Store for AnyStore {
    type Workers = dyn WorkerTable;

    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.execute_raw(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.execute_raw(sql).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.execute_raw(sql).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.execute_raw(sql).await,
        }
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.execute_raw_with_i64(sql, param).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.execute_raw_with_i64(sql, param).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.execute_raw_with_i64(sql, param).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.execute_raw_with_i64(sql, param).await,
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
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.execute_raw_with_two_i64(sql, param1, param2).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.execute_raw_with_two_i64(sql, param1, param2).await,
        }
    }

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_int(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_int(sql).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.query_int(sql).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.query_int(sql).await,
        }
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_string(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_string(sql).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.query_string(sql).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.query_string(sql).await,
        }
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.query_bool(sql).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.query_bool(sql).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.query_bool(sql).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.query_bool(sql).await,
        }
    }

    fn config(&self) -> &Config {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.config(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.config(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.config(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.config(),
        }
    }

    fn queues(&self) -> &dyn QueueTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.queues(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.queues(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.queues(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.queues(),
        }
    }

    fn messages(&self) -> &dyn MessageTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.messages(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.messages(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.messages(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.messages(),
        }
    }

    fn workers(&self) -> &Self::Workers {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workers(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workers(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.workers(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workers(),
        }
    }

    fn db_state(&self) -> &dyn DbStateTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.db_state(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.db_state(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.db_state(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.db_state(),
        }
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflows(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflows(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.workflows(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workflows(),
        }
    }

    fn workflow_runs(&self) -> &dyn RunRecordTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflow_runs(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflow_runs(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.workflow_runs(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workflow_runs(),
        }
    }

    fn workflow_steps(&self) -> &dyn StepRecordTable {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflow_steps(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflow_steps(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.workflow_steps(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workflow_steps(),
        }
    }

    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<Run> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.run(message).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.run(message).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.run(message).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.run(message).await,
        }
    }

    async fn bootstrap(&self) -> crate::error::Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.bootstrap().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.bootstrap().await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.bootstrap().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.bootstrap().await,
        }
    }

    async fn admin(&self, name: &str) -> crate::error::Result<crate::workers::Admin> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.admin(name).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.admin(name).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.admin(name).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.admin(name).await,
        }
    }

    async fn admin_ephemeral(&self) -> crate::error::Result<crate::workers::Admin> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.admin_ephemeral().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.admin_ephemeral().await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.admin_ephemeral().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.admin_ephemeral().await,
        }
    }

    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.workflow(name).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.workflow(name).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.workflow(name).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.workflow(name).await,
        }
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.concurrency_model(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.concurrency_model(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.concurrency_model(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.concurrency_model(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.backend_name(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.backend_name(),
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.backend_name(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.backend_name(),
        }
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Producer> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.producer_ephemeral(queue, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.producer_ephemeral(queue, config).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.producer_ephemeral(queue, config).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.producer_ephemeral(queue, config).await,
        }
    }

    async fn consumer_ephemeral(&self, queue: &str) -> crate::error::Result<Consumer> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.consumer_ephemeral(queue).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.consumer_ephemeral(queue).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.consumer_ephemeral(queue).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.consumer_ephemeral(queue).await,
        }
    }

    async fn producer(
        &self,
        queue: &str,
        name: &str,
        config: &Config,
    ) -> crate::error::Result<Producer> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.producer(queue, name, config).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.producer(queue, name, config).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.producer(queue, name, config).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.producer(queue, name, config).await,
        }
    }

    async fn consumer(&self, queue: &str, name: &str) -> crate::error::Result<Consumer> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.consumer(queue, name).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.consumer(queue, name).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.consumer(queue, name).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.consumer(queue, name).await,
        }
    }

    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
        match self {
            #[cfg(feature = "postgres")]
            AnyStore::Postgres(s) => s.queue(name).await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.queue(name).await,
            #[cfg(feature = "s3")]
            AnyStore::S3(s) => s.queue(name).await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.queue(name).await,
        }
    }
}
