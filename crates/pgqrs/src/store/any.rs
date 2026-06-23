//! AnyStore wrapper for the Postgres runtime.

use super::*;
use crate::config::Config;
use crate::store::postgres::PostgresStore;
use sqlx::postgres::PgPoolOptions;

/// Runtime-selectable database backend.
///
/// `AnyStore` wraps the Postgres implementation and provides a unified
/// interface via the `Store` trait.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum AnyStore {
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
    pub(crate) async fn connect(config: &Config) -> crate::error::Result<Self> {
        BackendType::detect(&config.dsn)?;

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

    /// Connect to a database using just a DSN string (simple connection).
    ///
    /// This method uses default configuration and the "public" schema.
    /// For custom schemas or advanced configuration, use `pgqrs::connect()` instead.
    ///
    /// The DSN must use the Postgres scheme.
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

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        match self {
            AnyStore::Postgres(s) => s.query_int(sql).await,
        }
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        match self {
            AnyStore::Postgres(s) => s.query_string(sql).await,
        }
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        match self {
            AnyStore::Postgres(s) => s.query_bool(sql).await,
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

    fn workers(&self) -> &Self::Workers {
        match self {
            AnyStore::Postgres(s) => s.workers(),
        }
    }

    fn db_state(&self) -> &dyn DbStateTable {
        match self {
            AnyStore::Postgres(s) => s.db_state(),
        }
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        match self {
            AnyStore::Postgres(s) => s.workflows(),
        }
    }

    fn workflow_runs(&self) -> &dyn RunRecordTable {
        match self {
            AnyStore::Postgres(s) => s.workflow_runs(),
        }
    }

    fn workflow_steps(&self) -> &dyn StepRecordTable {
        match self {
            AnyStore::Postgres(s) => s.workflow_steps(),
        }
    }

    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<Run> {
        match self {
            AnyStore::Postgres(s) => s.run(message).await,
        }
    }

    async fn bootstrap(&self) -> crate::error::Result<()> {
        match self {
            AnyStore::Postgres(s) => s.bootstrap().await,
        }
    }

    async fn admin(&self, name: &str) -> crate::error::Result<crate::workers::Admin> {
        match self {
            AnyStore::Postgres(s) => s.admin(name).await,
        }
    }

    async fn admin_ephemeral(&self) -> crate::error::Result<crate::workers::Admin> {
        match self {
            AnyStore::Postgres(s) => s.admin_ephemeral().await,
        }
    }

    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
        match self {
            AnyStore::Postgres(s) => s.workflow(name).await,
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
    ) -> crate::error::Result<Producer> {
        match self {
            AnyStore::Postgres(s) => s.producer_ephemeral(queue, config).await,
        }
    }

    async fn consumer_ephemeral(&self, queue: &str) -> crate::error::Result<Consumer> {
        match self {
            AnyStore::Postgres(s) => s.consumer_ephemeral(queue).await,
        }
    }

    async fn producer(
        &self,
        queue: &str,
        name: &str,
        config: &Config,
    ) -> crate::error::Result<Producer> {
        match self {
            AnyStore::Postgres(s) => s.producer(queue, name, config).await,
        }
    }

    async fn consumer(&self, queue: &str, name: &str) -> crate::error::Result<Consumer> {
        match self {
            AnyStore::Postgres(s) => s.consumer(queue, name).await,
        }
    }

    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
        match self {
            AnyStore::Postgres(s) => s.queue(name).await,
        }
    }
}
