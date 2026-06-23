//! Postgres implementation of the Store trait.

use crate::store::ConcurrencyModel;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;

pub(crate) mod dialect;
pub mod tables;

use self::tables::db_state::DbState as PostgresDbState;
use self::tables::pgqrs_messages::Messages as PostgresMessageTable;
use self::tables::pgqrs_queues::Queues as PostgresQueueTable;
use self::tables::pgqrs_workers::Workers as PostgresWorkerTable;
use self::tables::pgqrs_workflow_runs::RunRecords as PostgresRunRecordTable;
use self::tables::pgqrs_workflow_steps::StepRecords as PostgresStepRecordTable;
use self::tables::pgqrs_workflows::Workflows as PostgresWorkflowTable;
use crate::config::Config;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("migrations/postgres");

#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
    config: Config,
    queues: Arc<PostgresQueueTable>,
    messages: Arc<PostgresMessageTable>,
    workers: Arc<PostgresWorkerTable>,
    db_state: Arc<PostgresDbState>,
    workflows: Arc<PostgresWorkflowTable>,
    workflow_runs: Arc<PostgresRunRecordTable>,
    workflow_steps: Arc<PostgresStepRecordTable>,
}

impl Store {
    pub fn new(pool: PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            queues: Arc::new(PostgresQueueTable::new(pool.clone())),
            messages: Arc::new(PostgresMessageTable::new(pool.clone())),
            workers: Arc::new(PostgresWorkerTable::new(pool.clone())),
            db_state: Arc::new(PostgresDbState::new(pool.clone())),
            workflows: Arc::new(PostgresWorkflowTable::new(pool.clone())),
            workflow_runs: Arc::new(PostgresRunRecordTable::new(pool.clone())),
            workflow_steps: Arc::new(PostgresStepRecordTable::new(pool)),
        }
    }

    /// Connect to a database using a configuration object.
    pub async fn connect(config: &Config) -> crate::error::Result<Self> {
        const POSTGRES_PREFIXES: &[&str] = &["postgres://", "postgresql://", "postgres", "pg"];

        if !POSTGRES_PREFIXES.iter().any(|p| config.dsn.starts_with(p)) {
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: format!("Unsupported DSN format: {}", config.dsn),
            });
        }

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

        Ok(Store::new(pool, config))
    }

    /// Connect to a database using just a DSN string (simple connection).
    pub async fn connect_with_dsn(dsn: &str) -> crate::error::Result<Self> {
        let config = Config::from_dsn(dsn);
        Self::connect(&config).await
    }

    /// Get access to the underlying PgPool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Execute raw SQL without parameters.
    pub async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        sqlx::raw_sql(sql).execute(&self.pool).await?;
        Ok(())
    }

    /// Execute raw SQL with a single i64 parameter.
    pub async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        sqlx::query(sql).bind(param).execute(&self.pool).await?;
        Ok(())
    }

    /// Execute raw SQL with two i64 parameters.
    pub async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        sqlx::query(sql)
            .bind(param1)
            .bind(param2)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Query a single i64 value using raw SQL.
    pub async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    /// Query a single string value using raw SQL.
    pub async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    /// Query a single boolean value using raw SQL.
    pub async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    /// Get the configuration for this store
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get access to the repositories.
    pub fn queues(&self) -> &PostgresQueueTable {
        self.queues.as_ref()
    }

    pub fn messages(&self) -> &PostgresMessageTable {
        self.messages.as_ref()
    }

    pub fn workers(&self) -> &PostgresWorkerTable {
        self.workers.as_ref()
    }

    pub fn db_state(&self) -> &PostgresDbState {
        self.db_state.as_ref()
    }

    pub fn workflows(&self) -> &PostgresWorkflowTable {
        self.workflows.as_ref()
    }

    pub fn workflow_runs(&self) -> &PostgresRunRecordTable {
        self.workflow_runs.as_ref()
    }

    pub fn workflow_steps(&self) -> &PostgresStepRecordTable {
        self.workflow_steps.as_ref()
    }

    /// Initialize the pgqrs schema in the database.
    pub async fn bootstrap(&self) -> crate::error::Result<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Get an admin worker interface.
    pub async fn admin(&self, name: &str) -> crate::error::Result<crate::workers::Admin> {
        let worker_record = self.workers.register(None, name).await?;
        Ok(crate::workers::Admin::new(self.clone(), worker_record))
    }

    /// Get an ephemeral admin worker interface.
    pub async fn admin_ephemeral(&self) -> crate::error::Result<crate::workers::Admin> {
        let worker_record = self.workers().register_ephemeral(None).await?;
        Ok(crate::workers::Admin::new(self.clone(), worker_record))
    }

    /// Get a producer interface for a specific queue with worker identity.
    pub async fn producer(
        &self,
        queue: &str,
        name: &str,
        config: &Config,
    ) -> crate::error::Result<crate::workers::Producer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let worker_record = self.workers.register(Some(queue_info.id), name).await?;

        Ok(crate::workers::Producer::new(
            self.clone(),
            queue_info,
            worker_record,
            config.validation_config.clone(),
        ))
    }

    /// Get a consumer interface for a specific queue with worker identity.
    pub async fn consumer(
        &self,
        queue: &str,
        name: &str,
    ) -> crate::error::Result<crate::workers::Consumer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let worker_record = self.workers.register(Some(queue_info.id), name).await?;

        Ok(crate::workers::Consumer::new(
            self.clone(),
            queue_info,
            worker_record,
        ))
    }

    /// Create a new queue.
    pub async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
        let queue_exists = self.queues.exists(name).await?;
        if queue_exists {
            return Err(crate::error::Error::QueueAlreadyExists {
                name: name.to_string(),
            });
        }

        self.queues
            .insert(crate::types::NewQueueRecord {
                queue_name: name.to_string(),
            })
            .await
    }

    /// Get a workflow definition handle.
    pub async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
        // Ensure backing queue exists.
        let queue_exists = self.queues.exists(name).await?;
        if !queue_exists {
            let _queue = self
                .queues
                .insert(crate::types::NewQueueRecord {
                    queue_name: name.to_string(),
                })
                .await?;
        }

        let queue = self.queues.get_by_name(name).await?;

        // Create workflow definition (template).
        let workflow_record = self
            .workflows
            .insert(crate::types::NewWorkflowRecord {
                name: name.to_string(),
                queue_id: queue.id,
            })
            .await
            .map_err(|e| {
                if let crate::error::Error::QueryFailed { source, .. } = &e {
                    if let Some(sqlx::Error::Database(db_err)) =
                        source.downcast_ref::<sqlx::Error>()
                    {
                        if db_err.code().as_deref() == Some("23505") {
                            return crate::error::Error::WorkflowAlreadyExists {
                                name: name.to_string(),
                            };
                        }
                    }
                }
                e
            })?;

        Ok(workflow_record)
    }

    /// Create a local run handle from a message.
    pub async fn run(
        &self,
        message: crate::types::QueueMessage,
    ) -> crate::error::Result<crate::workers::Run> {
        // Try to find existing run by message_id
        match self.workflow_runs.get_by_message_id(message.id).await {
            Ok(record) => {
                return Ok(crate::workers::Run::new(self.clone(), record));
            }
            Err(crate::error::Error::NotFound { .. }) => {
                // Not found, continue to create new run
            }
            Err(e) => return Err(e),
        }

        // Otherwise, it's a new trigger. Create run record.
        let queue = self.queues.get(message.queue_id).await?;
        let workflow = self.workflows.get_by_name(&queue.queue_name).await?;

        let run_rec = self
            .workflow_runs
            .insert(crate::types::NewRunRecord {
                workflow_id: workflow.id,
                message_id: message.id,
                input: Some(message.payload.clone()),
            })
            .await?;

        Ok(crate::workers::Run::new(self.clone(), run_rec))
    }

    /// Returns the concurrency model supported by this backend.
    pub fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::MultiProcess
    }

    /// Returns the backend name.
    pub fn backend_name(&self) -> &'static str {
        "postgres"
    }

    /// Create an ephemeral producer (auto-cleanup).
    pub async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<crate::workers::Producer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let worker_record = self.workers.register_ephemeral(Some(queue_info.id)).await?;

        Ok(crate::workers::Producer::new(
            self.clone(),
            queue_info,
            worker_record,
            config.validation_config.clone(),
        ))
    }

    /// Create an ephemeral consumer (auto-cleanup).
    pub async fn consumer_ephemeral(
        &self,
        queue: &str,
    ) -> crate::error::Result<crate::workers::Consumer> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let worker_record = self.workers.register_ephemeral(Some(queue_info.id)).await?;

        Ok(crate::workers::Consumer::new(
            self.clone(),
            queue_info,
            worker_record,
        ))
    }
}
