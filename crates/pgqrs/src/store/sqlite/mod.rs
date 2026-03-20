use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::ConcurrencyModel;
use crate::store::{
    DbStateTable, DbTables, MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store,
    WorkerTable, WorkflowTable,
};
use crate::Worker;

use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use std::sync::Arc;

pub(crate) mod dialect;
pub mod tables;

use self::tables::db_state::SqliteDbState;
use self::tables::messages::SqliteMessageTable;
use self::tables::queues::SqliteQueueTable;
use self::tables::runs::SqliteRunRecordTable;
use self::tables::steps::SqliteStepRecordTable;
use self::tables::workers::SqliteWorkerTable;
use self::tables::workflows::SqliteWorkflowTable;

#[derive(Debug, Clone)]
pub(crate) struct SqliteTables {
    pool: SqlitePool,
    config: Config,
    queues: Arc<SqliteQueueTable>,
    messages: Arc<SqliteMessageTable>,
    workers: Arc<SqliteWorkerTable>,
    db_state: Arc<SqliteDbState>,
    workflows: Arc<SqliteWorkflowTable>,
    workflow_runs: Arc<SqliteRunRecordTable>,
    workflow_steps: Arc<SqliteStepRecordTable>,
}

impl SqliteTables {
    pub(crate) async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let journal_mode = if config.sqlite.use_wal {
            "WAL"
        } else {
            "DELETE"
        };
        let journal_mode_pragma = format!("PRAGMA journal_mode={}", journal_mode);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .after_connect(move |conn, _meta| {
                let journal_mode_pragma = journal_mode_pragma.clone();
                Box::pin(async move {
                    sqlx::query(&journal_mode_pragma)
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("PRAGMA busy_timeout=5000")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("PRAGMA foreign_keys=ON")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(dsn)
            .await
            .map_err(Error::Database)?;

        Ok(Self {
            pool: pool.clone(),
            config: config.clone(),
            queues: Arc::new(SqliteQueueTable::new(pool.clone())),
            messages: Arc::new(SqliteMessageTable::new(pool.clone())),
            workers: Arc::new(SqliteWorkerTable::new(pool.clone())),
            db_state: Arc::new(SqliteDbState::new(pool.clone())),
            workflows: Arc::new(SqliteWorkflowTable::new(pool.clone())),
            workflow_runs: Arc::new(SqliteRunRecordTable::new(pool.clone())),
            workflow_steps: Arc::new(SqliteStepRecordTable::new(pool)),
        })
    }
}

#[derive(Clone)]
pub struct SqliteStore {
    inner: SqliteTables,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore").finish()
    }
}

impl SqliteStore {
    /// Create a new SQLite store with default optimizations.
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        Ok(Self {
            inner: SqliteTables::new(dsn, config).await?,
        })
    }

    fn any_store(&self) -> crate::store::AnyStore {
        crate::store::AnyStore::Sqlite(self.clone())
    }
}

pub use crate::store::sqlite_utils::format_timestamp as format_sqlite_timestamp;
pub use crate::store::sqlite_utils::parse_timestamp as parse_sqlite_timestamp;

#[async_trait]
impl DbTables for SqliteTables {
    async fn execute_raw(&self, sql: &str) -> Result<()> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: "Failed to execute raw SQL".into(),
            })?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> Result<()> {
        sqlx::query(sql).bind(param).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(&self, sql: &str, param1: i64, param2: i64) -> Result<()> {
        sqlx::query(sql)
            .bind(param1)
            .bind(param2)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn query_int(&self, sql: &str) -> Result<i64> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_string(&self, sql: &str) -> Result<String> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_bool(&self, sql: &str) -> Result<bool> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }

    fn queues(&self) -> &dyn QueueTable {
        self.queues.as_ref()
    }

    fn messages(&self) -> &dyn MessageTable {
        self.messages.as_ref()
    }

    fn workers(&self) -> &dyn WorkerTable {
        self.workers.as_ref()
    }

    fn db_state(&self) -> &dyn DbStateTable {
        self.db_state.as_ref()
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        self.workflows.as_ref()
    }

    fn workflow_runs(&self) -> &dyn RunRecordTable {
        self.workflow_runs.as_ref()
    }

    fn workflow_steps(&self) -> &dyn StepRecordTable {
        self.workflow_steps.as_ref()
    }

    async fn bootstrap(&self) -> Result<()> {
        sqlx::migrate!("migrations/sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Database(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.inner.execute_raw(sql).await
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> Result<()> {
        self.inner.execute_raw_with_i64(sql, param).await
    }

    async fn execute_raw_with_two_i64(&self, sql: &str, param1: i64, param2: i64) -> Result<()> {
        self.inner
            .execute_raw_with_two_i64(sql, param1, param2)
            .await
    }

    async fn query_int(&self, sql: &str) -> Result<i64> {
        self.inner.query_int(sql).await
    }

    async fn query_string(&self, sql: &str) -> Result<String> {
        self.inner.query_string(sql).await
    }

    async fn query_bool(&self, sql: &str) -> Result<bool> {
        self.inner.query_bool(sql).await
    }

    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn queues(&self) -> &dyn QueueTable {
        self.inner.queues()
    }

    fn messages(&self) -> &dyn MessageTable {
        self.inner.messages()
    }

    fn workers(&self) -> &dyn WorkerTable {
        self.inner.workers()
    }

    fn db_state(&self) -> &dyn DbStateTable {
        self.inner.db_state()
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        self.inner.workflows()
    }

    fn workflow_runs(&self) -> &dyn RunRecordTable {
        self.inner.workflow_runs()
    }

    fn workflow_steps(&self) -> &dyn StepRecordTable {
        self.inner.workflow_steps()
    }

    async fn bootstrap(&self) -> Result<()> {
        self.inner.bootstrap().await
    }

    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<crate::workers::Admin> {
        let _ = config;
        crate::workers::Admin::new(self.any_store(), hostname, port).await
    }

    async fn admin_ephemeral(&self, config: &Config) -> Result<crate::workers::Admin> {
        let _ = config;
        crate::workers::Admin::new_ephemeral(self.any_store()).await
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<crate::workers::Producer> {
        let queue_info = self.inner.queues.get_by_name(queue).await?;
        let worker_record = self
            .inner
            .workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        Ok(crate::workers::Producer::new(
            self.any_store(),
            queue_info,
            worker_record,
            config.validation_config.clone(),
        ))
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<crate::workers::Consumer> {
        let _ = config;
        let queue_info = self.inner.queues.get_by_name(queue).await?;
        let worker_record = self
            .inner
            .workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        Ok(crate::workers::Consumer::new(
            self.any_store(),
            queue_info,
            worker_record,
        ))
    }

    async fn queue(&self, name: &str) -> Result<crate::types::QueueRecord> {
        let queue_exists = self.inner.queues.exists(name).await?;
        if queue_exists {
            return Err(crate::error::Error::QueueAlreadyExists {
                name: name.to_string(),
            });
        }

        self.inner
            .queues
            .insert(crate::types::NewQueueRecord {
                queue_name: name.to_string(),
            })
            .await
    }

    async fn workflow(&self, name: &str) -> Result<crate::types::WorkflowRecord> {
        let queue_exists = self.inner.queues.exists(name).await?;
        if !queue_exists {
            let _queue = self
                .inner
                .queues
                .insert(crate::types::NewQueueRecord {
                    queue_name: name.to_string(),
                })
                .await?;
        }

        let queue = self.inner.queues.get_by_name(name).await?;

        self.inner
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
                        if matches!(db_err.code().as_deref(), Some("2067" | "1555" | "19")) {
                            return crate::error::Error::WorkflowAlreadyExists {
                                name: name.to_string(),
                            };
                        }
                    }
                }
                e
            })
    }

    async fn run(&self, message: crate::types::QueueMessage) -> Result<crate::workers::Run> {
        match self.inner.workflow_runs.get_by_message_id(message.id).await {
            Ok(record) => {
                return Ok(crate::workers::Run::new(self.any_store(), record));
            }
            Err(crate::error::Error::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        let queue = self.inner.queues.get(message.queue_id).await?;
        let workflow = self.inner.workflows.get_by_name(&queue.queue_name).await?;

        let run_rec = self
            .inner
            .workflow_runs
            .insert(crate::types::NewRunRecord {
                workflow_id: workflow.id,
                message_id: message.id,
                input: Some(message.payload.clone()),
            })
            .await?;

        Ok(crate::workers::Run::new(self.any_store(), run_rec))
    }

    async fn worker(&self, id: i64) -> Result<Box<dyn Worker>> {
        let worker_record = self.inner.workers.get(id).await?;
        Ok(Box::new(crate::workers::WorkerHandle::new(
            self.any_store(),
            worker_record,
        )))
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.inner.concurrency_model()
    }

    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> Result<crate::workers::Producer> {
        let queue_info = self.inner.queues.get_by_name(queue).await?;
        let worker_record = self
            .inner
            .workers
            .register_ephemeral(Some(queue_info.id))
            .await?;

        Ok(crate::workers::Producer::new(
            self.any_store(),
            queue_info,
            worker_record,
            config.validation_config.clone(),
        ))
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> Result<crate::workers::Consumer> {
        let _ = config;
        let queue_info = self.inner.queues.get_by_name(queue).await?;
        let worker_record = self
            .inner
            .workers
            .register_ephemeral(Some(queue_info.id))
            .await?;

        Ok(crate::workers::Consumer::new(
            self.any_store(),
            queue_info,
            worker_record,
        ))
    }
}
