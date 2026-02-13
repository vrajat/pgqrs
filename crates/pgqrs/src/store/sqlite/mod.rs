use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::{
    Admin, ArchiveTable, ConcurrencyModel, Consumer, MessageTable, Producer, QueueTable, Run,
    RunRecordTable, StepGuard, StepRecordTable, Store, Worker, WorkerTable, Workflow,
    WorkflowTable,
};
use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use std::sync::Arc;

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::archive::SqliteArchiveTable;
use self::tables::messages::SqliteMessageTable;
use self::tables::queues::SqliteQueueTable;
use self::tables::runs::SqliteRunRecordTable;
use self::tables::steps::SqliteStepRecordTable;
use self::tables::workers::SqliteWorkerTable;
use self::tables::workflows::SqliteWorkflowTable;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    config: Config,
    queues: Arc<SqliteQueueTable>,
    messages: Arc<SqliteMessageTable>,
    workers: Arc<SqliteWorkerTable>,
    archive: Arc<SqliteArchiveTable>,
    workflows: Arc<SqliteWorkflowTable>,
    workflow_runs: Arc<SqliteRunRecordTable>,
    workflow_steps: Arc<SqliteStepRecordTable>,
}

impl SqliteStore {
    /// Create a new SQLite store with default optimizations.
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("PRAGMA journal_mode=WAL")
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
            archive: Arc::new(SqliteArchiveTable::new(pool.clone())),
            workflows: Arc::new(SqliteWorkflowTable::new(pool.clone())),
            workflow_runs: Arc::new(SqliteRunRecordTable::new(pool.clone())),
            workflow_steps: Arc::new(SqliteStepRecordTable::new(pool)),
        })
    }
}

pub use crate::store::sqlite_utils::format_timestamp as format_sqlite_timestamp;
pub use crate::store::sqlite_utils::parse_timestamp as parse_sqlite_timestamp;

#[async_trait]
impl Store for SqliteStore {
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

    fn queues(&self) -> &dyn QueueTable {
        self.queues.as_ref()
    }

    fn messages(&self) -> &dyn MessageTable {
        self.messages.as_ref()
    }

    fn workers(&self) -> &dyn WorkerTable {
        self.workers.as_ref()
    }

    fn archive(&self) -> &dyn ArchiveTable {
        self.archive.as_ref()
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

    async fn acquire_step(
        &self,
        run_id: i64,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::types::StepRecord> {
        use self::workflow::guard::SqliteStepGuard;
        SqliteStepGuard::acquire_record(&self.pool, run_id, step_id, current_time).await
    }

    async fn bootstrap(&self) -> Result<()> {
        sqlx::migrate!("migrations/sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Database(e.into()))?;
        Ok(())
    }

    fn step_guard(&self, run_id: i64, step_id: &str) -> Box<dyn StepGuard> {
        use self::workflow::guard::SqliteStepGuard;
        Box::new(SqliteStepGuard::new(self.pool.clone(), run_id, step_id))
    }

    async fn admin(&self, hostname: &str, port: i32, config: &Config) -> Result<Box<dyn Admin>> {
        use self::worker::admin::SqliteAdmin;
        let admin = SqliteAdmin::new(self.pool.clone(), hostname, port, config.clone()).await?;
        Ok(Box::new(admin))
    }

    async fn admin_ephemeral(&self, config: &Config) -> Result<Box<dyn Admin>> {
        use self::worker::admin::SqliteAdmin;
        let admin = SqliteAdmin::new_ephemeral(self.pool.clone(), config.clone()).await?;
        Ok(Box::new(admin))
    }

    async fn producer(
        &self,
        queue_name: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<Box<dyn Producer>> {
        use self::worker::producer::SqliteProducer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let producer =
            SqliteProducer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer(
        &self,
        queue_name: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        use self::worker::consumer::SqliteConsumer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let consumer =
            SqliteConsumer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(consumer))
    }

    async fn workflow(&self, name: &str) -> Result<Box<dyn Workflow>> {
        use self::workflow::handle::SqliteWorkflow;

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
                        if matches!(db_err.code().as_deref(), Some("2067" | "1555" | "19")) {
                            return crate::error::Error::WorkflowAlreadyExists {
                                name: name.to_string(),
                            };
                        }
                    }
                }
                e
            })?;

        Ok(Box::new(SqliteWorkflow::new(workflow_record)))
    }

    async fn trigger(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> Result<crate::types::QueueMessage> {
        use crate::types::NewQueueMessage;

        let workflow = self.workflows.get_by_name(name).await?;
        let queue = self.queues.get_by_name(name).await?;

        let now = chrono::Utc::now();
        let payload = serde_json::json!({
            "workflow_id": workflow.id,
            "input": input
        });

        let msg = self
            .messages
            .insert(NewQueueMessage {
                queue_id: queue.id,
                payload,
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await?;

        Ok(msg)
    }

    async fn run(&self, message: crate::types::QueueMessage) -> Result<Box<dyn Run>> {
        use self::workflow::handle::SqliteRun;

        let payload = &message.payload;

        let run_id = if let Some(run_id) = payload.get("run_id").and_then(|v| v.as_i64()) {
            run_id
        } else if let Some(workflow_id) = payload.get("workflow_id").and_then(|v| v.as_i64()) {
            let input = payload.get("input").cloned();
            let run = self
                .workflow_runs
                .insert(crate::types::NewRunRecord { workflow_id, input })
                .await?;
            run.id
        } else {
            return Err(crate::error::Error::Internal {
                message: "Invalid workflow message payload".to_string(),
            });
        };

        Ok(Box::new(SqliteRun::new(self.pool.clone(), run_id)))
    }

    async fn worker(&self, id: i64) -> Result<Box<dyn Worker>> {
        use self::worker::SqliteWorkerHandle;
        let worker_record = self.workers.get(id).await?;
        Ok(Box::new(SqliteWorkerHandle::new(
            self.pool.clone(),
            worker_record,
        )))
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }

    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    async fn producer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Producer>> {
        use self::worker::producer::SqliteProducer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let producer =
            SqliteProducer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        use self::worker::consumer::SqliteConsumer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let consumer =
            SqliteConsumer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(consumer))
    }
}
