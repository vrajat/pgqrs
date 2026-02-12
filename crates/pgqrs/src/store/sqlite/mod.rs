use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::{
    Admin, ArchiveTable, ConcurrencyModel, Consumer, MessageTable, Producer, QueueTable, Run,
    StepResult, Store, Worker, WorkerTable, Workflow, WorkflowRunTable, WorkflowStepTable,
    WorkflowTable,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use std::sync::Arc;

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::archive::SqliteArchiveTable;
use self::tables::messages::SqliteMessageTable;
use self::tables::queues::SqliteQueueTable;
use self::tables::runs::SqliteWorkflowRunTable;
use self::tables::steps::SqliteWorkflowStepTable;
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
    workflow_runs: Arc<SqliteWorkflowRunTable>,
    workflow_steps: Arc<SqliteWorkflowStepTable>,
}

impl SqliteStore {
    /// Create a new SQLite store with default optimizations.
    ///
    /// This initializes the pool with:
    /// - WAL mode enabled for concurrency
    /// - 5s busy timeout
    /// - Foreign Keys enforced
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
            workflow_runs: Arc::new(SqliteWorkflowRunTable::new(pool.clone())),
            workflow_steps: Arc::new(SqliteWorkflowStepTable::new(pool)),
        })
    }
}

/// Parse SQLite TEXT timestamp to DateTime<Utc>
pub fn parse_sqlite_timestamp(s: &str) -> Result<DateTime<Utc>> {
    // SQLite datetime() returns "YYYY-MM-DD HH:MM:SS" format
    // We append +0000 to parse it as UTC
    DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::Internal {
            message: format!("Invalid timestamp: {}", e),
        })
}

/// Format DateTime<Utc> for SQLite TEXT storage
pub fn format_sqlite_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

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
        sqlx::query(sql)
            .bind(param)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: format!("Failed to execute raw SQL with param {}", param),
            })?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(&self, sql: &str, param1: i64, param2: i64) -> Result<()> {
        sqlx::query(sql)
            .bind(param1)
            .bind(param2)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: format!(
                    "Failed to execute raw SQL with params {}, {}",
                    param1, param2
                ),
            })?;
        Ok(())
    }

    async fn query_int(&self, sql: &str) -> Result<i64> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: "Failed to query int".into(),
            })
    }

    async fn query_string(&self, sql: &str) -> Result<String> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: "Failed to query string".into(),
            })
    }

    async fn query_bool(&self, sql: &str) -> Result<bool> {
        let val: i64 = sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::QueryFailed {
                query: sql.to_string(),
                source: Box::new(e),
                context: "Failed to query bool".into(),
            })?;
        Ok(val != 0)
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

    fn workflow_runs(&self) -> &dyn WorkflowRunTable {
        self.workflow_runs.as_ref()
    }

    fn workflow_steps(&self) -> &dyn WorkflowStepTable {
        self.workflow_steps.as_ref()
    }

    async fn acquire_step(
        &self,
        run_id: i64,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<StepResult<serde_json::Value>> {
        use self::workflow::guard::SqliteStepGuard;
        SqliteStepGuard::acquire(&self.pool, run_id, step_id, current_time).await
    }

    async fn admin(&self, config: &Config) -> Result<Box<dyn Admin>> {
        use self::worker::admin::SqliteAdmin;
        let admin = SqliteAdmin::new(self.pool.clone(), config.clone());
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

    fn workflow(&self, name: &str) -> Result<Box<dyn Workflow>> {
        use self::workflow::handle::SqliteWorkflow;
        Ok(Box::new(SqliteWorkflow::new(name)))
    }

    async fn create_workflow(&self, name: &str) -> Result<()> {
        // Ensure backing queue exists (name is the queue name for now).
        // We avoid relying on backend-specific upsert SQL by doing: exists -> insert.
        let queue_exists = self.queues.exists(name).await?;
        if !queue_exists {
            let _queue = self
                .queues
                .insert(crate::types::NewQueue {
                    queue_name: name.to_string(),
                })
                .await?;
        }

        let queue = self.queues.get_by_name(name).await?;

        // Create workflow definition. This is strict: it errors if the workflow already exists.
        let _workflow = self
            .workflows
            .insert(crate::types::NewWorkflow {
                name: name.to_string(),
                queue_id: queue.id,
            })
            .await
            .map(|_| ())
            .map_err(|e| {
                // SQLite unique constraint violation code is 2067 (SQLITE_CONSTRAINT_UNIQUE)
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

        Ok(())
    }

    async fn trigger_workflow(&self, name: &str, input: Option<serde_json::Value>) -> Result<i64> {
        use crate::types::NewMessage;

        // Strict semantics: triggering requires the workflow definition (and queue) to exist.
        // Callers must create the workflow definition explicitly.
        let queue = self.queues.get_by_name(name).await?;

        // Create run record.
        let run = self
            .workflow_runs
            .insert(crate::types::NewWorkflowRun {
                workflow_name: name.to_string(),
                input,
            })
            .await?;

        // Enqueue message with only run_id.
        let now = chrono::Utc::now();
        let payload = serde_json::json!({ "run_id": run.id });

        let _msg = self
            .messages
            .insert(NewMessage {
                queue_id: queue.id,
                payload,
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await?;

        Ok(run.id)
    }

    async fn run(&self, run_id: i64) -> Result<Box<dyn Run>> {
        use self::workflow::handle::SqliteRun;
        Ok(Box::new(SqliteRun::new(self.pool.clone(), run_id)))
    }

    fn worker(&self, id: i64) -> Box<dyn Worker> {
        use self::worker::SqliteWorkerHandle;
        Box::new(SqliteWorkerHandle::new(self.pool.clone(), id))
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
