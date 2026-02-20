use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::ConcurrencyModel;
use crate::store::{
    MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store, WorkerTable, WorkflowTable,
};
use crate::{Admin, Worker};

use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use std::sync::Arc;

pub mod tables;
pub mod worker;

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
        _config: &Config,
    ) -> Result<crate::workers::Producer> {
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let worker_record = self
            .workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        Ok(crate::workers::Producer::new(
            crate::store::AnyStore::Sqlite(self.clone()),
            queue_info,
            worker_record,
            _config.validation_config.clone(),
        ))
    }

    async fn consumer(
        &self,
        queue_name: &str,
        hostname: &str,
        port: i32,
        _config: &Config,
    ) -> Result<crate::workers::Consumer> {
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let worker_record = self
            .workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        Ok(crate::workers::Consumer::new(
            crate::store::AnyStore::Sqlite(self.clone()),
            queue_info,
            worker_record,
        ))
    }

    async fn queue(&self, name: &str) -> Result<crate::types::QueueRecord> {
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

    async fn workflow(&self, name: &str) -> Result<crate::types::WorkflowRecord> {
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

        Ok(workflow_record)
    }

    async fn trigger(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> Result<crate::types::QueueMessage> {
        use crate::types::NewQueueMessage;

        let queue = self.queues.get_by_name(name).await?;
        let now = chrono::Utc::now();

        let payload = input.unwrap_or(serde_json::Value::Null);

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

    async fn run(&self, message: crate::types::QueueMessage) -> Result<crate::workers::Run> {
        let payload = &message.payload;

        // If payload has run_id, it's a resumption or already initialized
        if let Some(run_id) = payload.get("run_id").and_then(|v| v.as_i64()) {
            let record = self.workflow_runs.get(run_id).await?;
            return Ok(crate::workers::Run::new(
                crate::store::AnyStore::Sqlite(self.clone()),
                record,
            ));
        }

        // Otherwise, it's a new trigger. Create run record.
        let queue = self.queues.get(message.queue_id).await?;
        let workflow = self.workflows.get_by_name(&queue.queue_name).await?;

        let run_rec = self
            .workflow_runs
            .insert(crate::types::NewRunRecord {
                workflow_id: workflow.id,
                input: Some(payload.clone()),
            })
            .await?;

        // Update message payload to include run_id for future resumptions
        let mut new_payload = payload.clone();
        if let Some(obj) = new_payload.as_object_mut() {
            obj.insert("run_id".to_string(), serde_json::json!(run_rec.id));
        } else {
            // If payload is not an object, wrap it
            new_payload = serde_json::json!({
                "input": payload,
                "run_id": run_rec.id
            });
        }
        self.messages
            .update_payload(message.id, new_payload)
            .await?;

        Ok(crate::workers::Run::new(
            crate::store::AnyStore::Sqlite(self.clone()),
            run_rec,
        ))
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
        _config: &Config,
    ) -> Result<crate::workers::Producer> {
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let worker_record = self.workers.register_ephemeral(Some(queue_info.id)).await?;

        Ok(crate::workers::Producer::new(
            crate::store::AnyStore::Sqlite(self.clone()),
            queue_info,
            worker_record,
            _config.validation_config.clone(),
        ))
    }

    async fn consumer_ephemeral(
        &self,
        queue_name: &str,
        _config: &Config,
    ) -> Result<crate::workers::Consumer> {
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let worker_record = self.workers.register_ephemeral(Some(queue_info.id)).await?;

        Ok(crate::workers::Consumer::new(
            crate::store::AnyStore::Sqlite(self.clone()),
            queue_info,
            worker_record,
        ))
    }
}
