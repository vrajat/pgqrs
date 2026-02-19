//! Postgres implementation of the Store trait.

use crate::store::{
    Admin as AdminTrait, ArchiveTable, Consumer as ConsumerTrait, MessageTable,
    Producer as ProducerTrait, QueueTable, RunRecordTable, StepRecordTable, Store,
    Worker as WorkerTrait, WorkerTable, WorkflowTable,
};
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;

pub mod tables;
pub mod worker;

use self::tables::pgqrs_archive::Archive as PostgresArchiveTable;
use self::tables::pgqrs_messages::Messages as PostgresMessageTable;
use self::tables::pgqrs_queues::Queues as PostgresQueueTable;
use self::tables::pgqrs_workers::Workers as PostgresWorkerTable;
use self::tables::pgqrs_workflow_runs::RunRecords as PostgresRunRecordTable;
use self::tables::pgqrs_workflow_steps::StepRecords as PostgresStepRecordTable;
use self::tables::pgqrs_workflows::Workflows as PostgresWorkflowTable;
use crate::types::NewQueueMessage;

use self::worker::admin::Admin as PostgresAdmin;
use self::worker::consumer::Consumer as PostgresConsumer;
use self::worker::producer::Producer as PostgresProducer;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
    config: Config,
    queues: Arc<PostgresQueueTable>,
    messages: Arc<PostgresMessageTable>,
    workers: Arc<PostgresWorkerTable>,
    archive: Arc<PostgresArchiveTable>,
    workflows: Arc<PostgresWorkflowTable>,
    workflow_runs: Arc<PostgresRunRecordTable>,
    workflow_steps: Arc<PostgresStepRecordTable>,
}

impl PostgresStore {
    pub fn new(pool: PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            queues: Arc::new(PostgresQueueTable::new(pool.clone())),
            messages: Arc::new(PostgresMessageTable::new(pool.clone())),
            workers: Arc::new(PostgresWorkerTable::new(pool.clone())),
            archive: Arc::new(PostgresArchiveTable::new(pool.clone())),
            workflows: Arc::new(PostgresWorkflowTable::new(pool.clone())),
            workflow_runs: Arc::new(PostgresRunRecordTable::new(pool.clone())),
            workflow_steps: Arc::new(PostgresStepRecordTable::new(pool)),
        }
    }

    /// Get access to the underlying PgPool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()> {
        sqlx::raw_sql(sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()> {
        sqlx::query(sql).bind(param).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(
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

    async fn query_int(&self, sql: &str) -> crate::error::Result<i64> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_string(&self, sql: &str) -> crate::error::Result<String> {
        use sqlx::Row;
        let row = sqlx::raw_sql(sql).fetch_one(&self.pool).await?;
        Ok(row.try_get(0)?)
    }

    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool> {
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
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::types::StepRecord> {
        self.workflow_steps
            .acquire_step(run_id, step_name, current_time)
            .await
    }

    async fn bootstrap(&self) -> crate::error::Result<()> {
        use self::worker::admin::MIGRATOR;
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn AdminTrait>> {
        let admin = PostgresAdmin::new(self.pool.clone(), hostname, port, config.clone()).await?;
        Ok(Box::new(admin))
    }

    async fn admin_ephemeral(&self, config: &Config) -> crate::error::Result<Box<dyn AdminTrait>> {
        let admin = PostgresAdmin::new_ephemeral(self.pool.clone(), config.clone()).await?;
        Ok(Box::new(admin))
    }

    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ProducerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let producer =
            PostgresProducer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ConsumerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let consumer =
            PostgresConsumer::new(self.pool.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(consumer))
    }

    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord> {
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

    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord> {
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

    async fn trigger(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> crate::error::Result<crate::types::QueueMessage> {
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

    async fn run(
        &self,
        message: crate::types::QueueMessage,
    ) -> crate::error::Result<crate::workers::Run> {
        let payload = &message.payload;

        // If payload has run_id, it's a resumption or already initialized
        if let Some(run_id) = payload.get("run_id").and_then(|v| v.as_i64()) {
            let record = self.workflow_runs.get(run_id).await?;
            return Ok(crate::workers::Run::new(
                crate::store::AnyStore::Postgres(self.clone()),
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
            crate::store::AnyStore::Postgres(self.clone()),
            run_rec,
        ))
    }

    async fn worker(&self, id: i64) -> crate::error::Result<Box<dyn WorkerTrait>> {
        let worker_record = self.workers.get(id).await?;
        Ok(Box::new(self::worker::WorkerHandle::new(
            self.pool.clone(),
            worker_record,
        )))
    }

    fn concurrency_model(&self) -> crate::store::ConcurrencyModel {
        crate::store::ConcurrencyModel::MultiProcess
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ProducerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let producer =
            PostgresProducer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn ConsumerTrait>> {
        let queue_info = self.queues.get_by_name(queue).await?;
        let consumer =
            PostgresConsumer::new_ephemeral(self.pool.clone(), &queue_info, config).await?;
        Ok(Box::new(consumer))
    }
}
