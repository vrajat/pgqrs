//! Postgres implementation of the Store trait.

use crate::store::{
    Admin as AdminTrait, ArchiveTable, Consumer as ConsumerTrait, MessageTable,
    Producer as ProducerTrait, QueueTable, Run, Store, Worker as WorkerTrait, WorkerTable,
    Workflow as WorkflowTrait, WorkflowRunTable, WorkflowStepTable, WorkflowTable,
};
use crate::WorkflowRecord;
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::pgqrs_archive::Archive as PostgresArchiveTable;
use self::tables::pgqrs_messages::Messages as PostgresMessageTable;
use self::tables::pgqrs_queues::Queues as PostgresQueueTable;
use self::tables::pgqrs_workers::Workers as PostgresWorkerTable;
use self::tables::pgqrs_workflow_runs::WorkflowRuns as PostgresWorkflowRunTable;
use self::tables::pgqrs_workflow_steps::WorkflowSteps as PostgresWorkflowStepTable;
use self::tables::pgqrs_workflows::Workflows as PostgresWorkflowTable;
use crate::types::{NewMessage, WorkflowRun};

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
    workflow_runs: Arc<PostgresWorkflowRunTable>,
    workflow_steps: Arc<PostgresWorkflowStepTable>,
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
            workflow_runs: Arc::new(PostgresWorkflowRunTable::new(pool.clone())),
            workflow_steps: Arc::new(PostgresWorkflowStepTable::new(pool)),
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
        // Use proper parameter binding to prevent SQL injection
        sqlx::query(sql).bind(param).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()> {
        // Use proper parameter binding to prevent SQL injection
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

    fn workflow_runs(&self) -> &dyn WorkflowRunTable {
        self.workflow_runs.as_ref()
    }

    fn workflow_steps(&self) -> &dyn WorkflowStepTable {
        self.workflow_steps.as_ref()
    }

    async fn admin(&self, config: &Config) -> crate::error::Result<Box<dyn AdminTrait>> {
        let admin = PostgresAdmin::new(config).await?;
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

    fn workflow(&self, name: &str) -> crate::error::Result<Box<dyn WorkflowTrait>> {
        Ok(Box::new(self::workflow::handle::Workflow::new(
            self.pool.clone(),
            name,
        )))
    }

    async fn create_workflow(&self, name: &str) -> crate::error::Result<WorkflowRecord> {
        // Ensure backing queue exists.
        // NOTE: Postgres queues table uses column `queue_name`.
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

        // Create workflow definition (strict semantics).
        let workflow = self
            .workflows
            .insert(crate::types::NewWorkflow {
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

        Ok(workflow)
    }

    async fn trigger_workflow(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> crate::error::Result<WorkflowRun> {
        // Strict semantics: triggering requires the workflow definition + queue to exist.
        let workflow = self.workflows.get_by_name(name).await?;
        let queue = self.queues.get_by_name(name).await?;

        // Create run record.
        let run = self
            .workflow_runs()
            .insert(crate::types::NewWorkflowRun {
                workflow_id: workflow.id,
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

        Ok(run)
    }

    async fn run(&self, run_id: i64) -> crate::error::Result<Box<dyn Run>> {
        use self::workflow::run::PostgresRun;
        Ok(Box::new(PostgresRun::new(self.pool.clone(), run_id)))
    }

    fn worker(&self, id: i64) -> Box<dyn WorkerTrait> {
        Box::new(self::worker::WorkerHandle::new(self.pool.clone(), id))
    }

    async fn acquire_step(
        &self,
        run_id: i64,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        use self::workflow::guard::StepGuard;
        StepGuard::acquire::<serde_json::Value>(&self.pool, run_id, step_id, current_time).await
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
