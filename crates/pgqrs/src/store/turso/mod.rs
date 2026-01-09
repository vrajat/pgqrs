//! Turso backend (Rust-native SQLite rewrite).
//!
//! Stub implementation of the `Store` trait backed by Turso. This wires the
//! backend so feature-gated builds compile; functional behavior will be added
//! incrementally.

use std::sync::Arc;

use async_trait::async_trait;
use turso::{Builder, Connection, Database, Value};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::{
    Admin, ArchiveTable, ConcurrencyModel, Consumer, MessageTable, Producer, QueueTable, Store,
    Worker, WorkerTable, Workflow, WorkflowTable,
};
use crate::types::{
    ArchivedMessage, NewArchivedMessage, NewQueue, NewWorkflow, QueueInfo, QueueMessage,
    WorkerInfo, WorkerStatus, WorkflowRecord,
};

#[derive(Clone, Debug)]
pub struct TursoStore {
    db: Arc<Database>,
    config: Config,
    queues: Arc<TursoQueueTable>,
    messages: Arc<TursoMessageTable>,
    workers: Arc<TursoWorkerTable>,
    archive: Arc<TursoArchiveTable>,
    workflows: Arc<TursoWorkflowTable>,
}

impl TursoStore {
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let path = dsn
            .strip_prefix("turso://")
            .or_else(|| dsn.strip_prefix("file:"))
            .unwrap_or(dsn);

        // Vanilla builder; tuning will be added later as needed.
        let builder = Builder::new_local(path);
        let db = builder.build().await.map_err(|e| Error::Internal {
            message: format!("Failed to initialize Turso database: {e}"),
        })?;

        Ok(Self {
            db: Arc::new(db),
            config: config.clone(),
            queues: Arc::new(TursoQueueTable::default()),
            messages: Arc::new(TursoMessageTable::default()),
            workers: Arc::new(TursoWorkerTable::default()),
            archive: Arc::new(TursoArchiveTable::default()),
            workflows: Arc::new(TursoWorkflowTable::default()),
        })
    }

    fn not_implemented<T>(&self, what: &str) -> Result<T> {
        Err(Error::Internal {
            message: format!("turso backend not yet implemented: {what}"),
        })
    }

    fn connect(&self) -> Result<Connection> {
        self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open Turso connection: {e}"),
        })
    }
}

#[async_trait]
impl Store for TursoStore {
    // Turso is not backed by SQLx; we use the SQLite database type to satisfy the trait bound.
    type Db = sqlx::Sqlite;

    async fn execute_raw(&self, _sql: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(_sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Turso execute failed: {e}"),
        })?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, _sql: &str, _param: i64) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(_sql, [_param])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Turso execute failed: {e}"),
            })?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(&self, _sql: &str, _param1: i64, _param2: i64) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(_sql, [_param1, _param2])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Turso execute failed: {e}"),
            })?;
        Ok(())
    }

    async fn query_int(&self, _sql: &str) -> Result<i64> {
        let conn = self.connect()?;
        let mut rows = conn.query(_sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Turso query failed: {e}"),
        })?;
        match rows.next().await {
            Ok(Some(row)) => {
                let v: Value = row.get_value(0).map_err(|e| Error::Internal {
                    message: format!("Turso value get failed: {e}"),
                })?;
                Ok(*v.as_integer().unwrap_or(&0))
            }
            Ok(None) => Ok(0),
            Err(e) => Err(Error::Internal {
                message: format!("Turso row read failed: {e}"),
            }),
        }
    }

    async fn query_string(&self, _sql: &str) -> Result<String> {
        let conn = self.connect()?;
        let mut rows = conn.query(_sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Turso query failed: {e}"),
        })?;
        match rows.next().await {
            Ok(Some(row)) => {
                let v: Value = row.get_value(0).map_err(|e| Error::Internal {
                    message: format!("Turso value get failed: {e}"),
                })?;
                Ok(v.as_text().cloned().unwrap_or_default())
            }
            Ok(None) => Ok(String::new()),
            Err(e) => Err(Error::Internal {
                message: format!("Turso row read failed: {e}"),
            }),
        }
    }

    async fn query_bool(&self, _sql: &str) -> Result<bool> {
        Ok(self.query_int(_sql).await? != 0)
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

    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        _name: &str,
        _input: &T,
    ) -> Result<Box<dyn Workflow>> {
        self.not_implemented("create_workflow")
    }

    async fn acquire_step(
        &self,
        _workflow_id: i64,
        _step_id: &str,
    ) -> Result<crate::store::StepResult<serde_json::Value>> {
        self.not_implemented("acquire_step")
    }

    async fn admin(&self, _config: &Config) -> Result<Box<dyn Admin>> {
        Ok(Box::new(TursoAdmin::new(self.clone())))
    }

    async fn producer(
        &self,
        _queue: &str,
        _hostname: &str,
        _port: i32,
        _config: &Config,
    ) -> Result<Box<dyn Producer>> {
        Ok(Box::new(TursoProducer::new(self.clone())))
    }

    async fn consumer(
        &self,
        _queue: &str,
        _hostname: &str,
        _port: i32,
        _config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        Ok(Box::new(TursoConsumer::new(self.clone())))
    }

    fn workflow(&self, id: i64) -> Box<dyn Workflow> {
        Box::new(TursoWorkflowHandle { id })
    }

    fn worker(&self, id: i64) -> Box<dyn Worker> {
        Box::new(TursoWorkerHandle { id })
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }

    fn backend_name(&self) -> &'static str {
        "turso"
    }

    async fn producer_ephemeral(&self, queue: &str, config: &Config) -> Result<Box<dyn Producer>> {
        self.producer(queue, "", -1, config).await
    }

    async fn consumer_ephemeral(&self, queue: &str, config: &Config) -> Result<Box<dyn Consumer>> {
        self.consumer(queue, "", -1, config).await
    }
}

#[derive(Clone, Default, Debug)]
struct TursoQueueTable;

#[async_trait]
impl QueueTable for TursoQueueTable {
    async fn insert(&self, _data: NewQueue) -> Result<QueueInfo> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<QueueInfo> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn get_by_name(&self, _name: &str) -> Result<QueueInfo> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn exists(&self, _name: &str) -> Result<bool> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }

    async fn delete_by_name(&self, _name: &str) -> Result<u64> {
        Err(Error::Internal {
            message: "turso queue table not implemented".to_string(),
        })
    }
}

#[derive(Clone, Default, Debug)]
struct TursoMessageTable;

#[async_trait]
impl MessageTable for TursoMessageTable {
    async fn insert(&self, _data: crate::types::NewMessage) -> Result<QueueMessage> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<QueueMessage> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn batch_insert(
        &self,
        _queue_id: i64,
        _payloads: &[serde_json::Value],
        _params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn get_by_ids(&self, _ids: &[i64]) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn update_visibility_timeout(
        &self,
        _id: i64,
        _vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn extend_visibility(
        &self,
        _id: i64,
        _worker_id: i64,
        _additional_seconds: u32,
    ) -> Result<u64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn extend_visibility_batch(
        &self,
        _message_ids: &[i64],
        _worker_id: i64,
        _additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn release_messages_by_ids(
        &self,
        _message_ids: &[i64],
        _worker_id: i64,
    ) -> Result<Vec<bool>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn count_pending(&self, _queue_id: i64) -> Result<i64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn count_pending_filtered(&self, _queue_id: i64, _worker_id: Option<i64>) -> Result<i64> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }

    async fn delete_by_ids(&self, _ids: &[i64]) -> Result<Vec<bool>> {
        Err(Error::Internal {
            message: "turso message table not implemented".to_string(),
        })
    }
}

#[derive(Clone, Default, Debug)]
struct TursoWorkerTable;

#[async_trait]
impl WorkerTable for TursoWorkerTable {
    async fn insert(&self, _data: crate::types::NewWorker) -> Result<WorkerInfo> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<WorkerInfo> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<WorkerInfo>> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn count_for_queue(&self, _queue_id: i64, _state: WorkerStatus) -> Result<i64> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn count_zombies_for_queue(
        &self,
        _queue_id: i64,
        _older_than: chrono::Duration,
    ) -> Result<i64> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn list_for_queue(
        &self,
        _queue_id: i64,
        _state: WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn list_zombies_for_queue(
        &self,
        _queue_id: i64,
        _older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn register(
        &self,
        _queue_id: Option<i64>,
        _hostname: &str,
        _port: i32,
    ) -> Result<WorkerInfo> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }

    async fn register_ephemeral(&self, _queue_id: Option<i64>) -> Result<WorkerInfo> {
        Err(Error::Internal {
            message: "turso worker table not implemented".to_string(),
        })
    }
}

#[derive(Clone, Default, Debug)]
struct TursoArchiveTable;

#[async_trait]
impl ArchiveTable for TursoArchiveTable {
    async fn insert(&self, _data: NewArchivedMessage) -> Result<ArchivedMessage> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<ArchivedMessage> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn list_dlq_messages(
        &self,
        _max_attempts: i32,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn dlq_count(&self, _max_attempts: i32) -> Result<i64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn list_by_worker(
        &self,
        _worker_id: i64,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn count_by_worker(&self, _worker_id: i64) -> Result<i64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn delete_by_worker(&self, _worker_id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn replay_message(&self, _msg_id: i64) -> Result<Option<QueueMessage>> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }

    async fn count_for_queue(&self, _queue_id: i64) -> Result<i64> {
        Err(Error::Internal {
            message: "turso archive table not implemented".to_string(),
        })
    }
}

#[derive(Clone, Default, Debug)]
struct TursoWorkflowTable;

#[async_trait]
impl WorkflowTable for TursoWorkflowTable {
    async fn insert(&self, _data: NewWorkflow) -> Result<WorkflowRecord> {
        Err(Error::Internal {
            message: "turso workflow table not implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<WorkflowRecord> {
        Err(Error::Internal {
            message: "turso workflow table not implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        Err(Error::Internal {
            message: "turso workflow table not implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        Err(Error::Internal {
            message: "turso workflow table not implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso workflow table not implemented".to_string(),
        })
    }
}

#[derive(Clone)]
struct TursoWorkerHandle {
    id: i64,
}

#[async_trait]
impl Worker for TursoWorkerHandle {
    fn worker_id(&self) -> i64 {
        self.id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }

    async fn suspend(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }

    async fn resume(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }

    async fn shutdown(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }

    async fn heartbeat(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }

    async fn is_healthy(&self, _max_age: chrono::Duration) -> Result<bool> {
        Err(Error::Internal {
            message: "turso worker not implemented".to_string(),
        })
    }
}

struct TursoAdmin {
    worker: TursoWorkerHandle,
    store: TursoStore,
}

impl TursoAdmin {
    fn new(store: TursoStore) -> Self {
        Self {
            worker: TursoWorkerHandle { id: 0 },
            store,
        }
    }
}

#[async_trait]
impl Worker for TursoAdmin {
    fn worker_id(&self) -> i64 {
        self.worker.worker_id()
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.worker.status().await
    }

    async fn suspend(&self) -> Result<()> {
        self.worker.suspend().await
    }

    async fn resume(&self) -> Result<()> {
        self.worker.resume().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.worker.shutdown().await
    }

    async fn heartbeat(&self) -> Result<()> {
        self.worker.heartbeat().await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.worker.is_healthy(max_age).await
    }
}

#[async_trait]
impl Admin for TursoAdmin {
    async fn install(&self) -> Result<()> {
        let conn = self.store.connect()?;
        // Execute Turso-specific migrations (CHECK constraints removed)
        const Q1: &str = include_str!("../../../migrations/turso/01_create_queues.sql");
        const Q2: &str = include_str!("../../../migrations/turso/02_create_workers.sql");
        const Q3: &str = include_str!("../../../migrations/turso/03_create_messages.sql");
        const Q4: &str = include_str!("../../../migrations/turso/04_create_archive.sql");
        const Q5: &str = include_str!("../../../migrations/turso/05_create_workflows.sql");
        const Q6: &str = include_str!("../../../migrations/turso/06_create_workflow_steps.sql");

        for sql in [Q1, Q2, Q3, Q4, Q5, Q6] {
            conn.execute(sql, ()).await.map_err(|e| Error::Internal {
                message: format!("Turso migration failed: {e}"),
            })?;
        }
        Ok(())
    }

    async fn verify(&self) -> Result<()> {
        let conn = self.store.connect()?;

        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
            ("pgqrs_archive", "Unified archive table"),
            ("pgqrs_workflows", "Workflow table"),
            ("pgqrs_workflow_steps", "Workflow steps table"),
        ];

        for (table_name, description) in &required_tables {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?1",
                    [*table_name],
                )
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Turso verify query failed: {e}"),
                })?;

            let exists = match rows.next().await {
                Ok(Some(row)) => {
                    let v = row.get_value(0).map_err(|e| Error::Internal {
                        message: format!("Turso value get failed: {e}"),
                    })?;
                    *v.as_integer().unwrap_or(&0) > 0
                }
                Ok(None) => false,
                Err(e) => {
                    return Err(Error::Internal {
                        message: format!("Turso verify row failed: {e}"),
                    })
                }
            };

            if !exists {
                return Err(Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        Ok(())
    }

    async fn register(&mut self, _hostname: String, _port: i32) -> Result<WorkerInfo> {
        Err(Error::Internal {
            message: "turso admin register not yet implemented".to_string(),
        })
    }

    async fn create_queue(&self, _name: &str) -> Result<QueueInfo> {
        Err(Error::Internal {
            message: "turso admin create_queue not yet implemented".to_string(),
        })
    }

    async fn get_queue(&self, _name: &str) -> Result<QueueInfo> {
        Err(Error::Internal {
            message: "turso admin get_queue not yet implemented".to_string(),
        })
    }

    async fn delete_queue(&self, _queue_info: &QueueInfo) -> Result<()> {
        Err(Error::Internal {
            message: "turso admin delete_queue not yet implemented".to_string(),
        })
    }

    async fn purge_queue(&self, _name: &str) -> Result<()> {
        Err(Error::Internal {
            message: "turso admin purge_queue not yet implemented".to_string(),
        })
    }

    async fn dlq(&self) -> Result<Vec<i64>> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn queue_metrics(&self, _name: &str) -> Result<crate::types::QueueMetrics> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn all_queues_metrics(&self) -> Result<Vec<crate::types::QueueMetrics>> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn system_stats(&self) -> Result<crate::types::SystemStats> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn worker_health_stats(
        &self,
        _heartbeat_timeout: chrono::Duration,
        _group_by_queue: bool,
    ) -> Result<Vec<crate::types::WorkerHealthStats>> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn worker_stats(&self, _queue_name: &str) -> Result<crate::types::WorkerStats> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn delete_worker(&self, _worker_id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn get_worker_messages(&self, _worker_id: i64) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn reclaim_messages(
        &self,
        _queue_id: i64,
        _older_than: Option<chrono::Duration>,
    ) -> Result<u64> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn purge_old_workers(&self, _older_than: chrono::Duration) -> Result<u64> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }

    async fn release_worker_messages(&self, _worker_id: i64) -> Result<u64> {
        Err(Error::Internal {
            message: "turso admin not implemented".to_string(),
        })
    }
}

struct TursoProducer {
    _store: TursoStore,
    validation_config: crate::validation::ValidationConfig,
}

impl TursoProducer {
    fn new(store: TursoStore) -> Self {
        Self {
            _store: store,
            validation_config: crate::validation::ValidationConfig::default(),
        }
    }
}

#[async_trait]
impl Worker for TursoProducer {
    fn worker_id(&self) -> i64 {
        0
    }

    async fn status(&self) -> Result<WorkerStatus> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn suspend(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn resume(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn shutdown(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn heartbeat(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn is_healthy(&self, _max_age: chrono::Duration) -> Result<bool> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }
}

#[async_trait]
impl Producer for TursoProducer {
    async fn get_message_by_id(&self, _msg_id: i64) -> Result<QueueMessage> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn enqueue(&self, _payload: &serde_json::Value) -> Result<QueueMessage> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn enqueue_delayed(
        &self,
        _payload: &serde_json::Value,
        _delay_seconds: u32,
    ) -> Result<QueueMessage> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn batch_enqueue(&self, _payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn batch_enqueue_delayed(
        &self,
        _payloads: &[serde_json::Value],
        _delay_seconds: u32,
    ) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn insert_message(
        &self,
        _payload: &serde_json::Value,
        _now: chrono::DateTime<chrono::Utc>,
        _vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    async fn replay_dlq(&self, _archived_msg_id: i64) -> Result<Option<QueueMessage>> {
        Err(Error::Internal {
            message: "turso producer not implemented".to_string(),
        })
    }

    fn validation_config(&self) -> &crate::validation::ValidationConfig {
        &self.validation_config
    }

    fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        None
    }
}

struct TursoConsumer {
    _store: TursoStore,
}

impl TursoConsumer {
    fn new(store: TursoStore) -> Self {
        Self { _store: store }
    }
}

#[async_trait]
impl Worker for TursoConsumer {
    fn worker_id(&self) -> i64 {
        0
    }

    async fn status(&self) -> Result<WorkerStatus> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn suspend(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn resume(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn shutdown(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn heartbeat(&self) -> Result<()> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn is_healthy(&self, _max_age: chrono::Duration) -> Result<bool> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }
}

#[async_trait]
impl Consumer for TursoConsumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn dequeue_many(&self, _limit: usize) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn dequeue_delay(&self, _vt: u32) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn dequeue_many_with_delay(&self, _limit: usize, _vt: u32) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn dequeue_at(
        &self,
        _limit: usize,
        _vt: u32,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<QueueMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn extend_visibility(&self, _message_id: i64, _additional_seconds: u32) -> Result<bool> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn delete(&self, _message_id: i64) -> Result<bool> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn delete_many(&self, _message_ids: Vec<i64>) -> Result<Vec<bool>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn archive(&self, _msg_id: i64) -> Result<Option<ArchivedMessage>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn archive_many(&self, _msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }

    async fn release_messages(&self, _message_ids: &[i64]) -> Result<u64> {
        Err(Error::Internal {
            message: "turso consumer not implemented".to_string(),
        })
    }
}

struct TursoWorkflowHandle {
    id: i64,
}

#[async_trait]
impl Workflow for TursoWorkflowHandle {
    fn id(&self) -> i64 {
        self.id
    }

    async fn start(&mut self) -> Result<()> {
        Err(Error::Internal {
            message: "turso workflow not implemented".to_string(),
        })
    }

    async fn complete(&mut self, _output: serde_json::Value) -> Result<()> {
        Err(Error::Internal {
            message: "turso workflow not implemented".to_string(),
        })
    }

    async fn fail_with_json(&mut self, _error: serde_json::Value) -> Result<()> {
        Err(Error::Internal {
            message: "turso workflow not implemented".to_string(),
        })
    }

    async fn acquire_step(
        &self,
        _step_id: &str,
    ) -> Result<crate::store::StepResult<serde_json::Value>> {
        Err(Error::Internal {
            message: "turso workflow not implemented".to_string(),
        })
    }
}
