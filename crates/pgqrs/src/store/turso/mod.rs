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
    WorkerInfo, WorkerStatus, WorkflowRecord, WorkflowStatus,
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

        let db = Arc::new(db);
        Ok(Self {
            db: Arc::clone(&db),
            config: config.clone(),
            queues: Arc::new(TursoQueueTable::new(Arc::clone(&db))),
            messages: Arc::new(TursoMessageTable::new(Arc::clone(&db))),
            workers: Arc::new(TursoWorkerTable::new(Arc::clone(&db))),
            archive: Arc::new(TursoArchiveTable::new(Arc::clone(&db))),
            workflows: Arc::new(TursoWorkflowTable::new(Arc::clone(&db))),
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

#[derive(Clone, Debug)]
struct TursoQueueTable {
    db: Arc<Database>,
}

impl TursoQueueTable {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<QueueInfo> {
        let id: i64 = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "Failed to extract queue id from row".to_string(),
            })?;
        let queue_name: String = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .ok_or_else(|| Error::Internal {
                message: "Failed to extract queue name from row".to_string(),
            })?;
        let created_at_str: String = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .ok_or_else(|| Error::Internal {
                message: "Failed to extract created_at from row".to_string(),
            })?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .unwrap_or_else(|_| {
                chrono::DateTime::parse_from_rfc2822(&created_at_str).unwrap_or_else(|_| {
                    chrono::Local::now().with_timezone(&chrono::FixedOffset::east_opt(0).unwrap())
                })
            })
            .with_timezone(&chrono::Utc);

        Ok(QueueInfo {
            id,
            queue_name,
            created_at,
        })
    }
}

#[async_trait]
impl QueueTable for TursoQueueTable {
    async fn insert(&self, _data: NewQueue) -> Result<QueueInfo> {
        // TODO: Phase 2.1 - implement Turso queue insert
        Err(Error::Internal {
            message: "turso queue insert not yet implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<QueueInfo> {
        // TODO: Phase 2.1 - implement Turso queue get
        Err(Error::Internal {
            message: "turso queue get not yet implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        // TODO: Phase 2.1 - implement Turso queue list
        Err(Error::Internal {
            message: "turso queue list not yet implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso queue count
        Err(Error::Internal {
            message: "turso queue count not yet implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso queue delete
        Err(Error::Internal {
            message: "turso queue delete not yet implemented".to_string(),
        })
    }

    async fn get_by_name(&self, _name: &str) -> Result<QueueInfo> {
        // TODO: Phase 2.1 - implement Turso queue get_by_name
        Err(Error::Internal {
            message: "turso queue get_by_name not yet implemented".to_string(),
        })
    }

    async fn exists(&self, _name: &str) -> Result<bool> {
        // TODO: Phase 2.1 - implement Turso queue exists
        Err(Error::Internal {
            message: "turso queue exists not yet implemented".to_string(),
        })
    }

    async fn delete_by_name(&self, _name: &str) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso queue delete_by_name
        Err(Error::Internal {
            message: "turso queue delete_by_name not yet implemented".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct TursoMessageTable {
    db: Arc<Database>,
}

impl TursoMessageTable {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<QueueMessage> {
        let id: i64 = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid queue_id in message row".to_string(),
            })?;
        let queue_id: i64 = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid queue_id in message row".to_string(),
            })?;
        let payload_str: String = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid payload in message row".to_string(),
            })?;
        let payload: serde_json::Value =
            serde_json::from_str(&payload_str).unwrap_or(serde_json::json!({}));
        let vt_str: String = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid vt in message row".to_string(),
            })?;
        let vt = chrono::DateTime::parse_from_rfc3339(&vt_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&vt_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let enqueued_at_str: String = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid enqueued_at in message row".to_string(),
            })?;
        let enqueued_at = chrono::DateTime::parse_from_rfc3339(&enqueued_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&enqueued_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let read_ct: i32 = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_integer().copied().map(|i| i as i32))
            .unwrap_or(0);
        let dequeued_at_str: Option<String> = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let dequeued_at = dequeued_at_str.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .or_else(|| chrono::DateTime::parse_from_rfc2822(&s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
        let producer_worker_id: Option<i64> =
            row.get_value(7).ok().and_then(|v| v.as_integer().copied());
        let consumer_worker_id: Option<i64> =
            row.get_value(8).ok().and_then(|v| v.as_integer().copied());

        Ok(QueueMessage {
            id,
            queue_id,
            payload,
            vt,
            enqueued_at,
            read_ct,
            dequeued_at,
            producer_worker_id,
            consumer_worker_id,
        })
    }
}

#[async_trait]
impl MessageTable for TursoMessageTable {
    async fn insert(&self, _data: crate::types::NewMessage) -> Result<QueueMessage> {
        // TODO: Phase 2.1 - implement Turso message insert
        Err(Error::Internal {
            message: "turso message insert not yet implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<QueueMessage> {
        // TODO: Phase 2.1 - implement Turso message get
        Err(Error::Internal {
            message: "turso message get not yet implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        // TODO: Phase 2.1 - implement Turso message list
        Err(Error::Internal {
            message: "turso message list not yet implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso message count
        Err(Error::Internal {
            message: "turso message count not yet implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso message delete
        Err(Error::Internal {
            message: "turso message delete not yet implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<QueueMessage>> {
        // TODO: Phase 2.1 - implement Turso message filter_by_fk
        Err(Error::Internal {
            message: "turso message filter_by_fk not yet implemented".to_string(),
        })
    }

    async fn batch_insert(
        &self,
        _queue_id: i64,
        _payloads: &[serde_json::Value],
        _params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        // TODO: Phase 2.1 - implement Turso message batch_insert
        Err(Error::Internal {
            message: "turso message batch_insert not yet implemented".to_string(),
        })
    }

    async fn get_by_ids(&self, _ids: &[i64]) -> Result<Vec<QueueMessage>> {
        // TODO: Phase 2.1 - implement Turso message get_by_ids
        Err(Error::Internal {
            message: "turso message get_by_ids not yet implemented".to_string(),
        })
    }

    async fn update_visibility_timeout(
        &self,
        _id: i64,
        _vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso message update_visibility_timeout
        Err(Error::Internal {
            message: "turso message update_visibility_timeout not yet implemented".to_string(),
        })
    }

    async fn extend_visibility(
        &self,
        _id: i64,
        _worker_id: i64,
        _additional_seconds: u32,
    ) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso message extend_visibility
        Err(Error::Internal {
            message: "turso message extend_visibility not yet implemented".to_string(),
        })
    }

    async fn extend_visibility_batch(
        &self,
        _message_ids: &[i64],
        _worker_id: i64,
        _additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        // TODO: Phase 2.1 - implement Turso message extend_visibility_batch
        Err(Error::Internal {
            message: "turso message extend_visibility_batch not yet implemented".to_string(),
        })
    }

    async fn release_messages_by_ids(
        &self,
        _message_ids: &[i64],
        _worker_id: i64,
    ) -> Result<Vec<bool>> {
        // TODO: Phase 2.1 - implement Turso message release_messages_by_ids
        Err(Error::Internal {
            message: "turso message release_messages_by_ids not yet implemented".to_string(),
        })
    }

    async fn count_pending(&self, _queue_id: i64) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso message count_pending
        Err(Error::Internal {
            message: "turso message count_pending not yet implemented".to_string(),
        })
    }

    async fn count_pending_filtered(&self, _queue_id: i64, _worker_id: Option<i64>) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso message count_pending_filtered
        Err(Error::Internal {
            message: "turso message count_pending_filtered not yet implemented".to_string(),
        })
    }

    async fn delete_by_ids(&self, _ids: &[i64]) -> Result<Vec<bool>> {
        // TODO: Phase 2.1 - implement Turso message delete_by_ids
        Err(Error::Internal {
            message: "turso message delete_by_ids not yet implemented".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct TursoWorkerTable {
    db: Arc<Database>,
}

impl TursoWorkerTable {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<WorkerInfo> {
        let id: i64 = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid id in worker row".to_string(),
            })?;
        let queue_id: Option<i64> = row.get_value(1).ok().and_then(|v| v.as_integer().copied());
        let hostname: String = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid hostname in worker row".to_string(),
            })?;
        let port: i32 = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_integer().copied().map(|i| i as i32))
            .unwrap_or(0);
        let started_at_str: String = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid started_at in worker row".to_string(),
            })?;
        let started_at = chrono::DateTime::parse_from_rfc3339(&started_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&started_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let heartbeat_at_str: String = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid heartbeat_at in worker row".to_string(),
            })?;
        let heartbeat_at = chrono::DateTime::parse_from_rfc3339(&heartbeat_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&heartbeat_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let shutdown_at_str: Option<String> = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let shutdown_at = shutdown_at_str.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .or_else(|| chrono::DateTime::parse_from_rfc2822(&s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
        let status_str: String = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .unwrap_or_else(|| "ready".to_string());
        let status = status_str
            .parse::<WorkerStatus>()
            .unwrap_or(WorkerStatus::Ready);

        Ok(WorkerInfo {
            id,
            hostname,
            port,
            queue_id,
            started_at,
            heartbeat_at,
            shutdown_at,
            status,
        })
    }
}

#[async_trait]
impl WorkerTable for TursoWorkerTable {
    async fn insert(&self, _data: crate::types::NewWorker) -> Result<WorkerInfo> {
        // TODO: Phase 2.1 - implement Turso worker insert
        Err(Error::Internal {
            message: "turso worker insert not yet implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<WorkerInfo> {
        // TODO: Phase 2.1 - implement Turso worker get
        Err(Error::Internal {
            message: "turso worker get not yet implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        // TODO: Phase 2.1 - implement Turso worker list
        Err(Error::Internal {
            message: "turso worker list not yet implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso worker count
        Err(Error::Internal {
            message: "turso worker count not yet implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso worker delete
        Err(Error::Internal {
            message: "turso worker delete not yet implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<WorkerInfo>> {
        // TODO: Phase 2.1 - implement Turso worker filter_by_fk
        Err(Error::Internal {
            message: "turso worker filter_by_fk not yet implemented".to_string(),
        })
    }

    async fn count_for_queue(&self, _queue_id: i64, _state: WorkerStatus) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso worker count_for_queue
        Err(Error::Internal {
            message: "turso worker count_for_queue not yet implemented".to_string(),
        })
    }

    async fn count_zombies_for_queue(
        &self,
        _queue_id: i64,
        _older_than: chrono::Duration,
    ) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso worker count_zombies_for_queue
        Err(Error::Internal {
            message: "turso worker count_zombies_for_queue not yet implemented".to_string(),
        })
    }

    async fn list_for_queue(
        &self,
        _queue_id: i64,
        _state: WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        // TODO: Phase 2.1 - implement Turso worker list_for_queue
        Err(Error::Internal {
            message: "turso worker list_for_queue not yet implemented".to_string(),
        })
    }

    async fn list_zombies_for_queue(
        &self,
        _queue_id: i64,
        _older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        // TODO: Phase 2.1 - implement Turso worker list_zombies_for_queue
        Err(Error::Internal {
            message: "turso worker list_zombies_for_queue not yet implemented".to_string(),
        })
    }

    async fn register(
        &self,
        _queue_id: Option<i64>,
        _hostname: &str,
        _port: i32,
    ) -> Result<WorkerInfo> {
        // TODO: Phase 2.1 - implement Turso worker register
        Err(Error::Internal {
            message: "turso worker register not yet implemented".to_string(),
        })
    }

    async fn register_ephemeral(&self, _queue_id: Option<i64>) -> Result<WorkerInfo> {
        // TODO: Phase 2.1 - implement Turso worker register_ephemeral
        Err(Error::Internal {
            message: "turso worker register_ephemeral not yet implemented".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct TursoArchiveTable {
    db: Arc<Database>,
}

impl TursoArchiveTable {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<ArchivedMessage> {
        let id: i64 = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid id in archive row".to_string(),
            })?;
        let original_msg_id: i64 = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid original_msg_id in archive row".to_string(),
            })?;
        let queue_id: i64 = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid queue_id in archive row".to_string(),
            })?;
        let payload_str: String = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid payload in archive row".to_string(),
            })?;
        let payload: serde_json::Value =
            serde_json::from_str(&payload_str).unwrap_or(serde_json::json!({}));
        let enqueued_at_str: String = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid enqueued_at in archive row".to_string(),
            })?;
        let enqueued_at = chrono::DateTime::parse_from_rfc3339(&enqueued_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&enqueued_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let vt_str: String = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid vt in archive row".to_string(),
            })?;
        let vt = chrono::DateTime::parse_from_rfc3339(&vt_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&vt_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let read_ct: i32 = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_integer().copied().map(|i| i as i32))
            .unwrap_or(0);
        let archived_at_str: String = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid archived_at in archive row".to_string(),
            })?;
        let archived_at = chrono::DateTime::parse_from_rfc3339(&archived_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&archived_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let dequeued_at_str: Option<String> = row
            .get_value(8)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let dequeued_at = dequeued_at_str.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .or_else(|| chrono::DateTime::parse_from_rfc2822(&s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
        let producer_worker_id: Option<i64> =
            row.get_value(9).ok().and_then(|v| v.as_integer().copied());
        let consumer_worker_id: Option<i64> =
            row.get_value(10).ok().and_then(|v| v.as_integer().copied());

        Ok(ArchivedMessage {
            id,
            original_msg_id,
            queue_id,
            producer_worker_id,
            consumer_worker_id,
            payload,
            enqueued_at,
            vt,
            read_ct,
            archived_at,
            dequeued_at,
        })
    }
}

#[async_trait]
impl ArchiveTable for TursoArchiveTable {
    async fn insert(&self, _data: NewArchivedMessage) -> Result<ArchivedMessage> {
        // TODO: Phase 2.1 - implement Turso archive insert
        Err(Error::Internal {
            message: "turso archive insert not yet implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<ArchivedMessage> {
        // TODO: Phase 2.1 - implement Turso archive get
        Err(Error::Internal {
            message: "turso archive get not yet implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        // TODO: Phase 2.1 - implement Turso archive list
        Err(Error::Internal {
            message: "turso archive list not yet implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso archive count
        Err(Error::Internal {
            message: "turso archive count not yet implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso archive delete
        Err(Error::Internal {
            message: "turso archive delete not yet implemented".to_string(),
        })
    }

    async fn filter_by_fk(&self, _queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        // TODO: Phase 2.1 - implement Turso archive filter_by_fk
        Err(Error::Internal {
            message: "turso archive filter_by_fk not yet implemented".to_string(),
        })
    }

    async fn list_dlq_messages(
        &self,
        _max_attempts: i32,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        // TODO: Phase 2.1 - implement Turso archive list_dlq_messages
        Err(Error::Internal {
            message: "turso archive list_dlq_messages not yet implemented".to_string(),
        })
    }

    async fn dlq_count(&self, _max_attempts: i32) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso archive dlq_count
        Err(Error::Internal {
            message: "turso archive dlq_count not yet implemented".to_string(),
        })
    }

    async fn list_by_worker(
        &self,
        _worker_id: i64,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        // TODO: Phase 2.1 - implement Turso archive list_by_worker
        Err(Error::Internal {
            message: "turso archive list_by_worker not yet implemented".to_string(),
        })
    }

    async fn count_by_worker(&self, _worker_id: i64) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso archive count_by_worker
        Err(Error::Internal {
            message: "turso archive count_by_worker not yet implemented".to_string(),
        })
    }

    async fn delete_by_worker(&self, _worker_id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso archive delete_by_worker
        Err(Error::Internal {
            message: "turso archive delete_by_worker not yet implemented".to_string(),
        })
    }

    async fn replay_message(&self, _msg_id: i64) -> Result<Option<QueueMessage>> {
        // TODO: Phase 2.1 - implement Turso archive replay_message
        Err(Error::Internal {
            message: "turso archive replay_message not yet implemented".to_string(),
        })
    }

    async fn count_for_queue(&self, _queue_id: i64) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso archive count_for_queue
        Err(Error::Internal {
            message: "turso archive count_for_queue not yet implemented".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct TursoWorkflowTable {
    db: Arc<Database>,
}

impl TursoWorkflowTable {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<WorkflowRecord> {
        let workflow_id: i64 = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .ok_or_else(|| Error::Internal {
                message: "invalid workflow_id in workflow row".to_string(),
            })?;
        let name: String = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid name in workflow row".to_string(),
            })?;
        let status_str: String = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .unwrap_or_else(|| "PENDING".to_string());
        let status = status_str
            .parse::<WorkflowStatus>()
            .unwrap_or(WorkflowStatus::Pending);
        let input_str: Option<String> = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let input = input_str.and_then(|s| serde_json::from_str(&s).ok());
        let output_str: Option<String> = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let output = output_str.and_then(|s| serde_json::from_str(&s).ok());
        let error_str: Option<String> = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        let error = error_str.and_then(|s| serde_json::from_str(&s).ok());
        let created_at_str: String = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid created_at in workflow row".to_string(),
            })?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&created_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let updated_at_str: String = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .ok_or_else(|| Error::Internal {
                message: "invalid updated_at in workflow row".to_string(),
            })?;
        let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
            .ok()
            .or_else(|| chrono::DateTime::parse_from_rfc2822(&updated_at_str).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let executor_id_str: Option<String> = row
            .get_value(8)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));

        Ok(WorkflowRecord {
            workflow_id,
            name,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
            executor_id: executor_id_str,
        })
    }
}

#[async_trait]
impl WorkflowTable for TursoWorkflowTable {
    async fn insert(&self, _data: NewWorkflow) -> Result<WorkflowRecord> {
        // TODO: Phase 2.1 - implement Turso workflow insert
        Err(Error::Internal {
            message: "turso workflow insert not yet implemented".to_string(),
        })
    }

    async fn get(&self, _id: i64) -> Result<WorkflowRecord> {
        // TODO: Phase 2.1 - implement Turso workflow get
        Err(Error::Internal {
            message: "turso workflow get not yet implemented".to_string(),
        })
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        // TODO: Phase 2.1 - implement Turso workflow list
        Err(Error::Internal {
            message: "turso workflow list not yet implemented".to_string(),
        })
    }

    async fn count(&self) -> Result<i64> {
        // TODO: Phase 2.1 - implement Turso workflow count
        Err(Error::Internal {
            message: "turso workflow count not yet implemented".to_string(),
        })
    }

    async fn delete(&self, _id: i64) -> Result<u64> {
        // TODO: Phase 2.1 - implement Turso workflow delete
        Err(Error::Internal {
            message: "turso workflow delete not yet implemented".to_string(),
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
