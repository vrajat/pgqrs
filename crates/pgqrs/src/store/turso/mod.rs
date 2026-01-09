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
    async fn insert(&self, data: NewQueue) -> Result<QueueInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql =
            "INSERT INTO pgqrs_queues (queue_name) VALUES (?) RETURNING id, queue_name, created_at";
        let mut rows = conn
            .query(sql, [data.queue_name.clone()])
            .await
            .map_err(|e| {
                let err_msg = e.to_string();
                if err_msg.contains("UNIQUE constraint failed") || err_msg.contains("constraint") {
                    return Error::QueueAlreadyExists {
                        name: data.queue_name.clone(),
                    };
                }
                Error::Internal {
                    message: format!("Failed to insert queue: {e}"),
                }
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: "No row returned from insert".to_string(),
            })
        }
    }

    async fn get(&self, id: i64) -> Result<QueueInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_name, created_at FROM pgqrs_queues WHERE id = ?";
        let mut rows = conn.query(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query queue: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Queue {} not found", id),
            })
        }
    }

    async fn list(&self) -> Result<Vec<QueueInfo>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_name, created_at FROM pgqrs_queues ORDER BY created_at DESC";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to query queues: {e}"),
        })?;

        let mut queues = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            queues.push(Self::map_row(&row)?);
        }
        Ok(queues)
    }

    async fn count(&self) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_queues";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to count queues: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_queues WHERE id = ?";
        conn.execute(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to delete queue: {e}"),
        })?;

        Ok(1) // Turso doesn't directly return affected rows, so we assume 1
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_name, created_at FROM pgqrs_queues WHERE queue_name = ?";
        let mut rows = conn.query(sql, [name]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query queue: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Queue '{}' not found", name),
            })
        }
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = ?)";
        let mut rows = conn.query(sql, [name]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query queue existence: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .map(|val| val != 0)
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract exists from row".to_string(),
                })
        } else {
            Ok(false)
        }
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_queues WHERE queue_name = ?";
        conn.execute(sql, [name])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to delete queue: {e}"),
            })?;

        Ok(1) // Turso doesn't directly return affected rows
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
    async fn insert(&self, data: crate::types::NewMessage) -> Result<QueueMessage> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let payload_str = data.payload.to_string();
        let enqueued_at_str = data.enqueued_at.to_rfc3339();
        let vt_str = data.vt.to_rfc3339();

        let sql = "INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id";
        let mut rows = conn
            .query(
                sql,
                (
                    data.queue_id,
                    payload_str,
                    data.read_ct,
                    enqueued_at_str,
                    vt_str,
                    data.producer_worker_id,
                    data.consumer_worker_id,
                ),
            )
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to insert message: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id = ?";
        let mut rows = conn.query(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query message: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Message {} not found", id),
            })
        }
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages ORDER BY enqueued_at DESC";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to query messages: {e}"),
        })?;

        let mut messages = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn count(&self) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_messages";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to count messages: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_messages WHERE id = ?";
        conn.execute(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to delete message: {e}"),
        })?;

        Ok(1)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE queue_id = ? ORDER BY enqueued_at DESC LIMIT 1000";
        let mut rows = conn
            .query(sql, [queue_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query messages: {e}"),
            })?;

        let mut messages = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        if payloads.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let mut ids = Vec::with_capacity(payloads.len());
        let enqueued_at_str = params.enqueued_at.to_rfc3339();
        let vt_str = params.vt.to_rfc3339();

        for payload in payloads {
            let payload_str = payload.to_string();
            let sql = "INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id";
            let mut rows = conn
                .query(
                    sql,
                    (
                        queue_id,
                        payload_str,
                        params.read_ct,
                        enqueued_at_str.clone(),
                        vt_str.clone(),
                        params.producer_worker_id,
                        params.consumer_worker_id,
                    ),
                )
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Failed to batch insert message: {e}"),
                })?;

            if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
                message: format!("Failed to fetch inserted row: {e}"),
            })? {
                let id = row
                    .get_value(0)
                    .ok()
                    .and_then(|v| v.as_integer().copied())
                    .ok_or_else(|| Error::Internal {
                        message: "Failed to extract id from row".to_string(),
                    })?;
                ids.push(id);
            }
        }

        Ok(ids)
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!("SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id IN ({}) ORDER BY id", placeholders);

        let mut rows =
            conn.query(sql.as_str(), ids.to_vec())
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Failed to query messages by ids: {e}"),
                })?;

        let mut messages = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn update_visibility_timeout(
        &self,
        id: i64,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let vt_str = vt.to_rfc3339();
        let sql = "UPDATE pgqrs_messages SET vt = ? WHERE id = ?";
        conn.execute(sql, (vt_str, id))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to update visibility timeout: {e}"),
            })?;

        Ok(1)
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "UPDATE pgqrs_messages SET vt = datetime(vt, '+' || ? || ' seconds') WHERE id = ? AND consumer_worker_id = ?";
        conn.execute(sql, (additional_seconds as i32, id, worker_id))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to extend visibility: {e}"),
            })?;

        Ok(1)
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let placeholders = message_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("UPDATE pgqrs_messages SET vt = datetime(vt, '+' || ? || ' seconds') WHERE id IN ({}) AND consumer_worker_id = ? RETURNING id", placeholders);

        let mut params = vec![additional_seconds as i64];
        params.extend(message_ids.iter().copied());
        params.push(worker_id);

        let mut rows = conn
            .query(sql.as_str(), params)
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to batch extend visibility: {e}"),
            })?;

        let mut extended_ids = std::collections::HashSet::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            if let Some(id) = row.get_value(0).ok().and_then(|v| v.as_integer().copied()) {
                extended_ids.insert(id);
            }
        }

        Ok(message_ids
            .iter()
            .map(|id| extended_ids.contains(id))
            .collect())
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let placeholders = message_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("UPDATE pgqrs_messages SET vt = datetime('now'), consumer_worker_id = NULL WHERE id IN ({}) AND consumer_worker_id = ? RETURNING id", placeholders);

        let mut params = message_ids.to_vec();
        params.push(worker_id);

        let mut rows = conn
            .query(sql.as_str(), params)
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to release messages: {e}"),
            })?;

        let mut released_ids = std::collections::HashSet::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            if let Some(id) = row.get_value(0).ok().and_then(|v| v.as_integer().copied()) {
                released_ids.insert(id);
            }
        }

        Ok(message_ids
            .iter()
            .map(|id| released_ids.contains(id))
            .collect())
    }

    async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending_filtered(queue_id, None).await
    }

    async fn count_pending_filtered(&self, queue_id: i64, worker_id: Option<i64>) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let (sql, params): (String, Vec<i64>) = match worker_id {
            Some(wid) => (
                "SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = ? AND consumer_worker_id = ?".to_string(),
                vec![queue_id, wid],
            ),
            None => (
                "SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = ? AND (vt IS NULL OR vt <= datetime('now')) AND consumer_worker_id IS NULL".to_string(),
                vec![queue_id],
            ),
        };

        let mut rows = conn
            .query(sql.as_str(), params)
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to count pending messages: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let mut results = Vec::with_capacity(ids.len());
        let sql = "DELETE FROM pgqrs_messages WHERE id = ?";

        for &id in ids {
            let result = conn.execute(sql, [id]).await;
            results.push(result.is_ok());
        }

        Ok(results)
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
    async fn insert(&self, data: crate::types::NewWorker) -> Result<WorkerInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let now = chrono::Utc::now();
        let now_str = now.to_rfc3339();
        let status_str = WorkerStatus::Ready.to_string();

        let sql = "INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status) VALUES (?, ?, ?, ?, ?, ?) RETURNING id";
        let mut rows = conn
            .query(
                sql,
                (
                    data.hostname.clone(),
                    data.port,
                    data.queue_id,
                    now_str.clone(),
                    now_str,
                    status_str,
                ),
            )
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to insert worker: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            let id = row
                .get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract id from row".to_string(),
                })?;

            Ok(WorkerInfo {
                id,
                hostname: data.hostname,
                port: data.port,
                queue_id: data.queue_id,
                started_at: now,
                heartbeat_at: now,
                shutdown_at: None,
                status: WorkerStatus::Ready,
            })
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
    }

    async fn get(&self, id: i64) -> Result<WorkerInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE id = ?";
        let mut rows = conn.query(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query worker: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Worker {} not found", id),
            })
        }
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers ORDER BY started_at DESC";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to query workers: {e}"),
        })?;

        let mut workers = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn count(&self) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_workers";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to count workers: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_workers WHERE id = ?";
        conn.execute(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to delete worker: {e}"),
        })?;

        Ok(1)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerInfo>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = ? ORDER BY started_at DESC";
        let mut rows = conn
            .query(sql, [queue_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query workers: {e}"),
            })?;

        let mut workers = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn count_for_queue(&self, queue_id: i64, state: WorkerStatus) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let state_str = state.to_string();
        let sql = "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = ? AND status = ?";
        let mut rows =
            conn.query(sql, (queue_id, state_str))
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Failed to count workers: {e}"),
                })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let threshold = chrono::Utc::now() - older_than;
        let threshold_str = threshold.to_rfc3339();

        let sql = "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = ? AND status IN ('ready', 'suspended') AND heartbeat_at < ?";
        let mut rows = conn
            .query(sql, (queue_id, threshold_str))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to count zombie workers: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn list_for_queue(&self, queue_id: i64, state: WorkerStatus) -> Result<Vec<WorkerInfo>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let state_str = state.to_string();
        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = ? AND status = ? ORDER BY started_at DESC";
        let mut rows =
            conn.query(sql, (queue_id, state_str))
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Failed to query workers: {e}"),
                })?;

        let mut workers = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let threshold = chrono::Utc::now() - older_than;
        let threshold_str = threshold.to_rfc3339();

        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = ? AND status IN ('ready', 'suspended') AND heartbeat_at < ? ORDER BY heartbeat_at ASC";
        let mut rows = conn
            .query(sql, (queue_id, threshold_str))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query zombie workers: {e}"),
            })?;

        let mut workers = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE hostname = ? AND port = ?";
        let mut rows = conn
            .query(sql, (hostname, port))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to find existing worker: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            let worker = Self::map_row(&row)?;
            match worker.status {
                WorkerStatus::Stopped => {
                    let now = chrono::Utc::now();
                    let now_str = now.to_rfc3339();
                    let sql = "UPDATE pgqrs_workers SET status = 'ready', queue_id = ?, started_at = ?, heartbeat_at = ?, shutdown_at = NULL WHERE id = ? RETURNING id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status";
                    let mut rows = conn
                        .query(sql, (queue_id, now_str.clone(), now_str, worker.id))
                        .await
                        .map_err(|e| Error::Internal {
                            message: format!("Failed to reset worker: {e}"),
                        })?;

                    if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
                        message: format!("Failed to fetch row: {e}"),
                    })? {
                        Self::map_row(&row)
                    } else {
                        Err(Error::Internal {
                            message: "No row returned after update".to_string(),
                        })
                    }
                }
                WorkerStatus::Ready => Err(Error::ValidationFailed {
                    reason: format!(
                        "Worker {}:{} is already active. Cannot register duplicate.",
                        hostname, port
                    ),
                }),
                WorkerStatus::Suspended => Err(Error::ValidationFailed {
                    reason: format!(
                        "Worker {}:{} is suspended. Use resume() to reactivate.",
                        hostname, port
                    ),
                }),
            }
        } else {
            self.insert(crate::types::NewWorker {
                hostname: hostname.to_string(),
                port,
                queue_id,
            })
            .await
        }
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerInfo> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let hostname = format!("__ephemeral__{}", uuid::Uuid::new_v4());
        let sql = "INSERT INTO pgqrs_workers (hostname, port, queue_id, status) VALUES (?, -1, ?, 'ready') RETURNING id, queue_id, hostname, port, started_at, heartbeat_at, shutdown_at, status";
        let mut rows =
            conn.query(sql, (hostname, queue_id))
                .await
                .map_err(|e| Error::Internal {
                    message: format!("Failed to insert ephemeral worker: {e}"),
                })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
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
    async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let archived_at = chrono::Utc::now();
        let archived_at_str = archived_at.to_rfc3339();
        let dequeued_at_str = data.dequeued_at.map(|d| d.to_rfc3339());
        let enqueued_at_str = data.enqueued_at.to_rfc3339();
        let vt_str = data.vt.to_rfc3339();
        let payload_str = data.payload.to_string();

        let sql = "INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        let mut rows = conn
            .query(
                sql,
                (
                    data.original_msg_id,
                    data.queue_id,
                    data.producer_worker_id,
                    data.consumer_worker_id,
                    payload_str,
                    enqueued_at_str,
                    vt_str,
                    data.read_ct,
                    dequeued_at_str,
                    archived_at_str,
                ),
            )
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to insert archive: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            let id = row
                .get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract id from row".to_string(),
                })?;

            Ok(ArchivedMessage {
                id,
                original_msg_id: data.original_msg_id,
                queue_id: data.queue_id,
                producer_worker_id: data.producer_worker_id,
                consumer_worker_id: data.consumer_worker_id,
                payload: data.payload,
                enqueued_at: data.enqueued_at,
                vt: data.vt,
                read_ct: data.read_ct,
                archived_at,
                dequeued_at: data.dequeued_at,
            })
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
    }

    async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive WHERE id = ?";
        let mut rows = conn.query(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query archive: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Archive {} not found", id),
            })
        }
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive ORDER BY archived_at DESC";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to query archives: {e}"),
        })?;

        let mut archives = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            archives.push(Self::map_row(&row)?);
        }
        Ok(archives)
    }

    async fn count(&self) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_archive";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to count archives: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_archive WHERE id = ?";
        conn.execute(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to delete archive: {e}"),
        })?;

        Ok(1)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive WHERE queue_id = ? ORDER BY archived_at DESC";
        let mut rows = conn
            .query(sql, [queue_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query archives: {e}"),
            })?;

        let mut archives = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            archives.push(Self::map_row(&row)?);
        }
        Ok(archives)
    }

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive WHERE read_ct >= ? AND consumer_worker_id IS NULL AND dequeued_at IS NULL ORDER BY archived_at DESC LIMIT ? OFFSET ?";
        let mut rows = conn
            .query(sql, (max_attempts, limit, offset))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query DLQ messages: {e}"),
            })?;

        let mut archives = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            archives.push(Self::map_row(&row)?);
        }
        Ok(archives)
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_archive WHERE read_ct >= ? AND consumer_worker_id IS NULL AND dequeued_at IS NULL";
        let mut rows = conn
            .query(sql, [max_attempts])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to count DLQ messages: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive WHERE consumer_worker_id = ? ORDER BY archived_at DESC LIMIT ? OFFSET ?";
        let mut rows = conn
            .query(sql, (worker_id, limit, offset))
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query archives by worker: {e}"),
            })?;

        let mut archives = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            archives.push(Self::map_row(&row)?);
        }
        Ok(archives)
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_archive WHERE consumer_worker_id = ?";
        let mut rows = conn
            .query(sql, [worker_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to count archives by worker: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_archive WHERE consumer_worker_id = ?";
        conn.execute(sql, [worker_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to delete archives by worker: {e}"),
            })?;

        Ok(1)
    }

    async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        // Get archive
        let sql = "SELECT id, original_msg_id, queue_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_archive WHERE id = ?";
        let mut rows = conn
            .query(sql, [msg_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to query archive: {e}"),
            })?;

        let archive = if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)?
        } else {
            return Ok(None);
        };

        // Delete archive
        let sql = "DELETE FROM pgqrs_archive WHERE id = ?";
        conn.execute(sql, [msg_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to delete archive: {e}"),
            })?;

        // Insert into messages
        let now = chrono::Utc::now();
        let now_str = now.to_rfc3339();
        let payload_str = archive.payload.to_string();

        let sql = "INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id) VALUES (?, ?, 0, ?, ?, ?) RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id";
        let mut rows = conn
            .query(
                sql,
                (
                    archive.queue_id,
                    payload_str,
                    now_str.clone(),
                    now_str,
                    archive.producer_worker_id,
                ),
            )
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to insert replayed message: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            // Map row using TursoMessageTable logic (columns: id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id)
            let id = row
                .get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract id from row".to_string(),
                })?;
            let queue_id = row
                .get_value(1)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract queue_id from row".to_string(),
                })?;
            let payload_str = row
                .get_value(2)
                .ok()
                .and_then(|v| v.as_text().map(|s| s.to_string()))
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract payload from row".to_string(),
                })?;
            let payload: serde_json::Value =
                serde_json::from_str(&payload_str).unwrap_or(serde_json::json!({}));

            let vt_str = row
                .get_value(3)
                .ok()
                .and_then(|v| v.as_text().map(|s| s.to_string()))
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract vt from row".to_string(),
                })?;
            let vt = chrono::DateTime::parse_from_rfc3339(&vt_str)
                .ok()
                .or_else(|| chrono::DateTime::parse_from_rfc2822(&vt_str).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(now);

            let enqueued_at_str = row
                .get_value(4)
                .ok()
                .and_then(|v| v.as_text().map(|s| s.to_string()))
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract enqueued_at from row".to_string(),
                })?;
            let enqueued_at = chrono::DateTime::parse_from_rfc3339(&enqueued_at_str)
                .ok()
                .or_else(|| chrono::DateTime::parse_from_rfc2822(&enqueued_at_str).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(now);

            let read_ct = row
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
            let producer_worker_id = row.get_value(7).ok().and_then(|v| v.as_integer().copied());
            let consumer_worker_id = row.get_value(8).ok().and_then(|v| v.as_integer().copied());

            Ok(Some(QueueMessage {
                id,
                queue_id,
                payload,
                vt,
                enqueued_at,
                read_ct,
                dequeued_at,
                producer_worker_id,
                consumer_worker_id,
            }))
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
    }

    async fn count_for_queue(&self, queue_id: i64) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = ?";
        let mut rows = conn
            .query(sql, [queue_id])
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to count archives for queue: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
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
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let now = chrono::Utc::now();
        let now_str = now.to_rfc3339();
        let input_str = data.input.map(|v| v.to_string());

        let sql = "INSERT INTO pgqrs_workflows (name, status, input, created_at, updated_at) VALUES (?, 'PENDING', ?, ?, ?) RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id";
        let mut rows = conn
            .query(
                sql,
                (data.name.clone(), input_str, now_str.clone(), now_str),
            )
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to insert workflow: {e}"),
            })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch inserted row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: "No row returned after insert".to_string(),
            })
        }
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id FROM pgqrs_workflows WHERE workflow_id = ?";
        let mut rows = conn.query(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to query workflow: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            Self::map_row(&row)
        } else {
            Err(Error::Internal {
                message: format!("Workflow {} not found", id),
            })
        }
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id FROM pgqrs_workflows ORDER BY created_at DESC";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to query workflows: {e}"),
        })?;

        let mut workflows = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            workflows.push(Self::map_row(&row)?);
        }
        Ok(workflows)
    }

    async fn count(&self) -> Result<i64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "SELECT COUNT(*) FROM pgqrs_workflows";
        let mut rows = conn.query(sql, ()).await.map_err(|e| Error::Internal {
            message: format!("Failed to count workflows: {e}"),
        })?;

        if let Some(row) = rows.next().await.map_err(|e| Error::Internal {
            message: format!("Failed to fetch row: {e}"),
        })? {
            row.get_value(0)
                .ok()
                .and_then(|v| v.as_integer().copied())
                .ok_or_else(|| Error::Internal {
                    message: "Failed to extract count from row".to_string(),
                })
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let conn = self.db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to open database connection: {e}"),
        })?;

        let sql = "DELETE FROM pgqrs_workflows WHERE workflow_id = ?";
        conn.execute(sql, [id]).await.map_err(|e| Error::Internal {
            message: format!("Failed to delete workflow: {e}"),
        })?;

        Ok(1)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NewMessage, NewWorker};

    async fn create_test_db() -> Arc<Database> {
        let builder = Builder::new_local(":memory:");
        let db = builder.build().await.expect("Failed to create test db");
        
        // Run migrations to set up schema
        let conn = db.connect().expect("Failed to connect");
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
            (),
        )
        .await
        .expect("Failed to create queues table");

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                payload TEXT NOT NULL,
                read_ct INTEGER NOT NULL DEFAULT 0,
                enqueued_at TEXT NOT NULL,
                vt TEXT,
                dequeued_at TEXT,
                producer_worker_id INTEGER,
                consumer_worker_id INTEGER,
                FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id) ON DELETE CASCADE
            );
            "#,
            (),
        )
        .await
        .expect("Failed to create messages table");

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hostname TEXT NOT NULL,
                port INTEGER NOT NULL,
                queue_id INTEGER,
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                heartbeat_at TEXT NOT NULL DEFAULT (datetime('now')),
                shutdown_at TEXT,
                status TEXT NOT NULL DEFAULT 'ready',
                UNIQUE(hostname, port)
            );
            "#,
            (),
        )
        .await
        .expect("Failed to create workers table");

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_msg_id INTEGER NOT NULL,
                queue_id INTEGER NOT NULL,
                producer_worker_id INTEGER,
                consumer_worker_id INTEGER,
                payload TEXT NOT NULL,
                enqueued_at TEXT NOT NULL,
                vt TEXT,
                read_ct INTEGER NOT NULL DEFAULT 0,
                dequeued_at TEXT,
                archived_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
            (),
        )
        .await
        .expect("Failed to create archive table");

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_workflows (
                workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                input TEXT,
                output TEXT,
                error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                executor_id TEXT
            );
            "#,
            (),
        )
        .await
        .expect("Failed to create workflows table");

        Arc::new(db)
    }

    #[tokio::test]
    async fn test_queue_table_crud() {
        let db = create_test_db().await;
        let table = TursoQueueTable::new(Arc::clone(&db));

        // Insert
        let queue = table
            .insert(NewQueue {
                queue_name: "test_queue".to_string(),
            })
            .await
            .expect("Failed to insert queue");
        assert_eq!(queue.queue_name, "test_queue");
        assert!(queue.id > 0);

        // Get by ID
        let fetched = table.get(queue.id).await.expect("Failed to get queue");
        assert_eq!(fetched.queue_name, "test_queue");

        // Get by name
        let by_name = table
            .get_by_name("test_queue")
            .await
            .expect("Failed to get queue by name");
        assert_eq!(by_name.id, queue.id);

        // List
        let queues = table.list().await.expect("Failed to list queues");
        assert!(queues.len() >= 1);

        // Count
        let count = table.count().await.expect("Failed to count queues");
        assert!(count >= 1);

        // Exists
        let exists = table
            .exists("test_queue")
            .await
            .expect("Failed to check exists");
        assert!(exists);

        let not_exists = table
            .exists("nonexistent")
            .await
            .expect("Failed to check exists");
        assert!(!not_exists);

        // Delete by name
        let deleted = table
            .delete_by_name("test_queue")
            .await
            .expect("Failed to delete by name");
        assert_eq!(deleted, 1);

        let exists_after = table
            .exists("test_queue")
            .await
            .expect("Failed to check exists");
        assert!(!exists_after);
    }

    #[tokio::test]
    async fn test_message_table_crud() {
        let db = create_test_db().await;
        let queue_table = TursoQueueTable::new(Arc::clone(&db));
        let msg_table = TursoMessageTable::new(Arc::clone(&db));

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "msg_test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let now = chrono::Utc::now();
        let payload = serde_json::json!({"test": "data"});

        // Insert message
        let msg = msg_table
            .insert(NewMessage {
                queue_id: queue.id,
                payload: payload.clone(),
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await
            .expect("Failed to insert message");
        assert_eq!(msg.payload, payload);
        assert_eq!(msg.queue_id, queue.id);

        // Get
        let fetched = msg_table.get(msg.id).await.expect("Failed to get message");
        assert_eq!(fetched.id, msg.id);
        assert_eq!(fetched.payload, payload);

        // List
        let messages = msg_table.list().await.expect("Failed to list messages");
        assert!(messages.len() >= 1);

        // Count
        let count = msg_table.count().await.expect("Failed to count messages");
        assert!(count >= 1);

        // Filter by FK
        let filtered = msg_table
            .filter_by_fk(queue.id)
            .await
            .expect("Failed to filter by queue");
        assert!(filtered.len() >= 1);

        // Update visibility timeout
        let new_vt = now + chrono::Duration::seconds(60);
        msg_table
            .update_visibility_timeout(msg.id, new_vt)
            .await
            .expect("Failed to update vt");

        // Count pending
        let pending = msg_table
            .count_pending(queue.id)
            .await
            .expect("Failed to count pending");
        assert!(pending >= 0);

        // Delete
        let deleted = msg_table.delete(msg.id).await.expect("Failed to delete");
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_worker_table_crud() {
        let db = create_test_db().await;
        let queue_table = TursoQueueTable::new(Arc::clone(&db));
        let worker_table = TursoWorkerTable::new(Arc::clone(&db));

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "worker_test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        // Insert worker
        let worker = worker_table
            .insert(NewWorker {
                hostname: "test-host".to_string(),
                port: 8080,
                queue_id: Some(queue.id),
            })
            .await
            .expect("Failed to insert worker");
        assert_eq!(worker.hostname, "test-host");
        assert_eq!(worker.port, 8080);
        assert_eq!(worker.status, WorkerStatus::Ready);

        // Get
        let fetched = worker_table
            .get(worker.id)
            .await
            .expect("Failed to get worker");
        assert_eq!(fetched.hostname, "test-host");

        // List
        let workers = worker_table.list().await.expect("Failed to list workers");
        assert!(workers.len() >= 1);

        // Count
        let count = worker_table.count().await.expect("Failed to count workers");
        assert!(count >= 1);

        // Filter by FK
        let filtered = worker_table
            .filter_by_fk(queue.id)
            .await
            .expect("Failed to filter by queue");
        assert!(filtered.len() >= 1);

        // Count for queue
        let queue_count = worker_table
            .count_for_queue(queue.id, WorkerStatus::Ready)
            .await
            .expect("Failed to count for queue");
        assert!(queue_count >= 1);

        // List for queue
        let queue_workers = worker_table
            .list_for_queue(queue.id, WorkerStatus::Ready)
            .await
            .expect("Failed to list for queue");
        assert!(queue_workers.len() >= 1);

        // Register ephemeral
        let ephemeral = worker_table
            .register_ephemeral(Some(queue.id))
            .await
            .expect("Failed to register ephemeral");
        assert!(ephemeral.hostname.starts_with("__ephemeral__"));
        assert_eq!(ephemeral.port, -1);

        // Delete
        let deleted = worker_table
            .delete(worker.id)
            .await
            .expect("Failed to delete");
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_archive_table_crud() {
        let db = create_test_db().await;
        let queue_table = TursoQueueTable::new(Arc::clone(&db));
        let archive_table = TursoArchiveTable::new(Arc::clone(&db));

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "archive_test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let now = chrono::Utc::now();
        let payload = serde_json::json!({"archived": "message"});

        // Insert archived message
        let archive = archive_table
            .insert(NewArchivedMessage {
                original_msg_id: 123,
                queue_id: queue.id,
                producer_worker_id: None,
                consumer_worker_id: None,
                payload: payload.clone(),
                enqueued_at: now,
                vt: now,
                read_ct: 3,
                dequeued_at: None, // DLQ messages should not have dequeued_at
            })
            .await
            .expect("Failed to insert archive");
        assert_eq!(archive.payload, payload);
        assert_eq!(archive.read_ct, 3);

        // Get
        let fetched = archive_table
            .get(archive.id)
            .await
            .expect("Failed to get archive");
        assert_eq!(fetched.original_msg_id, 123);

        // List
        let archives = archive_table.list().await.expect("Failed to list archives");
        assert!(archives.len() >= 1);

        // Count
        let count = archive_table
            .count()
            .await
            .expect("Failed to count archives");
        assert!(count >= 1);

        // Filter by FK
        let filtered = archive_table
            .filter_by_fk(queue.id)
            .await
            .expect("Failed to filter by queue");
        assert!(filtered.len() >= 1);

        // Count for queue
        let queue_count = archive_table
            .count_for_queue(queue.id)
            .await
            .expect("Failed to count for queue");
        assert!(queue_count >= 1);

        // DLQ count (read_ct >= 3)
        let dlq = archive_table
            .dlq_count(3)
            .await
            .expect("Failed to count DLQ");
        assert!(dlq >= 1);

        // Delete
        let deleted = archive_table
            .delete(archive.id)
            .await
            .expect("Failed to delete");
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_workflow_table_crud() {
        let db = create_test_db().await;
        let workflow_table = TursoWorkflowTable::new(Arc::clone(&db));

        let input = Some(serde_json::json!({"step": 1}));

        // Insert workflow
        let workflow = workflow_table
            .insert(NewWorkflow {
                name: "test_workflow".to_string(),
                input: input.clone(),
            })
            .await
            .expect("Failed to insert workflow");
        assert_eq!(workflow.name, "test_workflow");
        assert_eq!(workflow.status, WorkflowStatus::Pending);
        assert_eq!(workflow.input, input);

        // Get
        let fetched = workflow_table
            .get(workflow.workflow_id)
            .await
            .expect("Failed to get workflow");
        assert_eq!(fetched.name, "test_workflow");

        // List
        let workflows = workflow_table
            .list()
            .await
            .expect("Failed to list workflows");
        assert!(workflows.len() >= 1);

        // Count
        let count = workflow_table
            .count()
            .await
            .expect("Failed to count workflows");
        assert!(count >= 1);

        // Delete
        let deleted = workflow_table
            .delete(workflow.workflow_id)
            .await
            .expect("Failed to delete");
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_batch_insert_messages() {
        let db = create_test_db().await;
        let queue_table = TursoQueueTable::new(Arc::clone(&db));
        let msg_table = TursoMessageTable::new(Arc::clone(&db));

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "batch_test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let payloads = vec![
            serde_json::json!({"msg": 1}),
            serde_json::json!({"msg": 2}),
            serde_json::json!({"msg": 3}),
        ];

        let now = chrono::Utc::now();
        let params = crate::types::BatchInsertParams {
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: None,
            consumer_worker_id: None,
        };

        // Batch insert
        let ids = msg_table
            .batch_insert(queue.id, &payloads, params)
            .await
            .expect("Failed to batch insert");
        assert_eq!(ids.len(), 3);

        // Get by IDs
        let messages = msg_table
            .get_by_ids(&ids)
            .await
            .expect("Failed to get by ids");
        assert_eq!(messages.len(), 3);
    }
}
