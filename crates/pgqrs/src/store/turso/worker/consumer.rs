use crate::config::Config;
use crate::error::Result;
use crate::store::turso::format_turso_timestamp;
use crate::store::turso::tables::messages::TursoMessageTable;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::{Consumer, MessageTable, Worker};
use crate::types::{ArchivedMessage, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use turso::Database;

const SELECT_MESSAGE_FOR_ARCHIVE: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = ? AND consumer_worker_id = ?
"#;

const INSERT_ARCHIVE: &str = r#"
    INSERT INTO pgqrs_archive (
        original_msg_id, queue_id, producer_worker_id, consumer_worker_id,
        payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
"#;

const DELETE_MESSAGE_AFTER_ARCHIVE: &str = r#"
    DELETE FROM pgqrs_messages WHERE id = ? AND consumer_worker_id = ?
"#;

pub struct TursoConsumer {
    db: Arc<Database>,
    worker_info: WorkerInfo,
    queue_info: QueueInfo,
    _config: Config,
    workers: Arc<TursoWorkerTable>,
    messages: Arc<TursoMessageTable>,
}

impl TursoConsumer {
    pub async fn new(
        db: Arc<Database>,
        queue_info: &QueueInfo,
        hostname: &str,
        port: i32,
        config: Config,
    ) -> Result<Self> {
        let workers = Arc::new(TursoWorkerTable::new(db.clone()));
        // Register worker
        let worker_info =
            crate::store::WorkerTable::register(&*workers, Some(queue_info.id), hostname, port)
                .await?;

        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db,
            worker_info,
            queue_info: queue_info.clone(),
            _config: config,
            workers,
            messages,
        })
    }

    pub async fn new_ephemeral(
        db: Arc<Database>,
        queue_info: &QueueInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(TursoWorkerTable::new(db.clone()));
        let worker_info =
            crate::store::WorkerTable::register_ephemeral(&*workers, Some(queue_info.id)).await?;
        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db,
            worker_info,
            queue_info: queue_info.clone(),
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl Worker for TursoConsumer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_info.id).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_info.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_info.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_info.id).await
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: Duration) -> Result<bool> {
        self.workers.is_healthy(self.worker_info.id, max_age).await
    }
}

#[async_trait]
impl Consumer for TursoConsumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, 30).await
    }

    async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    async fn dequeue_many_with_delay(&self, limit: usize, vt: u32) -> Result<Vec<QueueMessage>> {
        let now = Utc::now();
        self.dequeue_at(limit, vt, now).await
    }

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: DateTime<Utc>,
    ) -> Result<Vec<QueueMessage>> {
        // Implement Manual Transaction Dequeue
        let now_str = format_turso_timestamp(&now);
        let conn = crate::store::turso::connect_db(&self.db).await?;

        conn.execute("BEGIN IMMEDIATE", ()).await.map_err(|e| {
            crate::error::Error::QueryFailed {
                query: "BEGIN IMMEDIATE".into(),
                source: Box::new(e),
                context: "Dequeue start".into(),
            }
        })?;

        // 1. Select candidates
        let sql_select = r#"
            SELECT id
            FROM pgqrs_messages
            WHERE queue_id = ? AND (vt IS NULL OR vt <= ?) AND consumer_worker_id IS NULL
            ORDER BY enqueued_at ASC
            LIMIT ?
        "#;

        let mut grabbed_ids = Vec::new();

        let rows = crate::store::turso::query(sql_select)
            .bind(self.queue_info.id)
            .bind(now_str.clone())
            .bind(limit as i64)
            .fetch_all_on_connection(&conn)
            .await;

        let rows = match rows {
            Ok(res) => res,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        for row in rows {
            let id: i64 = row.get(0)?;
            grabbed_ids.push(id);
        }

        if grabbed_ids.is_empty() {
            conn.execute("ROLLBACK", ())
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "ROLLBACK".into(),
                    source: Box::new(e),
                    context: "Dequeue empty".into(),
                })?;
            return Ok(vec![]);
        }

        // 2. Update them
        let new_vt = now + chrono::Duration::seconds(vt as i64);
        let new_vt_str = format_turso_timestamp(&new_vt);

        for id in &grabbed_ids {
            let update_res = crate::store::turso::query("UPDATE pgqrs_messages SET consumer_worker_id = ?, vt = ?, read_ct = read_ct + 1, dequeued_at = ? WHERE id = ?")
                .bind(self.worker_info.id)
                .bind(new_vt_str.clone())
                .bind(now_str.clone())
                .bind(*id)
                .execute_on_connection(&conn)
                .await;

            if let Err(e) = update_res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Dequeue commit".into(),
            })?;

        // 3. Fetch full objects
        self.messages.get_by_ids(&grabbed_ids).await
    }

    async fn extend_visibility(&self, message_id: i64, additional_seconds: u32) -> Result<bool> {
        let count = self
            .messages
            .extend_visibility(message_id, self.worker_info.id, additional_seconds)
            .await?;
        Ok(count > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let count = self
            .messages
            .delete_owned(message_id, self.worker_info.id)
            .await?;
        Ok(count > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        self.messages
            .delete_many_owned(&message_ids, self.worker_info.id)
            .await
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
        let conn = crate::store::turso::connect_db(&self.db).await?;

        // 1. Begin Transaction
        conn.execute("BEGIN IMMEDIATE", ()).await.map_err(|e| {
            crate::error::Error::QueryFailed {
                query: "BEGIN IMMEDIATE".into(),
                source: Box::new(e),
                context: "Begin archive transaction".into(),
            }
        })?;

        // 2. Fetch message (ensure ownership)
        let row_opt_res = crate::store::turso::query(SELECT_MESSAGE_FOR_ARCHIVE)
            .bind(msg_id)
            .bind(self.worker_info.id)
            .fetch_optional_on_connection(&conn)
            .await;

        // 2a. Handle Query Error
        let row_opt = match row_opt_res {
            Ok(r) => r,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        // 2b. Handle Optional
        let row = match row_opt {
            Some(r) => r,
            None => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Ok(None);
            }
        };

        // Extract fields
        let queue_id: i64 = row.get(1).map_err(crate::error::Error::from)?;
        let payload_str: String = row.get(2).map_err(crate::error::Error::from)?;
        let vt_str: Option<String> = row.get(3).map_err(crate::error::Error::from)?;
        let enqueued_at_str: String = row.get(4).map_err(crate::error::Error::from)?;
        let read_ct: i32 = row.get(5).map_err(crate::error::Error::from)?;
        let dequeued_at_str: Option<String> = row.get(6).map_err(crate::error::Error::from)?;
        let producer_worker_id: Option<i64> = row.get(7).map_err(crate::error::Error::from)?;
        let consumer_worker_id: Option<i64> = row.get(8).map_err(crate::error::Error::from)?;
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        // 3. Insert into archive
        let archive_row_res = crate::store::turso::query(INSERT_ARCHIVE)
            .bind(msg_id)
            .bind(queue_id)
            .bind(match producer_worker_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(match consumer_worker_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(turso::Value::Text(payload_str))
            .bind(turso::Value::Text(enqueued_at_str))
            .bind(match vt_str {
                Some(s) => turso::Value::Text(s),
                None => turso::Value::Null,
            })
            .bind(read_ct as i64)
            .bind(match dequeued_at_str {
                Some(s) => turso::Value::Text(s),
                None => turso::Value::Null,
            })
            .bind(turso::Value::Text(now_str))
            .fetch_one_on_connection(&conn)
            .await;

        let archive_row = match archive_row_res {
            Ok(r) => r,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        // 4. Delete from messages (checking ownership again for safety, though transaction helps)
        let delete_res = crate::store::turso::query(DELETE_MESSAGE_AFTER_ARCHIVE)
            .bind(msg_id)
            .bind(self.worker_info.id)
            .execute_on_connection(&conn)
            .await;

        if let Err(e) = delete_res {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e);
        }

        // 5. Commit
        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Commit archive transaction".into(),
            })?;

        Ok(Some(
            crate::store::turso::tables::archive::TursoArchiveTable::map_row(&archive_row)?,
        ))
    }

    async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        let mut results = Vec::new();
        for id in msg_ids {
            results.push(self.archive(id).await?.is_some());
        }
        Ok(results)
    }

    async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
        let res = self
            .messages
            .release_messages_by_ids(message_ids, self.worker_info.id)
            .await?;
        Ok(res.iter().filter(|&&x| x).count() as u64)
    }
}

impl Drop for TursoConsumer {
    fn drop(&mut self) {
        if self.worker_info.hostname.starts_with("__ephemeral__") {
            let workers = self.workers.clone();
            let worker_id = self.worker_info.id;
            // Best effort spawn
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            }
        }
    }
}
