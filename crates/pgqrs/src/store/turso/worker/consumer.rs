use crate::config::Config;
use crate::error::Result;
use crate::store::turso::format_turso_timestamp;
use crate::store::turso::tables::messages::TursoMessageTable;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::{Consumer, MessageTable, Worker};
use crate::types::{QueueMessage, QueueRecord, WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use turso::Database;

pub struct TursoConsumer {
    db: Arc<Database>,
    worker_record: WorkerRecord,
    queue_info: QueueRecord,
    _config: Config,
    workers: Arc<TursoWorkerTable>,
    messages: Arc<TursoMessageTable>,
}

impl TursoConsumer {
    pub async fn new(
        db: Arc<Database>,
        queue_info: &QueueRecord,
        hostname: &str,
        port: i32,
        config: Config,
    ) -> Result<Self> {
        let workers = Arc::new(TursoWorkerTable::new(db.clone()));
        // Register worker
        let worker_record =
            crate::store::WorkerTable::register(&*workers, Some(queue_info.id), hostname, port)
                .await?;

        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db,
            worker_record,
            queue_info: queue_info.clone(),
            _config: config,
            workers,
            messages,
        })
    }

    pub async fn new_ephemeral(
        db: Arc<Database>,
        queue_info: &QueueRecord,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(TursoWorkerTable::new(db.clone()));
        let worker_record =
            crate::store::WorkerTable::register_ephemeral(&*workers, Some(queue_info.id)).await?;
        let messages = Arc::new(TursoMessageTable::new(db.clone()));

        Ok(Self {
            db,
            worker_record,
            queue_info: queue_info.clone(),
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl Worker for TursoConsumer {
    fn worker_record(&self) -> &WorkerRecord {
        &self.worker_record
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_record.id).await
    }

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_record.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_record.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_record.id).await
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_record.id).await
    }

    async fn is_healthy(&self, max_age: Duration) -> Result<bool> {
        self.workers
            .is_healthy(self.worker_record.id, max_age)
            .await
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
            WHERE queue_id = ? AND (vt IS NULL OR vt <= ?) AND consumer_worker_id IS NULL AND archived_at IS NULL AND read_ct < ?
            ORDER BY enqueued_at ASC
            LIMIT ?
        "#;

        let mut grabbed_ids = Vec::new();

        let rows = crate::store::turso::query(sql_select)
            .bind(self.queue_info.id)
            .bind(now_str.clone())
            .bind(self._config.max_read_ct as i64)
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
            let update_res = crate::store::turso::query("UPDATE pgqrs_messages SET consumer_worker_id = ?, vt = ?, read_ct = read_ct + 1, dequeued_at = COALESCE(dequeued_at, ?) WHERE id = ?")
                .bind(self.worker_record.id)
                .bind(new_vt_str.clone())
                .bind(now_str.clone())
                .bind(*id)
                .execute_once_on_connection(&conn)
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
            .extend_visibility(message_id, self.worker_record.id, additional_seconds)
            .await?;
        Ok(count > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let count = self
            .messages
            .delete_owned(message_id, self.worker_record.id)
            .await?;
        Ok(count > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        self.messages
            .delete_many_owned(&message_ids, self.worker_record.id)
            .await
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        let row = crate::store::turso::query(
            "UPDATE pgqrs_messages SET archived_at = ? WHERE id = ? AND consumer_worker_id = ? AND archived_at IS NULL RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at",
        )
        .bind(now_str)
        .bind(msg_id)
        .bind(self.worker_record.id)
        .fetch_optional(&self.db)
        .await?;

        if let Some(r) = row {
            Ok(Some(TursoMessageTable::map_row(&r)?))
        } else {
            Ok(None)
        }
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
            .release_messages_by_ids(message_ids, self.worker_record.id)
            .await?;
        Ok(res.iter().filter(|&&x| x).count() as u64)
    }

    async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: DateTime<Utc>,
    ) -> Result<bool> {
        let count = self
            .messages
            .release_with_visibility(message_id, self.worker_record.id, visible_at)
            .await?;
        Ok(count > 0)
    }
}

impl Drop for TursoConsumer {
    fn drop(&mut self) {
        if self.worker_record.hostname.starts_with("__ephemeral__") {
            let workers = self.workers.clone();
            let worker_id = self.worker_record.id;
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
