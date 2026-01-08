use crate::error::Result;
use crate::store::sqlite::parse_sqlite_timestamp;
use crate::store::sqlite::tables::messages::SqliteMessageTable;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::WorkerTable;
use crate::types::{ArchivedMessage, QueueMessage, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::sync::Arc;

const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = datetime('now', '+' || $3 || ' seconds'),
        read_ct = read_ct + 1,
        dequeued_at = datetime('now'),
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= datetime('now'))
          AND consumer_worker_id IS NULL
        ORDER BY enqueued_at ASC
        LIMIT $2
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const DEQUEUE_MESSAGES_AT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = datetime($5, '+' || $3 || ' seconds'),
        read_ct = read_ct + 1,
        dequeued_at = $5,
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= $5)
          AND consumer_worker_id IS NULL
        ORDER BY enqueued_at ASC
        LIMIT $2
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const DELETE_MESSAGE_OWNED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1 AND consumer_worker_id = $2
"#;

pub struct SqliteConsumer {
    pub pool: SqlitePool,
    queue_info: crate::types::QueueInfo,
    worker_info: crate::types::WorkerInfo,
    _config: crate::config::Config,
    workers: Arc<SqliteWorkerTable>,
    messages: Arc<SqliteMessageTable>,
}

impl SqliteConsumer {
    pub async fn new(
        pool: SqlitePool,
        queue_info: &crate::types::QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_info = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info,
            _config: config.clone(),
            workers,
            messages,
        })
    }

    pub async fn new_ephemeral(
        pool: SqlitePool,
        queue_info: &crate::types::QueueInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_info = workers.register_ephemeral(Some(queue_info.id)).await?;
        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info,
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl crate::store::Worker for SqliteConsumer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers.is_healthy(self.worker_info.id, max_age).await
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
        use crate::store::MessageTable;
        let pending = self
            .messages
            .count_pending_filtered(self.queue_info.id, Some(self.worker_info.id))
            .await?;

        if pending > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending as u64,
                reason: format!("Consumer has {} pending messages", pending),
            });
        }
        self.workers.shutdown(self.worker_info.id).await
    }
}

#[async_trait]
impl crate::store::Consumer for SqliteConsumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, 5).await // Default 5s VT like Postgres
    }

    async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    async fn dequeue_many_with_delay(&self, limit: usize, vt: u32) -> Result<Vec<QueueMessage>> {
        let rows = sqlx::query(DEQUEUE_MESSAGES)
            .bind(self.queue_info.id)
            .bind(limit as i64)
            .bind(vt as i32)
            .bind(self.worker_info.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE".into(),
                source: e,
                context: "Dequeue".into(),
            })?;

        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(SqliteMessageTable::map_row(row)?);
        }
        Ok(msgs)
    }

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<QueueMessage>> {
        // We must format 'now' for SQLite because we pass it as param $5 and also use it in query text logic is tricky.
        // But sqlx binds DateTime<Utc> as string or int depending on setup.
        // We handle sqlite timestamps as strings.
        use crate::store::sqlite::format_sqlite_timestamp;
        let now_str = format_sqlite_timestamp(&now);

        let rows = sqlx::query(DEQUEUE_MESSAGES_AT)
            .bind(self.queue_info.id)
            .bind(limit as i64)
            .bind(vt as i32)
            .bind(self.worker_info.id)
            .bind(now_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE_AT".into(),
                source: e,
                context: "Dequeue At".into(),
            })?;

        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(SqliteMessageTable::map_row(row)?);
        }
        Ok(msgs)
    }

    async fn extend_visibility(&self, message_id: i64, additional_seconds: u32) -> Result<bool> {
        use crate::store::MessageTable;
        let c = self
            .messages
            .extend_visibility(message_id, self.worker_info.id, additional_seconds)
            .await?;
        Ok(c > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows = sqlx::query(DELETE_MESSAGE_OWNED)
            .bind(message_id)
            .bind(self.worker_info.id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEL_OWNED".into(),
                source: e,
                context: "Del owned".into(),
            })?
            .rows_affected();
        Ok(rows > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        // Manual batch delete
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        // Transaction might be better, or QueryBuilder
        let mut res = Vec::new();
        for id in message_ids {
            res.push(self.delete(id).await?);
        }
        Ok(res)
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
        // Manual Move: Select -> Insert -> Delete
        // specific SQLite optimization: Use BEGIN IMMEDIATE to avoid "upgrade to write" deadlocks
        // caused by starting with SELECT (Reader) and then INSERT (Writer).
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN IMMEDIATE".into(),
                source: e,
                context: "Begin transaction".into(),
            })?;

        // Logic isolated to allow rollback on error
        async fn perform_archive(
            conn: &mut sqlx::SqliteConnection,
            msg_id: i64,
            worker_id: i64,
        ) -> Result<Option<ArchivedMessage>> {
            let row = sqlx::query(
                "SELECT * FROM pgqrs_messages WHERE id = $1 AND consumer_worker_id = $2",
            )
            .bind(msg_id)
            .bind(worker_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SelArchive".into(),
                source: e,
                context: "Select for archive".into(),
            })?;

            if let Some(r) = row {
                let q_id: i64 = r.try_get("queue_id")?;
                let p_wid: Option<i64> = r.try_get("producer_worker_id")?;
                let c_wid: Option<i64> = r.try_get("consumer_worker_id")?;
                let payload: String = r.try_get("payload")?;
                let enq: String = r.try_get("enqueued_at")?;
                let vt: String = r.try_get("vt")?;
                let read_ct: i32 = r.try_get("read_ct")?;
                let deq: Option<String> = r.try_get("dequeued_at")?;

                let arch_id: i64 = sqlx::query_scalar("INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id")
                     .bind(msg_id)
                     .bind(q_id)
                     .bind(p_wid)
                     .bind(c_wid)
                     .bind(payload.clone()) // payload string clone
                     .bind(enq.clone())
                     .bind(vt.clone())
                     .bind(read_ct)
                     .bind(deq.clone())
                     .fetch_one(&mut *conn)
                     .await
                     .map_err(|e| crate::error::Error::QueryFailed { query: "InsArchive".into(), source: e, context: "Insert archive".into() })?;

                sqlx::query("DELETE FROM pgqrs_messages WHERE id = $1")
                    .bind(msg_id)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "DelArchive".into(),
                        source: e,
                        context: "Delete archived".into(),
                    })?;

                use serde_json::Value;
                let val: Value = serde_json::from_str(&payload)?;
                Ok(Some(ArchivedMessage {
                    id: arch_id,
                    original_msg_id: msg_id,
                    queue_id: q_id,
                    producer_worker_id: p_wid,
                    consumer_worker_id: c_wid,
                    payload: val,
                    enqueued_at: parse_sqlite_timestamp(&enq)?,
                    vt: parse_sqlite_timestamp(&vt)?,
                    read_ct,
                    archived_at: Utc::now(), // Approx
                    dequeued_at: if let Some(d) = deq {
                        Some(parse_sqlite_timestamp(&d)?)
                    } else {
                        None
                    },
                }))
            } else {
                Ok(None)
            }
        }

        match perform_archive(&mut *conn, msg_id, self.worker_info.id).await {
            Ok(res) => {
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "COMMIT".into(),
                        source: e,
                        context: "Commit archive".into(),
                    })?;
                Ok(res)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        // Manual loop
        let mut res = Vec::new();
        for id in msg_ids {
            res.push(self.archive(id).await?.is_some());
        }
        Ok(res)
    }

    async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
        use crate::store::MessageTable;
        let res = self
            .messages
            .release_messages_by_ids(message_ids, self.worker_info.id)
            .await?;
        Ok(res.iter().filter(|&&b| b).count() as u64)
    }
}

impl Drop for SqliteConsumer {
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
