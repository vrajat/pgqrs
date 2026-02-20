use crate::error::Result;
use crate::store::sqlite::tables::messages::SqliteMessageTable;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::WorkerTable;
use crate::types::{QueueMessage, QueueRecord, WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::sync::Arc;

const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = datetime('now', '+' || $3 || ' seconds'),
        read_ct = read_ct + 1,
        dequeued_at = COALESCE(dequeued_at, datetime('now')),
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= datetime('now'))
          AND consumer_worker_id IS NULL
          AND archived_at IS NULL
          AND read_ct < $5
        ORDER BY enqueued_at ASC
        LIMIT $2
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const DEQUEUE_MESSAGES_AT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = datetime($5, '+' || $3 || ' seconds'),
        read_ct = read_ct + 1,
        dequeued_at = COALESCE(dequeued_at, $5),
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= $5)
          AND consumer_worker_id IS NULL
          AND archived_at IS NULL
          AND read_ct < $6
        ORDER BY enqueued_at ASC
        LIMIT $2
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at;
"#;

const DELETE_MESSAGE_OWNED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1 AND consumer_worker_id = $2
"#;

pub struct SqliteConsumer {
    pub pool: SqlitePool,
    queue_info: QueueRecord,
    worker_record: WorkerRecord,
    _config: crate::config::Config,
    workers: Arc<SqliteWorkerTable>,
    messages: Arc<SqliteMessageTable>,
}

impl SqliteConsumer {
    pub async fn new(
        pool: SqlitePool,
        queue_info: &QueueRecord,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_record = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }

    pub async fn new_ephemeral(
        pool: SqlitePool,
        queue_info: &QueueRecord,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = Arc::new(SqliteWorkerTable::new(pool.clone()));
        let worker_record = workers.register_ephemeral(Some(queue_info.id)).await?;
        let messages = Arc::new(SqliteMessageTable::new(pool.clone()));

        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_record,
            _config: config.clone(),
            workers,
            messages,
        })
    }
}

#[async_trait]
impl crate::store::Worker for SqliteConsumer {
    fn worker_record(&self) -> &WorkerRecord {
        &self.worker_record
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_record.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers
            .is_healthy(self.worker_record.id, max_age)
            .await
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
        use crate::store::MessageTable;
        let pending = self
            .messages
            .count_pending_filtered(self.queue_info.id, Some(self.worker_record.id))
            .await?;

        if pending > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending as u64,
                reason: format!("Consumer has {} pending messages", pending),
            });
        }
        self.workers.shutdown(self.worker_record.id).await
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
            .bind(self.worker_record.id)
            .bind(self._config.max_read_ct)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE".into(),
                source: Box::new(e),
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
            .bind(self.worker_record.id)
            .bind(now_str)
            .bind(self._config.max_read_ct)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE_AT".into(),
                source: Box::new(e),
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
            .extend_visibility(message_id, self.worker_record.id, additional_seconds)
            .await?;
        Ok(c > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows = sqlx::query(DELETE_MESSAGE_OWNED)
            .bind(message_id)
            .bind(self.worker_record.id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEL_OWNED".into(),
                source: Box::new(e),
                context: "Del owned".into(),
            })?
            .rows_affected();
        Ok(rows > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        // Use QueryBuilder to construct "DELETE ... WHERE id IN (...) RETURNING id"
        let mut query_builder =
            sqlx::QueryBuilder::new("DELETE FROM pgqrs_messages WHERE consumer_worker_id = ");
        query_builder.push_bind(self.worker_record.id);
        query_builder.push(" AND id IN (");
        let mut separated = query_builder.separated(", ");
        for id in &message_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") RETURNING id");

        let deleted_ids: Vec<i64> = query_builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MANY".into(),
                source: Box::new(e),
                context: "Batch delete".into(),
            })?
            .iter()
            .map(|row| row.try_get(0).unwrap_or(0))
            .collect();

        // Construct result vec preserving order
        let mut results = Vec::with_capacity(message_ids.len());
        for id in message_ids {
            results.push(deleted_ids.contains(&id));
        }
        Ok(results)
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        let row = sqlx::query(
            "UPDATE pgqrs_messages SET archived_at = datetime('now') WHERE id = $1 AND consumer_worker_id = $2 AND archived_at IS NULL RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at",
        )
        .bind(msg_id)
        .bind(self.worker_record.id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "Archive".into(),
            source: Box::new(e),
            context: "Archive message".into(),
        })?;

        if let Some(r) = row {
            Ok(Some(SqliteMessageTable::map_row(r)?))
        } else {
            Ok(None)
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
            .release_messages_by_ids(message_ids, self.worker_record.id)
            .await?;
        Ok(res.iter().filter(|&&b| b).count() as u64)
    }

    async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        use crate::store::MessageTable;
        let count = self
            .messages
            .release_with_visibility(message_id, self.worker_record.id, visible_at)
            .await?;
        Ok(count > 0)
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for SqliteConsumer {
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
