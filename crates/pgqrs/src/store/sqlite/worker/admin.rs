use crate::config::Config;
use crate::error::Result;
use crate::store::sqlite::parse_sqlite_timestamp;
use crate::store::sqlite::tables::archive::SqliteArchiveTable;
use crate::store::sqlite::tables::messages::SqliteMessageTable;
use crate::store::sqlite::tables::queues::SqliteQueueTable;
use crate::store::sqlite::tables::workers::SqliteWorkerTable;
use crate::store::{ArchiveTable, MessageTable, QueueTable, WorkerTable};
use crate::types::{
    QueueInfo, QueueMessage, QueueMetrics, SystemStats, WorkerHealthStats, WorkerInfo, WorkerStatus,
};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

// Verification queries
const CHECK_TABLE_EXISTS: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = ?
    )
"#;

const CHECK_ORPHANED_MESSAGES: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_messages m
    LEFT OUTER JOIN pgqrs_queues q ON m.queue_id = q.id
    WHERE q.id IS NULL
"#;

const CHECK_ORPHANED_MESSAGE_WORKERS: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_messages m
    LEFT OUTER JOIN pgqrs_workers pw ON m.producer_worker_id = pw.id
    LEFT OUTER JOIN pgqrs_workers cw ON m.consumer_worker_id = cw.id
    WHERE (m.producer_worker_id IS NOT NULL AND pw.id IS NULL)
       OR (m.consumer_worker_id IS NOT NULL AND cw.id IS NULL)
"#;

const CHECK_ORPHANED_ARCHIVE_QUEUES: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_archive a
    LEFT OUTER JOIN pgqrs_queues q ON a.queue_id = q.id
    WHERE q.id IS NULL
"#;

const CHECK_ORPHANED_ARCHIVE_WORKERS: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_archive a
    LEFT OUTER JOIN pgqrs_workers pw ON a.producer_worker_id = pw.id
    LEFT OUTER JOIN pgqrs_workers cw ON a.consumer_worker_id = cw.id
    WHERE (a.producer_worker_id IS NOT NULL AND pw.id IS NULL)
       OR (a.consumer_worker_id IS NOT NULL AND cw.id IS NULL)
"#;

const RELEASE_ZOMBIE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET consumer_worker_id = NULL,
        vt = datetime('now'),
        dequeued_at = NULL
    WHERE consumer_worker_id = ?
"#;

const SHUTDOWN_ZOMBIE_WORKER: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped',
        shutdown_at = datetime('now')
    WHERE id = ?
"#;

const GET_WORKER_MESSAGES: &str = r#"
    SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
    FROM pgqrs_messages
    WHERE consumer_worker_id = ?
    ORDER BY id
"#;

const RELEASE_WORKER_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NULL, consumer_worker_id = NULL
    WHERE consumer_worker_id = ?
"#;

const CHECK_WORKER_REFERENCES: &str = r#"
    SELECT COUNT(*) as total_references FROM (
        SELECT 1 FROM pgqrs_messages WHERE producer_worker_id = $1 OR consumer_worker_id = $1
        UNION ALL
        SELECT 1 FROM pgqrs_archive WHERE producer_worker_id = $1 OR consumer_worker_id = $1
    ) refs
"#;

const PURGE_OLD_WORKERS: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE status = 'stopped'
      AND heartbeat_at < datetime('now', '-' || ? || ' seconds')
      AND id NOT IN (
          SELECT DISTINCT worker_id
          FROM (
              SELECT producer_worker_id as worker_id FROM pgqrs_messages WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL
              UNION
              SELECT producer_worker_id as worker_id FROM pgqrs_archive WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_archive WHERE consumer_worker_id IS NOT NULL
          ) refs
      )
"#;

// SQLite doesn't strictly support COUNT FILTER until newer versions, use SUM(CASE...)
const GET_QUEUE_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL THEN 1 ELSE 0 END), 0) as pending_messages,
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL THEN 1 ELSE 0 END), 0) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id) as archived_messages,
        MIN(CASE WHEN m.consumer_worker_id IS NULL THEN m.enqueued_at END) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    WHERE q.id = ?
    GROUP BY q.id, q.queue_name
"#;

const GET_ALL_QUEUES_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL THEN 1 ELSE 0 END), 0) as pending_messages,
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL THEN 1 ELSE 0 END), 0) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id) as archived_messages,
        MIN(CASE WHEN m.consumer_worker_id IS NULL THEN m.enqueued_at END) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    GROUP BY q.id, q.queue_name
"#;

const GET_SYSTEM_STATS: &str = r#"
    SELECT
        (SELECT COUNT(*) FROM pgqrs_queues) as total_queues,
        (SELECT COUNT(*) FROM pgqrs_workers) as total_workers,
        (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready') as active_workers,
        (SELECT COUNT(*) FROM pgqrs_messages) as total_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL) as pending_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive) as archived_messages,
        '0.5.0' as schema_version
"#;

const GET_WORKER_HEALTH_GLOBAL: &str = r#"
    SELECT
        'Global' as queue_name,
        COUNT(*) as total_workers,
        SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) as ready_workers,
        SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) as suspended_workers,
        SUM(CASE WHEN status = 'stopped' THEN 1 ELSE 0 END) as stopped_workers,
        SUM(CASE WHEN status = 'ready' AND heartbeat_at < datetime('now', '-' || ? || ' seconds') THEN 1 ELSE 0 END) as stale_workers
    FROM pgqrs_workers
"#;

const GET_WORKER_HEALTH_BY_QUEUE: &str = r#"
    SELECT
        COALESCE(q.queue_name, 'Admin') as queue_name,
        COUNT(w.id) as total_workers,
        SUM(CASE WHEN w.status = 'ready' THEN 1 ELSE 0 END) as ready_workers,
        SUM(CASE WHEN w.status = 'suspended' THEN 1 ELSE 0 END) as suspended_workers,
        SUM(CASE WHEN w.status = 'stopped' THEN 1 ELSE 0 END) as stopped_workers,
        SUM(CASE WHEN w.status = 'ready' AND w.heartbeat_at < datetime('now', '-' || ? || ' seconds') THEN 1 ELSE 0 END) as stale_workers
    FROM pgqrs_workers w
    LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
    GROUP BY q.queue_name
"#;

#[derive(Debug, Clone)]
pub struct SqliteAdmin {
    pub pool: SqlitePool,
    pub config: Config,
    pub queues: Arc<SqliteQueueTable>,
    pub messages: Arc<SqliteMessageTable>,
    pub workers: Arc<SqliteWorkerTable>,
    pub archive: Arc<SqliteArchiveTable>,
    worker_info: Option<WorkerInfo>,
}

impl SqliteAdmin {
    pub fn new(pool: SqlitePool, config: Config) -> Self {
        Self {
            pool: pool.clone(),
            config,
            queues: Arc::new(SqliteQueueTable::new(pool.clone())),
            messages: Arc::new(SqliteMessageTable::new(pool.clone())),
            workers: Arc::new(SqliteWorkerTable::new(pool.clone())),
            archive: Arc::new(SqliteArchiveTable::new(pool.clone())),
            worker_info: None,
        }
    }

    async fn check_worker_references(&self, worker_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(CHECK_WORKER_REFERENCES)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_WORKER_REFERENCES".into(),
                source: e,
                context: format!("Failed to check worker references for worker {}", worker_id),
            })?;
        Ok(count)
    }

    fn try_worker_id(&self) -> Result<i64> {
        self.worker_info.as_ref().map(|w| w.id).ok_or_else(|| {
            crate::error::Error::WorkerNotRegistered {
                message:
                    "Admin must be registered before using Worker methods. Call register() first."
                        .to_string(),
            }
        })
    }

    fn map_queue_metrics_row(row: SqliteRow) -> Result<QueueMetrics> {
        let name: String = row.try_get("name")?;
        let total_messages: i64 = row.try_get("total_messages")?;
        let pending_messages: i64 = row.try_get("pending_messages")?; // SUM returns generic number, possibly i64 or f64, but generic map helps
        let locked_messages: i64 = row.try_get("locked_messages")?;
        let archived_messages: i64 = row.try_get("archived_messages")?;

        let oldest_pending_str: Option<String> = row.try_get("oldest_pending_message")?;
        let oldest_pending_message = match oldest_pending_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        let newest_message_str: Option<String> = row.try_get("newest_message")?;
        let newest_message = match newest_message_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        Ok(QueueMetrics {
            name,
            total_messages,
            pending_messages,
            locked_messages,
            archived_messages,
            oldest_pending_message,
            newest_message,
        })
    }

    fn map_system_stats_row(row: SqliteRow) -> Result<SystemStats> {
        Ok(SystemStats {
            total_queues: row.try_get("total_queues")?,
            total_workers: row.try_get("total_workers")?,
            active_workers: row.try_get("active_workers")?,
            total_messages: row.try_get("total_messages")?,
            pending_messages: row.try_get("pending_messages")?,
            locked_messages: row.try_get("locked_messages")?,
            archived_messages: row.try_get("archived_messages")?,
            schema_version: row.try_get("schema_version")?,
        })
    }

    fn map_worker_health_row(row: SqliteRow) -> Result<WorkerHealthStats> {
        Ok(WorkerHealthStats {
            queue_name: row.try_get("queue_name")?,
            total_workers: row.try_get("total_workers")?,
            ready_workers: row.try_get("ready_workers")?,
            suspended_workers: row.try_get("suspended_workers")?,
            stopped_workers: row.try_get("stopped_workers")?,
            stale_workers: row.try_get("stale_workers")?,
        })
    }
}

#[async_trait]
impl crate::store::Worker for SqliteAdmin {
    fn worker_id(&self) -> i64 {
        self.worker_info.as_ref().map(|w| w.id).unwrap_or(-1)
    }

    async fn heartbeat(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.heartbeat(worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        let worker_id = self.try_worker_id()?;
        self.workers.is_healthy(worker_id, max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        let worker_id = self.try_worker_id()?;
        self.workers.get_status(worker_id).await
    }

    async fn suspend(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.suspend(worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.resume(worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.shutdown(worker_id).await
    }
}

#[async_trait]
impl crate::store::Admin for SqliteAdmin {
    async fn install(&self) -> Result<()> {
        sqlx::migrate!("migrations/sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Database(e.into()))?;
        Ok(())
    }

    async fn verify(&self) -> Result<()> {
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
            ("pgqrs_archive", "Unified archive table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists: bool = sqlx::query_scalar(CHECK_TABLE_EXISTS)
                .bind(table_name)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("CHECK_TABLE_EXISTS ({})", table_name),
                    source: e,
                    context: format!("Failed to check if table {} exists", table_name),
                })?;

            if !table_exists {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        // Integrity checks
        let count: i64 = sqlx::query_scalar(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_MESSAGES".into(),
                source: e,
                context: "Check orphaned messages".into(),
            })?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} orphaned messages", count),
            });
        }

        let count: i64 = sqlx::query_scalar(CHECK_ORPHANED_MESSAGE_WORKERS)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_MESSAGE_WORKERS".into(),
                source: e,
                context: "Check orphaned message workers".into(),
            })?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} messages with invalid worker refs", count),
            });
        }

        let count: i64 = sqlx::query_scalar(CHECK_ORPHANED_ARCHIVE_QUEUES)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_ARCHIVE_QUEUES".into(),
                source: e,
                context: "Check orphaned archive queues".into(),
            })?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} orphaned archive entries", count),
            });
        }

        let count: i64 = sqlx::query_scalar(CHECK_ORPHANED_ARCHIVE_WORKERS)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_ARCHIVE_WORKERS".into(),
                source: e,
                context: "Check orphaned archive workers".into(),
            })?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} archive entries with invalid worker refs", count),
            });
        }

        Ok(())
    }

    async fn register(&mut self, hostname: String, port: i32) -> Result<WorkerInfo> {
        if let Some(ref info) = self.worker_info {
            return Ok(info.clone());
        }
        let info = self.workers.register(None, &hostname, port).await?;
        self.worker_info = Some(info.clone());
        Ok(info)
    }

    async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
        use crate::types::NewQueue;
        self.queues
            .insert(NewQueue {
                queue_name: name.to_string(),
            })
            .await
    }

    async fn get_queue(&self, name: &str) -> Result<QueueInfo> {
        self.queues.get_by_name(name).await
    }

    async fn delete_queue(&self, queue_info: &QueueInfo) -> Result<()> {
        // SQLite doesn't strictly need the complexity of FOR UPDATE locking since access is serialized.
        // We can just check and delete in a transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(crate::error::Error::Database)?;

        // Check active workers
        let ready = self
            .workers
            .count_for_queue(queue_info.id, WorkerStatus::Ready)
            .await?;
        let suspended = self
            .workers
            .count_for_queue(queue_info.id, WorkerStatus::Suspended)
            .await?;
        if ready + suspended > 0 {
            return Err(crate::error::Error::ValidationFailed {
                reason: "Cannot delete queue: active workers exist".to_string(),
            });
        }

        // Check references
        let msgs = self.messages.filter_by_fk(queue_info.id).await?.len();
        let arch = self.archive.filter_by_fk(queue_info.id).await?.len();
        if msgs > 0 || arch > 0 {
            return Err(crate::error::Error::ValidationFailed {
                reason: "Cannot delete queue: data exists".to_string(),
            });
        }

        sqlx::query("DELETE FROM pgqrs_queues WHERE id = ?")
            .bind(queue_info.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_QUEUE".into(),
                source: e,
                context: "Failed to delete queue".into(),
            })?;

        tx.commit().await.map_err(crate::error::Error::Database)?;
        Ok(())
    }

    async fn purge_queue(&self, name: &str) -> Result<()> {
        let queue = self.queues.get_by_name(name).await?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(crate::error::Error::Database)?;

        // Manual delete calls since table helpers might not take tx
        // Actually table helpers typically take &self (pool).
        // To use transaction, we need methods that accept executor, or execute raw sql on tx.
        // Given SqliteStore layout, we might need to just execute SQL for purge.

        sqlx::query("DELETE FROM pgqrs_messages WHERE queue_id = ?")
            .bind(queue.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PURGE_MSGS".into(),
                source: e,
                context: "Purge messages".into(),
            })?;
        sqlx::query("DELETE FROM pgqrs_archive WHERE queue_id = ?")
            .bind(queue.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PURGE_ARCHIVE".into(),
                source: e,
                context: "Purge archive".into(),
            })?;
        sqlx::query("DELETE FROM pgqrs_workers WHERE queue_id = ?")
            .bind(queue.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PURGE_WORKERS".into(),
                source: e,
                context: "Purge workers".into(),
            })?;

        tx.commit().await.map_err(crate::error::Error::Database)?;
        Ok(())
    }

    async fn dlq(&self) -> Result<Vec<i64>> {
        // SQLite: Manual MOVE (Select -> Insert -> Delete)
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(crate::error::Error::Database)?;

        // 1. Select
        let messages: Vec<SqliteRow> =
            sqlx::query("SELECT * FROM pgqrs_messages WHERE read_ct >= ?")
                .bind(self.config.max_read_ct)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "DLQ_Select".into(),
                    source: e,
                    context: "Select DLQ candidates".into(),
                })?;

        let mut moved_ids = Vec::new();

        for row in messages {
            let id: i64 = row.try_get("id")?;
            let q_id: i64 = row.try_get("queue_id")?;
            let p_wid: Option<i64> = row.try_get("producer_worker_id")?;
            let c_wid: Option<i64> = row.try_get("consumer_worker_id")?;
            let payload: String = row.try_get("payload")?;
            let vt: String = row.try_get("vt")?; // raw string
            let enq: String = row.try_get("enqueued_at")?;
            let read_ct: i32 = row.try_get("read_ct")?;
            let deq: Option<String> = row.try_get("dequeued_at")?;

            // 2. Insert into Archive
            sqlx::query("INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                 .bind(id).bind(q_id).bind(p_wid).bind(c_wid).bind(payload).bind(enq).bind(vt).bind(read_ct).bind(deq)
                 .execute(&mut *tx)
                 .await
                 .map_err(|e| crate::error::Error::QueryFailed { query: "DLQ_Insert".into(), source: e, context: "Insert DLQ archive".into() })?;

            // 3. Delete
            sqlx::query("DELETE FROM pgqrs_messages WHERE id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "DLQ_Delete".into(),
                    source: e,
                    context: "Delete DLQ message".into(),
                })?;

            moved_ids.push(id);
        }

        tx.commit().await.map_err(crate::error::Error::Database)?;
        Ok(moved_ids)
    }

    async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        let queue = self.queues.get_by_name(name).await?;
        let row = sqlx::query(GET_QUEUE_METRICS)
            .bind(queue.id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_QUEUE_METRICS".into(),
                source: e,
                context: "Queue metrics".into(),
            })?;
        Self::map_queue_metrics_row(row)
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let rows = sqlx::query(GET_ALL_QUEUES_METRICS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_ALL_QUEUES_METRICS".into(),
                source: e,
                context: "All queues metrics".into(),
            })?;

        let mut metrics = Vec::new();
        for row in rows {
            metrics.push(Self::map_queue_metrics_row(row)?);
        }
        Ok(metrics)
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        let row = sqlx::query(GET_SYSTEM_STATS)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_SYSTEM_STATS".into(),
                source: e,
                context: "System stats".into(),
            })?;
        Self::map_system_stats_row(row)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let seconds = heartbeat_timeout.num_seconds();
        let rows = if group_by_queue {
            sqlx::query(GET_WORKER_HEALTH_BY_QUEUE)
                .bind(seconds)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(GET_WORKER_HEALTH_GLOBAL)
                .bind(seconds)
                .fetch_all(&self.pool)
                .await
        };

        let rows = rows.map_err(|e| crate::error::Error::QueryFailed {
            query: "WORKER_HEALTH".into(),
            source: e,
            context: "Worker health".into(),
        })?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(Self::map_worker_health_row(row)?);
        }
        Ok(stats)
    }

    async fn worker_stats(&self, queue_name: &str) -> Result<crate::types::WorkerStats> {
        let queue_id = self.queues.get_by_name(queue_name).await?.id;
        let workers = self.workers.filter_by_fk(queue_id).await?;

        let total_workers = workers.len() as u32;
        let ready_workers = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Ready)
            .count() as u32;
        let stopped_workers = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Stopped)
            .count() as u32;
        let suspended_workers = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Suspended)
            .count() as u32;

        let mut total_messages = 0u64;
        for worker in &workers {
            let encoded_messages = self.get_worker_messages(worker.id).await?;
            total_messages += encoded_messages.len() as u64;
        }

        let average_messages_per_worker = if total_workers > 0 {
            total_messages as f64 / total_workers as f64
        } else {
            0.0
        };

        let now = Utc::now();
        let oldest_worker_age = workers
            .iter()
            .map(|w| now.signed_duration_since(w.started_at))
            .max()
            .unwrap_or(chrono::Duration::zero());
        let newest_heartbeat_age = workers
            .iter()
            .map(|w| now.signed_duration_since(w.heartbeat_at))
            .min()
            .unwrap_or(chrono::Duration::zero());

        Ok(crate::types::WorkerStats {
            total_workers,
            ready_workers,
            suspended_workers,
            stopped_workers,
            average_messages_per_worker,
            oldest_worker_age,
            newest_heartbeat_age,
        })
    }

    async fn delete_worker(&self, worker_id: i64) -> Result<u64> {
        let refs = self.check_worker_references(worker_id).await?;
        if refs > 0 {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Worker has {} references (associated messages/archives)",
                    refs
                ),
            });
        }
        self.workers.delete(worker_id).await
    }

    async fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        self.workers.list().await
    }

    async fn get_worker_messages(&self, worker_id: i64) -> Result<Vec<QueueMessage>> {
        let worker = self.workers.get(worker_id).await?;
        if worker.queue_id.is_none() {
            return Err(crate::error::Error::ValidationFailed {
                reason: "Cannot get messages for admin worker".into(),
            });
        }

        // Use SqliteMessageTable to map properly
        let rows = sqlx::query(GET_WORKER_MESSAGES)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_WORKER_MESSAGES".into(),
                source: e,
                context: "Get worker msgs".into(),
            })?;

        let mut msgs = Vec::new();
        for row in rows {
            // We can use SqliteMessageTable::map_row if we expose it or copy logic.
            // map_row is private. But SqliteMessageTable is available.
            // Actually, MessageTable::map_row is specific to the struct.
            // We'll duplicate the mapping logic briefly here or refactor.
            // Since SqliteMessageTable has it private, I'll copy the mapping logic which is safe.
            // Wait, I can't call private methods.
            // The query GET_WORKER_MESSAGES returns columns matching what map_row expects.
            // I'll define a local map function re-using what I wrote for MessageTable.
            msgs.push(SqliteMessageTable::map_row(row)?);
        }
        Ok(msgs)
    }

    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<chrono::Duration>,
    ) -> Result<u64> {
        let timeout = older_than
            .unwrap_or_else(|| chrono::Duration::seconds(self.config.heartbeat_interval as i64));
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(crate::error::Error::Database)?;

        // This is tricky. list_zombies_for_queue takes self (pool).
        // I need to use the transaction.
        // SqliteWorkerTable doesn't expose transactional methods yet.
        // I will implement the zombie check manually here using the query string.

        let seconds = timeout.num_seconds();
        let zombies_query = r#"
            SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
            FROM pgqrs_workers
            WHERE queue_id = $1
              AND status = 'ready'
              AND heartbeat_at < datetime('now', '-' || $2 || ' seconds')
        "#;

        let zombies_rows = sqlx::query(zombies_query)
            .bind(queue_id)
            .bind(seconds)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ZOMBIES".into(),
                source: e,
                context: "Find zombies".into(),
            })?;

        let mut total = 0;
        for row in zombies_rows {
            let id: i64 = row.get("id");

            let res = sqlx::query(RELEASE_ZOMBIE_MESSAGES)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "RELEASE_ZOMBIES".into(),
                    source: e,
                    context: "Release".into(),
                })?;

            total += res.rows_affected();

            sqlx::query(SHUTDOWN_ZOMBIE_WORKER)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SHUTDOWN_ZOMBIE".into(),
                    source: e,
                    context: "Shutdown".into(),
                })?;
        }

        tx.commit().await.map_err(crate::error::Error::Database)?;
        Ok(total)
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let seconds = older_than.num_seconds();
        let res = sqlx::query(PURGE_OLD_WORKERS)
            .bind(seconds)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PURGE_OLD".into(),
                source: e,
                context: "Purge old".into(),
            })?;
        Ok(res.rows_affected())
    }

    async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        let res = sqlx::query(RELEASE_WORKER_MESSAGES)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_WORKER_MSG".into(),
                source: e,
                context: "Release msg".into(),
            })?;
        Ok(res.rows_affected())
    }
}
