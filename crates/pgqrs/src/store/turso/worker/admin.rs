use crate::config::Config;
use crate::error::Result;
use crate::store::turso::parse_turso_timestamp;
use crate::store::turso::tables::archive::TursoArchiveTable;
use crate::store::turso::tables::messages::TursoMessageTable;
use crate::store::turso::tables::queues::TursoQueueTable;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::{Admin, ArchiveTable, MessageTable, QueueTable, Worker, WorkerTable};
use crate::types::{
    QueueInfo, QueueMessage, QueueMetrics, SystemStats, WorkerHealthStats, WorkerInfo, WorkerStats,
    WorkerStatus,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use std::sync::Arc;
use turso::Database;

// SQL Constants (Adapted from SQLite implementation)
const CHECK_TABLE_EXISTS: &str = r#"
    SELECT 1 FROM sqlite_master
    WHERE type = 'table' AND name = ?
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

const CHECK_WORKER_REFS_MESSAGES: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages WHERE producer_worker_id = ? OR consumer_worker_id = ?
"#;

const CHECK_WORKER_REFS_ARCHIVE: &str = r#"
    SELECT COUNT(*) FROM pgqrs_archive WHERE producer_worker_id = ? OR consumer_worker_id = ?
"#;

const FIND_OLD_WORKERS_TO_PURGE: &str = r#"
    SELECT w.id FROM pgqrs_workers w
    LEFT JOIN pgqrs_messages m ON m.producer_worker_id = w.id OR m.consumer_worker_id = w.id
    LEFT JOIN pgqrs_archive a ON a.producer_worker_id = w.id OR a.consumer_worker_id = w.id
    WHERE w.status = 'stopped'
      AND w.heartbeat_at < datetime('now', '-' || ? || ' seconds')
      AND m.id IS NULL
      AND a.id IS NULL
    GROUP BY w.id
"#;

const GET_QUEUE_METRICS: &str = r#"
    SELECT
        q.queue_name,
        COUNT(m.id),
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL THEN 1 ELSE 0 END), 0),
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id),
        MIN(CASE WHEN m.consumer_worker_id IS NULL THEN m.enqueued_at END),
        MAX(m.enqueued_at)
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    WHERE q.id = ?
    GROUP BY q.id, q.queue_name
"#;

const GET_ALL_QUEUES_METRICS: &str = r#"
    SELECT
        q.queue_name,
        COUNT(m.id),
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL THEN 1 ELSE 0 END), 0),
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id),
        MIN(CASE WHEN m.consumer_worker_id IS NULL THEN m.enqueued_at END),
        MAX(m.enqueued_at)
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    GROUP BY q.id, q.queue_name
"#;

const GET_SYSTEM_STATS: &str = r#"
    SELECT
        (SELECT COUNT(*) FROM pgqrs_queues),
        (SELECT COUNT(*) FROM pgqrs_workers),
        (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready'),
        (SELECT COUNT(*) FROM pgqrs_messages),
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL),
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL),
        (SELECT COUNT(*) FROM pgqrs_archive),
        '0.5.0'
"#;

const GET_WORKER_HEALTH_GLOBAL: &str = r#"
    SELECT
        'Global',
        COUNT(*),
        SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'stopped' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'ready' AND heartbeat_at < datetime('now', '-' || ? || ' seconds') THEN 1 ELSE 0 END)
    FROM pgqrs_workers
"#;

const GET_WORKER_HEALTH_BY_QUEUE: &str = r#"
    SELECT
        COALESCE(q.queue_name, 'Admin'),
        COUNT(w.id),
        SUM(CASE WHEN w.status = 'ready' THEN 1 ELSE 0 END),
        SUM(CASE WHEN w.status = 'suspended' THEN 1 ELSE 0 END),
        SUM(CASE WHEN w.status = 'stopped' THEN 1 ELSE 0 END),
        SUM(CASE WHEN w.status = 'ready' AND w.heartbeat_at < datetime('now', '-' || ? || ' seconds') THEN 1 ELSE 0 END)
    FROM pgqrs_workers w
    LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
    GROUP BY q.queue_name
"#;

#[derive(Debug, Clone)]
pub struct TursoAdmin {
    _worker_id: i64,
    db: Arc<Database>,
    config: Config,
    queues: Arc<TursoQueueTable>,
    messages: Arc<TursoMessageTable>,
    workers: Arc<TursoWorkerTable>,
    archive: Arc<TursoArchiveTable>,
    worker_info: Option<WorkerInfo>,
}

impl TursoAdmin {
    pub fn new(db: Arc<Database>, worker_id: i64, config: Config) -> Self {
        Self {
            _worker_id: worker_id,
            db: db.clone(),
            config,
            queues: Arc::new(TursoQueueTable::new(db.clone())),
            messages: Arc::new(TursoMessageTable::new(db.clone())),
            workers: Arc::new(TursoWorkerTable::new(db.clone())),
            archive: Arc::new(TursoArchiveTable::new(db.clone())),
            worker_info: None,
        }
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

    fn map_queue_metrics_row(row: &turso::Row) -> Result<QueueMetrics> {
        let name: String = row.get(0)?;
        let total_messages: i64 = row.get(1)?;
        let pending_messages: i64 = row.get(2)?;
        let locked_messages: i64 = row.get(3)?;
        let archived_messages: i64 = row.get(4)?;

        // Oldest and newest timestamps might be NULL
        let oldest_pending_str: Option<String> = row.get(5)?;
        let oldest_pending_message = match oldest_pending_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let newest_message_str: Option<String> = row.get(6)?;
        let newest_message = match newest_message_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
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

    fn map_system_stats_row(row: &turso::Row) -> Result<SystemStats> {
        Ok(SystemStats {
            total_queues: row.get(0)?,
            total_workers: row.get(1)?,
            active_workers: row.get(2)?,
            total_messages: row.get(3)?,
            pending_messages: row.get(4)?,
            locked_messages: row.get(5)?,
            archived_messages: row.get(6)?,
            // schema version is typically string or text
            schema_version: row.get(7)?,
        })
    }

    fn map_worker_health_row(row: &turso::Row) -> Result<WorkerHealthStats> {
        Ok(WorkerHealthStats {
            queue_name: row.get(0)?,
            total_workers: row.get(1)?,
            ready_workers: row.get(2)?,
            suspended_workers: row.get(3)?,
            stopped_workers: row.get(4)?,
            stale_workers: row.get(5)?,
        })
    }
}

// ... Implement Worker trait ...
#[async_trait]
impl Worker for TursoAdmin {
    fn worker_id(&self) -> i64 {
        self.worker_info.as_ref().map(|w| w.id).unwrap_or(-1)
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

    async fn heartbeat(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.heartbeat(worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        let worker_id = self.try_worker_id()?;
        self.workers.is_healthy(worker_id, max_age).await
    }
}

#[async_trait]
impl Admin for TursoAdmin {
    async fn install(&self) -> Result<()> {
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS pgqrs_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL,
                description TEXT
            );",
            (),
        )
        .await
        .map_err(|e| crate::error::Error::Internal {
            message: format!("Failed to create schema version table: {}", e),
        })?;

        // Migration list: (version, description, sql)
        const Q1: &str = include_str!("../../../../migrations/turso/01_create_queues.sql");
        const Q2: &str = include_str!("../../../../migrations/turso/02_create_workers.sql");
        const Q3: &str = include_str!("../../../../migrations/turso/03_create_messages.sql");
        const Q4: &str = include_str!("../../../../migrations/turso/04_create_archive.sql");
        const Q5: &str = include_str!("../../../../migrations/turso/05_create_workflows.sql");
        const Q6: &str = include_str!("../../../../migrations/turso/06_create_workflow_steps.sql");
        const Q7: &str = include_str!("../../../../migrations/turso/07_create_indices.sql");

        let migrations = vec![
            (1, "01_create_queues.sql", Q1),
            (2, "02_create_workers.sql", Q2),
            (3, "03_create_messages.sql", Q3),
            (4, "04_create_archive.sql", Q4),
            (5, "05_create_workflows.sql", Q5),
            (6, "06_create_workflow_steps.sql", Q6),
            (7, "07_create_indices.sql", Q7),
        ];

        for (version, description, sql) in migrations {
            // Check if already applied
            // We use query_row logic manually since turso crate might not have query_scalar/optional easily accessible here
            let mut rows = conn
                .query(
                    "SELECT version FROM pgqrs_schema_version WHERE version = ?",
                    (version,),
                )
                .await
                .map_err(|e| crate::error::Error::Internal {
                    message: format!("Failed to query migration {}: {}", version, e),
                })?;

            if rows
                .next()
                .await
                .map_err(|e| crate::error::Error::Internal {
                    message: format!("Failed to fetch migration row {}: {}", version, e),
                })?
                .is_some()
            {
                continue;
            }

            tracing::info!("Applying migration {}: {}", version, description);

            conn.execute(sql, ())
                .await
                .map_err(|e| crate::error::Error::Internal {
                    message: format!("Failed to run migration {}: {}", version, e),
                })?;

            // Record success
            conn.execute(
                "INSERT INTO pgqrs_schema_version (version, applied_at, description) VALUES (?, datetime('now'), ?)",
                (version, description),
            )
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to record migration {}: {}", version, e),
            })?;
        }
        Ok(())
    }

    async fn verify(&self) -> Result<()> {
        // Table existence check
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
            ("pgqrs_archive", "Unified archive table"),
        ];

        for (table_name, description) in &required_tables {
            let exists: Option<i64> = crate::store::turso::query_scalar(CHECK_TABLE_EXISTS)
                .bind(table_name.to_string())
                .fetch_optional(&self.db)
                .await?;

            if exists.is_none() {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        // Integrity checks
        let count: i64 = crate::store::turso::query_scalar(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&self.db)
            .await?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} messages with invalid queue_id references", count),
            });
        }

        let count: i64 = crate::store::turso::query_scalar(CHECK_ORPHANED_MESSAGE_WORKERS)
            .fetch_one(&self.db)
            .await?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} messages with invalid worker refs", count),
            });
        }

        let count: i64 = crate::store::turso::query_scalar(CHECK_ORPHANED_ARCHIVE_QUEUES)
            .fetch_one(&self.db)
            .await?;
        if count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!("Found {} orphaned archive entries", count),
            });
        }

        let count: i64 = crate::store::turso::query_scalar(CHECK_ORPHANED_ARCHIVE_WORKERS)
            .fetch_one(&self.db)
            .await?;
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

        self.queues.delete(queue_info.id).await.map(|_| ())
    }

    async fn purge_queue(&self, name: &str) -> Result<()> {
        let queue = self.queues.get_by_name(name).await?;

        // Manual delete calls using a single connection for transaction if possible
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        // Begin transaction
        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Purge queue".into(),
            })?;

        // Execute deletes
        let res = async {
            conn.execute(
                "DELETE FROM pgqrs_messages WHERE queue_id = ?",
                vec![turso::Value::Integer(queue.id)],
            )
            .await?;
            conn.execute(
                "DELETE FROM pgqrs_archive WHERE queue_id = ?",
                vec![turso::Value::Integer(queue.id)],
            )
            .await?;
            conn.execute(
                "DELETE FROM pgqrs_workers WHERE queue_id = ?",
                vec![turso::Value::Integer(queue.id)],
            )
            .await?;
            Ok::<_, turso::Error>(())
        };

        match res.await {
            Ok(_) => {
                conn.execute("COMMIT", ())
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "COMMIT".into(),
                        source: Box::new(e),
                        context: "Purge queue".into(),
                    })?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(crate::error::Error::QueryFailed {
                    query: "PURGE".into(),
                    source: Box::new(e),
                    context: "Purge queue logic".into(),
                })
            }
        }
    }

    async fn dlq(&self) -> Result<Vec<i64>> {
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "DLQ".into(),
            })?;

        // 1. Select candidates
        // NOTE: We list columns explicitly to ensure index based retrieval works
        let rows = conn
            .query(
                "SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at FROM pgqrs_messages WHERE read_ct >= ?",
                vec![turso::Value::Integer(self.config.max_read_ct as i64)],
            )
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DLQ_Select".into(),
                source: Box::new(e),
                context: "DLQ".into(),
            })?;

        // Collect all potential DLQ items
        let mut messages = Vec::new();
        let mut cursor = rows;
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DLQ_Cursor".into(),
                source: Box::new(e),
                context: "DLQ".into(),
            })?
        {
            // Map row manually
            let id: i64 = row.get(0).unwrap();
            let q_id: i64 = row.get(1).unwrap();
            let p_wid: Option<i64> = row.get(2).unwrap();
            let c_wid: Option<i64> = row.get(3).unwrap();
            let payload: String = row.get(4).unwrap();
            let vt: Option<String> = row.get(5).unwrap();
            let enq: String = row.get(6).unwrap();
            let read_ct: i32 = row.get(7).unwrap();
            let deq: Option<String> = row.get(8).unwrap();

            messages.push((id, q_id, p_wid, c_wid, payload, vt, enq, read_ct, deq));
        }

        let mut moved_ids = Vec::new();

        for (id, q_id, p_wid, c_wid, payload, vt, enq, read_ct, deq) in messages {
            // 2. Insert into Archive
            let now_str = crate::store::turso::format_turso_timestamp(&chrono::Utc::now());
            let res = conn.execute(
                 "INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 vec![
                     turso::Value::Integer(id),
                     turso::Value::Integer(q_id),
                     match p_wid { Some(v) => turso::Value::Integer(v), None => turso::Value::Null },
                     match c_wid { Some(v) => turso::Value::Integer(v), None => turso::Value::Null },
                     turso::Value::Text(payload),
                     turso::Value::Text(enq),
                     match vt { Some(v) => turso::Value::Text(v), None => turso::Value::Null },
                     turso::Value::Integer(read_ct as i64),
                     match deq { Some(v) => turso::Value::Text(v), None => turso::Value::Null },
                     turso::Value::Text(now_str)
                 ]
             ).await;

            if let Err(e) = res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(crate::error::Error::QueryFailed {
                    query: "DLQ_Insert".into(),
                    source: Box::new(e),
                    context: "DLQ insert".into(),
                });
            }

            // 3. Delete
            let res = conn
                .execute(
                    "DELETE FROM pgqrs_messages WHERE id = ?",
                    vec![turso::Value::Integer(id)],
                )
                .await;
            if let Err(e) = res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(crate::error::Error::QueryFailed {
                    query: "DLQ_Delete".into(),
                    source: Box::new(e),
                    context: "DLQ delete".into(),
                });
            }
            moved_ids.push(id);
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "DLQ".into(),
            })?;

        Ok(moved_ids)
    }

    async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        let queue = self.queues.get_by_name(name).await?;
        let row = crate::store::turso::query(GET_QUEUE_METRICS)
            .bind(queue.id)
            .fetch_one(&self.db)
            .await?;
        Self::map_queue_metrics_row(&row)
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let rows = crate::store::turso::query(GET_ALL_QUEUES_METRICS)
            .fetch_all(&self.db)
            .await?;

        let mut metrics = Vec::new();
        for row in rows {
            metrics.push(Self::map_queue_metrics_row(&row)?);
        }
        Ok(metrics)
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        let row = crate::store::turso::query(GET_SYSTEM_STATS)
            .fetch_one(&self.db)
            .await?;
        Self::map_system_stats_row(&row)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let seconds = heartbeat_timeout.num_seconds();
        let query_str = if group_by_queue {
            GET_WORKER_HEALTH_BY_QUEUE
        } else {
            GET_WORKER_HEALTH_GLOBAL
        };

        let rows = crate::store::turso::query(query_str)
            .bind(seconds)
            .fetch_all(&self.db)
            .await?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(Self::map_worker_health_row(&row)?);
        }
        Ok(stats)
    }

    async fn worker_stats(&self, queue_name: &str) -> Result<WorkerStats> {
        // Reuse similar logic to SQLite but using Turso methods
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
            let matches = self.get_worker_messages(worker.id).await?;
            total_messages += matches.len() as u64;
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

        Ok(WorkerStats {
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
        // Check refs first
        let msg_refs: i64 = crate::store::turso::query_scalar(CHECK_WORKER_REFS_MESSAGES)
            .bind(worker_id)
            .bind(worker_id)
            .fetch_one(&self.db)
            .await?;

        let arch_refs: i64 = crate::store::turso::query_scalar(CHECK_WORKER_REFS_ARCHIVE)
            .bind(worker_id)
            .bind(worker_id)
            .fetch_one(&self.db)
            .await?;

        let refs = msg_refs + arch_refs;

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

        let rows = crate::store::turso::query(GET_WORKER_MESSAGES)
            .bind(worker_id)
            .fetch_all(&self.db)
            .await?;

        let mut msgs = Vec::new();
        // Since we don't have public map_row for TursoMessageTable, we must map manually here
        // or expose it. Copying consumer.rs mapping logic essentially.
        // Columns: id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
        for row in rows {
            let id: i64 = row.get(0)?;
            let queue_id: i64 = row.get(1)?;
            let producer_worker_id: Option<i64> = row.get(2)?;
            let consumer_worker_id: Option<i64> = row.get(3)?;
            let payload_str: String = row.get(4)?;
            let payload: serde_json::Value =
                serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null);

            let vt_str: Option<String> = row.get(5)?;
            let vt = match vt_str {
                Some(s) => Some(parse_turso_timestamp(&s)?),
                None => None,
            };

            let enqueued_at_str: String = row.get(6)?;
            let enqueued_at = parse_turso_timestamp(&enqueued_at_str)?;

            let read_ct: i32 = row.get(7)?;

            let dequeued_at_str: Option<String> = row.get(8)?;

            let dequeued_at = match dequeued_at_str {
                Some(s) => Some(parse_turso_timestamp(&s)?),
                None => None,
            };

            msgs.push(QueueMessage {
                id,
                queue_id,
                producer_worker_id,
                consumer_worker_id,
                payload,
                vt: vt.expect("Worker message must have vt set"),
                enqueued_at,
                read_ct,
                dequeued_at,
            });
        }
        Ok(msgs)
    }

    async fn reclaim_messages(&self, queue_id: i64, older_than: Option<Duration>) -> Result<u64> {
        let timeout = older_than
            .unwrap_or_else(|| chrono::Duration::seconds(self.config.heartbeat_interval as i64));

        // Find zombies using pool (outside tx for read, then tx for update)
        // Actually we need to do this atomically or at least safely.
        // Reading zombies then updating them is fine if status hasn't changed.
        // TursoWorkerTable.list_zombies_for_queue exists.

        let zombies = self
            .workers
            .list_zombies_for_queue(queue_id, timeout)
            .await?;
        if zombies.is_empty() {
            return Ok(0);
        }

        // Now reclaim
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Reclaim".into(),
            })?;

        let mut total = 0;
        for zombie in zombies {
            // UPDATE messages
            let res = conn.execute(RELEASE_ZOMBIE_MESSAGES, (zombie.id,)).await;

            match res {
                Ok(count) => total += count,
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(crate::error::Error::QueryFailed {
                        query: "RELEASE_ZOMBIES".into(),
                        source: Box::new(e),
                        context: "Reclaim".into(),
                    });
                }
            }

            // UPDATE worker status
            let res = conn.execute(SHUTDOWN_ZOMBIE_WORKER, (zombie.id,)).await;

            if let Err(e) = res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(crate::error::Error::QueryFailed {
                    query: "SHUTDOWN_ZOMBIE".into(),
                    source: Box::new(e),
                    context: "Reclaim".into(),
                });
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Reclaim".into(),
            })?;

        Ok(total)
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let seconds = older_than.num_seconds();

        // 1. Find candidates
        let rows = crate::store::turso::query(FIND_OLD_WORKERS_TO_PURGE)
            .bind(seconds)
            .fetch_all(&self.db)
            .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut ids: Vec<i64> = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(row.get(0)?);
        }

        // 2. Delete them
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Purge Workers".into(),
            })?;

        for id in &ids {
            let res = conn
                .execute(
                    "DELETE FROM pgqrs_workers WHERE id = ?",
                    vec![turso::Value::Integer(*id)],
                )
                .await;
            if let Err(e) = res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(crate::error::Error::QueryFailed {
                    query: "DELETE_WORKER".into(),
                    source: Box::new(e),
                    context: "Purge Workers Loop".into(),
                });
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Purge Workers".into(),
            })?;

        Ok(ids.len() as u64)
    }

    async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        let count = crate::store::turso::query(RELEASE_WORKER_MESSAGES)
            .bind(worker_id)
            .execute(&self.db)
            .await?;
        Ok(count as u64)
    }
}
