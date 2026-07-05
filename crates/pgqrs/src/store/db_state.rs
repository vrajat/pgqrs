use crate::error::Result;
use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats};
use chrono::Utc;
use sqlx::PgPool;

const CHECK_TABLE_EXISTS: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = $1
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

const PURGE_QUEUE_MESSAGES: &str = r#"
    DELETE FROM pgqrs_messages WHERE queue_id = $1
"#;

const PURGE_QUEUE_WORKERS: &str = r#"
    DELETE FROM pgqrs_workers WHERE queue_id = $1
"#;

const QUEUE_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as pending_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL) as locked_messages,
        COUNT(m.id) FILTER (WHERE m.archived_at IS NOT NULL) as archived_messages,
        MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    WHERE q.id = $1
    GROUP BY q.id, q.queue_name
"#;

const ALL_QUEUES_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as pending_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL) as locked_messages,
        COUNT(m.id) FILTER (WHERE m.archived_at IS NOT NULL) as archived_messages,
        MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    GROUP BY q.id, q.queue_name
"#;

const SYSTEM_STATS: &str = r#"
    SELECT
        (SELECT COUNT(*) FROM pgqrs_queues) as total_queues,
        (SELECT COUNT(*) FROM pgqrs_workers) as total_workers,
        (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready') as active_workers,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL) as total_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL AND archived_at IS NULL) as pending_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL AND archived_at IS NULL) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NOT NULL) as archived_messages,
        '0.5.0' as schema_version
"#;

const WORKER_HEALTH_GLOBAL: &str = r#"
    SELECT
        'Global' as queue_name,
        COUNT(*) as total_workers,
        COUNT(*) FILTER (WHERE status = 'ready') as ready_workers,
        COUNT(*) FILTER (WHERE status = 'polling') as polling_workers,
        COUNT(*) FILTER (WHERE status = 'interrupted') as interrupted_workers,
        COUNT(*) FILTER (WHERE status = 'suspended') as suspended_workers,
        COUNT(*) FILTER (WHERE status = 'stopped') as stopped_workers,
        COUNT(*) FILTER (WHERE status IN ('ready', 'polling') AND heartbeat_at < $1) as stale_workers
    FROM pgqrs_workers
"#;

const WORKER_HEALTH_BY_QUEUE: &str = r#"
    SELECT
        COALESCE(q.queue_name, 'Admin') as queue_name,
        COUNT(w.id) as total_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'ready') as ready_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'polling') as polling_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'interrupted') as interrupted_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'suspended') as suspended_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'stopped') as stopped_workers,
        COUNT(w.id) FILTER (WHERE w.status IN ('ready', 'polling') AND w.heartbeat_at < $1) as stale_workers
    FROM pgqrs_workers w
    LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
    GROUP BY q.queue_name
"#;

const PURGE_OLD_WORKERS: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE status = 'stopped'
      AND heartbeat_at < $1
      AND id NOT IN (
          SELECT DISTINCT worker_id
          FROM (
              SELECT producer_worker_id as worker_id FROM pgqrs_messages WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL
          ) refs
      )
"#;

#[derive(Debug, Clone)]
pub struct DbState {
    pool: PgPool,
}

impl DbState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl DbState {
    pub async fn verify(&self) -> Result<()> {
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists: bool = sqlx::query_scalar(CHECK_TABLE_EXISTS)
                .bind(table_name)
                .fetch_one(&self.pool)
                .await?;

            if !table_exists {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        let orphaned_messages: i64 = sqlx::query_scalar(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&self.pool)
            .await?;
        if orphaned_messages > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Found {} messages with invalid queue_id references",
                    orphaned_messages
                ),
            });
        }

        let orphaned_message_workers: i64 = sqlx::query_scalar(CHECK_ORPHANED_MESSAGE_WORKERS)
            .fetch_one(&self.pool)
            .await?;
        if orphaned_message_workers > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Found {} messages with invalid worker refs",
                    orphaned_message_workers
                ),
            });
        }

        Ok(())
    }

    pub async fn purge_queue(&self, queue_id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(PURGE_QUEUE_MESSAGES)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(PURGE_QUEUE_WORKERS)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn queue_metrics(&self, queue_id: i64) -> Result<QueueMetrics> {
        sqlx::query_as(QUEUE_METRICS)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        sqlx::query_as(ALL_QUEUES_METRICS)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn system_stats(&self) -> Result<SystemStats> {
        sqlx::query_as(SYSTEM_STATS)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let threshold = Utc::now() - heartbeat_timeout;
        let query = if group_by_queue {
            WORKER_HEALTH_BY_QUEUE
        } else {
            WORKER_HEALTH_GLOBAL
        };
        sqlx::query_as(query)
            .bind(threshold)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let threshold = Utc::now() - older_than;
        let result = sqlx::query(PURGE_OLD_WORKERS)
            .bind(threshold)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
