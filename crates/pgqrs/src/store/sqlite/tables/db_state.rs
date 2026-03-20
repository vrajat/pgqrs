use crate::error::Result;
use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats};
use crate::store::dialect::SqlDialect;
use crate::store::sqlite::dialect::SqliteDialect;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone)]
pub struct SqliteDbState {
    pool: SqlitePool,
}

impl SqliteDbState {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn map_queue_metrics_row(row: SqliteRow) -> Result<QueueMetrics> {
    let oldest_pending_message = row
        .try_get::<Option<String>, _>("oldest_pending_message")?
        .map(|s| parse_sqlite_timestamp(&s))
        .transpose()?;
    let newest_message = row
        .try_get::<Option<String>, _>("newest_message")?
        .map(|s| parse_sqlite_timestamp(&s))
        .transpose()?;

    Ok(QueueMetrics {
        name: row.try_get("name")?,
        total_messages: row.try_get("total_messages")?,
        pending_messages: row.try_get("pending_messages")?,
        locked_messages: row.try_get("locked_messages")?,
        archived_messages: row.try_get("archived_messages")?,
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
        polling_workers: row.try_get("polling_workers")?,
        interrupted_workers: row.try_get("interrupted_workers")?,
        suspended_workers: row.try_get("suspended_workers")?,
        stopped_workers: row.try_get("stopped_workers")?,
        stale_workers: row.try_get("stale_workers")?,
    })
}

#[async_trait]
impl crate::store::DbStateTable for SqliteDbState {
    async fn verify(&self) -> Result<()> {
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists: bool = sqlx::query_scalar(SqliteDialect::DB_STATE.check_table_exists)
                .bind(table_name)
                .fetch_one(&self.pool)
                .await?;

            if !table_exists {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        let orphaned_messages: i64 =
            sqlx::query_scalar(SqliteDialect::DB_STATE.check_orphaned_messages)
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

        let orphaned_message_workers: i64 =
            sqlx::query_scalar(SqliteDialect::DB_STATE.check_orphaned_message_workers)
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

    async fn purge_queue(&self, queue_id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(SqliteDialect::DB_STATE.purge_queue_messages)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(SqliteDialect::DB_STATE.purge_queue_workers)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn queue_metrics(&self, queue_id: i64) -> Result<QueueMetrics> {
        let row = sqlx::query(SqliteDialect::DB_STATE.queue_metrics)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await?;
        map_queue_metrics_row(row)
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let rows = sqlx::query(SqliteDialect::DB_STATE.all_queues_metrics)
            .fetch_all(&self.pool)
            .await?;
        let mut metrics = Vec::with_capacity(rows.len());
        for row in rows {
            metrics.push(map_queue_metrics_row(row)?);
        }
        Ok(metrics)
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        let row = sqlx::query(SqliteDialect::DB_STATE.system_stats)
            .fetch_one(&self.pool)
            .await?;
        map_system_stats_row(row)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let threshold = Utc::now() - heartbeat_timeout;
        let query = if group_by_queue {
            SqliteDialect::DB_STATE.worker_health_by_queue
        } else {
            SqliteDialect::DB_STATE.worker_health_global
        };
        let rows = sqlx::query(query)
            .bind(format_sqlite_timestamp(&threshold))
            .fetch_all(&self.pool)
            .await?;
        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            stats.push(map_worker_health_row(row)?);
        }
        Ok(stats)
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let threshold = Utc::now() - older_than;
        let result = sqlx::query(SqliteDialect::DB_STATE.purge_old_workers)
            .bind(format_sqlite_timestamp(&threshold))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
