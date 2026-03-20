use crate::error::Result;
use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats};
use crate::store::dialect::SqlDialect;
use crate::store::turso::dialect::TursoDialect;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoDbState {
    db: Arc<Database>,
}

impl TursoDbState {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_queue_metrics_row(row: &turso::Row) -> Result<QueueMetrics> {
        Ok(QueueMetrics {
            name: row.get(0)?,
            total_messages: row.get(1)?,
            pending_messages: row.get(2)?,
            locked_messages: row.get(3)?,
            archived_messages: row.get(4)?,
            oldest_pending_message: row
                .get::<Option<String>>(5)?
                .map(|s| parse_turso_timestamp(&s))
                .transpose()?,
            newest_message: row
                .get::<Option<String>>(6)?
                .map(|s| parse_turso_timestamp(&s))
                .transpose()?,
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
            schema_version: row.get(7)?,
        })
    }

    fn map_worker_health_row(row: &turso::Row) -> Result<WorkerHealthStats> {
        Ok(WorkerHealthStats {
            queue_name: row.get(0)?,
            total_workers: row.get(1)?,
            ready_workers: row.get(2)?,
            polling_workers: row.get(3)?,
            interrupted_workers: row.get(4)?,
            suspended_workers: row.get(5)?,
            stopped_workers: row.get(6)?,
            stale_workers: row.get(7)?,
        })
    }
}

#[async_trait]
impl crate::store::DbStateTable for TursoDbState {
    async fn verify(&self) -> Result<()> {
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists: bool =
                crate::store::turso::query_scalar(TursoDialect::DB_STATE.check_table_exists)
                    .bind(*table_name)
                    .fetch_one(&self.db)
                    .await?;

            if !table_exists {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        let orphaned_messages: i64 =
            crate::store::turso::query_scalar(TursoDialect::DB_STATE.check_orphaned_messages)
                .fetch_one(&self.db)
                .await?;
        if orphaned_messages > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Found {} messages with invalid queue_id references",
                    orphaned_messages
                ),
            });
        }

        let orphaned_message_workers: i64 = crate::store::turso::query_scalar(
            TursoDialect::DB_STATE.check_orphaned_message_workers,
        )
        .fetch_one(&self.db)
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
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;
        conn.execute("BEGIN", ()).await?;
        if let Err(e) = crate::store::turso::query(TursoDialect::DB_STATE.purge_queue_messages)
            .bind(queue_id)
            .execute_once_on_connection(&conn)
            .await
        {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e);
        }
        if let Err(e) = crate::store::turso::query(TursoDialect::DB_STATE.purge_queue_workers)
            .bind(queue_id)
            .execute_once_on_connection(&conn)
            .await
        {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e);
        }
        conn.execute("COMMIT", ()).await?;
        Ok(())
    }

    async fn queue_metrics(&self, queue_id: i64) -> Result<QueueMetrics> {
        let row = crate::store::turso::query(TursoDialect::DB_STATE.queue_metrics)
            .bind(queue_id)
            .fetch_one(&self.db)
            .await?;
        Self::map_queue_metrics_row(&row)
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let rows = crate::store::turso::query(TursoDialect::DB_STATE.all_queues_metrics)
            .fetch_all(&self.db)
            .await?;
        let mut metrics = Vec::with_capacity(rows.len());
        for row in rows {
            metrics.push(Self::map_queue_metrics_row(&row)?);
        }
        Ok(metrics)
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        let row = crate::store::turso::query(TursoDialect::DB_STATE.system_stats)
            .fetch_one(&self.db)
            .await?;
        Self::map_system_stats_row(&row)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let threshold = Utc::now() - heartbeat_timeout;
        let query = if group_by_queue {
            TursoDialect::DB_STATE.worker_health_by_queue
        } else {
            TursoDialect::DB_STATE.worker_health_global
        };
        let rows = crate::store::turso::query(query)
            .bind(format_turso_timestamp(&threshold))
            .fetch_all(&self.db)
            .await?;
        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            stats.push(Self::map_worker_health_row(&row)?);
        }
        Ok(stats)
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let threshold = Utc::now() - older_than;
        let rows = crate::store::turso::query(TursoDialect::DB_STATE.purge_old_workers)
            .bind(format_turso_timestamp(&threshold))
            .fetch_all(&self.db)
            .await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(row.get::<i64>(0)?);
        }
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
                context: "Turso purge old workers".into(),
            })?;
        for id in &ids {
            if let Err(e) = conn
                .execute(
                    "DELETE FROM pgqrs_workers WHERE id = ?",
                    vec![turso::Value::Integer(*id)],
                )
                .await
            {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(crate::error::Error::QueryFailed {
                    query: "DELETE_OLD_WORKER".into(),
                    source: Box::new(e),
                    context: "Turso purge old workers".into(),
                });
            }
        }
        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Turso purge old workers".into(),
            })?;
        Ok(ids.len() as u64)
    }
}
