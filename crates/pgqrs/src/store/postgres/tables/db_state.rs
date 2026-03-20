use crate::error::Result;
use crate::stats::{QueueMetrics, SystemStats, WorkerHealthStats};
use crate::store::dialect::SqlDialect;
use crate::store::postgres::dialect::PostgresDialect;
use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct DbState {
    pool: PgPool,
}

impl DbState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::DbStateTable for DbState {
    async fn verify(&self) -> Result<()> {
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists: bool =
                sqlx::query_scalar(PostgresDialect::DB_STATE.check_table_exists)
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
            sqlx::query_scalar(PostgresDialect::DB_STATE.check_orphaned_messages)
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
            sqlx::query_scalar(PostgresDialect::DB_STATE.check_orphaned_message_workers)
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
        sqlx::query(PostgresDialect::DB_STATE.purge_queue_messages)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(PostgresDialect::DB_STATE.purge_queue_workers)
            .bind(queue_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn queue_metrics(&self, queue_id: i64) -> Result<QueueMetrics> {
        sqlx::query_as(PostgresDialect::DB_STATE.queue_metrics)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        sqlx::query_as(PostgresDialect::DB_STATE.all_queues_metrics)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        sqlx::query_as(PostgresDialect::DB_STATE.system_stats)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        let threshold = Utc::now() - heartbeat_timeout;
        let query = if group_by_queue {
            PostgresDialect::DB_STATE.worker_health_by_queue
        } else {
            PostgresDialect::DB_STATE.worker_health_global
        };
        sqlx::query_as(query)
            .bind(threshold)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let threshold = Utc::now() - older_than;
        let result = sqlx::query(PostgresDialect::DB_STATE.purge_old_workers)
            .bind(threshold)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
