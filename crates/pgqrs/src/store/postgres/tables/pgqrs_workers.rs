//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`Workers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table.

use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::postgres::dialect::PostgresDialect;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::tables::DialectWorkerTable;
use crate::types::{WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{PgPool, Postgres};

// SQL constants for worker table operations
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (name, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    ORDER BY started_at DESC
"#;

const DELETE_WORKER_BY_ID: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE id = $1
"#;

const DELETE_WORKERS_BY_QUEUE: &str = r#"
    DELETE FROM pgqrs_workers WHERE queue_id = $1
"#;

const COUNT_WORKERS_BY_QUEUE_TX: &str = r#"
    SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = $1
"#;

const LIST_WORKERS_BY_QUEUE_AND_STATE: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1 AND status = $2
    ORDER BY started_at DESC
"#;

const LIST_ZOMBIE_WORKERS: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    AND status IN ('ready', 'polling', 'suspended', 'interrupted')
    AND heartbeat_at < NOW() - $2
    ORDER BY heartbeat_at ASC
"#;

/// SQL to find existing worker by name
const FIND_WORKER_BY_NAME: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE name = $1
"#;

/// SQL to reset a stopped worker back to ready state
const RESET_WORKER_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready', queue_id = $2, started_at = NOW(), heartbeat_at = NOW(), shutdown_at = NULL
    WHERE id = $1 AND status = 'stopped'
    RETURNING id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

/// SQL to insert a new ephemeral worker
const INSERT_EPHEMERAL_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (name, queue_id, status)
    VALUES ($1, $2, 'ready')
    RETURNING id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

/// Workers table CRUD operations for pgqrs.
///
/// Provides pure CRUD operations on the `pgqrs_workers` table.
#[derive(Debug, Clone)]
pub struct Workers {
    pub pool: PgPool,
}

impl Workers {
    /// Create a new Workers instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_WORKERS_BY_QUEUE_TX)
            .bind(foreign_key_value)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKERS_BY_QUEUE_TX".into(),
                source: Box::new(e),
                context: format!("Failed to count workers for queue {}", foreign_key_value),
            })?;
        Ok(count)
    }

    pub async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKERS_BY_QUEUE)
            .bind(foreign_key_value)
            .execute(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_WORKERS_BY_QUEUE".into(),
                source: Box::new(e),
                context: format!("Failed to delete workers for queue {}", foreign_key_value),
            })?;
        Ok(result.rows_affected())
    }

    pub async fn list_zombies_for_queue_tx<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<Vec<WorkerRecord>> {
        let workers = sqlx::query_as::<_, WorkerRecord>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ZOMBIE_WORKERS".into(),
                source: Box::new(e),
                context: format!("Failed to list zombie workers for queue {}", queue_id),
            })?;
        Ok(workers)
    }

    /// Get the current status of a worker.
    pub async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        const GET_WORKER_STATUS: &str = "SELECT status FROM pgqrs_workers WHERE id = $1";
        let status: WorkerStatus = sqlx::query_scalar(GET_WORKER_STATUS)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_WORKER_STATUS".into(),
                source: Box::new(e),
                context: format!("Failed to get worker {} status", worker_id),
            })?;

        Ok(status)
    }

    /// Check if this worker is healthy based on heartbeat age
    pub async fn is_healthy(&self, worker_id: i64, max_age: chrono::Duration) -> Result<bool> {
        let threshold = Utc::now() - max_age;

        // Query returns true if heartbeat_at >= threshold (i.e., within max_age)
        let is_healthy: bool =
            sqlx::query_scalar("SELECT heartbeat_at >= $2 FROM pgqrs_workers WHERE id = $1")
                .bind(worker_id)
                .bind(threshold)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "CHECK_WORKER_HEALTH".into(),
                    source: Box::new(e),
                    context: format!("Failed to check health for worker {}", worker_id),
                })?;

        Ok(is_healthy)
    }

    fn bind_query<'a>(
        mut builder: sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments>,
        query: &'a QueryBuilder,
    ) -> sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments> {
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }
        builder
    }

    fn bind_returning_query<'a>(
        mut builder: sqlx::query::QueryScalar<'a, Postgres, i64, sqlx::postgres::PgArguments>,
        query: &'a QueryBuilder,
    ) -> sqlx::query::QueryScalar<'a, Postgres, i64, sqlx::postgres::PgArguments> {
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }
        builder
    }
}

// Implement the public WorkerTable trait by delegating to inherent methods
#[async_trait]
impl crate::store::WorkerTable for Workers {
    async fn insert(&self, data: crate::types::NewWorkerRecord) -> Result<WorkerRecord> {
        let now = Utc::now();

        let worker_id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.name)
            .bind(data.queue_id)
            .bind(now)
            .bind(now)
            .bind(WorkerStatus::Ready)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_WORKER".into(),
                source: Box::new(e),
                context: format!("Failed to insert worker {}", data.name),
            })?;

        Ok(WorkerRecord {
            id: worker_id,
            name: data.name,
            queue_id: data.queue_id,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    async fn get(&self, id: i64) -> Result<WorkerRecord> {
        let worker = sqlx::query_as::<_, WorkerRecord>(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_WORKER_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to get worker {}", id),
            })?;

        Ok(worker)
    }

    async fn list(&self) -> Result<Vec<WorkerRecord>> {
        let workers = sqlx::query_as::<_, WorkerRecord>(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_WORKERS".into(),
                source: Box::new(e),
                context: "Failed to list all workers".into(),
            })?;

        Ok(workers)
    }

    async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_workers";
        let row = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SELECT COUNT(*) FROM pgqrs_workers".into(),
                source: Box::new(e),
                context: "Failed to count workers".into(),
            })?;
        Ok(row)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKER_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete worker {}", id),
            })?;

        Ok(result.rows_affected())
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerRecord>> {
        let workers = sqlx::query_as::<_, WorkerRecord>(LIST_WORKERS_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_WORKERS_BY_QUEUE (queue_id={})", queue_id),
                source: Box::new(e),
                context: format!("Failed to filter workers by queue ID {}", queue_id),
            })?;
        Ok(workers)
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(COUNT_WORKERS_BY_QUEUE_TX)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKERS_BY_QUEUE".into(),
                source: Box::new(e),
                context: format!("Failed to count workers for queue {}", queue_id),
            })?;
        Ok(count)
    }

    async fn mark_stopped(&self, id: i64) -> Result<()> {
        sqlx::query(PostgresDialect::WORKER.mark_stopped)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "MARK_WORKER_STOPPED".into(),
                source: Box::new(e),
                context: format!("Failed to mark worker {} as stopped", id),
            })?;
        Ok(())
    }

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<i64> {
        const COUNT_WORKERS_BY_STATE: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_id = $1 AND status = $2
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_WORKERS_BY_STATE)
            .bind(queue_id)
            .bind(&state)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!(
                    "COUNT_WORKERS_BY_STATE (queue_id={}, state={:?})",
                    queue_id, state
                ),
                source: Box::new(e),
                context: format!(
                    "Failed to count workers for queue {} with state {:?}",
                    queue_id, state
                ),
            })?;

        Ok(count)
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        const COUNT_ZOMBIE_WORKERS: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_id = $1
            AND status IN ('ready', 'polling', 'suspended', 'interrupted')
            AND heartbeat_at < NOW() - $2
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("COUNT_ZOMBIE_WORKERS (queue_id={})", queue_id),
                source: Box::new(e),
                context: format!("Failed to count zombie workers for queue {}", queue_id),
            })?;

        Ok(count)
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerRecord>> {
        let workers = sqlx::query_as::<_, WorkerRecord>(LIST_WORKERS_BY_QUEUE_AND_STATE)
            .bind(queue_id)
            .bind(&state)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!(
                    "LIST_WORKERS_BY_QUEUE_AND_STATE (queue_id={}, state={:?})",
                    queue_id, state
                ),
                source: Box::new(e),
                context: format!(
                    "Failed to list workers for queue {} with state {:?}",
                    queue_id, state
                ),
            })?;

        Ok(workers)
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerRecord>> {
        let workers = sqlx::query_as::<_, WorkerRecord>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_ZOMBIE_WORKERS (queue_id={})", queue_id),
                source: Box::new(e),
                context: format!("Failed to list zombie workers for queue {}", queue_id),
            })?;
        Ok(workers)
    }

    async fn register(&self, queue_id: Option<i64>, name: &str) -> Result<WorkerRecord> {
        let existing_worker: Option<WorkerRecord> = sqlx::query_as(FIND_WORKER_BY_NAME)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("FIND_WORKER_BY_NAME ({})", name),
                source: Box::new(e),
                context: format!("Failed to find worker {}", name),
            })?;

        let worker_info = match existing_worker {
            Some(worker) => {
                match worker.status {
                    WorkerStatus::Stopped => {
                        // Reset stopped worker to ready
                        sqlx::query_as::<_, WorkerRecord>(RESET_WORKER_TO_READY)
                            .bind(worker.id)
                            .bind(queue_id)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|e| crate::error::Error::QueryFailed {
                                query: format!("RESET_WORKER_TO_READY ({})", worker.id),
                                source: Box::new(e),
                                context: format!("Failed to reset worker {}", name),
                            })?
                    }
                    WorkerStatus::Ready => {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {} is already active. Cannot register duplicate.",
                                name
                            ),
                        });
                    }
                    WorkerStatus::Suspended => {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {} is suspended. Use resume() to reactivate.",
                                name
                            ),
                        });
                    }
                    WorkerStatus::Polling | WorkerStatus::Interrupted => {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Worker {} is already active. Cannot register duplicate.",
                                name
                            ),
                        });
                    }
                }
            }
            None => {
                // Create new worker
                let now = Utc::now();
                let inserted_id: i64 = sqlx::query_scalar(INSERT_WORKER)
                    .bind(name)
                    .bind(queue_id)
                    .bind(now)
                    .bind(now)
                    .bind(WorkerStatus::Ready)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "INSERT_WORKER".into(),
                        source: Box::new(e),
                        context: format!("Failed to insert new worker {}", name),
                    })?;

                WorkerRecord {
                    id: inserted_id,
                    name: name.to_string(),
                    queue_id,
                    started_at: now,
                    heartbeat_at: now,
                    shutdown_at: None,
                    status: WorkerStatus::Ready,
                }
            }
        };
        Ok(worker_info)
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerRecord> {
        let name = format!("__ephemeral__{}", uuid::Uuid::new_v4());

        let worker_info = sqlx::query_as::<_, WorkerRecord>(INSERT_EPHEMERAL_WORKER)
            .bind(&name)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_EPHEMERAL_WORKER".into(),
                source: Box::new(e),
                context: "Failed to create ephemeral worker".into(),
            })?;

        Ok(worker_info)
    }

    async fn get_status(&self, id: i64) -> Result<WorkerStatus> {
        self.get_status(id).await
    }

    async fn suspend(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_suspend(self, id).await
    }

    async fn resume(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_resume(self, id).await
    }

    async fn complete_poll(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_complete_poll(self, id).await
    }

    async fn shutdown(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_shutdown(self, id).await
    }

    async fn poll(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_poll(self, id).await
    }

    async fn interrupt(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_interrupt(self, id).await
    }

    async fn heartbeat(&self, id: i64) -> Result<()> {
        <Self as DialectWorkerTable>::dialect_heartbeat(self, id).await
    }

    async fn is_healthy(&self, id: i64, max_age: chrono::Duration) -> Result<bool> {
        self.is_healthy(id, max_age).await
    }
}

#[async_trait]
impl DialectWorkerTable for Workers {
    type Dialect = PostgresDialect;

    async fn execute_worker_update(&self, query: QueryBuilder) -> Result<u64> {
        if query.sql().contains("RETURNING") {
            let maybe_id = Self::bind_returning_query(sqlx::query_scalar(query.sql()), &query)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "POSTGRES_WORKER_UPDATE_RETURNING".into(),
                    source: Box::new(e),
                    context: "Failed to execute postgres worker returning update".into(),
                })?;

            return Ok(u64::from(maybe_id.is_some()));
        }

        let result = Self::bind_query(sqlx::query(query.sql()), &query)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "POSTGRES_WORKER_UPDATE".into(),
                source: Box::new(e),
                context: "Failed to execute postgres worker update".into(),
            })?;

        Ok(result.rows_affected())
    }

    async fn query_worker_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        self.get_status(worker_id).await
    }
}
