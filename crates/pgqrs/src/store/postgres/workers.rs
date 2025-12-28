use crate::error::{Error, Result};
use crate::store::WorkerTable;
use crate::types::{WorkerInfo, WorkerStatus};
use chrono::{Duration, Utc};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresWorkerTable {
    pool: PgPool,
}

impl PostgresWorkerTable {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// SQL Constants from pgqrs_workers.rs
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

// SQL Constants from lifecycle.rs
const CHECK_WORKER_EXISTS: &str = r#"
    SELECT id, status
    FROM pgqrs_workers
    WHERE hostname = $1 AND port = $2 AND (queue_id = $3 OR (queue_id IS NULL AND $3 IS NULL))
"#;

const RESET_STOPPED_WORKER: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready', started_at = $2, heartbeat_at = $2, shutdown_at = NULL
    WHERE id = $1
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
"#;

const UPDATE_HEARTBEAT: &str = r#"
    UPDATE pgqrs_workers
    SET heartbeat_at = NOW()
    WHERE id = $1
"#;

const TRANSITION_READY_TO_SUSPENDED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'suspended'
    WHERE id = $1 AND status = 'ready'
    RETURNING id
"#;

const TRANSITION_SUSPENDED_TO_READY: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'ready'
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

const TRANSITION_SUSPENDED_TO_STOPPED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped', shutdown_at = NOW()
    WHERE id = $1 AND status = 'suspended'
    RETURNING id
"#;

#[async_trait::async_trait]
impl WorkerTable for PostgresWorkerTable {
    // === CRUD Operations ===

    async fn insert(&self, data: crate::tables::NewWorker) -> Result<WorkerInfo> {
        // Use register method which handles the logic
        self.register(data.queue_id, &data.hostname, data.port).await.map_err(Into::into)
    }

    async fn get(&self, id: i64) -> Result<WorkerInfo> {
        sqlx::query_as::<_, WorkerInfo>(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await.map_err(Into::into)
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        sqlx::query_as::<_, WorkerInfo>(
            "SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers ORDER BY id DESC"
        )
        .fetch_all(&self.pool)
        .await.map_err(Into::into)
    }

    async fn count(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workers")
            .fetch_one(&self.pool)
            .await.map_err(Into::into)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM pgqrs_workers WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    // === Worker-Specific Business Operations ===

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo> {
        let now = Utc::now();

        // Check if worker exists
        let existing = sqlx::query_as::<_, (i64, WorkerStatus)>(CHECK_WORKER_EXISTS)
            .bind(hostname)
            .bind(port)
            .bind(queue_id)
            .fetch_optional(&self.pool)
            .await?;

        if let Some((id, status)) = existing {
            match status {
                WorkerStatus::Stopped => {
                    // Reuse existing record
                    let worker = sqlx::query_as::<_, WorkerInfo>(RESET_STOPPED_WORKER)
                        .bind(id)
                        .bind(now)
                        .fetch_one(&self.pool)
                        .await?;
                    Ok(worker)
                }
                WorkerStatus::Suspended => Err(Error::ValidationFailed {
                    reason: format!("Worker {}:{}:{:?} is suspended", hostname, port, queue_id),
                }),
                WorkerStatus::Ready => Err(Error::ValidationFailed {
                    reason: format!("Worker {}:{}:{:?} is already running", hostname, port, queue_id),
                }),
            }
        } else {
            // Create new
            let worker = sqlx::query_as::<_, WorkerInfo>(INSERT_WORKER)
                .bind(hostname)
                .bind(port)
                .bind(queue_id)
                .bind(now)
                .bind(now)
                .bind(WorkerStatus::Ready)
                .fetch_one(&self.pool)
                .await?;
            Ok(worker)
        }
    }

    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        let status: WorkerStatus = sqlx::query_scalar("SELECT status FROM pgqrs_workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(status)
    }

    async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(UPDATE_HEARTBEAT)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(Error::WorkerNotFound { id: worker_id });
        }
        Ok(())
    }

    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool> {
        // Implementation from lifecycle.rs
        let row: Option<chrono::DateTime<Utc>> = sqlx::query_scalar("SELECT heartbeat_at FROM pgqrs_workers WHERE id = $1")
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(last_heartbeat) => {
                let age = Utc::now() - last_heartbeat;
                Ok(age <= max_age)
            }
            None => Ok(false),
        }
    }

    async fn suspend(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(TRANSITION_READY_TO_SUSPENDED)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            // Either doesn't exist or not in Ready state
            return Err(Error::InvalidStateTransition {
                from: "unknown".to_string(),
                to: "suspended".to_string(),
                reason: "Worker not found or not in Ready state".to_string(),
            });
        }
        Ok(())
    }

    async fn resume(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(TRANSITION_SUSPENDED_TO_READY)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
             return Err(Error::InvalidStateTransition {
                from: "unknown".to_string(),
                to: "ready".to_string(),
                reason: "Worker not found or not in Suspended state".to_string(),
            });
        }
        Ok(())
    }

    async fn shutdown(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(TRANSITION_SUSPENDED_TO_STOPPED)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
             return Err(Error::InvalidStateTransition {
                from: "unknown".to_string(),
                to: "stopped".to_string(),
                reason: "Worker not found or not in Suspended state".to_string(),
            });
        }
        Ok(())
    }

    async fn health_stats(&self, _timeout: Duration) -> Result<crate::types::WorkerHealthStats> {
        // TODO: Implement health stats calculation
        todo!("Implement health_stats()")
    }

    async fn count_pending_messages(&self, worker_id: i64) -> Result<i64> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id = $1 AND vt > NOW()"
        )
        .bind(worker_id)
        .fetch_one(&self.pool)
        .await.map_err(Into::into)
    }
}
