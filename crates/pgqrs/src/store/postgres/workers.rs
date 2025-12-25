use crate::store::WorkerStore;
use crate::types::{WorkerInfo, WorkerStatus};
use chrono::{Duration, Utc};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresWorkerStore {
    pool: PgPool,
}

impl PostgresWorkerStore {
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
impl WorkerStore for PostgresWorkerStore {
    type Error = sqlx::Error;

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo, Self::Error> {
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
                WorkerStatus::Suspended => {
                     return Err(sqlx::Error::Protocol(format!("Worker {}:{}:{:?} is suspended", hostname, port, queue_id)));
                }
                WorkerStatus::Ready => {
                     return Err(sqlx::Error::Protocol(format!("Worker {}:{}:{:?} is already running", hostname, port, queue_id)));
                }
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

    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus, Self::Error> {
        let status: WorkerStatus = sqlx::query_scalar("SELECT status FROM pgqrs_workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(status)
    }

    async fn heartbeat(&self, worker_id: i64) -> Result<(), Self::Error> {
        let result = sqlx::query(UPDATE_HEARTBEAT)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool, Self::Error> {
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

    async fn suspend(&self, worker_id: i64) -> Result<(), Self::Error> {
        let result = sqlx::query(TRANSITION_READY_TO_SUSPENDED)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
             // Could be because it doesn't exist or not in Ready state.
             // We return RowNotFound implies invalid transition context effectively.
             // Or we could check specific state to be more descriptive but let's stick to simple first.
             return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn resume(&self, worker_id: i64) -> Result<(), Self::Error> {
        let result = sqlx::query(TRANSITION_SUSPENDED_TO_READY)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
             return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn shutdown(&self, worker_id: i64) -> Result<(), Self::Error> {
        let result = sqlx::query(TRANSITION_SUSPENDED_TO_STOPPED)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
             return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }
}
