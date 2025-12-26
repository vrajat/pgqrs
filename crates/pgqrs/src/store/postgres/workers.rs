use crate::error::{Error, Result};
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

const GET_WORKER_HEALTH_GLOBAL: &str = r#"
    SELECT
        'Global' as queue_name,
        COUNT(*) as total_workers,
        COUNT(*) FILTER (WHERE status = 'ready') as ready_workers,
        COUNT(*) FILTER (WHERE status = 'suspended') as suspended_workers,
        COUNT(*) FILTER (WHERE status = 'stopped') as stopped_workers,
        COUNT(*) FILTER (WHERE heartbeat_at < NOW() - make_interval(secs => $1::double precision)) as stale_workers
    FROM pgqrs_workers;
"#;

const GET_WORKER_HEALTH_BY_QUEUE: &str = r#"
    SELECT
        COALESCE(q.queue_name, 'Unknown') as queue_name,
        COUNT(w.id) as total_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'ready') as ready_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'suspended') as suspended_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'stopped') as stopped_workers,
        COUNT(w.id) FILTER (WHERE w.heartbeat_at < NOW() - make_interval(secs => $1::double precision)) as stale_workers
    FROM pgqrs_workers w
    LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
    GROUP BY q.queue_name;
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
              UNION
              SELECT producer_worker_id as worker_id FROM pgqrs_archive WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_archive WHERE consumer_worker_id IS NOT NULL
          ) refs
      )
"#;

const COUNT_WORKER_MESSAGES: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id = $1
"#;

#[async_trait::async_trait]
impl WorkerStore for PostgresWorkerStore {
    type Error = Error;

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
            return Err(Error::WorkerNotFound { id: worker_id });
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
            // Either doesn't exist or not in Ready state
            return Err(Error::InvalidStateTransition {
                from: "unknown".to_string(),
                to: "suspended".to_string(),
                reason: "Worker not found or not in Ready state".to_string(),
            });
        }
        Ok(())
    }

    async fn resume(&self, worker_id: i64) -> Result<(), Self::Error> {
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

    async fn shutdown(&self, worker_id: i64) -> Result<(), Self::Error> {
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

    async fn health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<crate::types::WorkerHealthStats>, Self::Error> {
        let timeout_seconds = heartbeat_timeout.num_seconds() as f64;

        let stats = if group_by_queue {
            sqlx::query_as::<_, crate::types::WorkerHealthStats>(GET_WORKER_HEALTH_BY_QUEUE)
                .bind(timeout_seconds)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, crate::types::WorkerHealthStats>(GET_WORKER_HEALTH_GLOBAL)
                .bind(timeout_seconds)
                .fetch_all(&self.pool)
                .await
        };

        stats.map_err(|e| Error::Connection {
            message: format!("Failed to get worker health stats: {}", e),
        })
    }

    async fn purge_stopped(&self, max_age: chrono::Duration) -> Result<u64, Self::Error> {
        let threshold = Utc::now() - max_age;

        let result = sqlx::query(PURGE_OLD_WORKERS)
            .bind(threshold)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }

    async fn count_pending_messages(&self, worker_id: i64) -> Result<i64, Self::Error> {
        let count: i64 = sqlx::query_scalar(COUNT_WORKER_MESSAGES)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }
}
}
