use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id;
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1;
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC;
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    ORDER BY started_at DESC;
"#;

const DELETE_WORKER_BY_ID: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE id = $1;
"#;

const INSERT_EPHEMERAL_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, status)
    VALUES ($1, -1, $2, 'ready')
    RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status;
"#;

#[derive(Debug, Clone)]
pub struct SqliteWorkerTable {
    pool: SqlitePool,
}

impl SqliteWorkerTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<WorkerInfo> {
        let id: i64 = row.try_get("id")?;
        let hostname: String = row.try_get("hostname")?;
        let port: i32 = row.try_get("port")?;
        let queue_id: Option<i64> = row.try_get("queue_id")?;

        let started_at_str: String = row.try_get("started_at")?;
        let started_at = parse_sqlite_timestamp(&started_at_str)?;

        let heartbeat_at_str: String = row.try_get("heartbeat_at")?;
        let heartbeat_at = parse_sqlite_timestamp(&heartbeat_at_str)?;

        let shutdown_at_str: Option<String> = row.try_get("shutdown_at")?;
        let shutdown_at = match shutdown_at_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        let status_str: String = row.try_get("status")?;
        let status = WorkerStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        Ok(WorkerInfo {
            id,
            hostname,
            port,
            queue_id,
            started_at,
            heartbeat_at,
            shutdown_at,
            status,
        })
    }

    pub async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        let status_str: String =
            sqlx::query_scalar("SELECT status FROM pgqrs_workers WHERE id = $1")
                .bind(worker_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "GET_WORKER_STATUS".into(),
                    source: e,
                    context: format!("Failed to get worker {} status", worker_id),
                })?;

        WorkerStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })
    }

    pub async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = $1 WHERE id = $2")
            .bind(now_str)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "UPDATE_HEARTBEAT".into(),
                source: e,
                context: format!("Failed to update heartbeat for worker {}", worker_id),
            })?;

        Ok(())
    }

    pub async fn is_healthy(&self, worker_id: i64, max_age: chrono::Duration) -> Result<bool> {
        let threshold = Utc::now() - max_age;
        let threshold_str = format_sqlite_timestamp(&threshold);

        let is_healthy: bool =
            sqlx::query_scalar("SELECT heartbeat_at > $2 FROM pgqrs_workers WHERE id = $1")
                .bind(worker_id)
                .bind(threshold_str)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "CHECK_WORKER_HEALTH".into(),
                    source: e,
                    context: format!("Failed to check health for worker {}", worker_id),
                })?;

        Ok(is_healthy)
    }

    pub async fn suspend(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(
            "UPDATE pgqrs_workers SET status = 'suspended' WHERE id = $1 AND status = 'ready'",
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "TRANSITION_READY_TO_SUSPENDED".into(),
            source: e,
            context: format!("Failed to suspend worker {}", worker_id),
        })?;

        if result.rows_affected() == 0 {
            let current_status = self.get_status(worker_id).await?;
            // If status is already suspended, it's effectively a no-op / idempotent, but logic in postgres impl returns Err if not Ready.
            // Postgres impl uses RETURNING id to check if update happened.
            // Here we check rows_affected.
            // If status is not ready, we error.
            return Err(crate::error::Error::InvalidStateTransition {
                from: current_status.to_string(),
                to: "suspended".to_string(),
                reason: "Worker must be in Ready state to suspend".to_string(),
            });
        }
        Ok(())
    }

    pub async fn resume(&self, worker_id: i64) -> Result<()> {
        let result = sqlx::query(
            "UPDATE pgqrs_workers SET status = 'ready' WHERE id = $1 AND status = 'suspended'",
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "TRANSITION_SUSPENDED_TO_READY".into(),
            source: e,
            context: format!("Failed to resume worker {}", worker_id),
        })?;

        if result.rows_affected() == 0 {
            let current_status = self.get_status(worker_id).await?;
            return Err(crate::error::Error::InvalidStateTransition {
                from: current_status.to_string(),
                to: "ready".to_string(),
                reason: "Worker must be in Suspended state to resume".to_string(),
            });
        }
        Ok(())
    }

    pub async fn shutdown(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        let result = sqlx::query("UPDATE pgqrs_workers SET status = 'stopped', shutdown_at = $2 WHERE id = $1 AND status = 'suspended'")
            .bind(worker_id)
            .bind(now_str)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "TRANSITION_SUSPENDED_TO_STOPPED".into(),
                source: e,
                context: format!("Failed to shutdown worker {}", worker_id),
            })?;

        if result.rows_affected() == 0 {
            let current_status = self.get_status(worker_id).await?;
            return Err(crate::error::Error::InvalidStateTransition {
                from: current_status.to_string(),
                to: "stopped".to_string(),
                reason: "Worker must be in Suspended state to shutdown".to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl crate::store::WorkerTable for SqliteWorkerTable {
    async fn insert(&self, data: crate::types::NewWorker) -> Result<WorkerInfo> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        let status_str = WorkerStatus::Ready.to_string();

        let id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.hostname)
            .bind(data.port)
            .bind(data.queue_id)
            .bind(&now_str)
            .bind(&now_str)
            .bind(&status_str)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_WORKER".into(),
                source: e,
                context: format!("Failed to insert worker {}:{}", data.hostname, data.port),
            })?;

        Ok(WorkerInfo {
            id,
            hostname: data.hostname,
            port: data.port,
            queue_id: data.queue_id,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    async fn get(&self, id: i64) -> Result<WorkerInfo> {
        let row = sqlx::query(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_WORKER_BY_ID ({})", id),
                source: e,
                context: format!("Failed to get worker {}", id),
            })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        let rows = sqlx::query(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_WORKERS".into(),
                source: e,
                context: "Failed to list all workers".into(),
            })?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(row)?);
        }
        Ok(workers)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workers")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKERS".into(),
                source: e,
                context: "Failed to count workers".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKER_BY_ID ({})", id),
                source: e,
                context: format!("Failed to delete worker {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerInfo>> {
        let rows = sqlx::query(LIST_WORKERS_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_WORKERS_BY_QUEUE ({})", queue_id),
                source: e,
                context: format!("Failed to filter workers by queue ID {}", queue_id),
            })?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(row)?);
        }
        Ok(workers)
    }

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<i64> {
        let state_str = state.to_string();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = $1 AND status = $2",
        )
        .bind(queue_id)
        .bind(state_str)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("COUNT_WORKERS_BY_STATE (queue_id={})", queue_id),
            source: e,
            context: format!("Failed to count workers for queue {}", queue_id),
        })?;
        Ok(count)
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        let threshold = Utc::now() - older_than;
        let threshold_str = format_sqlite_timestamp(&threshold);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = $1 AND status IN ('ready', 'suspended') AND heartbeat_at < $2")
            .bind(queue_id)
            .bind(threshold_str)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("COUNT_ZOMBIE_WORKERS (queue_id={})", queue_id),
                source: e,
                context: format!("Failed to count zombie workers for queue {}", queue_id),
            })?;
        Ok(count)
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        let state_str = state.to_string();
        let rows = sqlx::query("SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = $1 AND status = $2 ORDER BY started_at DESC")
            .bind(queue_id)
            .bind(state_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_WORKERS_BY_QUEUE_AND_STATE (queue_id={})", queue_id),
                source: e,
                context: format!("Failed to list workers for queue {}", queue_id),
            })?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(row)?);
        }
        Ok(workers)
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        let threshold = Utc::now() - older_than;
        let threshold_str = format_sqlite_timestamp(&threshold);

        let rows = sqlx::query("SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = $1 AND status IN ('ready', 'suspended') AND heartbeat_at < $2 ORDER BY heartbeat_at ASC")
            .bind(queue_id)
            .bind(threshold_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_ZOMBIE_WORKERS (queue_id={})", queue_id),
                source: e,
                context: format!("Failed to list zombie workers for queue {}", queue_id),
            })?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(row)?);
        }
        Ok(workers)
    }

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo> {
        let existing = sqlx::query("SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE hostname = $1 AND port = $2")
            .bind(hostname)
            .bind(port)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("FIND_WORKER_BY_HOST_PORT ({}:{})", hostname, port),
                source: e,
                context: format!("Failed to find worker {}:{}", hostname, port),
            })?;

        if let Some(row) = existing {
            let worker = Self::map_row(row)?;
            match worker.status {
                WorkerStatus::Stopped => {
                    // Reset
                    let now = Utc::now();
                    let now_str = format_sqlite_timestamp(&now);
                    let row = sqlx::query("UPDATE pgqrs_workers SET status = 'ready', queue_id = $2, started_at = $3, heartbeat_at = $3, shutdown_at = NULL WHERE id = $1 RETURNING id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status")
                        .bind(worker.id)
                        .bind(queue_id)
                        .bind(now_str)
                        .fetch_one(&self.pool)
                        .await
                        .map_err(|e| crate::error::Error::QueryFailed {
                            query: format!("RESET_WORKER_TO_READY ({})", worker.id),
                            source: e,
                            context: format!("Failed to reset worker {}:{}", hostname, port),
                        })?;

                    Self::map_row(row)
                }
                WorkerStatus::Ready => Err(crate::error::Error::ValidationFailed {
                    reason: format!(
                        "Worker {}:{} is already active. Cannot register duplicate.",
                        hostname, port
                    ),
                }),
                WorkerStatus::Suspended => Err(crate::error::Error::ValidationFailed {
                    reason: format!(
                        "Worker {}:{} is suspended. Use resume() to reactivate.",
                        hostname, port
                    ),
                }),
            }
        } else {
            // Create new
            self.insert(crate::types::NewWorker {
                hostname: hostname.to_string(),
                port,
                queue_id,
            })
            .await
        }
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerInfo> {
        let hostname = format!("__ephemeral__{}", uuid::Uuid::new_v4());

        let row = sqlx::query(INSERT_EPHEMERAL_WORKER)
            .bind(&hostname)
            .bind(queue_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_EPHEMERAL_WORKER".into(),
                source: e,
                context: "Failed to create ephemeral worker".into(),
            })?;

        Self::map_row(row)
    }
}
