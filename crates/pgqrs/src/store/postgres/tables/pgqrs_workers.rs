//! Workers table CRUD operations for pgqrs.
//!
//! This module provides the [`Workers`] struct which implements pure CRUD operations
//! on the `pgqrs_workers` table.

use crate::error::Result;
use crate::types::{WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgPool;

// SQL constants for worker table operations
const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
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
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1 AND status = $2
    ORDER BY started_at DESC
"#;

const LIST_ZOMBIE_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = $1
    AND status IN ('ready', 'suspended')
    AND heartbeat_at < NOW() - $2
    ORDER BY heartbeat_at ASC
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

    pub async fn insert(&self, data: crate::types::NewWorker) -> Result<WorkerInfo> {
        let now = Utc::now();

        let worker_id: i64 = sqlx::query_scalar(INSERT_WORKER)
            .bind(&data.hostname)
            .bind(data.port)
            .bind(data.queue_id)
            .bind(now)
            .bind(now)
            .bind(WorkerStatus::Ready)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to insert worker: {}", e),
            })?;

        Ok(WorkerInfo {
            id: worker_id,
            hostname: data.hostname,
            port: data.port,
            queue_id: data.queue_id,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    pub async fn get(&self, id: i64) -> Result<WorkerInfo> {
        let worker = sqlx::query_as::<_, WorkerInfo>(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to get worker {}: {}", id, e),
            })?;

        Ok(worker)
    }

    pub async fn list(&self) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to list workers: {}", e),
            })?;

        Ok(workers)
    }

    pub async fn count(&self) -> Result<i64> {
        let query = "SELECT COUNT(*) FROM pgqrs_workers";
        let row = sqlx::query_scalar(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count workers: {}", e),
            })?;
        Ok(row)
    }

    pub async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete worker {}: {}", id, e),
            })?;

        Ok(result.rows_affected())
    }

    pub async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_WORKERS_BY_QUEUE)
            .bind(foreign_key_value)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to filter workers by queue ID {}: {}",
                    foreign_key_value, e
                ),
            })?;
        Ok(workers)
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
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count workers for queue {}: {}",
                    foreign_key_value, e
                ),
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
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to delete workers for queue {}: {}",
                    foreign_key_value, e
                ),
            })?;
        Ok(result.rows_affected())
    }

    pub async fn list_zombies_for_queue_tx<'a, 'b: 'a>(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list zombie workers for queue {} in tx: {}",
                    queue_id, e
                ),
            })?;
        Ok(workers)
    }

    pub async fn count_for_queue(
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
            .bind(state)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count workers for queue {} with state: {}",
                    queue_id, e
                ),
            })?;

        Ok(count)
    }

    pub async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        const COUNT_ZOMBIE_WORKERS: &str = r#"
            SELECT COUNT(*)
            FROM pgqrs_workers
            WHERE queue_id = $1
            AND status IN ('ready', 'suspended')
            AND heartbeat_at < NOW() - $2
        "#;

        let count: i64 = sqlx::query_scalar(COUNT_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to count zombie workers for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(count)
    }

    pub async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_WORKERS_BY_QUEUE_AND_STATE)
            .bind(queue_id)
            .bind(state)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list workers for queue {} with state: {}",
                    queue_id, e
                ),
            })?;

        Ok(workers)
    }

    pub async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(LIST_ZOMBIE_WORKERS)
            .bind(queue_id)
            .bind(older_than)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!(
                    "Failed to list zombie workers for queue {}: {}",
                    queue_id, e
                ),
            })?;

        Ok(workers)
    }
}

// Implement the public WorkerTable trait by delegating to inherent methods
#[async_trait]
impl crate::store::WorkerTable for Workers {
    async fn insert(&self, data: crate::types::NewWorker) -> Result<WorkerInfo> {
        self.insert(data).await
    }

    async fn get(&self, id: i64) -> Result<WorkerInfo> {
        self.get(id).await
    }

    async fn list(&self) -> Result<Vec<WorkerInfo>> {
        self.list().await
    }

    async fn count(&self) -> Result<i64> {
        self.count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.delete(id).await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerInfo>> {
        self.filter_by_fk(queue_id).await
    }

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<i64> {
        self.count_for_queue(queue_id, state).await
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        self.count_zombies_for_queue(queue_id, older_than).await
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerInfo>> {
        self.list_for_queue(queue_id, state).await
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerInfo>> {
        self.list_zombies_for_queue(queue_id, older_than).await
    }
}
