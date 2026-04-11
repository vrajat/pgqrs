use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::tables::DialectWorkerTable;
use crate::store::turso::dialect::TursoDialect;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::types::{WorkerRecord, WorkerStatus};
use async_trait::async_trait;
use chrono::Utc;
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (name, queue_id, started_at, heartbeat_at, status)
    VALUES (?, ?, ?, ?, ?)
    RETURNING id;
"#;

const GET_WORKER_BY_ID: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = ?;
"#;

const LIST_ALL_WORKERS: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC;
"#;

const LIST_WORKERS_BY_QUEUE: &str = r#"
    SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_id = ?
    ORDER BY started_at DESC;
"#;

const DELETE_WORKER_BY_ID: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE id = ?;
"#;

const INSERT_EPHEMERAL_WORKER_RETURNING: &str = r#"
    INSERT INTO pgqrs_workers (name, queue_id, status, started_at, heartbeat_at)
    VALUES (?, ?, 'ready', ?, ?)
    RETURNING id, name, queue_id, started_at, heartbeat_at, shutdown_at, status;
"#;

#[derive(Debug, Clone)]
pub struct TursoWorkerTable {
    db: Arc<Database>,
}

impl TursoWorkerTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<WorkerRecord> {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let queue_id: Option<i64> = row.get(2)?;

        let started_at_str: String = row.get(3)?;
        let started_at = parse_turso_timestamp(&started_at_str)?;

        let heartbeat_at_str: String = row.get(4)?;
        let heartbeat_at = parse_turso_timestamp(&heartbeat_at_str)?;

        let shutdown_at_str: Option<String> = row.get(5)?;
        let shutdown_at = match shutdown_at_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let status_str: String = row.get(6)?;
        let status = WorkerStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        Ok(WorkerRecord {
            id,
            name,
            queue_id,
            started_at,
            heartbeat_at,
            shutdown_at,
            status,
        })
    }

    pub async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        let status_str: String =
            crate::store::turso::query_scalar("SELECT status FROM pgqrs_workers WHERE id = ?")
                .bind(worker_id)
                .fetch_one(&self.db)
                .await?;

        WorkerStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })
    }

    pub async fn is_healthy(&self, worker_id: i64, max_age: chrono::Duration) -> Result<bool> {
        let threshold = Utc::now() - max_age;
        let threshold_str = format_turso_timestamp(&threshold);

        let is_healthy: bool = crate::store::turso::query_scalar(
            "SELECT heartbeat_at > ? FROM pgqrs_workers WHERE id = ?",
        )
        .bind(threshold_str)
        .bind(worker_id)
        .fetch_one(&self.db)
        .await?;

        Ok(is_healthy)
    }
}

#[async_trait]
impl crate::store::WorkerTable for TursoWorkerTable {
    async fn insert(&self, data: crate::types::NewWorkerRecord) -> Result<WorkerRecord> {
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);
        let status_str = WorkerStatus::Ready.to_string();

        let id: i64 = crate::store::turso::query_scalar(INSERT_WORKER)
            .bind(data.name.as_str())
            .bind(match data.queue_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(now_str.as_str())
            .bind(now_str.as_str())
            .bind(status_str.as_str())
            .fetch_one_once(&self.db)
            .await?;

        Ok(WorkerRecord {
            id,
            name: data.name,
            queue_id: data.queue_id,
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        })
    }

    async fn get(&self, id: i64) -> Result<WorkerRecord> {
        let row = crate::store::turso::query(GET_WORKER_BY_ID)
            .bind(id)
            .fetch_one(&self.db)
            .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<WorkerRecord>> {
        let rows = crate::store::turso::query(LIST_ALL_WORKERS)
            .fetch_all(&self.db)
            .await?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workers")
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query(DELETE_WORKER_BY_ID)
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerRecord>> {
        let rows = crate::store::turso::query(LIST_WORKERS_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.db)
            .await?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = ?",
        )
        .bind(queue_id)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    async fn mark_stopped(&self, id: i64) -> Result<()> {
        crate::store::turso::query(TursoDialect::WORKER.mark_stopped)
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(())
    }

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<i64> {
        let state_str = state.to_string();
        let count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = ? AND status = ?",
        )
        .bind(queue_id)
        .bind(turso::Value::Text(state_str))
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<i64> {
        let threshold = Utc::now() - older_than;
        let threshold_str = format_turso_timestamp(&threshold);

        let count: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workers WHERE queue_id = ? AND status IN ('ready', 'polling', 'suspended', 'interrupted') AND heartbeat_at < ?")
            .bind(queue_id)
            .bind(threshold_str)
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: crate::types::WorkerStatus,
    ) -> Result<Vec<WorkerRecord>> {
        let state_str = state.to_string();
        let rows = crate::store::turso::query("SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = ? AND status = ? ORDER BY started_at DESC")
            .bind(queue_id)
            .bind(state_str)
            .fetch_all(&self.db)
            .await?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> Result<Vec<WorkerRecord>> {
        let threshold = Utc::now() - older_than;
        let threshold_str = format_turso_timestamp(&threshold);

        let rows = crate::store::turso::query("SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE queue_id = ? AND status IN ('ready', 'polling', 'suspended', 'interrupted') AND heartbeat_at < ? ORDER BY heartbeat_at ASC")
            .bind(queue_id)
            .bind(threshold_str)
            .fetch_all(&self.db)
            .await?;

        let mut workers = Vec::with_capacity(rows.len());
        for row in rows {
            workers.push(Self::map_row(&row)?);
        }
        Ok(workers)
    }

    async fn register(&self, queue_id: Option<i64>, name: &str) -> Result<WorkerRecord> {
        let existing = crate::store::turso::query("SELECT id, name, queue_id, started_at, heartbeat_at, shutdown_at, status FROM pgqrs_workers WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.db)
            .await?;

        if let Some(row) = existing {
            let worker = Self::map_row(&row)?;
            match worker.status {
                WorkerStatus::Stopped => {
                    // Reset
                    let now = Utc::now();
                    let now_str = format_turso_timestamp(&now);
                    let row = crate::store::turso::query("UPDATE pgqrs_workers SET status = 'ready', queue_id = ?, started_at = ?, heartbeat_at = ?, shutdown_at = NULL WHERE id = ? RETURNING id, name, queue_id, started_at, heartbeat_at, shutdown_at, status")
                        .bind(match queue_id {
                            Some(id) => turso::Value::Integer(id),
                            None => turso::Value::Null,
                        })
                        .bind(now_str.clone())
                        .bind(now_str)
                        .bind(worker.id)
                        .fetch_one_once(&self.db)
                        .await?;

                    Self::map_row(&row)
                }
                WorkerStatus::Ready => Err(crate::error::Error::ValidationFailed {
                    reason: format!(
                        "Worker {} is already active. Cannot register duplicate.",
                        name
                    ),
                }),
                WorkerStatus::Suspended => Err(crate::error::Error::ValidationFailed {
                    reason: format!("Worker {} is suspended. Use resume() to reactivate.", name),
                }),
                WorkerStatus::Polling | WorkerStatus::Interrupted => {
                    Err(crate::error::Error::ValidationFailed {
                        reason: format!(
                            "Worker {} is already active. Cannot register duplicate.",
                            name
                        ),
                    })
                }
            }
        } else {
            // Create new
            self.insert(crate::types::NewWorkerRecord {
                name: name.to_string(),
                queue_id,
            })
            .await
        }
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerRecord> {
        let name = format!("__ephemeral__{}", uuid::Uuid::new_v4());
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        let row = crate::store::turso::query(INSERT_EPHEMERAL_WORKER_RETURNING)
            .bind(name.as_str())
            .bind(match queue_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(now_str.clone())
            .bind(now_str.clone())
            .fetch_one_once(&self.db)
            .await?;

        Self::map_row(&row)
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
impl DialectWorkerTable for TursoWorkerTable {
    type Dialect = TursoDialect;

    async fn execute_worker_update(&self, query: QueryBuilder) -> Result<u64> {
        let mut builder = crate::store::turso::query(query.sql());
        for param in query.params() {
            let value = match param {
                QueryParam::I64(value) => turso::Value::Integer(*value),
                QueryParam::I32(value) => turso::Value::Integer((*value).into()),
                QueryParam::String(value) => turso::Value::Text(value.clone()),
                QueryParam::Json(value) => turso::Value::Text(value.to_string()),
                QueryParam::DateTime(value) => match value {
                    Some(dt) => turso::Value::Text(format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }

        builder.execute_once(&self.db).await
    }

    async fn query_worker_status(&self, worker_id: i64) -> Result<WorkerStatus> {
        self.get_status(worker_id).await
    }

    async fn ensure_shutdown_allowed(&self, worker_id: i64) -> Result<()> {
        let held_count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id = ? AND archived_at IS NULL",
        )
        .bind(worker_id)
        .fetch_one(&self.db)
        .await?;

        if held_count > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                reason: format!("Worker has {} pending messages", held_count),
                count: held_count as u64,
            });
        }

        Ok(())
    }
}
