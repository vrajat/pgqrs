use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::sqlite::dialect::SqliteDialect;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewRunRecord, RunRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SqliteRunRecordTable {
    pool: SqlitePool,
}

impl SqliteRunRecordTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<RunRecord> {
        let id: i64 = row.try_get("id")?;
        let workflow_id: i64 = row.try_get("workflow_id")?;
        let message_id: i64 = row.try_get("message_id")?;

        let status_str: String = row.try_get("status")?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let input_str: Option<String> = row.try_get("input")?;
        let input: Option<Value> = match input_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let output_str: Option<String> = row.try_get("output")?;
        let output: Option<Value> = match output_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let error_str: Option<String> = row.try_get("error")?;
        let error: Option<Value> = match error_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let cancel_reason_str: Option<String> = row.try_get("cancel_reason")?;
        let cancel_reason: Option<Value> = match cancel_reason_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let cancelled_at = row
            .try_get::<Option<String>, _>("cancelled_at")?
            .map(|s| parse_sqlite_timestamp(&s))
            .transpose()?;
        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;
        let updated_at = parse_sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?;

        Ok(RunRecord {
            id,
            workflow_id,
            message_id,
            status,
            input,
            output,
            error,
            cancel_reason,
            cancelled_at,
            created_at,
            updated_at,
        })
    }

    pub async fn complete_run(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        output: serde_json::Value,
    ) -> Result<()> {
        let output_str = output.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(SqliteDialect::RUN.complete)
            .bind(id)
            .bind(output_str)
            .bind(now_str)
            .execute(executor)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMPLETE_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to complete run {}", id),
            })?;
        Ok(())
    }

    pub async fn fail_run(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        error: serde_json::Value,
    ) -> Result<()> {
        let error_str = error.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(SqliteDialect::RUN.fail)
            .bind(id)
            .bind(error_str)
            .bind(now_str)
            .execute(executor)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "FAIL_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to fail run {}", id),
            })?;
        Ok(())
    }

    pub async fn pause_run(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        error: serde_json::Value,
    ) -> Result<()> {
        let error_str = error.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(SqliteDialect::RUN.pause)
            .bind(id)
            .bind(error_str)
            .bind(now_str)
            .execute(executor)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PAUSE_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to pause run {}", id),
            })?;
        Ok(())
    }

    pub async fn cancel_run(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        reason: serde_json::Value,
    ) -> Result<()> {
        let reason_str = reason.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(SqliteDialect::RUN.cancel)
            .bind(id)
            .bind(reason_str)
            .bind(now_str)
            .execute(executor)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CANCEL_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to cancel run {}", id),
            })?;
        Ok(())
    }
}

#[async_trait]
impl crate::store::RunRecordTable for SqliteRunRecordTable {
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        let input_str = data.input.map(|v| v.to_string());

        let row = sqlx::query(SqliteDialect::RUN.insert)
            .bind(data.workflow_id)
            .bind(data.message_id)
            .bind(input_str)
            .bind(now_str)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_WORKFLOW_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to insert workflow run for '{}'", data.workflow_id),
            })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<RunRecord> {
        let row = sqlx::query(SqliteDialect::RUN.get)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_WORKFLOW_RUN ({})", id),
                source: Box::new(e),
                context: format!("Failed to get workflow run {}", id),
            })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(SqliteDialect::RUN.list)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_WORKFLOW_RUNS".into(),
                source: Box::new(e),
                context: "Failed to list workflow runs".into(),
            })?;

        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(Self::map_row(row)?);
        }
        Ok(runs)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(SqliteDialect::RUN.count)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOW_RUNS".into(),
                source: Box::new(e),
                context: "Failed to count workflow runs".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(SqliteDialect::RUN.delete)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKFLOW_RUN ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete workflow run {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn start_run(&self, id: i64) -> Result<RunRecord> {
        let row = sqlx::query(SqliteDialect::RUN.start)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_START_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to start run {}", id),
            })?;

        if let Some(row) = row {
            return Self::map_row(row);
        }

        let status_str: Option<String> = sqlx::query_scalar(SqliteDialect::RUN.get_status)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_RUN_STATUS".into(),
                source: Box::new(e),
                context: format!("Failed to check status for run {}", id),
            })?;

        if let Some(s) = status_str {
            if let Ok(status) = WorkflowStatus::from_str(&s) {
                if matches!(
                    status,
                    WorkflowStatus::Error | WorkflowStatus::Success | WorkflowStatus::Cancelled
                ) {
                    return Err(crate::error::Error::ValidationFailed {
                        reason: format!("Run {} is in terminal {} state", id, status),
                    });
                }
            }
        }

        self.get(id).await
    }

    async fn complete_run(&self, id: i64, output: serde_json::Value) -> Result<RunRecord> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        Self::complete_run(&mut conn, id, output).await?;
        drop(conn);
        self.get(id).await
    }

    async fn pause_run(
        &self,
        id: i64,
        message: String,
        resume_after: std::time::Duration,
    ) -> Result<RunRecord> {
        let error = serde_json::json!({
            "message": message,
            "resume_after": resume_after.as_secs()
        });
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        Self::pause_run(&mut conn, id, error).await?;
        drop(conn);
        self.get(id).await
    }

    async fn fail_run(&self, id: i64, error: serde_json::Value) -> Result<RunRecord> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        Self::fail_run(&mut conn, id, error).await?;
        drop(conn);
        self.get(id).await
    }

    async fn cancel_run(&self, id: i64, reason: serde_json::Value) -> Result<RunRecord> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        Self::cancel_run(&mut conn, id, reason).await?;
        drop(conn);
        self.get(id).await
    }

    async fn get_by_message_id(&self, message_id: i64) -> Result<RunRecord> {
        let row = sqlx::query(SqliteDialect::RUN.get_by_message_id)
            .bind(message_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => crate::error::Error::NotFound {
                    entity: "RunRecord".to_string(),
                    id: format!("message_id:{}", message_id),
                },
                _ => crate::error::Error::QueryFailed {
                    query: format!("GET_WORKFLOW_RUN_BY_MESSAGE_ID ({})", message_id),
                    source: Box::new(e),
                    context: format!("Failed to get workflow run for message {}", message_id),
                },
            })?;

        Self::map_row(row)
    }
}
