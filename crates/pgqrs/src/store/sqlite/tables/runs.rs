use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewRunRecord, RunRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

const SQL_START_RUN: &str = r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING',
    started_at = CASE WHEN status = 'QUEUED' THEN datetime('now') ELSE started_at END,
    updated_at = datetime('now')
WHERE id = $1 AND status IN ('QUEUED', 'PAUSED')
RETURNING id, workflow_id, status, input, output, error, created_at, updated_at
"#;

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

        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;
        let updated_at = parse_sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?;

        Ok(RunRecord {
            id,
            workflow_id,
            status,
            input,
            output,
            error,
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

        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'SUCCESS', output = $2, updated_at = $3
            WHERE id = $1
            "#,
        )
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

        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'ERROR', error = $2, updated_at = $3
            WHERE id = $1
            "#,
        )
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

        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'PAUSED', error = $2, paused_at = $3, updated_at = $3
            WHERE id = $1
            "#,
        )
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
}

#[async_trait]
impl crate::store::RunRecordTable for SqliteRunRecordTable {
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        let input_str = data.input.map(|v| v.to_string());

        let row = sqlx::query(
            r#"
            INSERT INTO pgqrs_workflow_runs (workflow_id, status, input, created_at, updated_at)
            VALUES ($1, 'QUEUED', $2, $3, $3)
            RETURNING id, workflow_id, status, input, output, error, created_at, updated_at
            "#,
        )
        .bind(data.workflow_id)
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
        let row = sqlx::query(
            r#"
            SELECT id, workflow_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_runs
            WHERE id = $1
            "#,
        )
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
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_runs
            ORDER BY created_at DESC
            "#,
        )
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
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflow_runs")
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
        let result = sqlx::query("DELETE FROM pgqrs_workflow_runs WHERE id = $1")
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
        let row = sqlx::query(SQL_START_RUN)
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

        let status_str: Option<String> =
            sqlx::query_scalar("SELECT status FROM pgqrs_workflow_runs WHERE id = $1")
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
                if matches!(status, WorkflowStatus::Error | WorkflowStatus::Success) {
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
}
