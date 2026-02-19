use crate::error::Result;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SqliteStepRecordTable {
    pool: SqlitePool,
}

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING', datetime('now'), 0)
ON CONFLICT (run_id, step_name) DO UPDATE
SET status = CASE
    WHEN status = 'SUCCESS' THEN 'SUCCESS'
    WHEN status = 'ERROR' THEN 'ERROR'
    ELSE 'RUNNING'
END,
started_at = CASE
    WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
    ELSE datetime('now')
END
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING', retry_at = NULL, error = NULL
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#;

const SQL_COMPLETE_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS', output = $2, completed_at = datetime('now')
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#;

const SQL_FAIL_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR', error = $2, completed_at = datetime('now'),
    retry_at = $3, retry_count = $4
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#;

impl SqliteStepRecordTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<StepRecord> {
        let id: i64 = row.try_get("id")?;
        let run_id: i64 = row.try_get("run_id")?;
        let step_name: String = row.try_get("step_name")?;

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

        let retry_at_str: Option<String> = row.try_get("retry_at")?;
        let retry_at = match retry_at_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        let retry_count: i32 = row.try_get("retry_count")?;

        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;
        let updated_at = parse_sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?;

        Ok(StepRecord {
            id,
            run_id,
            step_name,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
            retry_at,
            retry_count,
        })
    }
}

#[async_trait]
impl crate::store::StepRecordTable for SqliteStepRecordTable {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        let input_str = data.input.map(|v| v.to_string());

        let row = sqlx::query(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, input, created_at, updated_at)
            VALUES ($1, $2, 'PENDING', $3, $4, $4)
            RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            "#,
        )
        .bind(data.run_id)
        .bind(&data.step_name)
        .bind(input_str)
        .bind(now_str)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to insert workflow step '{}' for run {}", data.step_name, data.run_id),
        })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        let row = sqlx::query(
            r#"
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            FROM pgqrs_workflow_steps
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("GET_WORKFLOW_STEP ({})", id),
            source: Box::new(e),
            context: format!("Failed to get workflow step {}", id),
        })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            FROM pgqrs_workflow_steps
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_WORKFLOW_STEPS".into(),
            source: Box::new(e),
            context: "Failed to list workflow steps".into(),
        })?;

        let mut steps = Vec::with_capacity(rows.len());
        for row in rows {
            steps.push(Self::map_row(row)?);
        }
        Ok(steps)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflow_steps")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOW_STEPS".into(),
                source: Box::new(e),
                context: "Failed to count workflow steps".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM pgqrs_workflow_steps WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKFLOW_STEP ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete workflow step {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord> {
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.to_string()),
                QueryParam::DateTime(value) => {
                    builder.bind(value.map(|dt| format_sqlite_timestamp(&dt)))
                }
            };
        }

        let row =
            builder
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "STEP_EXECUTE".into(),
                    source: Box::new(e),
                    context: "Failed to execute step query".into(),
                })?;

        Self::map_row(row)
    }

    fn sql_acquire_step(&self) -> &'static str {
        SQL_ACQUIRE_STEP
    }

    fn sql_clear_retry(&self) -> &'static str {
        SQL_CLEAR_RETRY
    }

    fn sql_complete_step(&self) -> &'static str {
        SQL_COMPLETE_STEP
    }

    fn sql_fail_step(&self) -> &'static str {
        SQL_FAIL_STEP
    }
}
