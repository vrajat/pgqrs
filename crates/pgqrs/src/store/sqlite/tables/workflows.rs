use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewWorkflow, WorkflowRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SqliteWorkflowTable {
    pool: SqlitePool,
}

impl SqliteWorkflowTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<WorkflowRecord> {
        let workflow_id: i64 = row.try_get("workflow_id")?;
        let name: String = row.try_get("name")?;

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

        let executor_id: Option<String> = row.try_get("executor_id")?;

        Ok(WorkflowRecord {
            workflow_id,
            name,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
            executor_id,
        })
    }

    pub async fn complete_workflow(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        output: serde_json::Value,
    ) -> Result<()> {
        let output_str = output.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'SUCCESS', output = $2, updated_at = $3
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .bind(output_str)
        .bind(now_str)
        .execute(executor)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "COMPLETE_WORKFLOW".into(),
            source: e,
            context: format!("Failed to complete workflow {}", id),
        })?;
        Ok(())
    }

    pub async fn fail_workflow(
        executor: &mut sqlx::SqliteConnection,
        id: i64,
        error: serde_json::Value,
    ) -> Result<()> {
        let error_str = error.to_string();
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'ERROR', error = $2, updated_at = $3
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .bind(error_str)
        .bind(now_str)
        .execute(executor)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "FAIL_WORKFLOW".into(),
            source: e,
            context: format!("Failed to fail workflow {}", id),
        })?;
        Ok(())
    }
}

#[async_trait]
impl crate::store::WorkflowTable for SqliteWorkflowTable {
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        let input_str = data.input.map(|v| v.to_string());

        let row = sqlx::query(
            r#"
            INSERT INTO pgqrs_workflows (name, status, input, created_at, updated_at)
            VALUES ($1, 'PENDING', $2, $3, $3)
            RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            "#,
        )
        .bind(&data.name)
        .bind(input_str)
        .bind(now_str)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW".into(),
            source: e,
            context: format!("Failed to insert workflow '{}'", data.name),
        })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        let row = sqlx::query(
            r#"
            SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            FROM pgqrs_workflows
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("GET_WORKFLOW ({})", id),
            source: e,
            context: format!("Failed to get workflow {}", id),
        })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            FROM pgqrs_workflows
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_WORKFLOWS".into(),
            source: e,
            context: "Failed to list workflows".into(),
        })?;

        let mut workflows = Vec::with_capacity(rows.len());
        for row in rows {
            workflows.push(Self::map_row(row)?);
        }
        Ok(workflows)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflows")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOWS".into(),
                source: e,
                context: "Failed to count workflows".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM pgqrs_workflows WHERE workflow_id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKFLOW ({})", id),
                source: e,
                context: format!("Failed to delete workflow {}", id),
            })?;
        Ok(result.rows_affected())
    }
}
