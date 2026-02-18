use crate::error::Result;
use crate::types::{NewRunRecord, RunRecord, WorkflowStatus};
use async_trait::async_trait;
use sqlx::PgPool;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct RunRecords {
    pool: PgPool,
}

impl RunRecords {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::RunRecordTable for RunRecords {
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        let row = sqlx::query_as::<_, RunRecord>(
            r#"
            INSERT INTO pgqrs_workflow_runs (workflow_id, status, input)
            VALUES (
              $1,
              'QUEUED'::pgqrs_workflow_status,
              $2
            )
            RETURNING
              id,
              workflow_id,
              status,
              input,
              output,
              error,
              created_at,
              updated_at
            "#,
        )
        .bind(data.workflow_id)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to insert workflow run for '{}'", data.workflow_id),
        })?;

        Ok(row)
    }

    async fn get(&self, id: i64) -> Result<RunRecord> {
        let row = sqlx::query_as::<_, RunRecord>(
            r#"
            SELECT
              id,
              workflow_id,
              status,
              input,
              output,
              error,
              created_at,
              updated_at
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

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query_as::<_, RunRecord>(
            r#"
            SELECT
              id,
              workflow_id,
              status,
              input,
              output,
              error,
              created_at,
              updated_at
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

        Ok(rows)
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
        let result: Option<RunRecord> = sqlx::query_as(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'RUNNING'::pgqrs_workflow_status,
                started_at = CASE
                    WHEN status = 'QUEUED'::pgqrs_workflow_status THEN NOW()
                    ELSE started_at
                END,
                updated_at = NOW()
            WHERE id = $1
              AND status IN ('QUEUED'::pgqrs_workflow_status, 'PAUSED'::pgqrs_workflow_status)
            RETURNING id, workflow_id, status, input, output, error, created_at, updated_at
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "SQL_START_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to start run {}", id),
        })?;

        if let Some(record) = result {
            return Ok(record);
        }

        let status_str: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM pgqrs_workflow_runs WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "CHECK_RUN_STATUS".into(),
                    source: Box::new(e),
                    context: format!("Failed to check status for run {}", id),
                })?;

        if let Some(s) = status_str {
            if let Ok(WorkflowStatus::Error) = WorkflowStatus::from_str(&s) {
                return Err(crate::error::Error::ValidationFailed {
                    reason: format!("Run {} is in terminal ERROR state", id),
                });
            }
        }

        self.get(id).await
    }

    async fn complete_run(&self, id: i64, output: serde_json::Value) -> Result<RunRecord> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW(), updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(output)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "COMPLETE_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to complete run {}", id),
        })?;

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
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'PAUSED'::pgqrs_workflow_status,
                error = $2,
                paused_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "PAUSE_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to pause run {}", id),
        })?;

        self.get(id).await
    }

    async fn fail_run(&self, id: i64, error: serde_json::Value) -> Result<RunRecord> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW(), updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "FAIL_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to fail run {}", id),
        })?;

        self.get(id).await
    }
}

// Ensure sqlx maps the enum correctly via WorkflowStatus in types.
// This import is only to force the feature-gated derive linkage when sqlx is enabled.
#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
