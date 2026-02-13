use crate::error::Result;
use crate::types::{NewWorkflowRun, WorkflowRun, WorkflowStatus};
use async_trait::async_trait;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct WorkflowRuns {
    pool: PgPool,
}

impl WorkflowRuns {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::WorkflowRunTable for WorkflowRuns {
    async fn insert(&self, data: NewWorkflowRun) -> Result<WorkflowRun> {
        let row = sqlx::query_as::<_, WorkflowRun>(
            r#"
            INSERT INTO pgqrs_workflow_runs (workflow_id, status, input)
            VALUES (
              $1,
              'PENDING'::pgqrs_workflow_status,
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
              created_at as updated_at
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

    async fn get(&self, id: i64) -> Result<WorkflowRun> {
        let row = sqlx::query_as::<_, WorkflowRun>(
            r#"
            SELECT
              id,
              workflow_id,
              status,
              input,
              output,
              error,
              created_at,
              created_at as updated_at
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

    async fn list(&self) -> Result<Vec<WorkflowRun>> {
        let rows = sqlx::query_as::<_, WorkflowRun>(
            r#"
            SELECT
              id,
              workflow_id,
              status,
              input,
              output,
              error,
              created_at,
              created_at as updated_at
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
}

// Ensure sqlx maps the enum correctly via WorkflowStatus in types.
// This import is only to force the feature-gated derive linkage when sqlx is enabled.
#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
