use crate::error::Result;
use crate::types::{NewWorkflowStep, WorkflowStatus, WorkflowStep};
use async_trait::async_trait;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct WorkflowSteps {
    pool: PgPool,
}

impl WorkflowSteps {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::WorkflowStepTable for WorkflowSteps {
    async fn insert(&self, data: NewWorkflowStep) -> Result<WorkflowStep> {
        let row = sqlx::query_as::<_, WorkflowStep>(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_id, status, input, started_at)
            VALUES ($1, $2, 'PENDING'::pgqrs_workflow_status, $3, NOW())
            RETURNING
              0 as id,
              run_id,
              step_id,
              status,
              input,
              output,
              error,
              started_at as created_at,
              started_at as updated_at
            "#,
        )
        .bind(data.run_id)
        .bind(&data.step_id)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to insert workflow step '{}'", data.step_id),
        })?;

        Ok(row)
    }

    async fn get(&self, _id: i64) -> Result<WorkflowStep> {
        Err(crate::error::Error::ValidationFailed {
            reason:
                "Postgres workflow steps are keyed by (run_id, step_id); get(id) is unsupported"
                    .into(),
        })
    }

    async fn list(&self) -> Result<Vec<WorkflowStep>> {
        let rows = sqlx::query_as::<_, WorkflowStep>(
            r#"
            SELECT
              0 as id,
              run_id,
              step_id,
              status,
              input,
              output,
              error,
              COALESCE(started_at, NOW()) as created_at,
              COALESCE(started_at, NOW()) as updated_at
            FROM pgqrs_workflow_steps
            ORDER BY run_id, step_id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_WORKFLOW_STEPS".into(),
            source: Box::new(e),
            context: "Failed to list workflow steps".into(),
        })?;

        Ok(rows)
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

    async fn delete(&self, _id: i64) -> Result<u64> {
        Err(crate::error::Error::ValidationFailed {
            reason:
                "Postgres workflow steps are keyed by (run_id, step_id); delete(id) is unsupported"
                    .into(),
        })
    }
}

#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
