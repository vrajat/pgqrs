use crate::error::Result;
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct StepRecords {
    pool: PgPool,
}

impl StepRecords {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl crate::store::StepRecordTable for StepRecords {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        let row = sqlx::query_as::<_, StepRecord>(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, input, started_at)
            VALUES ($1, $2, 'PENDING'::pgqrs_workflow_status, $3, NOW())
            RETURNING
              id,
              run_id,
              step_name,
              status,
              input,
              output,
              error,
              retry_at,
              started_at as created_at,
              started_at as updated_at
            "#,
        )
        .bind(data.run_id)
        .bind(&data.step_name)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to insert workflow step '{}'", data.step_name),
        })?;

        Ok(row)
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        let row = sqlx::query_as::<_, StepRecord>(
            r#"
            SELECT
              id,
              run_id,
              step_name,
              status,
              input,
               output,
               error,
               retry_at,
               COALESCE(started_at, NOW()) as created_at,

              COALESCE(started_at, NOW()) as updated_at
            FROM pgqrs_workflow_steps
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "GET_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to get workflow step {}", id),
        })?;

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        let rows = sqlx::query_as::<_, StepRecord>(
            r#"
            SELECT
              id,
              run_id,
              step_name,
              status,
              input,
               output,
               error,
               retry_at,
               COALESCE(started_at, NOW()) as created_at,

              COALESCE(started_at, NOW()) as updated_at
            FROM pgqrs_workflow_steps
            ORDER BY id
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

    async fn delete(&self, id: i64) -> Result<u64> {
        let res = sqlx::query("DELETE FROM pgqrs_workflow_steps WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!("Failed to delete workflow step {}", id),
            })?;

        Ok(res.rows_affected())
    }
}

#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
