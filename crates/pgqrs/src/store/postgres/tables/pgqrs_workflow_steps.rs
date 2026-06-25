use crate::error::Result;
use crate::types::StepRecord;
use sqlx::PgPool;

const GET_STEP: &str = r#"
    SELECT
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
        retry_count
    FROM pgqrs_workflow_steps
    WHERE id = $1
"#;

const LIST_STEPS: &str = r#"
    SELECT
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
        retry_count
    FROM pgqrs_workflow_steps
    ORDER BY created_at DESC
"#;

const COUNT_STEPS: &str = r#"
    SELECT COUNT(*) FROM pgqrs_workflow_steps
"#;

const DELETE_STEP: &str = r#"
    DELETE FROM pgqrs_workflow_steps WHERE id = $1
"#;

const ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW(), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE
    SET status = CASE
        WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'::pgqrs_workflow_status
        WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'::pgqrs_workflow_status
        ELSE 'RUNNING'::pgqrs_workflow_status
    END,
    started_at = CASE
        WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
        ELSE NOW()
    END
    RETURNING
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
        retry_count
"#;

const CLEAR_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
    WHERE id = $1
    RETURNING
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
        retry_count
"#;

const COMPLETE_STEP: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
    WHERE id = $1
    RETURNING
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
        retry_count
"#;

const FAIL_STEP: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW(),
        retry_at = $3, retry_count = $4
    WHERE id = $1
    RETURNING
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
        retry_count
"#;

#[derive(Debug, Clone)]
pub struct StepRecords {
    pool: PgPool,
}

impl StepRecords {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get(&self, id: i64) -> Result<StepRecord> {
        sqlx::query_as::<_, StepRecord>(GET_STEP)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!("Failed to get workflow step {}", id),
            })
    }

    pub async fn list(&self) -> Result<Vec<StepRecord>> {
        sqlx::query_as::<_, StepRecord>(LIST_STEPS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_WORKFLOW_STEPS".into(),
                source: Box::new(e),
                context: "Failed to list workflow steps".into(),
            })
    }

    pub async fn count(&self) -> Result<i64> {
        sqlx::query_scalar(COUNT_STEPS)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOW_STEPS".into(),
                source: Box::new(e),
                context: "Failed to count workflow steps".into(),
            })
    }

    pub async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_STEP)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!("Failed to delete workflow step {}", id),
            })?;
        Ok(result.rows_affected())
    }

    pub async fn acquire_step(&self, run_id: i64, step_name: &str) -> Result<StepRecord> {
        sqlx::query_as::<_, StepRecord>(ACQUIRE_STEP)
            .bind(run_id)
            .bind(step_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ACQUIRE_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to acquire workflow step '{}' for run {}",
                    step_name, run_id
                ),
            })
    }

    pub async fn clear_retry(&self, id: i64) -> Result<StepRecord> {
        sqlx::query_as::<_, StepRecord>(CLEAR_RETRY)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CLEAR_WORKFLOW_STEP_RETRY".into(),
                source: Box::new(e),
                context: format!("Failed to clear retry for workflow step {}", id),
            })
    }

    pub async fn complete_step(&self, id: i64, output: serde_json::Value) -> Result<StepRecord> {
        sqlx::query_as::<_, StepRecord>(COMPLETE_STEP)
            .bind(id)
            .bind(output)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMPLETE_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!("Failed to complete workflow step {}", id),
            })
    }

    pub async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        sqlx::query_as::<_, StepRecord>(FAIL_STEP)
            .bind(id)
            .bind(error)
            .bind(retry_at)
            .bind(retry_count)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "FAIL_WORKFLOW_STEP".into(),
                source: Box::new(e),
                context: format!("Failed to fail workflow step {}", id),
            })
    }
}
