use crate::error::Result;
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

#[derive(Debug, Clone)]
pub struct StepRecords {
    pool: PgPool,
}

const SQL_ACQUIRE_STEP: &str = r#"
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
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, started_at
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
WHERE id = $1
"#;

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
              retry_count,
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
               retry_count,
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
               retry_count,
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

    async fn acquire_step(
        &self,
        run_id: i64,
        step_name: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepRecord> {
        let step_name_string = step_name.to_string();

        let row = sqlx::query(SQL_ACQUIRE_STEP)
            .bind(run_id)
            .bind(&step_name_string)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_ACQUIRE_STEP".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to acquire step {} for run {}",
                    step_name_string, run_id
                ),
            })?;

        let id: i64 = row.try_get("id")?;
        let mut status: WorkflowStatus = row.try_get("status")?;
        let retry_count: i32 = row.try_get("retry_count")?;
        let retry_at: Option<DateTime<Utc>> = row.try_get("retry_at")?;

        if status == WorkflowStatus::Error {
            if let Some(retry_at) = retry_at {
                if current_time < retry_at {
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }

                sqlx::query(SQL_CLEAR_RETRY)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "SQL_CLEAR_RETRY".into(),
                        source: Box::new(e),
                        context: format!("Failed to clear retry_at for step {}", id),
                    })?;

                status = WorkflowStatus::Running;
            } else {
                let error: Option<serde_json::Value> = row.try_get("error")?;
                let error_val = error.unwrap_or_else(|| {
                    serde_json::json!({
                        "is_transient": false,
                        "message": "Unknown error"
                    })
                });

                return Err(crate::error::Error::RetriesExhausted {
                    error: error_val,
                    attempts: retry_count as u32,
                });
            }
        }

        let started_at: DateTime<Utc> = row.try_get("started_at")?;

        Ok(StepRecord {
            id,
            run_id: row.try_get("run_id")?,
            step_name: row.try_get("step_name")?,
            status,
            input: row.try_get("input")?,
            output: row.try_get("output")?,
            error: row.try_get("error")?,
            created_at: started_at,
            updated_at: started_at,
            retry_at,
            retry_count,
        })
    }

    async fn complete_step(&self, id: i64, output: serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(output)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "COMPLETE_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to complete workflow step {}", id),
        })?;

        Ok(())
    }

    async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        new_retry_count: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW(),
                retry_at = $3, retry_count = $4
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .bind(retry_at)
        .bind(new_retry_count)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "FAIL_WORKFLOW_STEP".into(),
            source: Box::new(e),
            context: format!("Failed to fail workflow step {}", id),
        })?;

        Ok(())
    }
}

#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
