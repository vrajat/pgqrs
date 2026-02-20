use crate::error::Result;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
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
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, started_at
"#;

const SQL_COMPLETE_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, started_at
"#;

const SQL_FAIL_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW(),
    retry_at = $3, retry_count = $4
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, started_at
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

    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord> {
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.clone()),
                QueryParam::DateTime(value) => builder.bind(*value),
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

        let started_at: chrono::DateTime<Utc> = row.try_get("started_at")?;

        Ok(StepRecord {
            id: row.try_get("id")?,
            run_id: row.try_get("run_id")?,
            step_name: row.try_get("step_name")?,
            status: row.try_get("status")?,
            input: row.try_get("input")?,
            output: row.try_get("output")?,
            error: row.try_get("error")?,
            created_at: started_at,
            updated_at: started_at,
            retry_at: row.try_get("retry_at")?,
            retry_count: row.try_get("retry_count")?,
        })
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

#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
