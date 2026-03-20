use crate::error::Result;
use crate::store::postgres::dialect::PostgresDialect;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::tables::DialectStepTable;
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{PgPool, Row};

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

    async fn acquire_step(&self, run_id: i64, step_name: &str) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_acquire_step(self, run_id, step_name).await
    }

    async fn clear_retry(&self, id: i64) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_clear_retry(self, id).await
    }

    async fn complete_step(&self, id: i64, output: serde_json::Value) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_complete_step(self, id, output).await
    }

    async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_fail_step(self, id, error, retry_at, retry_count).await
    }

    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord> {
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }

        let row =
            builder
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "POSTGRES_EXECUTE_STEP".into(),
                    source: Box::new(e),
                    context: "Failed to execute postgres workflow step query".into(),
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
}

impl DialectStepTable for StepRecords {
    type Dialect = PostgresDialect;
}

#[allow(dead_code)]
fn _status_typecheck(_s: WorkflowStatus) {}
