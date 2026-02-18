use crate::error::Result;
use crate::types::{StepRecord, WorkflowStatus};
use crate::StepRetryPolicy;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

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

const SQL_SCHEDULE_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET retry_count = $1, retry_at = $2, last_retry_at = NOW()
WHERE id = $3
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
WHERE id = $1
"#;

const SQL_STEP_SUCCESS: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
WHERE id = $1
"#;

const SQL_STEP_FAIL: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW()
WHERE id = $1
"#;

/// RAII guard for a workflow step execution.
pub struct StepGuard {
    pool: PgPool,
    id: i64,
    completed: bool,
}

impl StepGuard {
    pub fn new(pool: PgPool, id: i64) -> Self {
        Self {
            pool,
            id,
            completed: false,
        }
    }

    pub async fn acquire_record(
        pool: &PgPool,
        run_id: i64,
        step_name: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepRecord> {
        let step_name_string = step_name.to_string();

        let row = sqlx::query(SQL_ACQUIRE_STEP)
            .bind(run_id)
            .bind(&step_name_string)
            .fetch_one(pool)
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
                    .execute(pool)
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
        })
    }
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let pool = self.pool.clone();
            let id = self.id;

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let error_json = serde_json::json!({
                        "is_transient": false,
                        "code": "GUARD_DROPPED",
                        "message": "Step execution interrupted (dropped without completion)",
                    });

                    let _ = sqlx::query(SQL_STEP_FAIL)
                        .bind(id)
                        .bind(error_json)
                        .execute(&pool)
                        .await;
                });
            }
        }
    }
}

#[async_trait]
impl crate::store::StepGuard for StepGuard {
    /// Mark the step as successfully completed and persist the output.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        sqlx::query(SQL_STEP_SUCCESS)
            .bind(self.id)
            .bind(output)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("SQL_STEP_SUCCESS ({})", self.id),
                source: Box::new(e),
                context: format!("Failed to complete step {}", self.id),
            })?;

        self.completed = true;
        Ok(())
    }

    async fn fail_with_json(
        &mut self,
        error: serde_json::Value,
        current_time: DateTime<Utc>,
    ) -> crate::error::Result<()> {
        let error_record = if error.get("is_transient").is_some() {
            error
        } else {
            serde_json::json!({
                "is_transient": false,
                "code": "NON_RETRYABLE",
                "message": error.to_string(),
            })
        };

        let is_transient = error_record
            .get("is_transient")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_transient {
            let row: (i32,) =
                sqlx::query_as("SELECT retry_count FROM pgqrs_workflow_steps WHERE id = $1")
                    .bind(self.id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "SELECT retry_count".into(),
                        source: Box::new(e),
                        context: format!("Failed to get retry_count for step {}", self.id),
                    })?;

            let retry_count = row.0;

            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                sqlx::query(SQL_STEP_FAIL)
                    .bind(self.id)
                    .bind(error_record)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: format!("SQL_STEP_FAIL ({})", self.id),
                        source: Box::new(e),
                        context: format!("Failed to fail step {}", self.id),
                    })?;

                self.completed = true;
                return Ok(());
            }

            let delay_seconds = policy.extract_retry_delay(&error_record, retry_count);
            let delay_i64 =
                delay_seconds
                    .try_into()
                    .map_err(|_| crate::error::Error::Internal {
                        message: format!(
                            "Retry delay {} seconds exceeds maximum allowed value (i64::MAX)",
                            delay_seconds
                        ),
                    })?;

            let retry_at = current_time + chrono::Duration::seconds(delay_i64);
            if retry_at < current_time {
                return Err(crate::error::Error::ValidationFailed {
                    reason: format!(
                        "Invalid retry_at: {} is before current_time {}",
                        retry_at, current_time
                    ),
                });
            }

            let new_retry_count = retry_count + 1;

            sqlx::query(SQL_STEP_FAIL)
                .bind(self.id)
                .bind(error_record)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("SQL_STEP_FAIL ({})", self.id),
                    source: Box::new(e),
                    context: format!("Failed to fail step {}", self.id),
                })?;

            sqlx::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(retry_at)
                .bind(self.id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQL_SCHEDULE_RETRY".into(),
                    source: Box::new(e),
                    context: format!("Failed to schedule retry for step {}", self.id),
                })?;
        } else {
            sqlx::query(SQL_STEP_FAIL)
                .bind(self.id)
                .bind(error_record)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("SQL_STEP_FAIL ({})", self.id),
                    source: Box::new(e),
                    context: format!("Failed to fail step {}", self.id),
                })?;
        }

        self.completed = true;
        Ok(())
    }
}
