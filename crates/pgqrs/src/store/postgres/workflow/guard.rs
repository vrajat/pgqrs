use crate::error::Result;
use crate::types::{StepRecord, WorkflowStatus};
use crate::StepRetryPolicy;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (run_id, step_id, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW(), 0)
ON CONFLICT (run_id, step_id) DO UPDATE
SET status = CASE
    WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'::pgqrs_workflow_status
    WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'::pgqrs_workflow_status
    ELSE 'RUNNING'::pgqrs_workflow_status
END,
started_at = CASE
    WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
    ELSE NOW()
END
RETURNING run_id, step_id, status, input, output, error, retry_count, retry_at
"#;

const SQL_SCHEDULE_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET retry_count = $1, retry_at = $2, last_retry_at = NOW()
WHERE run_id = $3 AND step_id = $4
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
WHERE run_id = $1 AND step_id = $2
"#;

const SQL_STEP_SUCCESS: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $3, completed_at = NOW()
WHERE run_id = $1 AND step_id = $2
"#;

const SQL_STEP_FAIL: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR'::pgqrs_workflow_status, error = $3, completed_at = NOW()
WHERE run_id = $1 AND step_id = $2
"#;

/// RAII guard for a workflow step execution.
pub struct StepGuard {
    pool: PgPool,
    run_id: i64,
    step_id: String,
    completed: bool,
}

impl StepGuard {
    pub fn new(pool: PgPool, run_id: i64, step_id: &str) -> Self {
        Self {
            pool,
            run_id,
            step_id: step_id.to_string(),
            completed: false,
        }
    }

    pub async fn acquire_record(
        pool: &PgPool,
        run_id: i64,
        step_id: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepRecord> {
        let step_id_string = step_id.to_string();

        let row = sqlx::query(SQL_ACQUIRE_STEP)
            .bind(run_id)
            .bind(&step_id_string)
            .fetch_one(pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_ACQUIRE_STEP".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to acquire step {} for run {}",
                    step_id_string, run_id
                ),
            })?;

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
                    .bind(run_id)
                    .bind(&step_id_string)
                    .execute(pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "SQL_CLEAR_RETRY".into(),
                        source: Box::new(e),
                        context: format!("Failed to clear retry_at for step {}", step_id_string),
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

        Ok(StepRecord {
            id: 0,
            run_id: row.try_get("run_id")?,
            step_id: row.try_get("step_id")?,
            status,
            input: row.try_get("input")?,
            output: row.try_get("output")?,
            error: row.try_get("error")?,
            created_at: Utc::now(), // Placeholder
            updated_at: Utc::now(), // Placeholder
        })
    }
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let pool = self.pool.clone();
            let run_id = self.run_id;
            let step_id = self.step_id.clone();

            tokio::spawn(async move {
                let error_json = serde_json::json!({
                    "is_transient": false,
                    "code": "GUARD_DROPPED",
                    "message": "Step execution interrupted (dropped without completion)",
                });

                let _ = sqlx::query(SQL_STEP_FAIL)
                    .bind(run_id)
                    .bind(step_id)
                    .bind(error_json)
                    .execute(&pool)
                    .await;
            });
        }
    }
}

#[async_trait]
impl crate::store::StepGuard for StepGuard {
    /// Mark the step as successfully completed and persist the output.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        sqlx::query(SQL_STEP_SUCCESS)
            .bind(self.run_id)
            .bind(&self.step_id)
            .bind(output)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("SQL_STEP_SUCCESS ({})", self.step_id),
                source: Box::new(e),
                context: format!("Failed to complete step {}", self.step_id),
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
            let row: (i32,) = sqlx::query_as(
                "SELECT retry_count FROM pgqrs_workflow_steps WHERE run_id = $1 AND step_id = $2",
            )
            .bind(self.run_id)
            .bind(&self.step_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SELECT retry_count".into(),
                source: Box::new(e),
                context: format!("Failed to get retry_count for step {}", self.step_id),
            })?;

            let retry_count = row.0;

            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                sqlx::query(SQL_STEP_FAIL)
                    .bind(self.run_id)
                    .bind(&self.step_id)
                    .bind(error_record)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: format!("SQL_STEP_FAIL ({})", self.step_id),
                        source: Box::new(e),
                        context: format!("Failed to fail step {}", self.step_id),
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
                .bind(self.run_id)
                .bind(&self.step_id)
                .bind(error_record)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("SQL_STEP_FAIL ({})", self.step_id),
                    source: Box::new(e),
                    context: format!("Failed to fail step {}", self.step_id),
                })?;

            sqlx::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(retry_at)
                .bind(self.run_id)
                .bind(&self.step_id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQL_SCHEDULE_RETRY".into(),
                    source: Box::new(e),
                    context: format!("Failed to schedule retry for step {}", self.step_id),
                })?;
        } else {
            sqlx::query(SQL_STEP_FAIL)
                .bind(self.run_id)
                .bind(&self.step_id)
                .bind(error_record)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("SQL_STEP_FAIL ({})", self.step_id),
                    source: Box::new(e),
                    context: format!("Failed to fail step {}", self.step_id),
                })?;
        }

        self.completed = true;
        Ok(())
    }
}
