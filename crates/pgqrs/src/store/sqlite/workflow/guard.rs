use crate::error::Result;
use crate::types::{StepRetryPolicy, WorkflowStatus};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (workflow_id, step_key, status, started_at)
VALUES ($1, $2, 'RUNNING', datetime('now'))
ON CONFLICT (workflow_id, step_key) DO UPDATE
SET status = CASE
    WHEN status = 'SUCCESS' THEN 'SUCCESS'
    WHEN status = 'ERROR' THEN 'ERROR'
    ELSE 'RUNNING'
END,
started_at = CASE
    WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
    ELSE datetime('now')
END
RETURNING status, output, error, retry_count
"#;

const SQL_RETRY_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING', retry_count = $3, last_retry_at = datetime('now'), error = NULL
WHERE workflow_id = $1 AND step_key = $2
"#;

const SQL_STEP_SUCCESS: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS', output = $3, completed_at = datetime('now')
WHERE workflow_id = $1 AND step_key = $2
"#;

const SQL_STEP_FAIL: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR', error = $3, completed_at = datetime('now')
WHERE workflow_id = $1 AND step_key = $2
"#;

pub struct SqliteStepGuard {
    pool: SqlitePool,
    workflow_id: i64,
    step_id: String,
    completed: bool,
}

impl SqliteStepGuard {
    pub async fn acquire<T: DeserializeOwned + 'static>(
        pool: &SqlitePool,
        workflow_id: i64,
        step_id: &str,
    ) -> Result<crate::store::StepResult<T>> {
        let step_id_string = step_id.to_string();

        let row = sqlx::query(SQL_ACQUIRE_STEP)
            .bind(workflow_id)
            .bind(&step_id_string)
            .fetch_one(pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_ACQUIRE_STEP".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to acquire step {} for workflow {}",
                    step_id_string, workflow_id
                ),
            })?;

        let status_str: String = row.try_get("status")?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let output_str: Option<String> = row.try_get("output")?;
        let error_str: Option<String> = row.try_get("error")?;
        let retry_count: i32 = row.try_get("retry_count").unwrap_or(0);

        if status == WorkflowStatus::Success {
            let output_val: serde_json::Value = if let Some(s) = output_str {
                serde_json::from_str(&s)?
            } else {
                serde_json::Value::Null
            };

            let result: T =
                serde_json::from_value(output_val).map_err(crate::error::Error::Serialization)?;
            return Ok(crate::store::StepResult::Skipped(result));
        }

        if status == WorkflowStatus::Error {
            let error_val: serde_json::Value = if let Some(s) = error_str {
                serde_json::from_str(&s)?
            } else {
                serde_json::json!({
                    "is_transient": false,
                    "message": "Unknown error"
                })
            };

            // Check if error is transient
            let is_transient = error_val
                .get("is_transient")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if !is_transient {
                // Non-transient error, fail immediately
                return Err(crate::error::Error::RetriesExhausted {
                    error: error_val,
                    attempts: retry_count as u32,
                });
            }

            // Check retry policy
            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                // Retries exhausted
                return Err(crate::error::Error::RetriesExhausted {
                    error: error_val,
                    attempts: retry_count as u32,
                });
            }

            // Calculate backoff delay
            let delay_seconds =
                if let Some(retry_after) = error_val.get("retry_after").and_then(|v| v.as_u64()) {
                    // Use custom delay from error (e.g., Retry-After header)
                    retry_after
                } else {
                    // Use policy backoff
                    policy.calculate_delay(retry_count as u32) as u64
                };

            tracing::info!(
                "Step {} (workflow {}) failed with transient error (attempt {}), retrying after {}s",
                step_id_string,
                workflow_id,
                retry_count,
                delay_seconds
            );

            // Sleep for backoff delay
            tokio::time::sleep(Duration::from_secs(delay_seconds)).await;

            // Increment retry count and reset step to RUNNING
            let new_retry_count = retry_count + 1;
            sqlx::query(SQL_RETRY_STEP)
                .bind(workflow_id)
                .bind(&step_id_string)
                .bind(new_retry_count)
                .execute(pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQL_RETRY_STEP".into(),
                    source: Box::new(e),
                    context: format!("Failed to reset step {} for retry", step_id_string),
                })?;

            tracing::info!(
                "Step {} (workflow {}) reset to RUNNING for retry attempt {}",
                step_id_string,
                workflow_id,
                new_retry_count
            );

            // Return guard for retry execution
            return Ok(crate::store::StepResult::Execute(Box::new(Self {
                pool: pool.clone(),
                workflow_id,
                step_id: step_id_string,
                completed: false,
            })));
        }

        Ok(crate::store::StepResult::Execute(Box::new(Self {
            pool: pool.clone(),
            workflow_id,
            step_id: step_id_string,
            completed: false,
        })))
    }
}

impl Drop for SqliteStepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let pool = self.pool.clone();
            let workflow_id = self.workflow_id;
            let step_id = self.step_id.clone();

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let error_json = serde_json::json!({
                        "is_transient": false,
                        "code": "GUARD_DROPPED",
                        "message": "Step execution interrupted (dropped without completion)",
                    })
                    .to_string();

                    let _ = sqlx::query(SQL_STEP_FAIL)
                        .bind(workflow_id)
                        .bind(step_id)
                        .bind(error_json)
                        .execute(&pool)
                        .await;
                });
            }
        }
    }
}

#[async_trait]
impl crate::store::StepGuard for SqliteStepGuard {
    async fn complete(&mut self, output: serde_json::Value) -> Result<()> {
        let output_str = output.to_string();
        sqlx::query(SQL_STEP_SUCCESS)
            .bind(self.workflow_id)
            .bind(&self.step_id)
            .bind(output_str)
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

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        // Ensure error has is_transient field
        let error_record = if error.get("is_transient").is_some() {
            // Error already has is_transient field (from TransientStepError)
            error
        } else {
            // Wrap as non-transient error
            serde_json::json!({
                "is_transient": false,
                "code": "NON_RETRYABLE",
                "message": error.to_string(),
            })
        };

        let error_str = error_record.to_string();
        sqlx::query(SQL_STEP_FAIL)
            .bind(self.workflow_id)
            .bind(&self.step_id)
            .bind(error_str)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("SQL_STEP_FAIL ({})", self.step_id),
                source: Box::new(e),
                context: format!("Failed to fail step {}", self.step_id),
            })?;

        self.completed = true;
        Ok(())
    }
}
