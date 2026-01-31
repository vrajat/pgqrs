use super::WorkflowStatus;
use crate::error::Result;
use crate::types::StepRetryPolicy;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::time::Duration;

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (workflow_id, step_id, status, started_at)
VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW())
ON CONFLICT (workflow_id, step_id) DO UPDATE
SET status = CASE
    WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'::pgqrs_workflow_status
    WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'::pgqrs_workflow_status
    ELSE 'RUNNING'::pgqrs_workflow_status
END,
started_at = CASE
    WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
    ELSE NOW()
END
RETURNING status, output, error, retry_count
"#;

const SQL_RETRY_STEP: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_count = $3, last_retry_at = NOW(), error = NULL
WHERE workflow_id = $1 AND step_id = $2
"#;

const SQL_STEP_SUCCESS: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $3, completed_at = NOW()
WHERE workflow_id = $1 AND step_id = $2
"#;

const SQL_STEP_FAIL: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR'::pgqrs_workflow_status, error = $3, completed_at = NOW()
WHERE workflow_id = $1 AND step_id = $2
"#;

/// RAII guard for a workflow step execution.
///
/// Ensures exactly-once logical execution of steps by persisting state to the database.
pub struct StepGuard {
    pool: PgPool,
    workflow_id: i64,
    step_id: String,
    completed: bool,
}

impl StepGuard {
    /// Attempt to start a step.
    ///
    /// Checks the database state for the given `workflow_id` and `step_id`.
    /// - If the step is already `SUCCESS`, returns `StepResult::Skipped` with the deserialized output.
    /// - If the step is `ERROR`, checks if it's transient and retries if configured.
    /// - If the step is `PENDING`, `RUNNING` (retry), marks it as `RUNNING` and returns `StepResult::Execute`.
    pub async fn acquire<T: DeserializeOwned + 'static>(
        pool: &PgPool,
        workflow_id: i64,
        step_id: &str,
    ) -> Result<crate::store::StepResult<T>> {
        let step_id_string = step_id.to_string();

        let row: (
            WorkflowStatus,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            i32,
        ) = sqlx::query_as(SQL_ACQUIRE_STEP)
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

        let (status, output, error, retry_count) = row;

        if status == WorkflowStatus::Success {
            // Step already completed, deserialize output
            let output_val = output.unwrap_or(serde_json::Value::Null);
            let result: T =
                serde_json::from_value(output_val).map_err(crate::error::Error::Serialization)?;
            return Ok(crate::store::StepResult::Skipped(result));
        }

        if status == WorkflowStatus::Error {
            // Step failed previously - check if we should retry
            let error_val = error.unwrap_or_else(|| {
                serde_json::json!({
                    "is_transient": false,
                    "message": "Unknown error"
                })
            });

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
            let guard = StepGuard {
                pool: pool.clone(),
                workflow_id,
                step_id: step_id_string,
                completed: false,
            };

            return Ok(crate::store::StepResult::Execute(Box::new(guard)));
        }

        let guard = StepGuard {
            pool: pool.clone(),
            workflow_id,
            step_id: step_id_string,
            completed: false,
        };

        Ok(crate::store::StepResult::Execute(Box::new(guard)))
    }
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let pool = self.pool.clone();
            let workflow_id = self.workflow_id;
            let step_id = self.step_id.clone();

            // Best-effort attempt to mark as error if dropped unexpectedly.
            // We use tokio::spawn to run this asynchronously.
            // Note: This relies on a tokio runtime being available.
            tokio::spawn(async move {
                let error_json = serde_json::json!({
                    "is_transient": false,
                    "code": "GUARD_DROPPED",
                    "message": "Step execution interrupted (dropped without completion)",
                });

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

#[async_trait]
impl crate::store::StepGuard for StepGuard {
    /// Mark the step as successfully completed and persist the output.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        sqlx::query(SQL_STEP_SUCCESS)
            .bind(self.workflow_id)
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

    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()> {
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

        sqlx::query(SQL_STEP_FAIL)
            .bind(self.workflow_id)
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
        Ok(())
    }
}
