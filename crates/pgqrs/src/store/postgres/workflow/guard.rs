use super::WorkflowStatus;
use crate::error::Result;
use crate::types::StepRetryPolicy;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use sqlx::PgPool;

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (workflow_id, step_id, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW(), 0)
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
RETURNING status, output, error, retry_count, retry_at
"#;

const SQL_SCHEDULE_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET retry_count = $1, retry_at = $2, last_retry_at = NOW()
WHERE workflow_id = $3 AND step_id = $4
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
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
    /// - If the step is `ERROR`, checks if it's transient and schedules retry if configured.
    /// - If the step is `PENDING`, `RUNNING` (retry), marks it as `RUNNING` and returns `StepResult::Execute`.
    pub async fn acquire<T: DeserializeOwned + 'static>(
        pool: &PgPool,
        workflow_id: i64,
        step_id: &str,
        current_time: DateTime<Utc>,
    ) -> Result<crate::store::StepResult<T>> {
        let step_id_string = step_id.to_string();

        let row: (
            WorkflowStatus,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            i32,
            Option<DateTime<Utc>>,
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

        let (status, output, error, retry_count, retry_at) = row;

        if status == WorkflowStatus::Success {
            // Step already completed, deserialize output
            let output_val = output.unwrap_or(serde_json::Value::Null);
            let result: T =
                serde_json::from_value(output_val).map_err(crate::error::Error::Serialization)?;
            return Ok(crate::store::StepResult::Skipped(result));
        }

        if status == WorkflowStatus::Error {
            // Check if retry is scheduled
            if let Some(retry_at_time) = retry_at {
                if current_time < retry_at_time {
                    // Not ready yet - return StepNotReady
                    return Err(crate::error::Error::StepNotReady {
                        retry_at: retry_at_time,
                        retry_count: retry_count as u32,
                    });
                }

                // WARNING: KNOWN RACE CONDITION - If two workers poll simultaneously when retry_at
                // is ready, both may receive Execute guards. This requires SELECT FOR UPDATE or
                // optimistic locking to fix properly. Accepted by design - requires larger redesign.

                // Time to retry! Clear retry_at and proceed
                sqlx::query(SQL_CLEAR_RETRY)
                    .bind(workflow_id)
                    .bind(&step_id_string)
                    .execute(pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "SQL_CLEAR_RETRY".into(),
                        source: Box::new(e),
                        context: format!("Failed to clear retry_at for step {}", step_id_string),
                    })?;

                tracing::info!(
                    "Step {} (workflow {}) ready for retry (attempt {}, scheduled at {})",
                    step_id_string,
                    workflow_id,
                    retry_count + 1,
                    retry_at_time
                );

                let guard = StepGuard {
                    pool: pool.clone(),
                    workflow_id,
                    step_id: step_id_string,
                    completed: false,
                };

                return Ok(crate::store::StepResult::Execute(Box::new(guard)));
            }

            // No retry scheduled, need to schedule one
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
            let delay_seconds = if let Some(retry_after_val) = error_val.get("retry_after") {
                if let Some(secs) = retry_after_val.as_u64() {
                    // Use custom delay from error as plain seconds (e.g., Retry-After header)
                    secs
                } else if let Some(secs) = retry_after_val.get("secs").and_then(|v| v.as_u64()) {
                    // Use custom delay from error when serialized as a Duration { secs, nanos }
                    secs
                } else {
                    // Use policy backoff if retry_after is present but not in a supported format
                    policy.calculate_delay(retry_count as u32) as u64
                }
            } else {
                // Use policy backoff when no custom retry_after is provided
                policy.calculate_delay(retry_count as u32) as u64
            };

            // Schedule retry for future
            let new_retry_count = retry_count + 1;

            // Validate delay_seconds fits within i64::MAX to prevent overflow
            let delay_i64 =
                delay_seconds
                    .try_into()
                    .map_err(|_| crate::error::Error::Internal {
                        message: format!(
                            "Retry delay {} seconds exceeds maximum allowed value (i64::MAX)",
                            delay_seconds
                        ),
                    })?;

            let retry_at_time = current_time + chrono::Duration::seconds(delay_i64);

            // Validate retry_at is not in the past (allow immediate retry when delay is 0)
            if retry_at_time < current_time {
                return Err(crate::error::Error::ValidationFailed {
                    reason: format!(
                        "Invalid retry_at: {} is before current_time {}",
                        retry_at_time, current_time
                    ),
                });
            }

            sqlx::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(retry_at_time)
                .bind(workflow_id)
                .bind(&step_id_string)
                .execute(pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQL_SCHEDULE_RETRY".into(),
                    source: Box::new(e),
                    context: format!("Failed to schedule retry for step {}", step_id_string),
                })?;

            tracing::info!(
                "Step {} (workflow {}) scheduled for retry at {} ({} previous failures, delay {}s)",
                step_id_string,
                workflow_id,
                retry_at_time,
                retry_count,
                delay_seconds
            );

            // Return StepNotReady - worker should try other work
            return Err(crate::error::Error::StepNotReady {
                retry_at: retry_at_time,
                retry_count: new_retry_count as u32,
            });
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
