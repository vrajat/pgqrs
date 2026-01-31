use crate::error::Result;
use crate::store::{StepGuard, StepResult};
use crate::types::StepRetryPolicy;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use turso::Database;

pub struct TursoStepGuard {
    db: Arc<Database>,
    workflow_id: i64,
    step_id: String,
    completed: bool,
}

const SQL_ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (workflow_id, step_key, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (workflow_id, step_key) DO UPDATE SET workflow_id=workflow_id
    RETURNING status, output, error, retry_count, retry_at
"#;

const SQL_SCHEDULE_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET retry_count = ?, retry_at = ?, last_retry_at = datetime('now')
    WHERE workflow_id = ? AND step_key = ?
"#;

const SQL_CLEAR_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE workflow_id = ? AND step_key = ?
"#;

const SQL_STEP_SUCCESS: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS', output = ?, completed_at = datetime('now')
    WHERE workflow_id = ? AND step_key = ?
"#;

const SQL_STEP_FAIL: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR', error = ?, completed_at = datetime('now')
    WHERE workflow_id = ? AND step_key = ?
"#;

impl TursoStepGuard {
    pub async fn acquire(
        db: &Arc<Database>,
        workflow_id: i64,
        step_id: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepResult<serde_json::Value>> {
        let row = crate::store::turso::query(SQL_ACQUIRE_STEP)
            .bind(workflow_id)
            .bind(step_id)
            .fetch_one_once(db)
            .await?;

        let status: String = row.get(0).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;

        if status == "SUCCESS" {
            let output_str: Option<String> =
                row.get(1).map_err(|e| crate::error::Error::Internal {
                    message: e.to_string(),
                })?;
            let output = if let Some(s) = output_str {
                serde_json::from_str(&s)?
            } else {
                serde_json::Value::Null
            };
            return Ok(StepResult::Skipped(output));
        }

        if status == "ERROR" {
            let error_str: Option<String> =
                row.get(2).map_err(|e| crate::error::Error::Internal {
                    message: e.to_string(),
                })?;
            let retry_count: i32 = row.get(3).map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;
            let retry_at_str: Option<String> =
                row.get(4).map_err(|e| crate::error::Error::Internal {
                    message: e.to_string(),
                })?;

            // Check if retry is scheduled
            if let Some(retry_at_s) = retry_at_str {
                let retry_at = crate::store::turso::parse_turso_timestamp(&retry_at_s)?;
                if current_time < retry_at {
                    // Not ready yet
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }
                // WARNING: KNOWN RACE CONDITION - If two workers poll simultaneously when retry_at
                // is ready, both may receive Execute guards. This requires SELECT FOR UPDATE or
                // optimistic locking to fix properly. Accepted by design - requires larger redesign.

                // Time to retry! Clear retry_at and proceed
                crate::store::turso::query(SQL_CLEAR_RETRY)
                    .bind(workflow_id)
                    .bind(step_id)
                    .execute_once(db)
                    .await?;

                tracing::info!(
                    "Step {} (workflow {}) ready for retry (attempt {}, scheduled at {})",
                    step_id,
                    workflow_id,
                    retry_count + 1,
                    retry_at
                );

                return Ok(StepResult::Execute(Box::new(Self {
                    db: db.clone(),
                    workflow_id,
                    step_id: step_id.to_string(),
                    completed: false,
                })));
            }

            // No retry scheduled, need to schedule one
            let error_json: serde_json::Value = if let Some(s) = &error_str {
                serde_json::from_str(s)?
            } else {
                serde_json::json!({
                    "is_transient": false,
                    "message": "Unknown error"
                })
            };

            // Check if error is transient
            let is_transient = error_json
                .get("is_transient")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if !is_transient {
                // Non-transient error, fail immediately
                return Err(crate::error::Error::RetriesExhausted {
                    error: error_json,
                    attempts: retry_count as u32,
                });
            }

            // Check retry policy
            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                // Retries exhausted
                return Err(crate::error::Error::RetriesExhausted {
                    error: error_json,
                    attempts: retry_count as u32,
                });
            }

            // Calculate backoff delay
            let delay_seconds = if let Some(retry_after_value) = error_json.get("retry_after") {
                if let Some(seconds) = retry_after_value.as_u64() {
                    seconds
                } else if let Some(seconds) = retry_after_value.get("secs").and_then(|v| v.as_u64())
                {
                    seconds
                } else {
                    policy.calculate_delay(retry_count as u32) as u64
                }
            } else {
                policy.calculate_delay(retry_count as u32) as u64
            };

            // Schedule retry for future
            let new_retry_count = retry_count + 1;
            let retry_at = current_time + chrono::Duration::seconds(delay_seconds as i64);

            // Validate retry_at is in the future
            if retry_at <= current_time {
                return Err(crate::error::Error::Internal {
                    message: format!(
                        "Invalid retry_at: {} is not after current_time {}",
                        retry_at, current_time
                    ),
                });
            }

            let retry_at_str = crate::store::turso::format_turso_timestamp(&retry_at);

            crate::store::turso::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(retry_at_str)
                .bind(workflow_id)
                .bind(step_id)
                .execute_once(db)
                .await?;

            tracing::info!(
                "Step {} (workflow {}) scheduled for retry at {} ({} previous failures, delay {}s)",
                step_id,
                workflow_id,
                retry_at,
                retry_count,
                delay_seconds
            );

            // Return StepNotReady - worker should try other work
            return Err(crate::error::Error::StepNotReady {
                retry_at,
                retry_count: new_retry_count as u32,
            });
        }

        // Step is RUNNING or in other state, proceed with execution
        Ok(StepResult::Execute(Box::new(Self {
            db: db.clone(),
            workflow_id,
            step_id: step_id.to_string(),
            completed: false,
        })))
    }
}

#[async_trait]
impl StepGuard for TursoStepGuard {
    async fn complete(&mut self, output: serde_json::Value) -> Result<()> {
        self.completed = true;
        let output_str = output.to_string();
        crate::store::turso::query(SQL_STEP_SUCCESS)
            .bind(output_str)
            .bind(self.workflow_id)
            .bind(self.step_id.as_str())
            .execute_once(&self.db)
            .await?;
        Ok(())
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        self.completed = true;

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
        crate::store::turso::query(SQL_STEP_FAIL)
            .bind(error_str)
            .bind(self.workflow_id)
            .bind(self.step_id.as_str())
            .execute_once(&self.db)
            .await?;
        Ok(())
    }
}

impl Drop for TursoStepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let db = self.db.clone();
            let workflow_id = self.workflow_id;
            let step_id = self.step_id.clone();

            tokio::spawn(async move {
                // Wrap drop error as non-transient
                let error = serde_json::json!({
                    "is_transient": false,
                    "code": "GUARD_DROPPED",
                    "message": "Step dropped without completion",
                });
                let error_str = error.to_string();

                let _ = crate::store::turso::query(SQL_STEP_FAIL)
                    .bind(error_str)
                    .bind(workflow_id)
                    .bind(step_id)
                    .execute_once(&db)
                    .await;
            });
        }
    }
}
