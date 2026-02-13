use crate::error::Result;
use crate::policy::StepRetryPolicy;
use crate::store::StepGuard;
use crate::types::{StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

pub struct TursoStepGuard {
    db: Arc<Database>,
    id: i64,
    completed: bool,
}

const SQL_ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE SET run_id=run_id
    RETURNING id, status, output, error, retry_count, retry_at
"#;

const SQL_SCHEDULE_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET retry_count = ?, retry_at = ?, last_retry_at = datetime('now')
    WHERE id = ?
"#;

const SQL_CLEAR_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE id = ?
"#;

const SQL_STEP_SUCCESS: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS', output = ?, completed_at = datetime('now')
    WHERE id = ?
"#;

const SQL_STEP_FAIL: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR', error = ?, completed_at = datetime('now')
    WHERE id = ?
"#;

impl TursoStepGuard {
    pub fn new(db: Arc<Database>, id: i64) -> Self {
        Self {
            db,
            id,
            completed: false,
        }
    }

    pub async fn acquire_record(
        db: &Arc<Database>,
        run_id: i64,
        step_name: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepRecord> {
        let row = crate::store::turso::query(SQL_ACQUIRE_STEP)
            .bind(run_id)
            .bind(step_name)
            .fetch_one_once(db)
            .await?;

        let id: i64 = row.get(0).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let status_str: String = row.get(1).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let error_str: Option<String> = row.get(3).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let retry_count: i32 = row.get(4).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let retry_at_str: Option<String> =
            row.get(5).map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        if status == WorkflowStatus::Error {
            if let Some(retry_at_s) = retry_at_str {
                let retry_at = crate::store::turso::parse_turso_timestamp(&retry_at_s)?;
                if current_time < retry_at {
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }

                crate::store::turso::query(SQL_CLEAR_RETRY)
                    .bind(id)
                    .execute_once(db)
                    .await?;
            } else {
                let error_val: serde_json::Value = if let Some(s) = error_str {
                    serde_json::from_str(&s)?
                } else {
                    serde_json::json!({
                        "is_transient": false,
                        "message": "Unknown error"
                    })
                };

                return Err(crate::error::Error::RetriesExhausted {
                    error: error_val,
                    attempts: retry_count as u32,
                });
            }
        }

        // Fetch full record
        let row = crate::store::turso::query("SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at FROM pgqrs_workflow_steps WHERE id = ?")
            .bind(id)
            .fetch_one_once(db)
            .await?;

        Ok(StepRecord {
            id: row.get(0).unwrap(),
            run_id: row.get(1).unwrap(),
            step_name: row.get(2).unwrap(),
            status: WorkflowStatus::from_str(&row.get::<String>(3).unwrap()).unwrap(),
            input: row
                .get::<Option<String>>(4)
                .unwrap()
                .and_then(|s| serde_json::from_str(&s).ok()),
            output: row
                .get::<Option<String>>(5)
                .unwrap()
                .and_then(|s| serde_json::from_str(&s).ok()),
            error: row
                .get::<Option<String>>(6)
                .unwrap()
                .and_then(|s| serde_json::from_str(&s).ok()),
            created_at: crate::store::turso::parse_turso_timestamp(&row.get::<String>(7).unwrap())?,
            updated_at: crate::store::turso::parse_turso_timestamp(&row.get::<String>(8).unwrap())?,
        })
    }
}

#[async_trait]
impl StepGuard for TursoStepGuard {
    async fn complete(&mut self, output: serde_json::Value) -> Result<()> {
        self.completed = true;
        let output_str = output.to_string();
        crate::store::turso::query(SQL_STEP_SUCCESS)
            .bind(output_str)
            .bind(self.id)
            .execute_once(&self.db)
            .await?;
        Ok(())
    }

    async fn fail_with_json(
        &mut self,
        error: serde_json::Value,
        current_time: DateTime<Utc>,
    ) -> Result<()> {
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
            let row = crate::store::turso::query(
                "SELECT retry_count FROM pgqrs_workflow_steps WHERE id = ?",
            )
            .bind(self.id)
            .fetch_one_once(&self.db)
            .await?;

            let retry_count: i32 = row.get(0).map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                let error_str = error_record.to_string();
                crate::store::turso::query(SQL_STEP_FAIL)
                    .bind(error_str)
                    .bind(self.id)
                    .execute_once(&self.db)
                    .await?;
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

            let retry_at_str = crate::store::turso::format_turso_timestamp(&retry_at);
            let new_retry_count = retry_count + 1;

            let error_str = error_record.to_string();
            crate::store::turso::query(SQL_STEP_FAIL)
                .bind(error_str)
                .bind(self.id)
                .execute_once(&self.db)
                .await?;

            crate::store::turso::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(retry_at_str)
                .bind(self.id)
                .execute_once(&self.db)
                .await?;
        } else {
            let error_str = error_record.to_string();
            crate::store::turso::query(SQL_STEP_FAIL)
                .bind(error_str)
                .bind(self.id)
                .execute_once(&self.db)
                .await?;
        }

        self.completed = true;
        Ok(())
    }
}

impl Drop for TursoStepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let db = self.db.clone();
            let id = self.id;

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let error = serde_json::json!({
                        "is_transient": false,
                        "code": "GUARD_DROPPED",
                        "message": "Step dropped without completion",
                    });
                    let error_str = error.to_string();

                    let _ = crate::store::turso::query(SQL_STEP_FAIL)
                        .bind(error_str)
                        .bind(id)
                        .execute_once(&db)
                        .await;
                });
            }
        }
    }
}
