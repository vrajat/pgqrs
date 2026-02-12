use crate::error::Result;
use crate::types::{StepRetryPolicy, WorkflowStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

const SQL_ACQUIRE_STEP: &str = r#"
INSERT INTO pgqrs_workflow_steps (run_id, step_id, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING', datetime('now'), 0)
ON CONFLICT (run_id, step_id) DO UPDATE
SET status = CASE
    WHEN status = 'SUCCESS' THEN 'SUCCESS'
    WHEN status = 'ERROR' THEN 'ERROR'
    ELSE 'RUNNING'
END,
started_at = CASE
    WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
    ELSE datetime('now')
END
RETURNING status, output, error, retry_count, retry_at
"#;

const SQL_SCHEDULE_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET retry_count = $1, retry_at = $2, last_retry_at = datetime('now')
WHERE run_id = $3 AND step_id = $4
"#;

const SQL_CLEAR_RETRY: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING', retry_at = NULL, error = NULL
WHERE run_id = $1 AND step_id = $2
"#;

const SQL_STEP_SUCCESS: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS', output = $3, completed_at = datetime('now')
WHERE run_id = $1 AND step_id = $2
"#;

const SQL_STEP_FAIL: &str = r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR', error = $3, completed_at = datetime('now')
WHERE run_id = $1 AND step_id = $2
"#;

pub struct SqliteStepGuard {
    pool: SqlitePool,
    run_id: i64,
    step_id: String,
    completed: bool,
}

impl SqliteStepGuard {
    pub async fn acquire<T: DeserializeOwned + 'static>(
        pool: &SqlitePool,
        run_id: i64,
        step_id: &str,
        current_time: DateTime<Utc>,
    ) -> Result<crate::store::StepResult<T>> {
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

        let status_str: String = row.try_get("status")?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let output_str: Option<String> = row.try_get("output")?;
        let error_str: Option<String> = row.try_get("error")?;
        let retry_count: i32 = row.try_get("retry_count")?;
        let retry_at_str: Option<String> = row.try_get("retry_at")?;

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
            if let Some(retry_at_s) = retry_at_str {
                let retry_at = parse_sqlite_timestamp(&retry_at_s)?;
                if current_time < retry_at {
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }

                // time to retry: clear retry fields and proceed
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

                return Ok(crate::store::StepResult::Execute(Box::new(Self {
                    pool: pool.clone(),
                    run_id,
                    step_id: step_id_string,
                    completed: false,
                })));
            }

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

        Ok(crate::store::StepResult::Execute(Box::new(Self {
            pool: pool.clone(),
            run_id,
            step_id: step_id_string,
            completed: false,
        })))
    }
}

impl Drop for SqliteStepGuard {
    fn drop(&mut self) {
        if !self.completed {
            let pool = self.pool.clone();
            let run_id = self.run_id;
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
                        .bind(run_id)
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
            .bind(self.run_id)
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
            let row = sqlx::query(
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

            let retry_count: i32 = row.try_get("retry_count")?;

            let policy = StepRetryPolicy::default();
            if !policy.should_retry(retry_count as u32) {
                let error_str = error_record.to_string();
                sqlx::query(SQL_STEP_FAIL)
                    .bind(self.run_id)
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

            let retry_at_str = format_sqlite_timestamp(&retry_at);
            let new_retry_count = retry_count + 1;

            let error_str = error_record.to_string();
            sqlx::query(SQL_STEP_FAIL)
                .bind(self.run_id)
                .bind(&self.step_id)
                .bind(error_str)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("SQL_STEP_FAIL ({})", self.step_id),
                    source: Box::new(e),
                    context: format!("Failed to fail step {}", self.step_id),
                })?;

            sqlx::query(SQL_SCHEDULE_RETRY)
                .bind(new_retry_count)
                .bind(&retry_at_str)
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
            let error_str = error_record.to_string();
            sqlx::query(SQL_STEP_FAIL)
                .bind(self.run_id)
                .bind(&self.step_id)
                .bind(error_str)
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

fn format_sqlite_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_sqlite_timestamp(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| crate::error::Error::Internal {
            message: format!("Invalid timestamp '{}': {}", s, e),
        })
}
