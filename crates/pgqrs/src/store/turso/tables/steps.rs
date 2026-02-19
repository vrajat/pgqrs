use crate::error::Result;
use crate::store::turso::parse_turso_timestamp;
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoStepRecordTable {
    db: Arc<Database>,
}

const SQL_ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE
    SET status = CASE
        WHEN status = 'SUCCESS' THEN 'SUCCESS'
        WHEN status = 'ERROR' THEN 'ERROR'
        ELSE 'RUNNING'
    END,
    started_at = CASE
        WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
        ELSE datetime('now')
    END
    RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#;

const SQL_CLEAR_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE id = ?
"#;

impl TursoStepRecordTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<StepRecord> {
        let id: i64 = row.get(0)?;
        let run_id: i64 = row.get(1)?;
        let step_name: String = row.get(2)?;

        let status_str: String = row.get(3)?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let input_str: Option<String> = row.get(4)?;
        let input: Option<Value> = match input_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let output_str: Option<String> = row.get(5)?;
        let output: Option<Value> = match output_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let error_str: Option<String> = row.get(6)?;
        let error: Option<Value> = match error_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let created_at = parse_turso_timestamp(&row.get::<String>(7)?)?;
        let updated_at = parse_turso_timestamp(&row.get::<String>(8)?)?;
        let retry_at_str: Option<String> = row.get(9)?;
        let retry_at = match retry_at_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let retry_count: i32 = row.get(10)?;

        Ok(StepRecord {
            id,
            run_id,
            step_name,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
            retry_at,
            retry_count,
        })
    }
}

#[async_trait]
impl crate::store::StepRecordTable for TursoStepRecordTable {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        let input_str = data.input.map(|v| v.to_string());

        let row = crate::store::turso::query(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, input)
            VALUES (?, ?, 'PENDING', ?)
            RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            "#,
        )
        .bind(data.run_id)
        .bind(data.step_name.as_str())
        .bind(input_str)
        .fetch_one_once(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        let row = crate::store::turso::query(
            r#"
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            FROM pgqrs_workflow_steps
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        let rows = crate::store::turso::query(
            r#"
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            FROM pgqrs_workflow_steps
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.db)
        .await?;

        let mut steps = Vec::with_capacity(rows.len());
        for row in rows {
            steps.push(Self::map_row(&row)?);
        }

        Ok(steps)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 =
            crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workflow_steps")
                .fetch_one(&self.db)
                .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query("DELETE FROM pgqrs_workflow_steps WHERE id = ?")
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }

    async fn acquire_step(
        &self,
        run_id: i64,
        step_name: &str,
        current_time: DateTime<Utc>,
    ) -> Result<StepRecord> {
        let row = crate::store::turso::query(SQL_ACQUIRE_STEP)
            .bind(run_id)
            .bind(step_name)
            .fetch_one_once(&self.db)
            .await?;

        let id: i64 = row.get(0).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let status_str: String = row.get(3).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let mut status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let retry_count: i32 = row.get(7).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        let retry_at_str: Option<String> =
            row.get(8).map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        if status == WorkflowStatus::Error {
            if let Some(ref retry_at_s) = retry_at_str {
                let retry_at = crate::store::turso::parse_turso_timestamp(retry_at_s)?;
                if current_time < retry_at {
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }

                crate::store::turso::query(SQL_CLEAR_RETRY)
                    .bind(id)
                    .execute_once(&self.db)
                    .await?;

                status = WorkflowStatus::Running;
            } else {
                let error_str: Option<String> =
                    row.get(6).map_err(|e| crate::error::Error::Internal {
                        message: e.to_string(),
                    })?;
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

        // If we updated the status, we should ideally return the updated record.
        let row = if status == WorkflowStatus::Running && retry_at_str.is_some() {
            crate::store::turso::query("SELECT id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at FROM pgqrs_workflow_steps WHERE id = ?")
                .bind(id)
                .fetch_one_once(&self.db)
                .await?
        } else {
            row
        };

        Self::map_row(&row)
    }

    async fn complete_step(&self, id: i64, output: serde_json::Value) -> Result<()> {
        let output_str = output.to_string();
        crate::store::turso::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'SUCCESS', output = ?, completed_at = datetime('now')
            WHERE id = ?
            "#,
        )
        .bind(output_str)
        .bind(id)
        .execute_once(&self.db)
        .await?;

        Ok(())
    }

    async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        new_retry_count: i32,
    ) -> Result<()> {
        let error_str = error.to_string();
        let retry_at_str = retry_at.map(|dt| crate::store::turso::format_turso_timestamp(&dt));
        crate::store::turso::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'ERROR', error = ?, completed_at = datetime('now'),
                retry_at = ?, retry_count = ?
            WHERE id = ?
            "#,
        )
        .bind(error_str)
        .bind(retry_at_str)
        .bind(new_retry_count)
        .bind(id)
        .execute_once(&self.db)
        .await?;

        Ok(())
    }
}
