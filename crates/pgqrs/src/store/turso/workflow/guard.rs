use crate::error::Result;
use crate::store::{StepGuard, StepResult};
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

pub struct TursoStepGuard {
    db: Arc<Database>,
    workflow_id: i64,
    step_id: String,
    completed: bool,
}

const SQL_ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (workflow_id, step_key, status, started_at)
    VALUES (?, ?, 'RUNNING', datetime('now'))
    ON CONFLICT (workflow_id, step_key) DO UPDATE SET workflow_id=workflow_id
    RETURNING status, output, error
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
    ) -> Result<StepResult<serde_json::Value>> {
        let row = crate::store::turso::query(SQL_ACQUIRE_STEP)
            .bind(workflow_id)
            .bind(step_id) // step_id is &str, implements Into<Value>
            .fetch_one(db)
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
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Step {} failed: {}",
                    step_id,
                    error_str.unwrap_or_else(|| "Unknown error".to_string())
                ),
            });
        }

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
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        self.completed = true;
        let error_str = error.to_string();
        crate::store::turso::query(SQL_STEP_FAIL)
            .bind(error_str)
            .bind(self.workflow_id)
            .bind(self.step_id.as_str())
            .execute(&self.db)
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
                let _ = crate::store::turso::query(SQL_STEP_FAIL)
                    .bind("Step dropped without completion")
                    .bind(workflow_id)
                    .bind(step_id)
                    .execute(&db)
                    .await;
            });
        }
    }
}
