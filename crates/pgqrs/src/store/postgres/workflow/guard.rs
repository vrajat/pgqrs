use super::WorkflowStatus;
use crate::error::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::PgPool;

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
RETURNING status, output, error
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

/// The result of attempting to start a step.
pub enum StepResult<T> {
    /// The step needs to be executed. The returned guard MUST be used to report success or failure.
    Execute(StepGuard),
    /// The step was already completed successfully in a previous run. Contains the cached output.
    Skipped(T),
}

impl StepGuard {
    /// Attempt to start a step.
    ///
    /// Checks the database state for the given `workflow_id` and `step_id`.
    /// - If the step is already `SUCCESS`, returns `StepResult::Skipped` with the deserialized output.
    /// - If the step is `ERROR`, fails with the previous error (terminal state).
    /// - If the step is `PENDING`, `RUNNING` (retry), marks it as `RUNNING` and returns `StepResult::Execute`.
    pub async fn acquire<T: DeserializeOwned>(
        pool: &PgPool,
        workflow_id: i64,
        step_id: &str,
    ) -> Result<StepResult<T>> {
        let step_id_string = step_id.to_string();

        let row: (
            WorkflowStatus,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
        ) = sqlx::query_as(SQL_ACQUIRE_STEP)
            .bind(workflow_id)
            .bind(&step_id_string)
            .fetch_one(pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to initialize step {}: {}", step_id_string, e),
            })?;

        let (status, output, error) = row;

        if status == WorkflowStatus::Success {
            // Step already completed, deserialize output
            let output_val = output.unwrap_or(serde_json::Value::Null);
            let result: T =
                serde_json::from_value(output_val).map_err(crate::error::Error::Serialization)?;
            return Ok(StepResult::Skipped(result));
        }

        if status == WorkflowStatus::Error {
            // Step failed previously and is terminal. Return the error as a failure.
            let error_val = error.unwrap_or(serde_json::Value::Null);
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Step {} is in terminal ERROR state: {}",
                    step_id_string, error_val
                ),
            });
        }

        Ok(StepResult::Execute(StepGuard {
            pool: pool.clone(),
            workflow_id,
            step_id: step_id_string,
            completed: false,
        }))
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
                    "msg": "Step execution interrupted (dropped without completion)",
                    "code": "STEP_DROPPED"
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
    async fn success<T: serde::Serialize + Send + Sync>(
        mut self,
        output: T,
    ) -> crate::error::Result<()> {
        let output_json =
            serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;

        sqlx::query(SQL_STEP_SUCCESS)
            .bind(self.workflow_id)
            .bind(&self.step_id)
            .bind(output_json)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to mark step {} as success: {}", self.step_id, e),
            })?;

        self.completed = true;
        Ok(())
    }

    /// Mark the step as failed and persist the error.
    async fn fail<E: serde::Serialize + Send + Sync>(
        mut self,
        error: E,
    ) -> crate::error::Result<()> {
        let error_json = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;

        sqlx::query(SQL_STEP_FAIL)
            .bind(self.workflow_id)
            .bind(&self.step_id)
            .bind(error_json)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to mark step {} as error: {}", self.step_id, e),
            })?;

        self.completed = true;
        Ok(())
    }
}
