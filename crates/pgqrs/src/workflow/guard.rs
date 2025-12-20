use super::WorkflowStatus;
use crate::error::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

/// RAII guard for a workflow step execution.
///
/// Ensures exactly-once logical execution of steps by persisting state to the database.
pub struct StepGuard {
    pool: PgPool,
    workflow_id: Uuid,
    step_id: String,
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
    /// - If the step is `PENDING`, `RUNNING`, or `ERROR` (retry), marks it as `RUNNING` and returns `StepResult::Execute`.
    pub async fn acquire<T: DeserializeOwned>(
        pool: &PgPool,
        workflow_id: Uuid,
        step_id: &str,
    ) -> Result<StepResult<T>> {
        let step_id_string = step_id.to_string();

        let row: (WorkflowStatus, Option<serde_json::Value>) = sqlx::query_as(
            r#"
            INSERT INTO pgqrs_workflow_steps (workflow_id, step_id, status, started_at)
            VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW())
            ON CONFLICT (workflow_id, step_id) DO UPDATE
            SET status = CASE
                WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN pgqrs_workflow_steps.status
                ELSE 'RUNNING'::pgqrs_workflow_status
            END,
            started_at = CASE
                WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN pgqrs_workflow_steps.started_at
                ELSE NOW()
            END
            RETURNING status, output
            "#,
        )
        .bind(workflow_id)
        .bind(&step_id_string)
        .fetch_one(pool)
        .await
        .map_err(|e| crate::error::Error::Connection {
            message: format!("Failed to initialize step {}: {}", step_id_string, e),
        })?;

        let (status, output) = row;

        if status == WorkflowStatus::Success {
            // Step already completed, deserialize output
            let output_val = output.unwrap_or(serde_json::Value::Null);
            let result: T =
                serde_json::from_value(output_val).map_err(crate::error::Error::Serialization)?;
            return Ok(StepResult::Skipped(result));
        }

        Ok(StepResult::Execute(StepGuard {
            pool: pool.clone(),
            workflow_id,
            step_id: step_id_string,
        }))
    }

    /// Mark the step as successfully completed and persist the output.
    pub async fn success<T: Serialize>(self, output: T) -> Result<()> {
        let output_json =
            serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;

        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $3, completed_at = NOW()
            WHERE workflow_id = $1 AND step_id = $2
            "#,
        )
        .bind(self.workflow_id)
        .bind(&self.step_id)
        .bind(output_json)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::Connection {
            message: format!("Failed to mark step {} as success: {}", self.step_id, e),
        })?;

        Ok(())
    }

    /// Mark the step as failed and persist the error.
    pub async fn fail<E: Serialize>(self, error: E) -> Result<()> {
        let error_json = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;

        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'ERROR'::pgqrs_workflow_status, error = $3, completed_at = NOW()
            WHERE workflow_id = $1 AND step_id = $2
            "#,
        )
        .bind(self.workflow_id)
        .bind(&self.step_id)
        .bind(error_json)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::Connection {
            message: format!("Failed to mark step {} as error: {}", self.step_id, e),
        })?;

        Ok(())
    }
}
