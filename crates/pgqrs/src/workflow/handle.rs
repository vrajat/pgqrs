use crate::error::Result;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

const SQL_START_WORKFLOW: &str = r#"
INSERT INTO pgqrs_workflows (workflow_id, name, status, input, created_at, updated_at)
VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, $3, NOW(), NOW())
ON CONFLICT (workflow_id) DO UPDATE
SET status = CASE
    WHEN pgqrs_workflows.status = 'SUCCESS' THEN 'SUCCESS'::pgqrs_workflow_status
    WHEN pgqrs_workflows.status = 'ERROR' THEN 'ERROR'::pgqrs_workflow_status
    ELSE 'RUNNING'::pgqrs_workflow_status
END,
updated_at = NOW()
RETURNING status, error
"#;

const SQL_WORKFLOW_SUCCESS: &str = r#"
UPDATE pgqrs_workflows
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, updated_at = NOW()
WHERE workflow_id = $1
"#;

const SQL_WORKFLOW_FAIL: &str = r#"
UPDATE pgqrs_workflows
SET status = 'ERROR'::pgqrs_workflow_status, error = $2, updated_at = NOW()
WHERE workflow_id = $1
"#;

/// Handle for a durable workflow execution.
pub struct Workflow {
    id: Uuid,
    pool: PgPool,
}

impl Workflow {
    /// Create a new workflow instance connected to the database.
    pub fn new(pool: PgPool, id: Uuid) -> Self {
        Self { id, pool }
    }

    /// Get the workflow ID.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Initialize or resume the workflow execution.
    ///
    /// Persists the workflow metadata to `pgqrs_workflows`.
    /// - If fresh: Inserts RUNNING.
    /// - If running/pending: Updates timestamp, keeps RUNNING.
    /// - If SUCCESS/ERROR (terminal): Returns success (idempotent) or error (if failed).
    pub async fn start<T: Serialize>(&self, name: &str, input: &T) -> Result<()> {
        let input_json = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        // UPSERT and return declared status
        let row: (crate::workflow::WorkflowStatus, Option<serde_json::Value>) =
            sqlx::query_as(SQL_START_WORKFLOW)
                .bind(self.id)
                .bind(name)
                .bind(input_json)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::Connection {
                    message: format!("Failed to start workflow {}: {}", self.id, e),
                })?;

        let (status, error) = row;

        // If explicitly in ERROR state, return failure
        if status == crate::workflow::WorkflowStatus::Error {
            let error_val = error.unwrap_or(serde_json::Value::Null);
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Workflow {} is in terminal ERROR state: {}",
                    self.id, error_val
                ),
            });
        }

        Ok(())
    }

    /// Mark the workflow as successfully completed.
    pub async fn success<T: Serialize>(&self, output: T) -> Result<()> {
        let output_json =
            serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;

        sqlx::query(SQL_WORKFLOW_SUCCESS)
            .bind(self.id)
            .bind(output_json)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to complete workflow {}: {}", self.id, e),
            })?;

        Ok(())
    }

    /// Mark the workflow as failed.
    pub async fn fail<E: Serialize>(&self, error: E) -> Result<()> {
        let error_json = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;

        sqlx::query(SQL_WORKFLOW_FAIL)
            .bind(self.id)
            .bind(error_json)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to fail workflow {}: {}", self.id, e),
            })?;

        Ok(())
    }
}
