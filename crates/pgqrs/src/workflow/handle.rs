use crate::error::Result;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

/// Handle for a durable workflow execution.
pub struct Workflow {
    pub id: Uuid,
    pub pool: PgPool,
}

impl Workflow {
    /// Create a new workflow instance connected to the database.
    pub fn new(pool: PgPool, id: Uuid) -> Self {
        Self { id, pool }
    }

    /// Initialize or resume the workflow execution.
    ///
    /// Persists the workflow metadata to `pgqrs_workflows`.
    pub async fn start<T: Serialize>(&self, name: &str, input: &T) -> Result<()> {
        let input_json = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        // Upsert workflow state.
        // If it exists, we assume we are resuming (status might be anything).
        // If resuming, do we update status to RUNNING? Yes.
        // Do we reset input? Maybe not if it's identical.
        // For simplicity: Update status to RUNNING, and updated_at to NOW().
        sqlx::query(
            r#"
            INSERT INTO pgqrs_workflows (workflow_id, name, status, input, created_at, updated_at)
            VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, $3, NOW(), NOW())
            ON CONFLICT (workflow_id) DO UPDATE
            SET status = CASE
                WHEN pgqrs_workflows.status = 'SUCCESS' THEN pgqrs_workflows.status
                ELSE 'RUNNING'::pgqrs_workflow_status
            END,
            updated_at = NOW()
            "#,
        )
        .bind(self.id)
        .bind(name)
        .bind(input_json)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::Connection {
            message: format!("Failed to start workflow {}: {}", self.id, e),
        })?;

        Ok(())
    }

    /// Mark the workflow as successfully completed.
    pub async fn success<T: Serialize>(&self, output: T) -> Result<()> {
        let output_json =
            serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;

        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, updated_at = NOW()
            WHERE workflow_id = $1
            "#,
        )
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

        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'ERROR'::pgqrs_workflow_status, error = $2, updated_at = NOW()
            WHERE workflow_id = $1
            "#,
        )
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
