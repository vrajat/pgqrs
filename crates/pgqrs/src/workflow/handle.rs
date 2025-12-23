use crate::error::Result;
use serde::Serialize;
use sqlx::PgPool;



const SQL_CREATE_WORKFLOW: &str = r#"
INSERT INTO pgqrs_workflows (name, status, input)
VALUES ($1, 'PENDING'::pgqrs_workflow_status, $2)
RETURNING workflow_id
"#;

const SQL_START_WORKFLOW: &str = r#"
UPDATE pgqrs_workflows
SET status = 'RUNNING'::pgqrs_workflow_status, updated_at = NOW()
WHERE workflow_id = $1 AND status = 'PENDING'::pgqrs_workflow_status
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
    id: i64,
    pool: PgPool,
}

impl Workflow {
    /// Create a new workflow instance connected to the database.
    ///
    /// This is used when the ID is already known (e.g. loaded from DB).
    pub fn new(pool: PgPool, id: i64) -> Self {
        Self { id, pool }
    }

    /// Create a new workflow in the database.
    pub async fn create<T: Serialize>(pool: PgPool, name: &str, input: &T) -> Result<Self> {
        let input_json = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        let id: i64 = sqlx::query_scalar(SQL_CREATE_WORKFLOW)
            .bind(name)
            .bind(input_json)
            .fetch_one(&pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to create workflow {}: {}", name, e),
            })?;

        Ok(Self { id, pool })
    }

    /// Get the workflow ID.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Start the workflow execution.
    ///
    /// Transitions status from PENDING to RUNNING.
    pub async fn start(&self) -> Result<()> {
        // Try to transition to RUNNING
        let result = sqlx::query_as::<_, (crate::workflow::WorkflowStatus, Option<serde_json::Value>)>(SQL_START_WORKFLOW)
                .bind(self.id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| crate::error::Error::Connection {
                    message: format!("Failed to start workflow {}: {}", self.id, e),
                })?;

        // If no row update, check current status
        if result.is_none() {
             let status: Option<crate::workflow::WorkflowStatus> = sqlx::query_scalar(
                "SELECT status FROM pgqrs_workflows WHERE workflow_id = $1"
             )
             .bind(self.id)
             .fetch_optional(&self.pool)
             .await
             .ok()
             .flatten();

             if let Some(crate::workflow::WorkflowStatus::Error) = status {
                 return Err(crate::error::Error::ValidationFailed {
                     reason: format!("Workflow {} is in terminal ERROR state", self.id),
                 });
             }
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
