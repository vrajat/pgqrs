use crate::error::Result;
use async_trait::async_trait;
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

/// Handle for a durable workflow execution.
pub struct Workflow {
    id: i64,
    pool: PgPool,
}

impl Workflow {
    /// Create a new workflow in the database.
    pub async fn create<T: Serialize>(pool: PgPool, name: &str, input: &T) -> Result<Self> {
        let input_json = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        let id: i64 = sqlx::query_scalar(SQL_CREATE_WORKFLOW)
            .bind(name)
            .bind(input_json)
            .fetch_one(&pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_CREATE_WORKFLOW".into(),
                source: e,
                context: format!("Failed to create workflow '{}'", name),
            })?;

        Ok(Self { id, pool })
    }

    /// Create a new workflow instance connected to the database.
    ///
    /// This is used when the ID is already known (e.g. loaded from DB).
    pub fn new(pool: PgPool, id: i64) -> Self {
        Self { id, pool }
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl crate::store::Workflow for Workflow {
    fn id(&self) -> i64 {
        self.id
    }

    /// Start the workflow execution.
    ///
    /// Transitions status from PENDING to RUNNING.
    ///
    /// # Behavior
    ///
    /// - **Idempotent**: If the workflow is already `RUNNING` or `SUCCESS`, this method succeeds without changes.
    /// - **Error**: If the workflow is in the `ERROR` state, this method returns a `ValidationFailed` error.
    async fn start(&mut self) -> crate::error::Result<()> {
        // Try to transition to RUNNING
        let result =
            sqlx::query_as::<_, (crate::types::WorkflowStatus, Option<serde_json::Value>)>(
                SQL_START_WORKFLOW,
            )
            .bind(self.id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_START_WORKFLOW".into(),
                source: e,
                context: format!("Failed to start workflow {}", self.id),
            })?;

        // If no row update, check current status
        if result.is_none() {
            let status: Option<crate::types::WorkflowStatus> =
                sqlx::query_scalar("SELECT status FROM pgqrs_workflows WHERE workflow_id = $1")
                    .bind(self.id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "CHECK_WORKFLOW_STATUS".into(),
                        source: e,
                        context: format!("Failed to check status for workflow {}", self.id),
                    })?;

            if let Some(crate::types::WorkflowStatus::Error) = status {
                return Err(crate::error::Error::ValidationFailed {
                    reason: format!("Workflow {} is in terminal ERROR state", self.id),
                });
            }
        }

        Ok(())
    }

    /// Mark the workflow as successfully completed.
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        let mut conn =
            self.pool
                .acquire()
                .await
                .map_err(|e| crate::error::Error::PoolExhausted {
                    source: e,
                    context: "Failed to acquire connection for complete_workflow".into(),
                })?;

        crate::store::postgres::tables::pgqrs_workflows::Workflows::complete_workflow(
            &mut conn, self.id, output,
        )
        .await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()> {
        let mut conn =
            self.pool
                .acquire()
                .await
                .map_err(|e| crate::error::Error::PoolExhausted {
                    source: e,
                    context: "Failed to acquire connection for fail_with_json".into(),
                })?;

        crate::store::postgres::tables::pgqrs_workflows::Workflows::fail_workflow(
            &mut conn, self.id, error,
        )
        .await?;

        Ok(())
    }

    async fn acquire_step(
        &self,
        step_id: &str,
    ) -> crate::error::Result<crate::store::StepResult<serde_json::Value>> {
        crate::store::postgres::workflow::guard::StepGuard::acquire(&self.pool, self.id, step_id)
            .await
    }
}
