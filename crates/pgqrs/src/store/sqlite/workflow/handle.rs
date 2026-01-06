use crate::error::Result;
use crate::store::sqlite::tables::workflows::SqliteWorkflowTable;
use crate::store::sqlite::workflow::guard::SqliteStepGuard;
use crate::types::WorkflowStatus;
use async_trait::async_trait;
use serde::Serialize;
use sqlx::SqlitePool;
use std::str::FromStr;

const SQL_CREATE_WORKFLOW: &str = r#"
INSERT INTO pgqrs_workflows (name, status, input, created_at, updated_at)
VALUES ($1, 'PENDING', $2, datetime('now'), datetime('now'))
RETURNING workflow_id
"#;

const SQL_START_WORKFLOW: &str = r#"
UPDATE pgqrs_workflows
SET status = 'RUNNING', updated_at = datetime('now')
WHERE workflow_id = $1 AND status = 'PENDING'
RETURNING status, error
"#;

#[derive(Debug, Clone)]
pub struct SqliteWorkflow {
    id: i64,
    pool: SqlitePool,
}

impl SqliteWorkflow {
    /// Create a new workflow in the database.
    pub async fn create<T: Serialize>(pool: SqlitePool, name: &str, input: &T) -> Result<Self> {
        let input_json = serde_json::to_value(input)
            .map_err(crate::error::Error::Serialization)?
            .to_string();

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
    pub fn new(pool: SqlitePool, id: i64) -> Self {
        Self { id, pool }
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl crate::store::Workflow for SqliteWorkflow {
    fn id(&self) -> i64 {
        self.id
    }

    async fn start(&mut self) -> Result<()> {
        // Try to transition to RUNNING
        let result = sqlx::query(SQL_START_WORKFLOW)
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
            let status_str: Option<String> =
                sqlx::query_scalar("SELECT status FROM pgqrs_workflows WHERE workflow_id = $1")
                    .bind(self.id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "CHECK_WORKFLOW_STATUS".into(),
                        source: e,
                        context: format!("Failed to check status for workflow {}", self.id),
                    })?;

            if let Some(s) = status_str {
                if let Ok(WorkflowStatus::Error) = WorkflowStatus::from_str(&s) {
                    return Err(crate::error::Error::ValidationFailed {
                        reason: format!("Workflow {} is in terminal ERROR state", self.id),
                    });
                }
            }
        }

        Ok(())
    }

    async fn complete(&mut self, output: serde_json::Value) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        SqliteWorkflowTable::complete_workflow(&mut conn, self.id, output).await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        SqliteWorkflowTable::fail_workflow(&mut conn, self.id, error).await
    }

    async fn acquire_step(
        &self,
        step_id: &str,
    ) -> Result<crate::store::StepResult<serde_json::Value>> {
        SqliteStepGuard::acquire(&self.pool, self.id, step_id).await
    }
}
