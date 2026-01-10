use crate::error::Result;
use crate::store::turso::tables::workflows::TursoWorkflowTable;
use crate::store::turso::workflow::guard::TursoStepGuard;
use crate::types::WorkflowStatus;
use async_trait::async_trait;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

const SQL_CREATE_WORKFLOW: &str = r#"
INSERT INTO pgqrs_workflows (name, status, input, created_at, updated_at)
VALUES (?, 'PENDING', ?, datetime('now'), datetime('now'))
RETURNING workflow_id
"#;

const SQL_START_WORKFLOW: &str = r#"
UPDATE pgqrs_workflows
SET status = 'RUNNING', updated_at = datetime('now')
WHERE workflow_id = ? AND status = 'PENDING'
RETURNING status, error
"#;

#[derive(Debug, Clone)]
pub struct TursoWorkflow {
    id: i64,
    db: Arc<Database>,
}

impl TursoWorkflow {
    /// Create a new workflow in the database.
    pub async fn create<T: Serialize>(db: Arc<Database>, name: &str, input: &T) -> Result<Self> {
        let input_json = serde_json::to_value(input)
            .map_err(crate::error::Error::Serialization)?
            .to_string();

        let row = crate::store::turso::query(SQL_CREATE_WORKFLOW)
            .bind(name)
            .bind(input_json)
            .fetch_one(&db)
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to create workflow: {}", e),
            })?;

        let id: i64 = row
            .get::<i64>(0)
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        Ok(Self { id, db })
    }

    /// Create a new workflow instance connected to the database.
    ///
    /// This is used when the ID is already known (e.g. loaded from DB).
    pub fn new(db: Arc<Database>, id: i64) -> Self {
        Self { id, db }
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &Database {
        &self.db
    }
}

#[async_trait]
impl crate::store::Workflow for TursoWorkflow {
    fn id(&self) -> i64 {
        self.id
    }

    async fn start(&mut self) -> Result<()> {
        // Try to transition to RUNNING
        let result = crate::store::turso::query(SQL_START_WORKFLOW)
            .bind(self.id)
            .fetch_optional(&self.db)
            .await?;

        // If no row update, check current status
        if result.is_none() {
            let status_str: Option<String> = crate::store::turso::query_scalar(
                "SELECT status FROM pgqrs_workflows WHERE workflow_id = ?",
            )
            .bind(self.id)
            .fetch_optional(&self.db)
            .await?;

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
        let table = TursoWorkflowTable::new(self.db.clone());
        table.complete_workflow(self.id, output).await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        let table = TursoWorkflowTable::new(self.db.clone());
        table.fail_workflow(self.id, error).await
    }

    async fn acquire_step(
        &self,
        step_id: &str,
    ) -> Result<crate::store::StepResult<serde_json::Value>> {
        TursoStepGuard::acquire(&self.db, self.id, step_id).await
    }
}
