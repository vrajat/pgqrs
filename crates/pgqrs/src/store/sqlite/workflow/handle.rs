use crate::error::Result;
use crate::store::sqlite::tables::runs::SqliteRunRecordTable;
use crate::store::sqlite::workflow::guard::SqliteStepGuard;
use crate::types::WorkflowStatus;
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::str::FromStr;

const SQL_START_RUN: &str = r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING', updated_at = datetime('now')
WHERE id = $1 AND status = 'PENDING'
RETURNING status, error
"#;

#[derive(Debug, Clone)]
pub struct SqliteWorkflow {
    name: String,
}

impl SqliteWorkflow {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl crate::store::Workflow for SqliteWorkflow {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub struct SqliteRun {
    id: i64,
    pool: SqlitePool,
}

impl SqliteRun {
    pub fn new(pool: SqlitePool, id: i64) -> Self {
        Self { id, pool }
    }
}

#[async_trait]
impl crate::store::Run for SqliteRun {
    fn id(&self) -> i64 {
        self.id
    }

    async fn start(&mut self) -> Result<()> {
        // Try to transition to RUNNING
        let result = sqlx::query(SQL_START_RUN)
            .bind(self.id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_START_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to start run {}", self.id),
            })?;

        // If no row update, check current status
        if result.is_none() {
            let status_str: Option<String> =
                sqlx::query_scalar("SELECT status FROM pgqrs_workflow_runs WHERE id = $1")
                    .bind(self.id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "CHECK_RUN_STATUS".into(),
                        source: Box::new(e),
                        context: format!("Failed to check status for run {}", self.id),
                    })?;

            if let Some(s) = status_str {
                if let Ok(WorkflowStatus::Error) = WorkflowStatus::from_str(&s) {
                    return Err(crate::error::Error::ValidationFailed {
                        reason: format!("Run {} is in terminal ERROR state", self.id),
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
        SqliteRunRecordTable::complete_run(&mut conn, self.id, output).await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        SqliteRunRecordTable::fail_run(&mut conn, self.id, error).await
    }

    async fn acquire_step(
        &self,
        step_id: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::store::StepResult<serde_json::Value>> {
        SqliteStepGuard::acquire::<serde_json::Value>(&self.pool, self.id, step_id, current_time)
            .await
    }
}
