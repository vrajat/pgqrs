use crate::error::Result;
use crate::store::sqlite::tables::runs::SqliteRunRecordTable;
use crate::store::sqlite::workflow::guard::SqliteStepGuard;
use crate::types::{WorkflowRecord, WorkflowStatus};
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::str::FromStr;

const SQL_START_RUN: &str = r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING', updated_at = datetime('now')
WHERE id = $1 AND status = 'QUEUED'
RETURNING status, error
"#;

#[derive(Debug, Clone)]
pub struct SqliteWorkflow {
    record: WorkflowRecord,
}

impl SqliteWorkflow {
    pub fn new(record: WorkflowRecord) -> Self {
        Self { record }
    }
}

#[async_trait]
impl crate::store::Workflow for SqliteWorkflow {
    fn workflow_record(&self) -> &WorkflowRecord {
        &self.record
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

    async fn pause(&mut self, message: String, resume_after: std::time::Duration) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(crate::error::Error::Database)?;
        let error = serde_json::json!({
            "message": message,
            "resume_after": resume_after.as_secs()
        });
        SqliteRunRecordTable::pause_run(&mut conn, self.id, error).await
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
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::types::StepRecord> {
        SqliteStepGuard::acquire_record(&self.pool, self.id, step_name, current_time).await
    }

    async fn complete_step(
        &self,
        step_name: &str,
        output: serde_json::Value,
    ) -> crate::error::Result<()> {
        let step = self.acquire_step(step_name, chrono::Utc::now()).await?;
        let mut guard = SqliteStepGuard::new(self.pool.clone(), step.id);
        crate::store::StepGuard::complete(&mut guard, output).await
    }

    async fn fail_step(
        &self,
        step_name: &str,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()> {
        let step = self.acquire_step(step_name, current_time).await?;
        let mut guard = SqliteStepGuard::new(self.pool.clone(), step.id);
        crate::store::StepGuard::fail_with_json(&mut guard, error, current_time).await
    }
}
