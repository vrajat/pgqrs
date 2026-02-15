use crate::error::Result;
use crate::store::postgres::workflow::guard::StepGuard;
use crate::store::postgres::workflow::WorkflowStatus as PgWorkflowStatus;
use crate::types::WorkflowStatus;
use async_trait::async_trait;
use sqlx::PgPool;
use std::str::FromStr;

const SQL_START_RUN: &str = r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING'::pgqrs_workflow_status,
    started_at = CASE
        WHEN status = 'QUEUED'::pgqrs_workflow_status THEN NOW()
        ELSE started_at
    END,
    updated_at = NOW()
WHERE id = $1
  AND status IN ('QUEUED'::pgqrs_workflow_status, 'PAUSED'::pgqrs_workflow_status)
RETURNING status
"#;

#[derive(Debug, Clone)]
pub struct PostgresRun {
    id: i64,
    pool: PgPool,
}

impl PostgresRun {
    pub fn new(pool: PgPool, id: i64) -> Self {
        Self { id, pool }
    }
}

#[async_trait]
impl crate::store::Run for PostgresRun {
    fn id(&self) -> i64 {
        self.id
    }

    async fn start(&mut self) -> Result<()> {
        let result: Option<PgWorkflowStatus> = sqlx::query_scalar(SQL_START_RUN)
            .bind(self.id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_START_RUN".into(),
                source: Box::new(e),
                context: format!("Failed to start run {}", self.id),
            })?;

        if result.is_none() {
            // If no row updated, check if we're in terminal ERROR state.
            let status_str: Option<String> =
                sqlx::query_scalar("SELECT status::text FROM pgqrs_workflow_runs WHERE id = $1")
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
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(self.id)
        .bind(output)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "COMPLETE_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to complete run {}", self.id),
        })?;

        Ok(())
    }

    async fn pause(&mut self, message: String, resume_after: std::time::Duration) -> Result<()> {
        let error = serde_json::json!({
            "message": message,
            "resume_after": resume_after.as_secs()
        });
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'PAUSED'::pgqrs_workflow_status,
                error = $2,
                paused_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(self.id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "PAUSE_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to pause run {}", self.id),
        })?;

        Ok(())
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(self.id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "FAIL_RUN".into(),
            source: Box::new(e),
            context: format!("Failed to fail run {}", self.id),
        })?;

        Ok(())
    }

    async fn acquire_step(
        &self,
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::types::StepRecord> {
        StepGuard::acquire_record(&self.pool, self.id, step_name, current_time).await
    }

    async fn complete_step(
        &self,
        step_name: &str,
        output: serde_json::Value,
    ) -> crate::error::Result<()> {
        let step = self.acquire_step(step_name, chrono::Utc::now()).await?;
        let mut guard = StepGuard::new(self.pool.clone(), step.id);
        crate::store::StepGuard::complete(&mut guard, output).await
    }

    async fn fail_step(
        &self,
        step_name: &str,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()> {
        let step = self.acquire_step(step_name, current_time).await?;
        let mut guard = StepGuard::new(self.pool.clone(), step.id);
        crate::store::StepGuard::fail_with_json(&mut guard, error, current_time).await
    }
}
