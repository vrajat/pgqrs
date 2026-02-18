use crate::error::Result;
use crate::store::turso::parse_turso_timestamp;
use crate::types::{NewRunRecord, RunRecord, WorkflowStatus};
use async_trait::async_trait;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoRunRecordTable {
    db: Arc<Database>,
}

impl TursoRunRecordTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<RunRecord> {
        let id: i64 = row.get(0)?;
        let workflow_id: i64 = row.get(1)?;

        let status_str: String = row.get(2)?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let input_str: Option<String> = row.get(3)?;
        let input: Option<Value> = match input_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let output_str: Option<String> = row.get(4)?;
        let output: Option<Value> = match output_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let error_str: Option<String> = row.get(5)?;
        let error: Option<Value> = match error_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let created_at = parse_turso_timestamp(&row.get::<String>(6)?)?;
        let updated_at = parse_turso_timestamp(&row.get::<String>(7)?)?;

        Ok(RunRecord {
            id,
            workflow_id,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
        })
    }
}

#[async_trait]
impl crate::store::RunRecordTable for TursoRunRecordTable {
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        let input_str = data.input.map(|v| v.to_string());

        let row = crate::store::turso::query(
            r#"
            INSERT INTO pgqrs_workflow_runs (workflow_id, status, input)
            VALUES (?, 'QUEUED', ?)
            RETURNING id, workflow_id, status, input, output, error, created_at, updated_at
            "#,
        )
        .bind(data.workflow_id)
        .bind(input_str)
        .fetch_one_once(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<RunRecord> {
        let row = crate::store::turso::query(
            r#"
            SELECT id, workflow_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_runs
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<RunRecord>> {
        let rows = crate::store::turso::query(
            r#"
            SELECT id, workflow_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_runs
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.db)
        .await?;

        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(Self::map_row(&row)?);
        }

        Ok(runs)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 =
            crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workflow_runs")
                .fetch_one(&self.db)
                .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query("DELETE FROM pgqrs_workflow_runs WHERE id = ?")
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }

    async fn start_run(&self, id: i64) -> Result<RunRecord> {
        let row = crate::store::turso::query(
            r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'RUNNING',
                updated_at = datetime('now'),
                started_at = CASE WHEN status = 'QUEUED' THEN datetime('now') ELSE started_at END
            WHERE id = ? AND status IN ('QUEUED', 'PAUSED')
            RETURNING id, workflow_id, status, input, output, error, created_at, updated_at
            "#,
        )
        .bind(id)
        .fetch_optional(&self.db)
        .await?;

        if let Some(row) = row {
            return Self::map_row(&row);
        }

        let status_str: Option<String> = crate::store::turso::query_scalar(
            "SELECT status FROM pgqrs_workflow_runs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.db)
        .await?;

        if let Some(s) = status_str {
            if let Ok(status) = WorkflowStatus::from_str(&s) {
                if matches!(status, WorkflowStatus::Error | WorkflowStatus::Success) {
                    return Err(crate::error::Error::ValidationFailed {
                        reason: format!("Run {} is in terminal {} state", id, status),
                    });
                }
            }
        }

        self.get(id).await
    }

    async fn complete_run(&self, id: i64, output: serde_json::Value) -> Result<RunRecord> {
        let output_str = output.to_string();
        let _rows = crate::store::turso::query(
            "UPDATE pgqrs_workflow_runs SET status = 'SUCCESS', output = $2, updated_at = datetime('now'), completed_at = datetime('now') WHERE id = $1",
        )
        .bind(id)
        .bind(output_str)
        .execute_once(&self.db)
        .await?;
        self.get(id).await
    }

    async fn pause_run(
        &self,
        id: i64,
        message: String,
        resume_after: std::time::Duration,
    ) -> Result<RunRecord> {
        let error = serde_json::json!({
            "message": message,
            "resume_after": resume_after.as_secs()
        });
        let error_str = error.to_string();
        let _rows = crate::store::turso::query(
            "UPDATE pgqrs_workflow_runs SET status = 'PAUSED', error = $2, paused_at = datetime('now'), updated_at = datetime('now') WHERE id = $1",
        )
        .bind(id)
        .bind(error_str)
        .execute_once(&self.db)
        .await?;
        self.get(id).await
    }

    async fn fail_run(&self, id: i64, error: serde_json::Value) -> Result<RunRecord> {
        let error_str = error.to_string();
        let _rows = crate::store::turso::query(
            "UPDATE pgqrs_workflow_runs SET status = 'ERROR', error = $2, updated_at = datetime('now'), completed_at = datetime('now') WHERE id = $1",
        )
        .bind(id)
        .bind(error_str)
        .execute_once(&self.db)
        .await?;
        self.get(id).await
    }
}
