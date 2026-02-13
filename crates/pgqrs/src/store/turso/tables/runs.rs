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
            VALUES (?, 'PENDING', ?)
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
}
