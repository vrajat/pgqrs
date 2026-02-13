use crate::error::Result;
use crate::store::turso::parse_turso_timestamp;
use crate::types::{NewStepRecord, StepRecord, WorkflowStatus};
use async_trait::async_trait;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoStepRecordTable {
    db: Arc<Database>,
}

impl TursoStepRecordTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<StepRecord> {
        let id: i64 = row.get(0)?;
        let run_id: i64 = row.get(1)?;
        let step_id: String = row.get(2)?;

        let status_str: String = row.get(3)?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let input_str: Option<String> = row.get(4)?;
        let input: Option<Value> = match input_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let output_str: Option<String> = row.get(5)?;
        let output: Option<Value> = match output_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let error_str: Option<String> = row.get(6)?;
        let error: Option<Value> = match error_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let created_at = parse_turso_timestamp(&row.get::<String>(7)?)?;
        let updated_at = parse_turso_timestamp(&row.get::<String>(8)?)?;

        Ok(StepRecord {
            id,
            run_id,
            step_id,
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
impl crate::store::StepRecordTable for TursoStepRecordTable {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        let input_str = data.input.map(|v| v.to_string());

        let row = crate::store::turso::query(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_id, status, input)
            VALUES (?, ?, 'PENDING', ?)
            RETURNING run_id, step_id, status, input, output, error, created_at, updated_at
            "#,
        )
        .bind(data.run_id)
        .bind(data.step_id.as_str())
        .bind(input_str)
        .fetch_one_once(&self.db)
        .await?;

        // pgqrs_workflow_steps uses a composite PK (run_id, step_id) in Turso migrations,
        // and has no numeric `id` column. Construct a stable surrogate `id` for the Table
        // trait by hashing is out of scope; use 0 and rely on (run_id, step_id) access via
        // workflow codepaths.
        // NOTE: This table interface may not be used by workflow execution; it exists to
        // avoid panics in generic code paths.
        let run_id: i64 = row.get(0)?;
        let step_id: String = row.get(1)?;
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

        Ok(StepRecord {
            id: 0,
            run_id,
            step_id,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
        })
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        let row = crate::store::turso::query(
            r#"
            SELECT 0 as id, run_id, step_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_steps
            WHERE run_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        let rows = crate::store::turso::query(
            r#"
            SELECT 0 as id, run_id, step_id, status, input, output, error, created_at, updated_at
            FROM pgqrs_workflow_steps
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.db)
        .await?;

        let mut steps = Vec::with_capacity(rows.len());
        for row in rows {
            steps.push(Self::map_row(&row)?);
        }

        Ok(steps)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 =
            crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workflow_steps")
                .fetch_one(&self.db)
                .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        // No stable numeric id; treat `id` as run_id and delete all its steps.
        let count = crate::store::turso::query("DELETE FROM pgqrs_workflow_steps WHERE run_id = ?")
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }
}
