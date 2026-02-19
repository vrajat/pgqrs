use crate::error::Result;
use crate::store::query::{QueryBuilder, QueryParam};
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

const SQL_ACQUIRE_STEP: &str = r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE
    SET status = CASE
        WHEN status = 'SUCCESS' THEN 'SUCCESS'
        WHEN status = 'ERROR' THEN 'ERROR'
        ELSE 'RUNNING'
    END,
    started_at = CASE
        WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
        ELSE datetime('now')
    END
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#;

const SQL_CLEAR_RETRY: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE id = ?
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#;

const SQL_COMPLETE_STEP: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS', output = ?2, completed_at = datetime('now')
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#;

const SQL_FAIL_STEP: &str = r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR', error = ?2, completed_at = datetime('now'),
        retry_at = ?3, retry_count = ?4
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#;

impl TursoStepRecordTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<StepRecord> {
        let id: i64 = row.get(0)?;
        let run_id: i64 = row.get(1)?;
        let step_name: String = row.get(2)?;

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
        let retry_at_str: Option<String> = row.get(9)?;
        let retry_at = match retry_at_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let retry_count: i32 = row.get(10)?;

        Ok(StepRecord {
            id,
            run_id,
            step_name,
            status,
            input,
            output,
            error,
            created_at,
            updated_at,
            retry_at,
            retry_count,
        })
    }
}

#[async_trait]
impl crate::store::StepRecordTable for TursoStepRecordTable {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        let input_str = data.input.map(|v| v.to_string());

        let row = crate::store::turso::query(
            r#"
            INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, input)
            VALUES (?, ?, 'PENDING', ?)
            RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            "#,
        )
        .bind(data.run_id)
        .bind(data.step_name.as_str())
        .bind(input_str)
        .fetch_one_once(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        let row = crate::store::turso::query(
            r#"
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
            FROM pgqrs_workflow_steps
            WHERE id = ?
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
            SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
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
        let count = crate::store::turso::query("DELETE FROM pgqrs_workflow_steps WHERE id = ?")
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }

    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord> {
        let mut builder = crate::store::turso::query(query.sql());
        for param in query.params() {
            let value = match param {
                QueryParam::I64(value) => turso::Value::Integer(*value),
                QueryParam::I32(value) => turso::Value::Integer((*value).into()),
                QueryParam::String(value) => turso::Value::Text(value.clone()),
                QueryParam::Json(value) => turso::Value::Text(value.to_string()),
                QueryParam::DateTime(value) => match value {
                    Some(dt) => turso::Value::Text(crate::store::turso::format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }

        let row = builder.fetch_one_once(&self.db).await?;

        Self::map_row(&row)
    }

    fn sql_acquire_step(&self) -> &'static str {
        SQL_ACQUIRE_STEP
    }

    fn sql_clear_retry(&self) -> &'static str {
        SQL_CLEAR_RETRY
    }

    fn sql_complete_step(&self) -> &'static str {
        SQL_COMPLETE_STEP
    }

    fn sql_fail_step(&self) -> &'static str {
        SQL_FAIL_STEP
    }
}
