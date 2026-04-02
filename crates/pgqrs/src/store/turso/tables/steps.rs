use crate::error::Result;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::tables::DialectStepTable;
use crate::store::turso::dialect::TursoDialect;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::types::{StepRecord, WorkflowStatus};
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
    async fn get(&self, id: i64) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_get_step(self, id).await
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        <Self as DialectStepTable>::dialect_list_steps(self).await
    }

    async fn count(&self) -> Result<i64> {
        <Self as DialectStepTable>::dialect_count_steps(self).await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        <Self as DialectStepTable>::dialect_delete_step(self, id).await
    }

    async fn acquire_step(&self, run_id: i64, step_name: &str) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_acquire_step(self, run_id, step_name).await
    }

    async fn clear_retry(&self, id: i64) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_clear_retry(self, id).await
    }

    async fn complete_step(&self, id: i64, output: Value) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_complete_step(self, id, output).await
    }

    async fn fail_step(
        &self,
        id: i64,
        error: Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_fail_step(self, id, error, retry_at, retry_count).await
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
                    Some(dt) => turso::Value::Text(format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }

        let row = builder.fetch_one_once(&self.db).await?;
        Self::map_row(&row)
    }
}

#[async_trait]
impl DialectStepTable for TursoStepRecordTable {
    type Dialect = TursoDialect;

    async fn fetch_all_steps(&self, query: QueryBuilder) -> Result<Vec<StepRecord>> {
        let mut builder = crate::store::turso::query(query.sql());
        for param in query.params() {
            let value = match param {
                QueryParam::I64(value) => turso::Value::Integer(*value),
                QueryParam::I32(value) => turso::Value::Integer((*value).into()),
                QueryParam::String(value) => turso::Value::Text(value.clone()),
                QueryParam::Json(value) => turso::Value::Text(value.to_string()),
                QueryParam::DateTime(value) => match value {
                    Some(dt) => turso::Value::Text(format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }

        let rows = builder.fetch_all(&self.db).await?;
        let mut steps = Vec::with_capacity(rows.len());
        for row in rows {
            steps.push(Self::map_row(&row)?);
        }
        Ok(steps)
    }

    async fn query_step_count(&self, query: QueryBuilder) -> Result<i64> {
        let mut builder = crate::store::turso::query_scalar(query.sql());
        for param in query.params() {
            let value = match param {
                QueryParam::I64(value) => turso::Value::Integer(*value),
                QueryParam::I32(value) => turso::Value::Integer((*value).into()),
                QueryParam::String(value) => turso::Value::Text(value.clone()),
                QueryParam::Json(value) => turso::Value::Text(value.to_string()),
                QueryParam::DateTime(value) => match value {
                    Some(dt) => turso::Value::Text(format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }
        builder.fetch_one(&self.db).await
    }

    async fn execute_step_delete(&self, query: QueryBuilder) -> Result<u64> {
        let mut builder = crate::store::turso::query(query.sql());
        for param in query.params() {
            let value = match param {
                QueryParam::I64(value) => turso::Value::Integer(*value),
                QueryParam::I32(value) => turso::Value::Integer((*value).into()),
                QueryParam::String(value) => turso::Value::Text(value.clone()),
                QueryParam::Json(value) => turso::Value::Text(value.to_string()),
                QueryParam::DateTime(value) => match value {
                    Some(dt) => turso::Value::Text(format_turso_timestamp(dt)),
                    None => turso::Value::Null,
                },
            };
            builder = builder.bind(value);
        }
        builder.execute_once(&self.db).await
    }
}
