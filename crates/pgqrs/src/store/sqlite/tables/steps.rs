use crate::error::Result;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::sqlite::dialect::SqliteDialect;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::store::tables::DialectStepTable;
use crate::types::{StepRecord, WorkflowStatus};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SqliteStepRecordTable {
    pool: SqlitePool,
}

impl SqliteStepRecordTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<StepRecord> {
        let id: i64 = row.try_get("id")?;
        let run_id: i64 = row.try_get("run_id")?;
        let step_name: String = row.try_get("step_name")?;

        let status_str: String = row.try_get("status")?;
        let status = WorkflowStatus::from_str(&status_str)
            .map_err(|e| crate::error::Error::Internal { message: e })?;

        let input_str: Option<String> = row.try_get("input")?;
        let input: Option<Value> = match input_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let output_str: Option<String> = row.try_get("output")?;
        let output: Option<Value> = match output_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let error_str: Option<String> = row.try_get("error")?;
        let error: Option<Value> = match error_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        let retry_at_str: Option<String> = row.try_get("retry_at")?;
        let retry_at = match retry_at_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        let retry_count: i32 = row.try_get("retry_count")?;

        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;
        let updated_at = parse_sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?;

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
impl crate::store::StepRecordTable for SqliteStepRecordTable {
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
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.to_string()),
                QueryParam::DateTime(value) => {
                    builder.bind(value.map(|dt| format_sqlite_timestamp(&dt)))
                }
            };
        }

        let row =
            builder
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQLITE_EXECUTE_STEP".into(),
                    source: Box::new(e),
                    context: "Failed to execute sqlite workflow step query".into(),
                })?;
        Self::map_row(row)
    }
}

#[async_trait]
impl DialectStepTable for SqliteStepRecordTable {
    type Dialect = SqliteDialect;

    async fn fetch_all_steps(&self, query: QueryBuilder) -> Result<Vec<StepRecord>> {
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.to_string()),
                QueryParam::DateTime(value) => {
                    builder.bind(value.map(|dt| format_sqlite_timestamp(&dt)))
                }
            };
        }

        let rows =
            builder
                .fetch_all(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQLITE_FETCH_ALL_STEP".into(),
                    source: Box::new(e),
                    context: "Failed to fetch sqlite workflow step rows".into(),
                })?;

        let mut steps = Vec::with_capacity(rows.len());
        for row in rows {
            steps.push(Self::map_row(row)?);
        }
        Ok(steps)
    }

    async fn query_step_count(&self, query: QueryBuilder) -> Result<i64> {
        let mut builder = sqlx::query_scalar(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.to_string()),
                QueryParam::DateTime(value) => {
                    builder.bind(value.map(|dt| format_sqlite_timestamp(&dt)))
                }
            };
        }

        builder
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQLITE_COUNT_STEP".into(),
                source: Box::new(e),
                context: "Failed to count sqlite workflow steps".into(),
            })
    }

    async fn execute_step_delete(&self, query: QueryBuilder) -> Result<u64> {
        let mut builder = sqlx::query(query.sql());
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value.to_string()),
                QueryParam::DateTime(value) => {
                    builder.bind(value.map(|dt| format_sqlite_timestamp(&dt)))
                }
            };
        }

        let result =
            builder
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SQLITE_DELETE_STEP".into(),
                    source: Box::new(e),
                    context: "Failed to delete sqlite workflow step".into(),
                })?;
        Ok(result.rows_affected())
    }
}
