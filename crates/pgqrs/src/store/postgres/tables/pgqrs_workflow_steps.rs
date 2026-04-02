use crate::error::Result;
use crate::store::postgres::dialect::PostgresDialect;
use crate::store::query::{QueryBuilder, QueryParam};
use crate::store::tables::DialectStepTable;
use crate::types::StepRecord;
use async_trait::async_trait;
use sqlx::{PgPool, Postgres};

#[derive(Debug, Clone)]
pub struct StepRecords {
    pool: PgPool,
}

impl StepRecords {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn bind_query<'a>(
        mut builder: sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments>,
        query: &'a QueryBuilder,
    ) -> sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments> {
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }
        builder
    }

    fn bind_query_as<'a>(
        mut builder: sqlx::query::QueryAs<'a, Postgres, StepRecord, sqlx::postgres::PgArguments>,
        query: &'a QueryBuilder,
    ) -> sqlx::query::QueryAs<'a, Postgres, StepRecord, sqlx::postgres::PgArguments> {
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }
        builder
    }

    fn bind_scalar_query<'a>(
        mut builder: sqlx::query::QueryScalar<'a, Postgres, i64, sqlx::postgres::PgArguments>,
        query: &'a QueryBuilder,
    ) -> sqlx::query::QueryScalar<'a, Postgres, i64, sqlx::postgres::PgArguments> {
        for param in query.params() {
            builder = match param {
                QueryParam::I64(value) => builder.bind(*value),
                QueryParam::I32(value) => builder.bind(*value),
                QueryParam::String(value) => builder.bind(value),
                QueryParam::Json(value) => builder.bind(value),
                QueryParam::DateTime(value) => builder.bind(*value),
            };
        }
        builder
    }
}

#[async_trait]
impl crate::store::StepRecordTable for StepRecords {
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

    async fn complete_step(&self, id: i64, output: serde_json::Value) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_complete_step(self, id, output).await
    }

    async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        <Self as DialectStepTable>::dialect_fail_step(self, id, error, retry_at, retry_count).await
    }

    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord> {
        Self::bind_query_as(sqlx::query_as::<_, StepRecord>(query.sql()), &query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "POSTGRES_EXECUTE_STEP".into(),
                source: Box::new(e),
                context: "Failed to execute postgres workflow step query".into(),
            })
    }
}

#[async_trait]
impl DialectStepTable for StepRecords {
    type Dialect = PostgresDialect;

    async fn fetch_all_steps(&self, query: QueryBuilder) -> Result<Vec<StepRecord>> {
        Self::bind_query_as(sqlx::query_as::<_, StepRecord>(query.sql()), &query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "POSTGRES_FETCH_ALL_STEPS".into(),
                source: Box::new(e),
                context: "Failed to fetch postgres workflow step rows".into(),
            })
    }

    async fn query_step_count(&self, query: QueryBuilder) -> Result<i64> {
        Self::bind_scalar_query(sqlx::query_scalar(query.sql()), &query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "POSTGRES_COUNT_STEPS".into(),
                source: Box::new(e),
                context: "Failed to count postgres workflow steps".into(),
            })
    }

    async fn execute_step_delete(&self, query: QueryBuilder) -> Result<u64> {
        let res = Self::bind_query(sqlx::query(query.sql()), &query)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "POSTGRES_DELETE_STEP".into(),
                source: Box::new(e),
                context: "Failed to delete postgres workflow step".into(),
            })?;
        Ok(res.rows_affected())
    }
}
