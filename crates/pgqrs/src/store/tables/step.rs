use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::query::QueryBuilder;
use crate::types::StepRecord;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait DialectStepTable: crate::store::StepRecordTable + Sync {
    type Dialect: SqlDialect;

    async fn fetch_all_steps(&self, query: QueryBuilder) -> Result<Vec<StepRecord>>;
    async fn query_step_count(&self, query: QueryBuilder) -> Result<i64>;
    async fn execute_step_delete(&self, query: QueryBuilder) -> Result<u64>;

    async fn dialect_get_step(&self, id: i64) -> Result<StepRecord> {
        self.execute(QueryBuilder::new(Self::Dialect::STEP.get).bind_i64(id))
            .await
    }

    async fn dialect_list_steps(&self) -> Result<Vec<StepRecord>> {
        self.fetch_all_steps(QueryBuilder::new(Self::Dialect::STEP.list))
            .await
    }

    async fn dialect_count_steps(&self) -> Result<i64> {
        self.query_step_count(QueryBuilder::new(Self::Dialect::STEP.count))
            .await
    }

    async fn dialect_delete_step(&self, id: i64) -> Result<u64> {
        self.execute_step_delete(QueryBuilder::new(Self::Dialect::STEP.delete).bind_i64(id))
            .await
    }

    async fn dialect_acquire_step(&self, run_id: i64, step_name: &str) -> Result<StepRecord> {
        self.execute(
            QueryBuilder::new(Self::Dialect::STEP.acquire)
                .bind_i64(run_id)
                .bind_string(step_name.to_string()),
        )
        .await
    }

    async fn dialect_clear_retry(&self, id: i64) -> Result<StepRecord> {
        self.execute(QueryBuilder::new(Self::Dialect::STEP.clear_retry).bind_i64(id))
            .await
    }

    async fn dialect_complete_step(
        &self,
        id: i64,
        output: serde_json::Value,
    ) -> Result<StepRecord> {
        self.execute(
            QueryBuilder::new(Self::Dialect::STEP.complete)
                .bind_i64(id)
                .bind_json(output),
        )
        .await
    }

    async fn dialect_fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        self.execute(
            QueryBuilder::new(Self::Dialect::STEP.fail)
                .bind_i64(id)
                .bind_json(error)
                .bind_datetime(retry_at)
                .bind_i32(retry_count),
        )
        .await
    }
}
