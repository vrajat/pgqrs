
use crate::error::Result;
use crate::store::WorkflowStore;
use crate::types::{NewWorkflow, WorkflowRecord};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct PostgresWorkflowStore {
    pool: PgPool,
}

impl PostgresWorkflowStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// SQL Constants (ported from src/tables/pgqrs_workflows.rs)
const INSERT_WORKFLOW: &str = r#"
    INSERT INTO pgqrs_workflows (name, status, input)
    VALUES ($1, 'PENDING'::pgqrs_workflow_status, $2)
    RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
"#;

const GET_WORKFLOW: &str = r#"
    SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
    FROM pgqrs_workflows
    WHERE workflow_id = $1
"#;

const LIST_WORKFLOWS: &str = r#"
    SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
    FROM pgqrs_workflows
    ORDER BY created_at DESC
"#;

const COUNT_WORKFLOWS: &str = "SELECT COUNT(*) FROM pgqrs_workflows";

const DELETE_WORKFLOW: &str = "DELETE FROM pgqrs_workflows WHERE workflow_id = $1";

#[async_trait::async_trait]
impl WorkflowStore for PostgresWorkflowStore {
    type Error = sqlx::Error;

    async fn insert(&self, data: NewWorkflow) -> std::result::Result<WorkflowRecord, Self::Error> {
        let row = sqlx::query_as::<_, WorkflowRecord>(INSERT_WORKFLOW)
            .bind(data.name)
            .bind(data.input)
            .fetch_one(&self.pool)
            .await?;
        Ok(row)
    }

    async fn get(&self, id: i64) -> std::result::Result<WorkflowRecord, Self::Error> {
        let row = sqlx::query_as::<_, WorkflowRecord>(GET_WORKFLOW)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row)
    }

    async fn list(&self) -> std::result::Result<Vec<WorkflowRecord>, Self::Error> {
        let rows = sqlx::query_as::<_, WorkflowRecord>(LIST_WORKFLOWS)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn count(&self) -> std::result::Result<i64, Self::Error> {
        let count = sqlx::query_scalar(COUNT_WORKFLOWS)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> std::result::Result<u64, Self::Error> {
        let rows = sqlx::query(DELETE_WORKFLOW)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(rows)
    }
}
