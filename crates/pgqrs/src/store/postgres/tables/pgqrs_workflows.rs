use crate::error::Result;
use sqlx::PgPool;

// Import shared types instead of redefining them
// Import shared types instead of redefining them
pub use crate::types::{NewWorkflow, WorkflowRecord};

#[derive(Debug, Clone)]
pub struct Workflows {
    pool: PgPool,
}

impl Workflows {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn complete_workflow(
        executor: &mut sqlx::PgConnection,
        id: i64,
        output: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, updated_at = NOW()
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .bind(output)
        .execute(executor)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "COMPLETE_WORKFLOW".into(),
            source: e,
            context: format!("Failed to complete workflow {}", id),
        })?;
        Ok(())
    }

    pub async fn fail_workflow(
        executor: &mut sqlx::PgConnection,
        id: i64,
        error: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pgqrs_workflows
            SET status = 'ERROR'::pgqrs_workflow_status, error = $2, updated_at = NOW()
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .execute(executor)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "FAIL_WORKFLOW".into(),
            source: e,
            context: format!("Failed to fail workflow {}", id),
        })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::store::WorkflowTable for Workflows {
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let row = sqlx::query_as::<_, WorkflowRecord>(
            r#"
            INSERT INTO pgqrs_workflows (name, status, input)
            VALUES ($1, 'PENDING'::pgqrs_workflow_status, $2)
            RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            "#,
        )
        .bind(&data.name)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW".into(),
            source: e,
            context: format!("Failed to insert workflow '{}'", data.name),
        })?;

        Ok(row)
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        let row = sqlx::query_as::<_, WorkflowRecord>(
            r#"
            SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            FROM pgqrs_workflows
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("GET_WORKFLOW ({})", id),
            source: e,
            context: format!("Failed to get workflow {}", id),
        })?;

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        let rows = sqlx::query_as::<_, WorkflowRecord>(
            r#"
            SELECT workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            FROM pgqrs_workflows
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_WORKFLOWS".into(),
            source: e,
            context: "Failed to list workflows".into(),
        })?;

        Ok(rows)
    }

    async fn count(&self) -> Result<i64> {
        let count = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflows")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOWS".into(),
                source: e,
                context: "Failed to count workflows".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query("DELETE FROM pgqrs_workflows WHERE workflow_id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKFLOW ({})", id),
                source: e,
                context: format!("Failed to delete workflow {}", id),
            })?
            .rows_affected();
        Ok(rows_affected)
    }
}
