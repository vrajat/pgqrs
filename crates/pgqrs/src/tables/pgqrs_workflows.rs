use crate::error::Result;
use crate::tables::table::Table;
use crate::workflow::WorkflowStatus;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};

#[derive(Debug, FromRow, Clone)]
pub struct WorkflowRecord {
    pub workflow_id: i64,
    pub name: String,
    pub status: WorkflowStatus,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub error: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub executor_id: Option<String>,
}

pub struct NewWorkflow {
    pub name: String,
    pub input: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct Workflows {
    pool: PgPool,
}

impl Workflows {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Table for Workflows {
    type Entity = WorkflowRecord;
    type NewEntity = NewWorkflow;

    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let row = sqlx::query_as::<_, WorkflowRecord>(
            r#"
            INSERT INTO pgqrs_workflows (name, status, input)
            VALUES ($1, 'PENDING'::pgqrs_workflow_status, $2)
            RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            "#,
        )
        .bind(data.name)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await?;

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
        .await?;

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
        .await?;

        Ok(rows)
    }

    async fn filter_by_fk(&self, _foreign_key_value: i64) -> Result<Vec<WorkflowRecord>> {
        // Workflows don't have FKs to filter by yet
        Ok(vec![])
    }

    async fn count(&self) -> Result<i64> {
        let count = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflows")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to count workflows: {}", e),
            })?;
        Ok(count)
    }

    async fn count_for_fk<'a, 'b: 'a>(
        &self,
        _foreign_key_value: i64,
        _tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64> {
        Ok(0)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows_affected = sqlx::query("DELETE FROM pgqrs_workflows WHERE workflow_id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to delete workflow {}: {}", id, e),
            })?
            .rows_affected();
        Ok(rows_affected)
    }

    async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        _foreign_key_value: i64,
        _tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64> {
        Ok(0)
    }
}
