use crate::error::Result;
use crate::workflow::WorkflowStatus;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, FromRow, Clone)]
pub struct WorkflowRecord {
    pub workflow_id: Uuid,
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
    pub workflow_id: Uuid,
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

    pub async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let row = sqlx::query_as::<_, WorkflowRecord>(
            r#"
            INSERT INTO pgqrs_workflows (workflow_id, name, status, input)
            VALUES ($1, $2, 'PENDING'::pgqrs_workflow_status, $3)
            RETURNING workflow_id, name, status, input, output, error, created_at, updated_at, executor_id
            "#,
        )
        .bind(data.workflow_id)
        .bind(data.name)
        .bind(data.input)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    pub async fn get(&self, id: Uuid) -> Result<WorkflowRecord> {
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
}
