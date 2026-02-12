use crate::error::Result;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::types::{NewWorkflow, WorkflowRecord};
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoWorkflowTable {
    db: Arc<Database>,
}

impl TursoWorkflowTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn map_row(row: &turso::Row) -> Result<WorkflowRecord> {
        let workflow_id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let created_at = parse_turso_timestamp(&row.get::<String>(2)?)?;

        Ok(WorkflowRecord {
            workflow_id,
            name,
            created_at,
        })
    }

    pub async fn complete_workflow(&self, id: i64, output: serde_json::Value) -> Result<()> {
        let output_str = output.to_string();
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        crate::store::turso::query(
            r#"
            UPDATE pgqrs_workflows
            SET output = ?, updated_at = ?
            WHERE workflow_id = ?
            "#,
        )
        .bind(output_str)
        .bind(now_str)
        .bind(id)
        .execute_once(&self.db)
        .await?;

        Ok(())
    }

    pub async fn fail_workflow(&self, id: i64, error: serde_json::Value) -> Result<()> {
        let error_str = error.to_string();
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        crate::store::turso::query(
            r#"
            UPDATE pgqrs_workflows
            SET error = ?, updated_at = ?
            WHERE workflow_id = ?
            "#,
        )
        .bind(error_str)
        .bind(now_str)
        .bind(id)
        .execute_once(&self.db)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl crate::store::WorkflowTable for TursoWorkflowTable {
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let now_str = format_turso_timestamp(&now);

        let row = crate::store::turso::query(
            r#"
            INSERT INTO pgqrs_workflows (name, created_at)
            VALUES (?, ?)
            RETURNING workflow_id, name, created_at
            "#,
        )
        .bind(data.name.as_str())
        .bind(now_str)
        .fetch_one_once(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        let row = crate::store::turso::query(
            r#"
            SELECT workflow_id, name, created_at
            FROM pgqrs_workflows
            WHERE workflow_id = ?
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        let rows = crate::store::turso::query(
            r#"
            SELECT workflow_id, name, created_at
            FROM pgqrs_workflows
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.db)
        .await?;

        let mut workflows = Vec::with_capacity(rows.len());
        for row in rows {
            workflows.push(Self::map_row(&row)?);
        }
        Ok(workflows)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_workflows")
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query("DELETE FROM pgqrs_workflows WHERE workflow_id = ?")
            .bind(id)
            .execute_once(&self.db)
            .await?;
        Ok(count)
    }
}
