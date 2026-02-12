use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewWorkflow, WorkflowRecord};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone)]
pub struct SqliteWorkflowTable {
    pool: SqlitePool,
}

impl SqliteWorkflowTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<WorkflowRecord> {
        let workflow_id: i64 = row.try_get("workflow_id")?;
        let name: String = row.try_get("name")?;
        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;

        Ok(WorkflowRecord {
            workflow_id,
            name,
            created_at,
        })
    }
}

#[async_trait]
impl crate::store::WorkflowTable for SqliteWorkflowTable {
    async fn insert(&self, data: NewWorkflow) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        let row = sqlx::query(
            r#"
            INSERT INTO pgqrs_workflows (name, created_at)
            VALUES ($1, $2)
            RETURNING workflow_id, name, created_at
            "#,
        )
        .bind(&data.name)
        .bind(now_str)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "INSERT_WORKFLOW".into(),
            source: Box::new(e),
            context: format!("Failed to insert workflow '{}'", data.name),
        })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        let row = sqlx::query(
            r#"
            SELECT workflow_id, name, created_at
            FROM pgqrs_workflows
            WHERE workflow_id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: format!("GET_WORKFLOW ({})", id),
            source: Box::new(e),
            context: format!("Failed to get workflow {}", id),
        })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT workflow_id, name, created_at
            FROM pgqrs_workflows
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "LIST_WORKFLOWS".into(),
            source: Box::new(e),
            context: "Failed to list workflows".into(),
        })?;

        let mut workflows = Vec::with_capacity(rows.len());
        for row in rows {
            workflows.push(Self::map_row(row)?);
        }
        Ok(workflows)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_workflows")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_WORKFLOWS".into(),
                source: Box::new(e),
                context: "Failed to count workflows".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM pgqrs_workflows WHERE workflow_id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_WORKFLOW ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete workflow {}", id),
            })?;
        Ok(result.rows_affected())
    }
}
