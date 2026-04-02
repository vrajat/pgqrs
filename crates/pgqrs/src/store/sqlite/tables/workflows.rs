use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::sqlite::dialect::SqliteDialect;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{NewWorkflowRecord, WorkflowRecord};
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
        let id: i64 = row.try_get("id")?;
        let name: String = row.try_get("name")?;
        let queue_id: i64 = row.try_get("queue_id")?;
        let created_at = parse_sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?;

        Ok(WorkflowRecord {
            id,
            name,
            queue_id,
            created_at,
        })
    }
}

#[async_trait]
impl crate::store::WorkflowTable for SqliteWorkflowTable {
    async fn get_by_name(&self, name: &str) -> Result<WorkflowRecord> {
        let row = sqlx::query(SqliteDialect::WORKFLOW.get_by_name)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_WORKFLOW_BY_NAME".into(),
                source: Box::new(e),
                context: format!("Failed to get workflow '{}' by name", name),
            })?;

        Self::map_row(row)
    }

    async fn insert(&self, data: NewWorkflowRecord) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);

        let row = sqlx::query(SqliteDialect::WORKFLOW.insert)
            .bind(&data.name)
            .bind(data.queue_id)
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
        let row = sqlx::query(SqliteDialect::WORKFLOW.get)
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
        let rows = sqlx::query(SqliteDialect::WORKFLOW.list)
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
        let count: i64 = sqlx::query_scalar(SqliteDialect::WORKFLOW.count)
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
        let result = sqlx::query(SqliteDialect::WORKFLOW.delete)
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
