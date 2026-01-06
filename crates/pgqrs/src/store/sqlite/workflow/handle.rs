use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteWorkflow {
    pool: SqlitePool,
}
