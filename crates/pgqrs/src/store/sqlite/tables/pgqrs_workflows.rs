use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteWorkflowTable {
    pool: SqlitePool,
}

impl SqliteWorkflowTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
