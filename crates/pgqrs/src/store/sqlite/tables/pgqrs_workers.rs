use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteWorkerTable {
    pool: SqlitePool,
}

impl SqliteWorkerTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
