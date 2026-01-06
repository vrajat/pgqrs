use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteQueueTable {
    pool: SqlitePool,
}

impl SqliteQueueTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
