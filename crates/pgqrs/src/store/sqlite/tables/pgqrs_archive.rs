use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteArchiveTable {
    pool: SqlitePool,
}

impl SqliteArchiveTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
