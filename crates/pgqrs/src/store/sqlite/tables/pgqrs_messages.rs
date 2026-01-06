use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteMessageTable {
    pool: SqlitePool,
}

impl SqliteMessageTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
