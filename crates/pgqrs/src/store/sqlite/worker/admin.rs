use sqlx::SqlitePool;
use crate::Config;

#[derive(Debug, Clone)]
pub struct SqliteAdmin {
    pool: SqlitePool,
    config: Config,
}
