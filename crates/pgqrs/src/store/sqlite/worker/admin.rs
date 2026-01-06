use crate::Config;
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteAdmin {
    pool: SqlitePool,
    config: Config,
}
