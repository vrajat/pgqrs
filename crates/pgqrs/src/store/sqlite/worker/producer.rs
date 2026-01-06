use sqlx::SqlitePool;
use crate::Config;

#[derive(Debug, Clone)]
pub struct SqliteProducer {
    pool: SqlitePool,
    config: Config,
}
