use crate::Config;
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteProducer {
    pool: SqlitePool,
    config: Config,
}
