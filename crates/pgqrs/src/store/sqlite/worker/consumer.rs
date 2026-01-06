use sqlx::SqlitePool;
use crate::Config;

#[derive(Debug, Clone)]
pub struct SqliteConsumer {
    pool: SqlitePool,
    config: Config,
}
