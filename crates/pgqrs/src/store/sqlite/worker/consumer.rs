use crate::Config;
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct SqliteConsumer {
    pool: SqlitePool,
    config: Config,
}
