use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub database_url: String,
    pub max_connections: u32,
    pub visibility_timeout: u64,
    pub dead_letter_after: u32,
}
