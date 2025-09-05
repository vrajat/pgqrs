pub(crate) mod constants;

use crate::schema::pgqrs::meta;

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{Queryable, Selectable};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// A message in the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMessage {
    pub id: Uuid,
    pub queue_name: String,
    pub payload: serde_json::Value,
    pub message_type: Option<String>,
    pub enqueued_at: DateTime<Utc>,
    pub locked_until: Option<DateTime<Utc>>,
    pub read_count: i32,
    pub created_at: DateTime<Utc>,
}

/// Queue metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    pub name: String,
    pub total_messages: i64,
    pub pending_messages: i64,
    pub locked_messages: i64,
    pub archived_messages: i64,
    pub oldest_pending_message: Option<DateTime<Utc>>,
    pub newest_message: Option<DateTime<Utc>>,
}

/// Configuration for reading messages
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub lock_time_seconds: u32,
    pub batch_size: Option<usize>,
    pub message_type: Option<String>, // Filter by message type
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            lock_time_seconds: 5,
            batch_size: Some(1),
            message_type: None,
        }
    }
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = meta)]
pub struct MetaResult {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub queue_name: String,
    pub created_at: NaiveDateTime,
}

impl fmt::Display for MetaResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MetaResult {{ queue_name: {}, created_at: {} }}",
            self.queue_name, self.created_at
        )
    }
}
