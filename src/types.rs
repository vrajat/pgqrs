use crate::schema::pgqrs::meta;

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{prelude::QueryableByName, Queryable, Selectable};
use serde::{Deserialize, Serialize};
use std::fmt::{self};

/// A message in the queue
#[derive(Debug, Clone, Serialize, Deserialize, QueryableByName)]
pub struct QueueMessage {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub msg_id: i64,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub read_ct: i32,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub vt: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub message: serde_json::Value,
}

impl fmt::Display for QueueMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueueMessage {{ msg_id: {}, read_ct: {}, enqueued_at: {}, vt: {}, message: {} }}",
            self.msg_id, self.read_ct, self.enqueued_at, self.vt, self.message
        )
    }
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

#[derive(Queryable, Selectable, PartialEq, Debug)]
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
