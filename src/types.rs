//! Core types for pgqrs: queue messages, metrics, and metadata.
//!
//! This module defines the main data structures used for queue operations, metrics, and metadata.
//!
//! ## What
//!
//! - [`QueueMessage`] represents a job/message in the queue.
//! - [`QueueMetrics`] provides statistics about a queue.
//! - [`MetaResult`] describes queue metadata from the database.
//!
//! ## How
//!
//! Use these types for interacting with queue data, inspecting metrics, and reading metadata.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::types::QueueMessage;
//! let msg: QueueMessage = /* ... */;
//! println!("{}", msg);
//! ```
use crate::schema::pgqrs::meta;

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{prelude::QueryableByName, Queryable, Selectable};
use serde::{Deserialize, Serialize};
use std::fmt::{self};

/// A message in the queue
#[derive(Debug, Clone, Serialize, Deserialize, QueryableByName)]
pub struct QueueMessage {
    /// Unique message ID
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub msg_id: i64,
    /// Number of times this message has been read
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub read_ct: i32,
    /// Timestamp when the message was enqueued
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Visibility timeout (when the message becomes available again)
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub vt: chrono::DateTime<chrono::Utc>,
    /// The actual message payload (JSON)
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
    /// Name of the queue
    pub name: String,
    /// Total number of messages ever enqueued
    pub total_messages: i64,
    /// Number of messages currently pending
    pub pending_messages: i64,
    /// Number of messages currently locked (being processed)
    pub locked_messages: i64,
    /// Number of messages archived
    pub archived_messages: i64,
    /// Timestamp of the oldest pending message
    pub oldest_pending_message: Option<DateTime<Utc>>,
    /// Timestamp of the newest message
    pub newest_message: Option<DateTime<Utc>>,
}

#[derive(Queryable, Selectable, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = meta)]
pub struct MetaResult {
    /// Name of the queue
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub queue_name: String,
    /// Timestamp when the queue was created
    pub created_at: NaiveDateTime,
    /// Whether the queue is unlogged (PostgreSQL optimization)
    pub unlogged: bool,
}

impl fmt::Display for MetaResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MetaResult {{ queue_name: {}, created_at: {}, unlogged: {} }}",
            self.queue_name, self.created_at, self.unlogged
        )
    }
}
