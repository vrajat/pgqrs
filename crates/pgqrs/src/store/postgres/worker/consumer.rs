//! Consumer operations and consumer interface for pgqrs.
//!
//! This module defines the [`Consumer`] struct, which provides methods for dequeuing, and managing jobs in a PostgreSQL-backed queue.
//! For message production, use the [`crate::producer::Producer`] struct.
//!
//! ## What
//!
//! - [`Consumer`] is the consumer interface for interacting with a queue: fetching jobs, updating visibility, archiving and deleting messages.
//! - [`crate::producer::Producer`] handles message production and is defined in the `producer` module.
//! - Implements the [`Worker`] trait for lifecycle management
//!
//! ## How
//!
//! Create a [`Consumer`] using `Consumer::register()` which handles worker registration automatically.
//! Create a [`crate::producer::Producer`] for enqueueing messages.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::consumer::Consumer;
//! use pgqrs::producer::Producer;
//! // let consumer = Consumer::register(pool, &queue_info, "localhost", 8080, &config).await?;
//! // let producer = Producer::register(pool, &queue_info, "localhost", 8081, &config).await?;
//! // producer.enqueue(...)
//! // consumer.dequeue(...)
//! ```

use super::lifecycle::WorkerLifecycle;
use crate::error::Result;
use crate::store::postgres::tables::Messages;
use crate::store::MessageTable;
use crate::types::{ArchivedMessage, QueueMessage, WorkerStatus};
use async_trait::async_trait;
use sqlx::PgPool;

/// Default visibility timeout in seconds for locked messages
const VISIBILITY_TIMEOUT: u32 = 5;

/// Delete batch of messages
const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1) AND consumer_worker_id = $2
    RETURNING id;
"#;

/// Archive single message (atomic operation)
const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_messages
        WHERE id = $1 AND consumer_worker_id = $2
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msg
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at;
"#;

/// Archive batch of messages (efficient batch operation)
const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE id = ANY($1) AND consumer_worker_id = $2
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NOW() + make_interval(secs => $3::double precision),
        read_ct = read_ct + 1,
        dequeued_at = NOW(),
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= NOW())
          AND consumer_worker_id IS NULL
        ORDER BY enqueued_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const DELETE_MESSAGE_OWNED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1 AND consumer_worker_id = $2
"#;

/// Consumer interface for a specific queue.
///
/// A Consumer instance provides methods for dequeuing messages, reading messages,
/// and managing message lifecycle within a specific PostgreSQL-backed queue.
/// Each Consumer corresponds to a row in the pgqrs_queues table.
///
/// For message production, use the Producer struct.
///
/// Implements the [`Worker`] trait for lifecycle management.
pub struct Consumer {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    queue_info: crate::types::QueueInfo,
    /// Configuration for the queue
    config: crate::config::Config,
    /// Worker information for this consumer
    worker_info: crate::types::WorkerInfo,
    /// Worker lifecycle manager
    lifecycle: WorkerLifecycle,
}

impl Consumer {
    pub async fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let lifecycle = WorkerLifecycle::new(pool.clone());
        let worker_info = lifecycle.register(queue_info, hostname, port).await?;
        tracing::debug!(
            "Registered consumer worker {} ({}:{}) for queue '{}'",
            worker_info.id,
            hostname,
            port,
            queue_info.queue_name
        );

        Ok(Self {
            pool: pool.clone(),
            queue_info: queue_info.clone(),
            worker_info,
            config: config.clone(),
            lifecycle,
        })
    }

    /// Create an ephemeral consumer (NULL hostname/port, auto-cleanup).
    ///
    /// Used by high-level API functions like `consume()`.
    pub async fn new_ephemeral(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let lifecycle = WorkerLifecycle::new(pool.clone());
        let worker_info = lifecycle.register_ephemeral(queue_info).await?;
        tracing::debug!(
            "Registered ephemeral consumer worker {} for queue '{}'",
            worker_info.id,
            queue_info.queue_name
        );

        Ok(Self {
            pool: pool.clone(),
            queue_info: queue_info.clone(),
            worker_info,
            config: config.clone(),
            lifecycle,
        })
    }
}

#[async_trait]
impl crate::store::Worker for Consumer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.lifecycle.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.lifecycle
            .is_healthy(self.worker_info.id, max_age)
            .await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.lifecycle.get_status(self.worker_info.id).await
    }

    /// Suspend this consumer (transition from Ready to Suspended).
    async fn suspend(&self) -> Result<()> {
        self.lifecycle.suspend(self.worker_info.id).await
    }

    /// Resume this consumer (transition from Suspended to Ready).
    async fn resume(&self) -> Result<()> {
        self.lifecycle.resume(self.worker_info.id).await
    }

    /// Gracefully shutdown this consumer.
    async fn shutdown(&self) -> Result<()> {
        // Check if consumer has pending messages
        let pending_count = self
            .lifecycle
            .count_pending_messages(self.worker_info.id)
            .await?;

        if pending_count > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending_count as u64,
                reason: "Consumer must release all messages before shutdown".to_string(),
            });
        }

        self.lifecycle.shutdown(self.worker_info.id).await
    }
}

#[async_trait]
impl crate::store::Consumer for Consumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, VISIBILITY_TIMEOUT)
            .await
    }

    async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    async fn dequeue_many_with_delay(&self, limit: usize, vt: u32) -> Result<Vec<QueueMessage>> {
        // FIXED: Corrected parameter order to match SQL
        // SQL expects: $1=queue_id, $2=limit, $3=vt, $4=worker_id
        let messages = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES)
            .bind(self.queue_info.id) // $1 - queue_id
            .bind(limit as i64) // $2 - limit
            .bind(vt as i32) // $3 - vt (visibility timeout)
            .bind(self.worker_info.id) // $4 - worker_id
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: e.to_string(),
            })?;

        Ok(messages)
    }

    async fn extend_visibility(&self, message_id: i64, additional_seconds: u32) -> Result<bool> {
        let messages = Messages::new(self.pool.clone());
        let rows_affected = messages
            .extend_visibility(message_id, self.worker_info.id, additional_seconds)
            .await?;
        Ok(rows_affected > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let rows_affected = sqlx::query(DELETE_MESSAGE_OWNED)
            .bind(message_id)
            .bind(self.worker_info.id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: e.to_string(),
            })?
            .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        let deleted_ids: Vec<i64> = sqlx::query_scalar(DELETE_MESSAGE_BATCH)
            .bind(&message_ids)
            .bind(self.worker_info.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: e.to_string(),
            })?;

        // For each input id, true if it was deleted, false otherwise
        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = message_ids
            .into_iter()
            .map(|id| deleted_set.contains(&id))
            .collect();
        Ok(result)
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
        let result: Option<ArchivedMessage> = sqlx::query_as::<_, ArchivedMessage>(ARCHIVE_MESSAGE)
            .bind(msg_id)
            .bind(self.worker_info.id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to archive message {msg_id}: {e}"),
            })?;

        Ok(result)
    }

    async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        if msg_ids.is_empty() {
            return Ok(vec![]);
        }

        let archived_ids: Vec<i64> = sqlx::query_scalar(ARCHIVE_BATCH)
            .bind(&msg_ids)
            .bind(self.worker_info.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::Connection {
                message: format!("Failed to archive batch messages: {e}"),
            })?;

        // For each input id, true if it was archived, false otherwise
        let archived_set: std::collections::HashSet<i64> = archived_ids.into_iter().collect();
        let result = msg_ids
            .into_iter()
            .map(|id| archived_set.contains(&id))
            .collect();
        Ok(result)
    }

    async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
        let messages = Messages::new(self.pool.clone());
        let results = messages
            .release_messages_by_ids(message_ids, self.worker_info.id)
            .await?;
        // Count how many were successfully released
        Ok(results.into_iter().filter(|&released| released).count() as u64)
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for Consumer {
    fn drop(&mut self) {
        // Check if this is an ephemeral worker by hostname prefix
        if self.worker_info.hostname.starts_with("__ephemeral__") {
            // Spawn a task to properly shutdown the worker
            let lifecycle = self.lifecycle.clone();
            let worker_id = self.worker_info.id;

            // Best-effort shutdown - ignore errors since we're in Drop
            tokio::task::spawn(async move {
                // Suspend then shutdown the worker
                let _ = lifecycle.suspend(worker_id).await;
                let _ = lifecycle.shutdown(worker_id).await;
            });
        }
    }
}
