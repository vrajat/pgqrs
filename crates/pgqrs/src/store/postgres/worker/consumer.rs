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
//! ```rust,no_run
//! # use pgqrs::{Consumer, Producer, Config};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let config = Config::from_dsn("postgresql://localhost/test");
//! # let store = pgqrs::connect_with_config(&config).await?;
//! let consumer = pgqrs::consumer("localhost", 8080, "jobs")
//!     .create(&store)
//!     .await?;
//! let producer = pgqrs::producer("localhost", 8081, "jobs")
//!     .create(&store)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::error::Result;
use crate::store::postgres::tables::pgqrs_workers::Workers;
use crate::store::postgres::tables::Messages;
use crate::store::WorkerTable;
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

const DEQUEUE_MESSAGES_AT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = $5 + make_interval(secs => $3::double precision),
        read_ct = read_ct + 1,
        dequeued_at = $5,
        consumer_worker_id = $4
    WHERE id IN (
        SELECT id
        FROM pgqrs_messages
        WHERE queue_id = $1
          AND (vt IS NULL OR vt <= $5)
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
    _config: crate::config::Config,
    /// Worker information for this consumer
    worker_info: crate::types::WorkerInfo,
    /// Worker lifecycle manager (Workers repository handles this now)
    workers: Workers,
}

impl Consumer {
    pub async fn new(
        pool: PgPool,
        queue_info: &crate::types::QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_info = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

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
            _config: config.clone(),
            workers,
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
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_info = workers.register_ephemeral(Some(queue_info.id)).await?;

        tracing::debug!(
            "Registered ephemeral consumer worker {} for queue '{}'",
            worker_info.id,
            queue_info.queue_name
        );

        Ok(Self {
            pool: pool.clone(),
            queue_info: queue_info.clone(),
            worker_info,
            _config: config.clone(),
            workers,
        })
    }
}

#[async_trait]
impl crate::store::Worker for Consumer {
    fn worker_id(&self) -> i64 {
        self.worker_info.id
    }

    async fn heartbeat(&self) -> Result<()> {
        self.workers.heartbeat(self.worker_info.id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.workers.is_healthy(self.worker_info.id, max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        self.workers.get_status(self.worker_info.id).await
    }

    /// Suspend this consumer (transition from Ready to Suspended).
    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_info.id).await
    }

    /// Resume this consumer (transition from Suspended to Ready).
    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_info.id).await
    }

    /// Gracefully shutdown this consumer.
    async fn shutdown(&self) -> Result<()> {
        // Check if consumer has pending messages
        // Use count_pending_filtered from Messages for cleaner abstraction
        let messages = Messages::new(self.pool.clone());
        let pending_count = messages
            .count_pending_filtered(self.queue_info.id, Some(self.worker_info.id))
            .await?;

        if pending_count > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending_count as u64,
                reason: "Consumer must release all messages before shutdown".to_string(),
            });
        }

        self.workers.shutdown(self.worker_info.id).await
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE_MESSAGES".into(),
                source: e,
                context: format!(
                    "Failed to dequeue messages for worker {}",
                    self.worker_info.id
                ),
            })?;

        Ok(messages)
    }

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<QueueMessage>> {
        // SQL expects: $1=queue_id, $2=limit, $3=vt, $4=worker_id, $5=now
        let messages = sqlx::query_as::<_, QueueMessage>(DEQUEUE_MESSAGES_AT)
            .bind(self.queue_info.id) // $1 - queue_id
            .bind(limit as i64) // $2 - limit
            .bind(vt as i32) // $3 - vt (visibility timeout)
            .bind(self.worker_info.id) // $4 - worker_id
            .bind(now) // $5 - now (reference time)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DEQUEUE_MESSAGES".into(),
                source: e,
                context: "Failed to dequeue messages".into(),
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_OWNED".into(),
                source: e,
                context: format!("Failed to delete message {}", message_id),
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_MESSAGE_BATCH".into(),
                source: e,
                context: "Failed to delete message batch".into(),
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_MESSAGE ({})", msg_id),
                source: e,
                context: format!("Failed to archive message {}", msg_id),
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ARCHIVE_BATCH".into(),
                source: e,
                context: "Failed to archive message batch".into(),
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
            let workers = self.workers.clone();
            let worker_id = self.worker_info.id;

            // Best-effort shutdown - check if we are in a runtime context
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    // Suspend then shutdown the worker
                    let _ = workers.suspend(worker_id).await;
                    let _ = workers.shutdown(worker_id).await;
                });
            } else {
                tracing::warn!(
                    "Skipping ephemeral worker cleanup for {} - no tokio runtime available",
                    worker_id
                );
            }
        }
    }
}
