//! Producer interface for pgqrs queues.
//!
//! This module defines the [`Producer`] struct, which provides methods for enqueueing
//! messages and managing message visibility in a PostgreSQL-backed queue.
//!
//! ## What
//!
//! - [`Producer`] handles message production: adding jobs to queues with validation and rate limiting
//! - Supports single message enqueue, delayed messages, and batch operations
//! - Includes message visibility management for producers
//! - Implements the [`Worker`] trait for lifecycle management
//!
//! ## How
//!
//! Create a [`Producer`] using `Producer::new()` which handles worker registration automatically.
//!
//! ### Example
//!
//! ```rust,no_run
//! # use pgqrs::{Producer, Config};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let config = Config::from_dsn("postgresql://localhost/test");
//! # let store = pgqrs::connect_with_config(&config).await?;
//! let producer = pgqrs::producer("localhost", 8080, "jobs")
//!     .create(&store)
//!     .await?;
//! let message = producer.enqueue(&serde_json::json!({"foo": "bar"})).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::Result;
use crate::store::postgres::tables::pgqrs_workers::Workers;
use crate::store::postgres::tables::Messages;
use crate::store::WorkerTable;
use crate::types::{QueueInfo, QueueMessage, WorkerStatus};
use crate::validation::PayloadValidator;
use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgPool;

/// SQL for replaying a message from the DLQ (archive) back to the active queue
const REPLAY_FROM_DLQ: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_archive
        WHERE id = $1
        RETURNING queue_id, payload, enqueued_at, read_ct
    )
    INSERT INTO pgqrs_messages
        (queue_id, payload, enqueued_at, vt, read_ct)
    SELECT queue_id, payload, enqueued_at, NOW(), read_ct
    FROM archived_msg
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

/// Producer interface for enqueueing messages to a specific queue.
///
/// A Producer instance provides methods for adding messages to a queue,
/// including validation, rate limiting, and batch operations.
/// Each Producer corresponds to a specific queue.
///
/// Implements the [`Worker`] trait for lifecycle management.
pub struct Producer {
    /// Connection pool for PostgreSQL
    pub pool: PgPool,
    /// Queue information including ID and name
    queue_info: QueueInfo,
    /// Worker information for this producer
    worker_info: crate::types::WorkerInfo,
    /// Configuration for the queue including validation settings
    config: crate::config::Config,
    /// Payload validator for this queue
    validator: PayloadValidator,
    /// Messages table operations
    messages: Messages,
    /// Worker lifecycle manager (Workers repository handles this now)
    workers: Workers,
}

impl Producer {
    /// Create a new Producer instance for the specified queue and worker.
    ///
    /// This method creates a Producer instance asynchronously. You must `.await` the result.
    ///
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `queue_info` - Queue information including ID and name
    /// * `worker_info` - Worker information for this producer
    /// * `config` - Configuration including validation settings
    pub async fn new(
        pool: PgPool,
        queue_info: &QueueInfo,
        hostname: &str,
        port: i32,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_info = workers
            .register(Some(queue_info.id), hostname, port)
            .await?;

        tracing::debug!(
            "Registered producer worker {} ({}:{}) for queue '{}'",
            worker_info.id,
            hostname,
            port,
            queue_info.queue_name
        );
        let messages = Messages::new(pool.clone());
        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info: worker_info.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers,
            messages,
        })
    }

    /// Create an ephemeral producer (NULL hostname/port, auto-cleanup).
    ///
    /// Used by high-level API functions like `produce()`.
    pub async fn new_ephemeral(
        pool: PgPool,
        queue_info: &QueueInfo,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let workers = crate::store::postgres::tables::Workers::new(pool.clone());
        let worker_info = workers.register_ephemeral(Some(queue_info.id)).await?;

        let messages = Messages::new(pool.clone());
        Ok(Self {
            pool,
            queue_info: queue_info.clone(),
            worker_info: worker_info.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
            config: config.clone(),
            workers,
            messages,
        })
    }

    pub fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

#[async_trait]
impl crate::store::Worker for Producer {
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

    async fn suspend(&self) -> Result<()> {
        self.workers.suspend(self.worker_info.id).await
    }

    async fn resume(&self) -> Result<()> {
        self.workers.resume(self.worker_info.id).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.workers.shutdown(self.worker_info.id).await
    }
}

#[async_trait]
impl crate::store::Producer for Producer {
    /// Retrieve a message by its ID from the queue.
    ///
    /// This is useful for producers to verify that messages were enqueued correctly.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to retrieve
    ///
    /// # Returns
    /// The message if found, or an error if not found.
    async fn get_message_by_id(&self, msg_id: i64) -> Result<QueueMessage> {
        self.messages.get(msg_id).await
    }

    /// Add a single message to the queue.
    ///
    /// This method validates the payload according to the queue's validation configuration
    /// before enqueueing. Validation includes rate limiting, size checks, structure validation,
    /// and content filtering based on the configuration.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    ///
    /// # Returns
    /// The enqueued message if validation passes.
    ///
    /// # Errors
    /// Returns validation errors if the payload fails validation rules:
    /// - `ValidationFailed` for structure/content violations
    /// - `PayloadTooLarge` if payload exceeds size limits
    /// - `RateLimited` if rate limits are exceeded
    async fn enqueue(&self, payload: &serde_json::Value) -> Result<QueueMessage> {
        // Use enqueue_delayed with 0 delay - it already includes validation
        self.enqueue_delayed(payload, 0).await
    }

    /// Schedule a message to be available for consumption after a delay.
    ///
    /// This method validates the payload according to the queue's validation configuration
    /// before enqueueing with a delay.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `delay_seconds` - Seconds to delay before message becomes available
    ///
    /// # Returns
    /// The enqueued message if validation passes.
    ///
    /// # Errors
    /// Returns validation errors if the payload fails validation rules.
    async fn enqueue_delayed(
        &self,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        // Validate the payload before enqueueing
        self.validator.validate(payload)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));
        let id = self.insert_message(payload, now, vt).await?;
        self.get_message_by_id(id).await
    }

    /// Add multiple messages to the queue in a single batch operation.
    ///
    /// This method validates all payloads according to the queue's validation configuration
    /// before enqueueing any of them. Rate limiting is applied atomically to the entire batch.
    /// If any payload fails validation, the entire batch is rejected and no messages are enqueued.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to enqueue
    ///
    /// # Returns
    /// Vector of enqueued messages if all payloads pass validation.
    ///
    /// # Errors
    /// Returns validation errors if any payload fails validation rules.
    /// The database transaction is atomic - either all messages are enqueued or none are.
    /// Rate limiting is also atomic - tokens are only consumed if the entire batch succeeds.
    async fn batch_enqueue(&self, payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        // Use batch_enqueue_delayed with 0 delay (immediate)
        self.batch_enqueue_delayed(payloads, 0).await
    }

    /// Add multiple messages to the queue with a delay in a single batch operation.
    ///
    /// This method validates all payloads according to the queue's validation configuration
    /// before enqueueing. Rate limiting is applied atomically to the entire batch.
    async fn batch_enqueue_delayed(
        &self,
        payloads: &[serde_json::Value],
        delay_seconds: u32,
    ) -> Result<Vec<QueueMessage>> {
        // Validate all payloads atomically (including rate limiting)
        self.validator.validate_batch(payloads)?;

        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));

        // Use the batch insert method from the messages table
        let ids = self
            .messages
            .batch_insert(
                self.queue_info.id,
                payloads,
                crate::types::BatchInsertParams {
                    read_ct: 0,
                    enqueued_at: now,
                    vt,
                    producer_worker_id: Some(self.worker_info.id),
                    consumer_worker_id: None,
                },
            )
            .await?;

        // Fetch all messages in a single query
        let queue_messages = self.messages.get_by_ids(&ids).await?;

        Ok(queue_messages)
    }

    /// Internal method to insert a message with specific timestamps.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `now` - Current timestamp for enqueued_at field
    /// * `vt` - Visibility timeout timestamp (when message becomes available)
    ///
    /// # Returns
    /// The ID of the inserted message.
    async fn insert_message(
        &self,
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        use crate::types::NewMessage;

        let new_message = NewMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
            producer_worker_id: Some(self.worker_info.id),
            consumer_worker_id: None,
        };

        let message = self.messages.insert(new_message).await?;
        Ok(message.id)
    }

    /// Replay a failed message from the DLQ (archive) back to the active queue.
    ///
    /// # Arguments
    /// * `archived_msg_id` - The ID of the archived message to replay
    async fn replay_dlq(&self, archived_msg_id: i64) -> Result<Option<QueueMessage>> {
        let rec = sqlx::query_as(REPLAY_FROM_DLQ)
            .bind(archived_msg_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "REPLAY_FROM_DLQ".into(),
                source: e,
                context: format!("Failed to replay message {}", archived_msg_id),
            })?;
        Ok(rec)
    }

    fn validation_config(&self) -> &crate::validation::ValidationConfig {
        &self.config.validation_config
    }

    fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

// Auto-cleanup for ephemeral workers
impl Drop for Producer {
    fn drop(&mut self) {
        // Check if this is an ephemeral worker by hostname prefix
        if self.worker_info.hostname.starts_with("__ephemeral__") {
            // Spawn a task to properly shutdown the worker
            let workers = self.workers.clone();
            let worker_id = self.worker_info.id;

            // Best-effort shutdown - ignore errors since we're in Drop
            tokio::task::spawn(async move {
                // Suspend then shutdown the worker
                let _ = workers.suspend(worker_id).await;
                let _ = workers.shutdown(worker_id).await;
            });
        }
    }
}
