//! EnqueueBuilder for advanced enqueue options

use crate::error::Result;
use crate::store::Store;
use serde::Serialize;

/// Builder for enqueue operations with advanced options.
///
/// Supports two modes:
/// 1. Ephemeral worker (auto-managed): `.to(queue).execute(store)`
/// 2. Managed worker: `.worker(&producer).execute()`
pub struct EnqueueBuilder<'a, T> {
    message: &'a T,
    queue: Option<String>,
    worker: Option<&'a dyn crate::store::Producer>,
    delay_seconds: Option<u32>,
}

impl<'a, T: Serialize + Send + Sync> EnqueueBuilder<'a, T> {
    pub fn new(message: &'a T) -> Self {
        Self {
            message,
            queue: None,
            worker: None,
            delay_seconds: None,
        }
    }

    /// Specify queue (for ephemeral worker mode)
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Use a managed worker instead of ephemeral
    pub fn worker(mut self, producer: &'a dyn crate::store::Producer) -> Self {
        self.worker = Some(producer);
        self
    }

    /// Set delay in seconds before message becomes visible
    pub fn delay(mut self, seconds: u32) -> Self {
        self.delay_seconds = Some(seconds);
        self
    }

    /// Set delay using Duration (more ergonomic than seconds).
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    /// pgqrs::enqueue(&payload)
    ///     .to("my_queue")
    ///     .with_delay(Duration::from_secs(300))
    ///     .execute(&store).await?;
    /// ```
    pub fn with_delay(mut self, duration: std::time::Duration) -> Self {
        self.delay_seconds = Some(duration.as_secs() as u32);
        self
    }

    /// Execute with ephemeral worker (requires .to())
    pub async fn execute<S: Store + Send + Sync>(self, store: &S) -> Result<i64> {
        if let Some(producer) = self.worker {
            // Managed worker mode
            let json =
                serde_json::to_value(self.message).map_err(crate::error::Error::Serialization)?;

            let msg = if let Some(delay) = self.delay_seconds {
                producer.enqueue_delayed(&json, delay).await?
            } else {
                producer.enqueue(&json).await?
            };
            Ok(msg.id)
        } else {
            // Ephemeral worker mode
            let queue = self
                .queue
                .ok_or_else(|| crate::error::Error::ValidationFailed {
                    reason: "Queue name is required. Use .to(\"queue-name\") or .worker(&producer)"
                        .to_string(),
                })?;

            let producer = store.producer_ephemeral(&queue, store.config()).await?;
            let json =
                serde_json::to_value(self.message).map_err(crate::error::Error::Serialization)?;

            let msg = if let Some(delay) = self.delay_seconds {
                producer.enqueue_delayed(&json, delay).await?
            } else {
                producer.enqueue(&json).await?
            };
            Ok(msg.id)
        }
    }
}

/// Create a new enqueue builder for a message
pub fn enqueue<T: Serialize + Send + Sync>(message: &T) -> EnqueueBuilder<'_, T> {
    EnqueueBuilder::new(message)
}

/// Builder for batch enqueue operations
pub struct EnqueueBatchBuilder<'a, T> {
    messages: &'a [T],
    queue: Option<String>,
    worker: Option<&'a dyn crate::store::Producer>,
}

impl<'a, T: Serialize + Send + Sync> EnqueueBatchBuilder<'a, T> {
    pub fn new(messages: &'a [T]) -> Self {
        Self {
            messages,
            queue: None,
            worker: None,
        }
    }

    /// Specify queue (for ephemeral worker mode)
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Use a managed worker instead of ephemeral
    pub fn worker(mut self, producer: &'a dyn crate::store::Producer) -> Self {
        self.worker = Some(producer);
        self
    }

    /// Execute batch enqueue
    pub async fn execute<S: Store + Send + Sync>(self, store: &S) -> Result<Vec<i64>> {
        if let Some(producer) = self.worker {
            // Managed worker mode
            let json_messages: Result<Vec<serde_json::Value>> = self
                .messages
                .iter()
                .map(|m| serde_json::to_value(m).map_err(crate::error::Error::Serialization))
                .collect();

            let msgs = producer.batch_enqueue(&json_messages?).await?;
            Ok(msgs.iter().map(|m| m.id).collect())
        } else {
            // Ephemeral worker mode
            let queue = self
                .queue
                .ok_or_else(|| crate::error::Error::ValidationFailed {
                    reason: "Queue name is required. Use .to(\"queue-name\") or .worker(&producer)"
                        .to_string(),
                })?;

            let producer = store.producer_ephemeral(&queue, store.config()).await?;
            let json_messages: Result<Vec<serde_json::Value>> = self
                .messages
                .iter()
                .map(|m| serde_json::to_value(m).map_err(crate::error::Error::Serialization))
                .collect();

            let msgs = producer.batch_enqueue(&json_messages?).await?;
            Ok(msgs.iter().map(|m| m.id).collect())
        }
    }
}

/// Create a new batch enqueue builder for multiple messages
pub fn enqueue_batch<T: Serialize + Send + Sync>(messages: &[T]) -> EnqueueBatchBuilder<'_, T> {
    EnqueueBatchBuilder::new(messages)
}
