//! EnqueueBuilder for advanced enqueue options
//!
//! # Example
//! ```rust,no_run
//! # use pgqrs::{enqueue, Config};
//! # use pgqrs::store::AnyStore;
//! # use serde::Serialize;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::from_dsn("postgres://localhost/mydb");
//! let store = pgqrs::connect_with_config(&config).await?;
//!
//! // Single message
//! let ids = pgqrs::enqueue()
//!     .message(&"payload")
//!     .to("queue")
//!     .execute(&store).await?;
//!
//! // Batch messages
//! let ids = pgqrs::enqueue()
//!     .messages(&[&"msg1", &"msg2"])
//!     .to("queue")
//!     .execute(&store).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::Result;
use crate::store::Store;
use serde::Serialize;

/// Builder for enqueue operations.
pub struct EnqueueBuilder<'a, T> {
    messages: Vec<&'a T>,
    queue: Option<String>,
    worker: Option<&'a dyn crate::store::Producer>,
    delay_seconds: Option<u32>,
}

impl Default for EnqueueBuilder<'static, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl EnqueueBuilder<'static, ()> {
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            queue: None,
            worker: None,
            delay_seconds: None,
        }
    }

    /// Set the message payload to enqueue.
    pub fn message<'a, T: Serialize + Send + Sync>(self, message: &'a T) -> EnqueueBuilder<'a, T> {
        EnqueueBuilder {
            messages: vec![message],
            queue: self.queue,
            worker: self.worker,
            delay_seconds: self.delay_seconds,
        }
    }

    /// Set multiple message payloads to enqueue.
    pub fn messages<'a, T: Serialize + Send + Sync>(
        self,
        messages: &'a [T],
    ) -> EnqueueBuilder<'a, T> {
        EnqueueBuilder {
            messages: messages.iter().collect(),
            queue: self.queue,
            worker: self.worker,
            delay_seconds: self.delay_seconds,
        }
    }
}

impl<'a, T: Serialize + Send + Sync> EnqueueBuilder<'a, T> {
    /// Specify target queue (for ephemeral worker mode)
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

    /// Set delay using Duration
    pub fn with_delay(mut self, duration: std::time::Duration) -> Self {
        self.delay_seconds = Some(duration.as_secs().min(u32::MAX as u64) as u32);
        self
    }

    /// Execute the enqueue operation.
    /// Returns a vector of message IDs (even for single message).
    pub async fn execute<S: Store + Send + Sync>(self, store: &S) -> Result<Vec<i64>> {
        if self.messages.is_empty() {
            return Err(crate::error::Error::ValidationFailed {
                reason: "No messages to enqueue. Use .message() or .messages() before .execute()."
                    .to_string(),
            });
        }
        // Prepare JSON payloads
        let mut json_payloads = Vec::with_capacity(self.messages.len());
        for msg in &self.messages {
            json_payloads
                .push(serde_json::to_value(msg).map_err(crate::error::Error::Serialization)?);
        }

        if let Some(producer) = self.worker {
            // Batch or standard enqueue
            if let Some(delay) = self.delay_seconds {
                let msgs = producer
                    .batch_enqueue_delayed(&json_payloads, delay)
                    .await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            } else {
                let msgs = producer.batch_enqueue(&json_payloads).await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            }
        } else {
            // Ephemeral worker mode
            let queue = self
                .queue
                .ok_or_else(|| crate::error::Error::ValidationFailed {
                    reason: "Queue name is required. Use .to(\"queue-name\") or .worker(&producer)"
                        .to_string(),
                })?;

            let producer = store.producer_ephemeral(&queue, store.config()).await?;

            if let Some(delay) = self.delay_seconds {
                let msgs = producer
                    .batch_enqueue_delayed(&json_payloads, delay)
                    .await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            } else {
                let msgs = producer.batch_enqueue(&json_payloads).await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            }
        }
    }
}

/// Start an enqueue operation.
pub fn enqueue() -> EnqueueBuilder<'static, ()> {
    EnqueueBuilder::new()
}
