//! Builder for enqueue operations with optional delays and batching.

use crate::error::Result;
use crate::store::Store;
use crate::workers::Producer;
use serde::Serialize;

/// Start an enqueue operation.
///
/// ```rust,no_run
/// # use pgqrs;
/// # use serde_json::json;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let ids = pgqrs::enqueue()
///     .message(&json!({"task": "send_email"}))
///     .to("tasks")
///     .execute(&store)
///     .await?;
/// # Ok(()) }
/// ```
pub fn enqueue() -> EnqueueBuilder<'static, ()> {
    EnqueueBuilder::new()
}

/// Builder for enqueue operations.
///
/// Use `.message()` or `.messages()` with either `.to(queue)` or `.worker(&producer)`.
pub struct EnqueueBuilder<'a, T> {
    messages: Vec<&'a T>,
    queue: Option<String>,
    worker: Option<&'a Producer>,
    delay_seconds: Option<u32>,
    at: Option<chrono::DateTime<chrono::Utc>>,
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
            at: None,
        }
    }

    /// Set a single message payload to enqueue.
    pub fn message<'a, T: Serialize + Send + Sync>(self, message: &'a T) -> EnqueueBuilder<'a, T> {
        EnqueueBuilder {
            messages: vec![message],
            queue: self.queue,
            worker: self.worker,
            delay_seconds: self.delay_seconds,
            at: self.at,
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
            at: self.at,
        }
    }
}

impl<'a, T: Serialize + Send + Sync> EnqueueBuilder<'a, T> {
    /// Specify target queue (ephemeral producer mode).
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Use a managed producer instead of an ephemeral one.
    pub fn worker(mut self, producer: &'a Producer) -> Self {
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

    /// Set a custom reference time for enqueue (test helper).
    pub fn at(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.at = Some(time);
        self
    }

    /// Execute the enqueue operation.
    ///
    /// Returns message IDs for all payloads.
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
            // Check if using custom time (for testing)
            if let Some(at) = self.at {
                let delay = self.delay_seconds.unwrap_or(0);
                let msgs = producer.batch_enqueue_at(&json_payloads, at, delay).await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            } else if let Some(delay) = self.delay_seconds {
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

            // Check if using custom time (for testing)
            if let Some(at) = self.at {
                let delay = self.delay_seconds.unwrap_or(0);
                let msgs = producer.batch_enqueue_at(&json_payloads, at, delay).await?;
                Ok(msgs.iter().map(|m| m.id).collect())
            } else if let Some(delay) = self.delay_seconds {
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
