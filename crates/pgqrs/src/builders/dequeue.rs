//! DequeueBuilder for advanced dequeue options

use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;

/// Builder for dequeue operations with advanced options.
///
/// Supports two modes:
/// 1. Ephemeral worker (auto-managed): `.from(queue).execute(store)`
/// 2. Managed worker: `.worker(&consumer).execute()`
pub struct DequeueBuilder<'a> {
    queue: Option<String>,
    batch_size: usize,
    worker: Option<&'a dyn crate::store::Consumer>,
}

impl<'a> Default for DequeueBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> DequeueBuilder<'a> {
    pub fn new() -> Self {
        Self {
            queue: None,
            batch_size: 1,
            worker: None,
        }
    }

    /// Specify queue (for ephemeral worker mode)
    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Set batch size
    pub fn batch(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Use a managed worker instead of ephemeral
    pub fn worker(mut self, consumer: &'a dyn crate::store::Consumer) -> Self {
        self.worker = Some(consumer);
        self
    }

    /// Fetch one message
    pub async fn fetch_one<S: Store>(self, store: &S) -> Result<Option<QueueMessage>> {
        if let Some(consumer) = self.worker {
            // Managed worker mode
            let msgs = consumer.dequeue().await?;
            Ok(msgs.into_iter().next())
        } else {
            // Ephemeral worker mode
            let queue = self
                .queue
                .ok_or_else(|| crate::error::Error::ValidationFailed {
                    reason:
                        "Queue name is required. Use .from(\"queue-name\") or .worker(&consumer)"
                            .to_string(),
                })?;

            let consumer = store.consumer_ephemeral(&queue, store.config()).await?;
            let msgs = consumer.dequeue().await?;
            Ok(msgs.into_iter().next())
        }
    }

    /// Fetch all messages (up to batch size)
    pub async fn fetch_all<S: Store>(self, store: &S) -> Result<Vec<QueueMessage>> {
        if let Some(consumer) = self.worker {
            // Managed worker mode
            consumer.dequeue_many(self.batch_size).await
        } else {
            // Ephemeral worker mode
            let queue = self
                .queue
                .ok_or_else(|| crate::error::Error::ValidationFailed {
                    reason:
                        "Queue name is required. Use .from(\"queue-name\") or .worker(&consumer)"
                            .to_string(),
                })?;

            let consumer = store.consumer_ephemeral(&queue, store.config()).await?;
            consumer.dequeue_many(self.batch_size).await
        }
    }
}

/// Create a new dequeue builder
pub fn dequeue() -> DequeueBuilder<'static> {
    DequeueBuilder::new()
}
