//! DequeueBuilder for advanced dequeue options

use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;
use std::future::Future;

/// Builder for dequeue operations with advanced options.
///
/// Supports two modes:
/// 1. Ephemeral worker (auto-managed): `.from(queue).execute(store)`
/// 2. Managed worker: `.worker(&consumer).execute()`
pub struct DequeueBuilder<'a> {
    queue: Option<String>,
    batch_size: usize,
    worker: Option<&'a dyn crate::store::Consumer>,
    vt_offset_seconds: Option<u32>,
    at: Option<chrono::DateTime<chrono::Utc>>,
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
            vt_offset_seconds: None,
            at: None,
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

    /// Set visibility timeout offset in seconds
    pub fn vt_offset(mut self, seconds: u32) -> Self {
        self.vt_offset_seconds = Some(seconds);
        self
    }

    /// Set visibility timeout using Duration (more ergonomic than seconds).
    pub fn with_vt(mut self, duration: std::time::Duration) -> Self {
        self.vt_offset_seconds = Some(duration.as_secs().min(u32::MAX as u64) as u32);
        self
    }

    /// Set batch size (alias for `batch` with clearer naming).
    pub fn limit(mut self, count: usize) -> Self {
        self.batch_size = count;
        self
    }

    /// Set a custom reference time for the dequeue operation (useful for testing delays)
    pub fn at(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.at = Some(time);
        self
    }

    /// Fetch one message
    pub async fn fetch_one<S: Store>(self, store: &S) -> Result<Option<QueueMessage>> {
        let consumer = self.resolve_consumer(store).await?;
        let msgs = if let Some(at) = self.at {
            let vt = self.vt_offset_seconds.unwrap_or(5);
            consumer.dequeue_at(1, vt, at).await?
        } else if let Some(vt_offset) = self.vt_offset_seconds {
            consumer.dequeue_many_with_delay(1, vt_offset).await?
        } else {
            consumer.dequeue().await?
        };
        Ok(msgs.into_iter().next())
    }

    /// Fetch all messages (up to batch size)
    pub async fn fetch_all<S: Store>(self, store: &S) -> Result<Vec<QueueMessage>> {
        let consumer = self.resolve_consumer(store).await?;
        if let Some(at) = self.at {
            let vt = self.vt_offset_seconds.unwrap_or(5);
            consumer.dequeue_at(self.batch_size, vt, at).await
        } else if let Some(vt_offset) = self.vt_offset_seconds {
            consumer
                .dequeue_many_with_delay(self.batch_size, vt_offset)
                .await
        } else {
            consumer.dequeue_many(self.batch_size).await
        }
    }

    /// Set handler for single-message processing.
    /// Returns a DequeueHandlerBuilder for execution.
    pub fn handle<F, Fut>(self, handler: F) -> DequeueHandlerBuilder<'a, F>
    where
        F: FnOnce(QueueMessage) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        DequeueHandlerBuilder {
            base: self,
            handler,
        }
    }

    /// Set handler for batch-message processing.
    /// Returns a DequeueBatchHandlerBuilder for execution.
    pub fn handle_batch<F, Fut>(self, handler: F) -> DequeueBatchHandlerBuilder<'a, F>
    where
        F: FnOnce(Vec<QueueMessage>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        DequeueBatchHandlerBuilder {
            base: self,
            handler,
        }
    }

    /// Helper to resolve consumer (managed or ephemeral)
    async fn resolve_consumer<S: Store>(&self, store: &S) -> Result<ResolvedConsumer<'_>> {
        if let Some(consumer) = self.worker {
            return Ok(ResolvedConsumer::Borrowed(consumer));
        }

        let queue = self
            .queue
            .as_ref()
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\") or .worker(&consumer)"
                    .to_string(),
            })?;

        let worker = store.consumer_ephemeral(queue, store.config()).await?;
        Ok(ResolvedConsumer::Owned(worker))
    }
}

enum ResolvedConsumer<'a> {
    Owned(Box<dyn crate::store::Consumer>),
    Borrowed(&'a dyn crate::store::Consumer),
}

impl<'a> std::ops::Deref for ResolvedConsumer<'a> {
    type Target = dyn crate::store::Consumer + 'a;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(b) => b.as_ref(),
            Self::Borrowed(b) => *b,
        }
    }
}

/// Builder for operations with a single-message handler.
pub struct DequeueHandlerBuilder<'a, F> {
    base: DequeueBuilder<'a>,
    handler: F,
}

impl<'a, F, Fut> DequeueHandlerBuilder<'a, F>
where
    F: FnOnce(QueueMessage) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    /// Execute the dequeue and handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let consumer = self.base.resolve_consumer(store).await?;
        // Dequeue one message (force 1 for single handler semantics)
        let msgs = if let Some(vt_offset) = self.base.vt_offset_seconds {
            consumer.dequeue_many_with_delay(1, vt_offset).await?
        } else {
            consumer.dequeue().await?
        };

        if let Some(msg) = msgs.into_iter().next() {
            let msg_id = msg.id;
            // Call the handler
            match (self.handler)(msg).await {
                Ok(_) => {
                    // Success - archive the message
                    consumer.archive(msg_id).await?;
                }
                Err(e) => {
                    // Error - release the message back to the queue
                    consumer.release_messages(&[msg_id]).await?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

/// Builder for operations with a batch-message handler.
pub struct DequeueBatchHandlerBuilder<'a, F> {
    base: DequeueBuilder<'a>,
    handler: F,
}

impl<'a, F, Fut> DequeueBatchHandlerBuilder<'a, F>
where
    F: FnOnce(Vec<QueueMessage>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    /// Execute the dequeue and batch handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let consumer = self.base.resolve_consumer(store).await?;
        // Dequeue batch
        let msgs = if let Some(vt_offset) = self.base.vt_offset_seconds {
            consumer
                .dequeue_many_with_delay(self.base.batch_size, vt_offset)
                .await?
        } else {
            consumer.dequeue_many(self.base.batch_size).await?
        };

        if !msgs.is_empty() {
            let msg_ids: Vec<i64> = msgs.iter().map(|m| m.id).collect();

            // Run handler
            match (self.handler)(msgs).await {
                Ok(_) => {
                    // Success - archive all messages
                    consumer.archive_many(msg_ids).await?;
                }
                Err(e) => {
                    // Error - release all messages
                    consumer.release_messages(&msg_ids).await?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

/// Create a new dequeue builder
pub fn dequeue() -> DequeueBuilder<'static> {
    DequeueBuilder::new()
}
