//! Builder for dequeue operations with worker selection and visibility timeouts.

use crate::error::{Error, Result};
use crate::store::Store;
use crate::types::QueueMessage;
use crate::workers::Consumer;
use std::future::Future;
use std::time::Duration as StdDuration;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

struct DequeueParams {
    batch_size: usize,
    vt_offset_seconds: Option<u32>,
    at: Option<chrono::DateTime<chrono::Utc>>,
}

impl DequeueParams {
    async fn dequeue(&self, consumer: &Consumer) -> Result<Vec<QueueMessage>> {
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
}

struct Poller<'a> {
    consumer: &'a Consumer,
    dequeue: DequeueParams,
    poll_interval: Option<StdDuration>,
}

impl<'a> Poller<'a> {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue.dequeue(self.consumer).await
    }

    async fn poll_messages(&mut self) -> Result<Vec<QueueMessage>> {
        if self.dequeue.batch_size == 0 {
            return Err(Error::ValidationFailed {
                reason: "batch size must be >= 1 for poll".to_string(),
            });
        }

        // Consumer-only states: Ready -> Polling
        self.consumer.poll().await?;

        let config = self.consumer.store().config();

        let poll_interval = self
            .poll_interval
            .unwrap_or_else(|| StdDuration::from_millis(config.poll_interval_ms));
        let heartbeat_interval = StdDuration::from_secs(config.heartbeat_interval);

        let now = Instant::now();
        let mut poll_interval = interval_at(now + poll_interval, poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut heartbeat_interval = interval_at(now + heartbeat_interval, heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            let messages = self.dequeue().await?;
            if !messages.is_empty() {
                return Ok(messages);
            }

            tokio::select! {
                _ = poll_interval.tick() => {
                    let status = self.consumer.status().await?;
                    if status == crate::types::WorkerStatus::Interrupted {
                        // Consumer-only states: Interrupted -> Suspended
                        self.consumer.suspend().await?;
                        return Err(Error::Suspended {
                            reason: "worker interrupted".to_string(),
                        });
                    }
                }
                _ = heartbeat_interval.tick() => {
                    // Heartbeat implies liveness checks too.
                    self.consumer.heartbeat().await?;
                    let status = self.consumer.status().await?;
                    if status == crate::types::WorkerStatus::Interrupted {
                        self.consumer.suspend().await?;
                        return Err(Error::Suspended {
                            reason: "worker interrupted".to_string(),
                        });
                    }
                }
            }
        }
    }

    async fn handle_one<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(QueueMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send,
    {
        self.dequeue.batch_size = 1;
        let mut messages = self.poll_messages().await?;
        let msg = messages
            .pop()
            .expect("poll_messages returns at least one message");
        let msg_id = msg.id;

        match handler(msg).await {
            Ok(_) => {
                self.consumer.archive(msg_id).await?;
                Ok(())
            }
            Err(e) => {
                #[cfg(any(test, feature = "test-utils"))]
                if matches!(e, crate::error::Error::TestCrash) {
                    return Err(e);
                }
                self.consumer.release_messages(&[msg_id]).await?;
                Err(e)
            }
        }
    }

    async fn handle_batch<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send,
    {
        let messages = self.poll_messages().await?;
        let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();

        match handler(messages).await {
            Ok(_) => {
                self.consumer.archive_many(msg_ids).await?;
                Ok(())
            }
            Err(e) => {
                self.consumer.release_messages(&msg_ids).await?;
                Err(e)
            }
        }
    }

    async fn run_forever_one<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(QueueMessage) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<()>> + Send,
    {
        loop {
            self.handle_one(handler.clone()).await?;
        }
    }

    async fn run_forever_batch<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<()>> + Send,
    {
        loop {
            self.handle_batch(handler.clone()).await?;
        }
    }
}

/// Start a dequeue operation.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let messages = pgqrs::dequeue()
///     .from("tasks")
///     .batch(5)
///     .fetch_all(&store)
///     .await?;
/// # Ok(()) }
/// ```
pub fn dequeue() -> DequeueBuilder<'static> {
    DequeueBuilder::new()
}

/// Builder for dequeue operations with advanced options.
///
/// Supports:
/// - Ephemeral workers via `.from(queue)`.
/// - Managed workers via `.worker(&consumer)`.
/// - Visibility timeout tweaks via `.with_vt()`.
pub struct DequeueBuilder<'a> {
    queue: Option<String>,
    batch_size: usize,
    worker: Option<&'a Consumer>,
    vt_offset_seconds: Option<u32>,
    at: Option<chrono::DateTime<chrono::Utc>>,
    poll_interval: Option<StdDuration>,
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
            poll_interval: None,
        }
    }

    /// Specify queue (ephemeral worker mode).
    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Set the batch size for dequeue.
    pub fn batch(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Use a managed consumer instead of an ephemeral one.
    pub fn worker(mut self, consumer: &'a Consumer) -> Self {
        self.worker = Some(consumer);
        self
    }

    /// Set visibility timeout using Duration (more ergonomic than seconds).
    pub fn with_vt(mut self, duration: std::time::Duration) -> Self {
        self.vt_offset_seconds = Some(duration.as_secs().min(u32::MAX as u64) as u32);
        self
    }

    /// Set the poll interval for long-lived polling loops.
    pub fn poll_interval(mut self, interval: StdDuration) -> Self {
        self.poll_interval = Some(interval);
        self
    }

    /// Set a custom reference time for the dequeue operation (useful for testing delays)
    pub fn at(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.at = Some(time);
        self
    }

    /// Fetch one message (if available).
    pub async fn fetch_one<S: Store>(self, store: &S) -> Result<Option<QueueMessage>> {
        let consumer = self.resolve_consumer(store).await?;
        let params = DequeueParams {
            batch_size: 1,
            vt_offset_seconds: self.vt_offset_seconds,
            at: self.at,
        };
        let msgs = params.dequeue(&consumer).await?;
        Ok(msgs.into_iter().next())
    }

    /// Fetch all messages (up to batch size).
    pub async fn fetch_all<S: Store>(self, store: &S) -> Result<Vec<QueueMessage>> {
        let consumer = self.resolve_consumer(store).await?;
        let poller = Poller {
            consumer: &consumer,
            dequeue: DequeueParams {
                batch_size: self.batch_size,
                vt_offset_seconds: self.vt_offset_seconds,
                at: self.at,
            },
            poll_interval: self.poll_interval,
        };
        poller.dequeue().await
    }

    /// Poll until at least one message is available or interrupted.
    pub async fn poll<S>(self, store: &S) -> Result<Vec<QueueMessage>>
    where
        S: Store,
    {
        let consumer = self.resolve_consumer(store).await?;
        let mut poller = Poller {
            consumer: &consumer,
            dequeue: DequeueParams {
                batch_size: self.batch_size,
                vt_offset_seconds: self.vt_offset_seconds,
                at: self.at,
            },
            poll_interval: self.poll_interval,
        };
        poller.poll_messages().await
    }

    // (polling implementation is in Poller)

    /// Set a handler for single-message processing.
    pub fn handle<F, Fut>(self, handler: F) -> DequeueHandlerBuilder<'a, F>
    where
        F: Fn(QueueMessage) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<()>> + Send,
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
        F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<()>> + Send,
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
        Ok(ResolvedConsumer::Owned(Box::new(worker)))
    }
}

enum ResolvedConsumer<'a> {
    Owned(Box<Consumer>),
    Borrowed(&'a Consumer),
}

impl<'a> std::ops::Deref for ResolvedConsumer<'a> {
    type Target = Consumer;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(b) => b,
            Self::Borrowed(b) => b,
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
    F: Fn(QueueMessage) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    /// Execute the dequeue and handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let base = self.base;
        let consumer = base.resolve_consumer(store).await?;
        let params = DequeueParams {
            batch_size: 1,
            vt_offset_seconds: base.vt_offset_seconds,
            at: base.at,
        };
        let msgs = params.dequeue(&consumer).await?;

        if let Some(msg) = msgs.into_iter().next() {
            let msg_id = msg.id;
            // Call the handler
            match (self.handler)(msg).await {
                Ok(_) => {
                    // Success - archive the message
                    consumer.archive(msg_id).await?;
                }
                Err(e) => {
                    #[cfg(any(test, feature = "test-utils"))]
                    if matches!(e, crate::error::Error::TestCrash) {
                        return Err(e);
                    }
                    // Error - release the message back to the queue
                    consumer.release_messages(&[msg_id]).await?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Poll until a message is available or interrupted, then handle it.
    pub async fn poll<S>(self, store: &S) -> Result<()>
    where
        S: Store,
    {
        let base = self.base;
        let consumer = base.resolve_consumer(store).await?;
        let handler = self.handler;

        let mut poller = Poller {
            consumer: &consumer,
            dequeue: DequeueParams {
                batch_size: 1,
                vt_offset_seconds: base.vt_offset_seconds,
                at: base.at,
            },
            poll_interval: base.poll_interval,
        };

        poller.run_forever_one(handler).await
    }
}

/// Builder for operations with a batch-message handler.
pub struct DequeueBatchHandlerBuilder<'a, F> {
    base: DequeueBuilder<'a>,
    handler: F,
}

impl<'a, F, Fut> DequeueBatchHandlerBuilder<'a, F>
where
    F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    /// Execute the dequeue and batch handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let base = self.base;
        let consumer = base.resolve_consumer(store).await?;
        let params = DequeueParams {
            batch_size: base.batch_size,
            vt_offset_seconds: base.vt_offset_seconds,
            at: base.at,
        };
        let msgs = params.dequeue(&consumer).await?;

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

    /// Poll until at least one message is available or interrupted, then handle them.
    pub async fn poll<S>(self, store: &S) -> Result<()>
    where
        S: Store,
    {
        let base = self.base;
        let consumer = base.resolve_consumer(store).await?;
        let handler = self.handler;

        let mut poller = Poller {
            consumer: &consumer,
            dequeue: DequeueParams {
                batch_size: base.batch_size,
                vt_offset_seconds: base.vt_offset_seconds,
                at: base.at,
            },
            poll_interval: base.poll_interval,
        };

        poller.run_forever_batch(handler).await
    }
}
