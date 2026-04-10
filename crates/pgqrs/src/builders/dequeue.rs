//! Builder for dequeue operations with worker selection and visibility timeouts.

use crate::error::{Error, Result};
use crate::store::Store;
use crate::types::QueueMessage;
use crate::workers::Consumer;
use std::future::Future;
use std::time::Duration as StdDuration;
use tokio::time::{interval_at, sleep_until, Instant, MissedTickBehavior};

struct Poller {
    consumer: Consumer,
    batch_size: usize,
    vt_offset_seconds: Option<u32>,
    at: Option<chrono::DateTime<chrono::Utc>>,
    poll_interval: Option<StdDuration>,
    poll_timeout: Option<StdDuration>,
}

impl Poller {
    async fn enter_polling(&self) -> Result<()> {
        if let Err(e) = self.consumer.poll().await {
            if matches!(
                &e,
                Error::InvalidStateTransition { from, to, .. }
                    if (from == "interrupted" || from == "suspended") && to == "polling"
            ) {
                self.check_terminal_status().await?;
            }
            return Err(e);
        }

        Ok(())
    }

    async fn check_terminal_status(&self) -> Result<()> {
        match self.consumer.status().await? {
            crate::types::WorkerStatus::Interrupted => {
                self.consumer.suspend().await?;
                Err(Error::Suspended {
                    reason: "worker interrupted".to_string(),
                })
            }
            crate::types::WorkerStatus::Suspended => Err(Error::Suspended {
                reason: "worker suspended".to_string(),
            }),
            _ => Ok(()),
        }
    }

    async fn finish_one_shot_poll(&self) -> Result<()> {
        match self.consumer.complete_poll().await {
            Ok(()) => Ok(()),
            Err(Error::InvalidStateTransition { from, to, .. })
                if to == "ready" && (from == "interrupted" || from == "suspended") =>
            {
                self.check_terminal_status().await
            }
            Err(Error::InvalidStateTransition { from, to, .. })
                if to == "ready" && from == "stopped" =>
            {
                Err(Error::Suspended {
                    reason: "worker stopped".to_string(),
                })
            }
            Err(e) => Err(e),
        }
    }

    async fn dequeue_n(&self, batch_size: usize) -> Result<Vec<QueueMessage>> {
        self.check_terminal_status().await?;

        if let Some(at) = self.at {
            let vt = self.vt_offset_seconds.unwrap_or(5);
            self.consumer.dequeue_at(batch_size, vt, at).await
        } else if let Some(vt_offset) = self.vt_offset_seconds {
            self.consumer
                .dequeue_many_with_delay(batch_size, vt_offset)
                .await
        } else {
            self.consumer.dequeue_many(batch_size).await
        }
    }

    async fn fetch_one(&self) -> Result<Option<QueueMessage>> {
        let mut msgs = self.dequeue_n(1).await?;
        Ok(msgs.pop())
    }

    async fn fetch_all(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_n(self.batch_size).await
    }

    async fn poll_messages(&mut self) -> Result<Vec<QueueMessage>> {
        self.poll_messages_until(self.poll_timeout).await
    }

    async fn poll_messages_until(
        &mut self,
        poll_timeout: Option<StdDuration>,
    ) -> Result<Vec<QueueMessage>> {
        if self.batch_size == 0 {
            return Err(Error::ValidationFailed {
                reason: "batch size must be >= 1 for poll".to_string(),
            });
        }

        let config = self.consumer.store().config();

        let poll_interval = self
            .poll_interval
            .unwrap_or_else(|| StdDuration::from_millis(config.poll_interval_ms));
        let heartbeat_interval = StdDuration::from_secs(config.heartbeat_interval);

        let now = Instant::now();
        let deadline = poll_timeout.map(|timeout| now + timeout);
        let mut poll_interval = interval_at(now + poll_interval, poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut heartbeat_interval = interval_at(now + heartbeat_interval, heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            let messages = self.fetch_all().await?;
            if !messages.is_empty() {
                return Ok(messages);
            }

            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = poll_interval.tick() => {
                        self.check_terminal_status().await?;
                    }
                    _ = heartbeat_interval.tick() => {
                        // Heartbeat implies liveness checks too.
                        self.consumer.heartbeat().await?;
                        self.check_terminal_status().await?;
                    }
                    _ = sleep_until(deadline) => {
                        return Ok(Vec::new());
                    }
                }
            } else {
                tokio::select! {
                    _ = poll_interval.tick() => {
                        self.check_terminal_status().await?;
                    }
                    _ = heartbeat_interval.tick() => {
                        // Heartbeat implies liveness checks too.
                        self.consumer.heartbeat().await?;
                        self.check_terminal_status().await?;
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

    async fn execute_one<F, Fut>(&self, handler: F) -> Result<()>
    where
        F: Fn(QueueMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send,
    {
        if let Some(msg) = self.fetch_one().await? {
            let msg_id = msg.id;

            match handler(msg).await {
                Ok(_) => {
                    self.consumer.archive(msg_id).await?;
                }
                Err(e) => {
                    #[cfg(any(test, feature = "test-utils"))]
                    if matches!(e, crate::error::Error::TestCrash) {
                        return Err(e);
                    }
                    self.consumer.release_messages(&[msg_id]).await?;
                    return Err(e);
                }
            }
        }
        Ok(())
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

    async fn execute_batch<F, Fut>(&self, handler: F) -> Result<()>
    where
        F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send,
    {
        let messages = self.fetch_all().await?;
        if !messages.is_empty() {
            let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();

            match handler(messages).await {
                Ok(_) => {
                    self.consumer.archive_many(msg_ids).await?;
                }
                Err(e) => {
                    self.consumer.release_messages(&msg_ids).await?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn run_forever_one<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(QueueMessage) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<()>> + Send,
    {
        self.enter_polling().await?;
        loop {
            self.handle_one(handler.clone()).await?;
        }
    }

    async fn run_forever_batch<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(Vec<QueueMessage>) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<()>> + Send,
    {
        self.enter_polling().await?;
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
    poll_timeout: Option<StdDuration>,
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
            poll_timeout: None,
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

    /// Bound polling to a maximum wait duration.
    ///
    /// When the deadline expires without messages, `.poll()` returns an empty vector.
    pub fn until(mut self, timeout: StdDuration) -> Self {
        self.poll_timeout = Some(timeout);
        self
    }

    /// Set a custom reference time for the dequeue operation (useful for testing delays)
    pub fn at(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.at = Some(time);
        self
    }

    /// Fetch one message (if available).
    pub async fn fetch_one<S: Store>(self, store: &S) -> Result<Option<QueueMessage>> {
        self.into_poller(store).await?.fetch_one().await
    }

    /// Fetch all messages (up to batch size).
    pub async fn fetch_all<S: Store>(self, store: &S) -> Result<Vec<QueueMessage>> {
        self.into_poller(store).await?.fetch_all().await
    }

    /// Poll until at least one message is available or interrupted.
    ///
    /// When `.until(duration)` is configured, returns an empty vector on timeout.
    pub async fn poll<S>(self, store: &S) -> Result<Vec<QueueMessage>>
    where
        S: Store,
    {
        let mut poller = self.into_poller(store).await?;
        poller.enter_polling().await?;
        let messages = poller.poll_messages().await?;
        poller.finish_one_shot_poll().await?;
        Ok(messages)
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
    async fn resolve_consumer<S: Store>(&self, store: &S) -> Result<Consumer> {
        if let Some(consumer) = self.worker {
            return Ok(consumer.clone());
        }

        let queue = self
            .queue
            .as_ref()
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\") or .worker(&consumer)"
                    .to_string(),
            })?;

        store.consumer_ephemeral(queue).await
    }

    async fn into_poller<S: Store>(self, store: &S) -> Result<Poller> {
        let consumer = self.resolve_consumer(store).await?;
        Ok(Poller {
            consumer,
            batch_size: self.batch_size,
            vt_offset_seconds: self.vt_offset_seconds,
            at: self.at,
            poll_interval: self.poll_interval,
            poll_timeout: self.poll_timeout,
        })
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
    fn validate_batch_size(&self) -> Result<()> {
        if self.base.batch_size != 1 {
            return Err(Error::ValidationFailed {
                reason: format!(
                    "single-message handlers require batch size = 1, got {}",
                    self.base.batch_size
                ),
            });
        }
        Ok(())
    }

    fn validate_poll_configuration(&self) -> Result<()> {
        if self.base.poll_timeout.is_some() {
            return Err(Error::ValidationFailed {
                reason: "until() is not supported with handler poll loops".to_string(),
            });
        }
        Ok(())
    }

    /// Execute the dequeue and handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        self.validate_batch_size()?;
        self.base
            .into_poller(store)
            .await?
            .execute_one(self.handler)
            .await
    }

    /// Poll until a message is available or interrupted, then handle it.
    pub async fn poll<S>(self, store: &S) -> Result<()>
    where
        S: Store,
    {
        self.validate_batch_size()?;
        self.validate_poll_configuration()?;
        let mut poller = self.base.into_poller(store).await?;
        poller.run_forever_one(self.handler).await
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
    fn validate_poll_configuration(&self) -> Result<()> {
        if self.base.poll_timeout.is_some() {
            return Err(Error::ValidationFailed {
                reason: "until() is not supported with handler poll loops".to_string(),
            });
        }
        Ok(())
    }

    /// Execute the dequeue and batch handle operation.
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        self.base
            .into_poller(store)
            .await?
            .execute_batch(self.handler)
            .await
    }

    /// Poll until at least one message is available or interrupted, then handle them.
    pub async fn poll<S>(self, store: &S) -> Result<()>
    where
        S: Store,
    {
        self.validate_poll_configuration()?;
        let mut poller = self.base.into_poller(store).await?;
        poller.run_forever_batch(self.handler).await
    }
}
