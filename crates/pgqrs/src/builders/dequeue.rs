//! DequeueBuilder for advanced dequeue options
//! TODO: Implement builder pattern for batch size, visibility timeout

use crate::config::Config;
use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;

/// Builder for dequeue operations with advanced options.
///
/// This will support:
/// - `.from(queue)` - specify queue
/// - `.batch(size)` - batch size
/// - `.visibility_timeout(Duration)` - visibility timeout
/// - `.fetch_one(store)` - fetch one message
/// - `.fetch_all(store)` - fetch batch
pub struct DequeueBuilder {
    queue: Option<String>,
    batch_size: usize,
}

impl Default for DequeueBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DequeueBuilder {
    pub fn new() -> Self {
        Self {
            queue: None,
            batch_size: 1,
        }
    }

    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    pub fn batch(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub async fn fetch_one<S: Store>(
        self,
        store: &S,
        config: &Config,
    ) -> Result<Option<QueueMessage>> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\")".to_string(),
            })?;

        let consumer = store.consumer_ephemeral(&queue, config).await?;
        let msgs = consumer.dequeue().await?;
        Ok(msgs.into_iter().next())
    }

    pub async fn fetch_all<S: Store>(
        self,
        store: &S,
        config: &Config,
    ) -> Result<Vec<QueueMessage>> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\")".to_string(),
            })?;

        let consumer = store.consumer_ephemeral(&queue, config).await?;
        consumer.dequeue_many(self.batch_size).await
    }
}
