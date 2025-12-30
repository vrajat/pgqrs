//! ConsumeBuilder - builder pattern for consuming messages

use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;
use std::future::Future;

/// Builder for consuming messages with ephemeral workers.
///
/// # Example
/// ```rust,no_run
/// # use pgqrs::{consume, Config, AnyStore};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config::from_dsn("postgres://localhost/mydb")?;
/// let store = AnyStore::connect(&config).await?;
///
/// // Simple consume
/// pgqrs::consume()
///     .from("orders")
///     .handler(|msg| async move {
///         println!("Processing: {:?}", msg.payload);
///         Ok(())
///     })
///     .execute(&store, &config)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ConsumeBuilder<F> {
    queue: Option<String>,
    handler: Option<F>,
}

impl<F> Default for ConsumeBuilder<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> ConsumeBuilder<F> {
    pub fn new() -> Self {
        Self {
            queue: None,
            handler: None,
        }
    }

    /// Specify the source queue
    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Specify the message handler
    pub fn handler(mut self, handler: F) -> Self {
        self.handler = Some(handler);
        self
    }
}

impl<F, Fut> ConsumeBuilder<F>
where
    F: FnOnce(QueueMessage) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    /// Execute the consume operation
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\")".to_string(),
            })?;

        let handler = self
            .handler
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Handler is required. Use .handler(|msg| async { ... })".to_string(),
            })?;

        let consumer = store.consumer_ephemeral(&queue, store.config()).await?;

        // Dequeue one message
        let messages = consumer.dequeue().await?;

        if let Some(msg) = messages.into_iter().next() {
            // Call the handler
            match handler(msg.clone()).await {
                Ok(_) => {
                    // Success - archive the message
                    consumer.archive(msg.id).await?;
                }
                Err(e) => {
                    // Error - release the message back to the queue
                    consumer.release_messages(&[msg.id]).await?;
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

/// Builder for consuming multiple messages with ephemeral workers.
pub struct ConsumeBatchBuilder<F> {
    queue: Option<String>,
    batch_size: usize,
    handler: Option<F>,
}

impl<F> Default for ConsumeBatchBuilder<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> ConsumeBatchBuilder<F> {
    pub fn new() -> Self {
        Self {
            queue: None,
            batch_size: 10,
            handler: None,
        }
    }

    /// Specify the source queue
    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Specify the batch size
    pub fn batch(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Specify the message handler
    pub fn handler(mut self, handler: F) -> Self {
        self.handler = Some(handler);
        self
    }
}

impl<F, Fut> ConsumeBatchBuilder<F>
where
    F: FnOnce(Vec<QueueMessage>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    /// Execute the batch consume operation
    pub async fn execute<S: Store>(self, store: &S) -> Result<()> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .from(\"queue-name\")".to_string(),
            })?;

        let handler = self
            .handler
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Handler is required. Use .handler(|msgs| async { ... })".to_string(),
            })?;

        let consumer = store.consumer_ephemeral(&queue, store.config()).await?;

        // Dequeue batch
        let msgs = consumer.dequeue_many(self.batch_size).await?;

        if !msgs.is_empty() {
            let msg_ids: Vec<i64> = msgs.iter().map(|m| m.id).collect();

            // Run handler
            match handler(msgs).await {
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

/// Create a ConsumeBuilder
pub fn consume<F>() -> ConsumeBuilder<F> {
    ConsumeBuilder::new()
}

/// Create a ConsumeBatchBuilder
pub fn consume_batch<F>() -> ConsumeBatchBuilder<F> {
    ConsumeBatchBuilder::new()
}
