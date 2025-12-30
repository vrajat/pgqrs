//! ProduceBuilder - builder pattern for producing messages

use crate::error::Result;
use crate::store::Store;
use serde::Serialize;

/// Builder for producing messages with ephemeral workers.
///
/// # Example
/// ```rust,no_run
/// # use pgqrs::{produce, Config, AnyStore};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config::from_dsn("postgres://localhost/mydb")?;
/// let store = AnyStore::connect(&config).await?;
///
/// // Simple produce
/// let msg_id = pgqrs::produce(&"order data")
///     .to("orders")
///     .execute(&store, &config)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ProduceBuilder<'a, T> {
    message: &'a T,
    queue: Option<String>,
}

impl<'a, T: Serialize + Send + Sync> ProduceBuilder<'a, T> {
    pub fn new(message: &'a T) -> Self {
        Self {
            message,
            queue: None,
        }
    }

    /// Specify the target queue
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Execute the produce operation
    pub async fn execute<S: Store>(self, store: &S) -> Result<i64> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .to(\"queue-name\")".to_string(),
            })?;

        let producer = store.producer_ephemeral(&queue, store.config()).await?;
        let json =
            serde_json::to_value(self.message).map_err(crate::error::Error::Serialization)?;
        let queue_msg = producer.enqueue(&json).await?;
        Ok(queue_msg.id)
    }
}

/// Builder for producing multiple messages with ephemeral workers.
pub struct ProduceBatchBuilder<'a, T> {
    messages: &'a [T],
    queue: Option<String>,
}

impl<'a, T: Serialize + Send + Sync> ProduceBatchBuilder<'a, T> {
    pub fn new(messages: &'a [T]) -> Self {
        Self {
            messages,
            queue: None,
        }
    }

    /// Specify the target queue
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    /// Execute the batch produce operation
    pub async fn execute<S: Store>(self, store: &S) -> Result<Vec<i64>> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required. Use .to(\"queue-name\")".to_string(),
            })?;

        let producer = store.producer_ephemeral(&queue, store.config()).await?;

        // Convert all messages to JSON
        let json_messages: Result<Vec<serde_json::Value>> = self
            .messages
            .iter()
            .map(|m| serde_json::to_value(m).map_err(crate::error::Error::Serialization))
            .collect();

        let queue_msgs = producer.batch_enqueue(&json_messages?).await?;
        Ok(queue_msgs.iter().map(|m| m.id).collect())
    }
}

/// Create a ProduceBuilder for a single message
pub fn produce<T: Serialize + Send + Sync>(message: &T) -> ProduceBuilder<'_, T> {
    ProduceBuilder::new(message)
}

/// Create a ProduceBatchBuilder for multiple messages
pub fn produce_batch<T: Serialize + Send + Sync>(messages: &[T]) -> ProduceBatchBuilder<'_, T> {
    ProduceBatchBuilder::new(messages)
}
