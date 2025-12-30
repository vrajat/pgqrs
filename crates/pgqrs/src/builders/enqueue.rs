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
}

impl<'a, T: Serialize + Send + Sync> EnqueueBuilder<'a, T> {
    pub fn new(message: &'a T) -> Self {
        Self {
            message,
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

    /// Execute with ephemeral worker (requires .to())
    pub async fn execute<S: Store + Send + Sync>(self, store: &S) -> Result<i64> {
        if let Some(producer) = self.worker {
            // Managed worker mode
            let json =
                serde_json::to_value(self.message).map_err(crate::error::Error::Serialization)?;
            let msg = producer.enqueue(&json).await?;
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
            let msg = producer.enqueue(&json).await?;
            Ok(msg.id)
        }
    }
}

/// Create a new enqueue builder for a message
pub fn enqueue<T: Serialize + Send + Sync>(message: &T) -> EnqueueBuilder<'_, T> {
    EnqueueBuilder::new(message)
}
