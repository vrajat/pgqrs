//! EnqueueBuilder for advanced enqueue options
//! TODO: Implement builder pattern for priority, delay, metadata

use crate::error::Result;
use crate::store::Store;
use serde::Serialize;

/// Builder for enqueue operations with advanced options.
///
/// This will support:
/// - `.to(queue)` - specify queue
/// - `.priority(Priority::High)` - set priority
/// - `.delay(Duration)` - delay message
/// - `.metadata(Value)` - add metadata
/// - `.execute(store)` - execute the enqueue
pub struct EnqueueBuilder<'a, T> {
    message: &'a T,
    queue: Option<String>,
}

impl<'a, T: Serialize + Send + Sync> EnqueueBuilder<'a, T> {
    pub fn new(message: &'a T) -> Self {
        Self {
            message,
            queue: None,
        }
    }

    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    pub async fn execute<S: Store + Send + Sync>(self, store: &S) -> Result<i64> {
        let queue = self
            .queue
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Queue name is required".to_string(),
            })?;

        let producer = store.producer_ephemeral(&queue, store.config()).await?;
        let json =
            serde_json::to_value(self.message).map_err(crate::error::Error::Serialization)?;
        let msg = producer.enqueue(&json).await?;
        Ok(msg.id)
    }
}
