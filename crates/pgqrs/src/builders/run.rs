use crate::error::Result;
use crate::store::{Run, Store};
use crate::types::QueueMessage;

/// Builder for creating local run handles.
pub struct RunBuilder<'a, S: Store> {
    pub(crate) store: Option<&'a S>,
    pub(crate) message: Option<QueueMessage>,
}

impl<'a, S: Store> RunBuilder<'a, S> {
    pub fn new() -> Self {
        Self {
            store: None,
            message: None,
        }
    }

    /// Set the store.
    pub fn store<'b, T: Store>(self, store: &'b T) -> RunBuilder<'b, T> {
        RunBuilder {
            store: Some(store),
            message: self.message,
        }
    }

    /// Set the message to use for the run.
    pub fn message(mut self, message: QueueMessage) -> Self {
        self.message = Some(message);
        self
    }

    /// Execute the build and return a run handle.
    pub async fn execute(self) -> Result<Box<dyn Run>> {
        let store = self
            .store
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Store is required for RunBuilder::execute".to_string(),
            })?;
        let message = self
            .message
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "QueueMessage is required for RunBuilder::execute".to_string(),
            })?;
        let run = store.run(message).await?;
        Ok(run)
    }
}

impl<'a, S: Store> Default for RunBuilder<'a, S> {
    fn default() -> Self {
        Self::new()
    }
}
