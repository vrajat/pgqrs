use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;
use crate::workers::Run;

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

    /// Non-blocking check for the result of the workflow run.
    ///
    /// Returns:
    /// - `Ok(Some(T))` if the workflow succeeded.
    /// - `Err(Error::ExecutionFailed)` if the workflow failed.
    /// - `Ok(None)` if the workflow is still running, queued, or not yet started.
    pub async fn get<T>(self) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let store = self
            .store
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Store is required for RunBuilder::get".to_string(),
            })?;
        let message = self
            .message
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "QueueMessage is required for RunBuilder::get".to_string(),
            })?;

        match store.workflow_runs().get_by_message_id(message.id).await {
            Ok(record) => match record.status {
                crate::types::WorkflowStatus::Success => {
                    let output = record.output.unwrap_or(serde_json::Value::Null);
                    let val = match serde_json::from_value(output) {
                        Ok(v) => v,
                        Err(e) => return Err(crate::error::Error::Serialization(e)),
                    };
                    Ok(Some(val))
                }
                crate::types::WorkflowStatus::Error => Err(crate::error::Error::ExecutionFailed {
                    run_id: record.id,
                    error: record.error.unwrap_or(serde_json::Value::Null),
                }),
                _ => Ok(None),
            },
            Err(crate::error::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Poll for the result of the workflow run.
    ///
    /// Returns:
    /// - `Ok(T)` if the workflow succeeded.
    /// - `Err(Error::ExecutionFailed)` if the workflow failed.
    /// - Continues polling if the workflow is still running or not yet started.
    pub async fn result<T>(self) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let store = self
            .store
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Store is required for RunBuilder::result".to_string(),
            })?;
        let message = self
            .message
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "QueueMessage is required for RunBuilder::result".to_string(),
            })?;

        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            interval.tick().await;

            let record_res = store.workflow_runs().get_by_message_id(message.id).await;
            let record = match record_res {
                Ok(r) => r,
                Err(crate::error::Error::NotFound { .. }) => {
                    // Not created yet, continue polling
                    continue;
                }
                Err(e) => return Err(e),
            };

            match record.status {
                crate::types::WorkflowStatus::Success => {
                    let output = record.output.unwrap_or(serde_json::Value::Null);
                    return serde_json::from_value(output)
                        .map_err(crate::error::Error::Serialization);
                }
                crate::types::WorkflowStatus::Error => {
                    return Err(crate::error::Error::ExecutionFailed {
                        run_id: record.id,
                        error: record.error.unwrap_or(serde_json::Value::Null),
                    });
                }
                _ => {
                    // Still running or queued, continue polling
                }
            }
        }
    }

    /// Execute the build and return a run handle.
    pub async fn execute(self) -> Result<Run> {
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
