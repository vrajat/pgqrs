use crate::error::Result;
use crate::store::Store;
use crate::types::{QueueMessage, WorkflowRecord};
use serde::Serialize;

/// Start a workflow builder.
///
/// ```rust,no_run
/// # use pgqrs;
/// # use serde_json::json;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// pgqrs::workflow()
///     .name("archive_files")
///     .store(&store)
///     .create()
///     .await?;
/// let message = pgqrs::workflow()
///     .name("archive_files")
///     .store(&store)
///     .trigger(&json!({"path": "/tmp/report.csv"}))?
///     .execute()
///     .await?;
/// # Ok(()) }
/// ```
pub fn workflow() -> WorkflowBuilder<'static, crate::store::AnyStore> {
    WorkflowBuilder::new()
}

/// Workflow definition handle builder.
///
/// Use `.name()` plus `.store()` before `.create()` or `.trigger()`.
pub struct WorkflowBuilder<'a, S: Store> {
    pub(crate) store: Option<&'a S>,
    pub(crate) name: Option<String>,
    pub(crate) id: Option<i64>,
}

impl<'a, S: Store> WorkflowBuilder<'a, S> {
    /// Create a new workflow builder.
    pub fn new() -> Self {
        Self {
            store: None,
            name: None,
            id: None,
        }
    }

    /// Set the store.
    pub fn store<'b, T: Store>(self, store: &'b T) -> WorkflowBuilder<'b, T> {
        WorkflowBuilder {
            store: Some(store),
            name: self.name,
            id: self.id,
        }
    }

    /// Set the workflow name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the workflow ID.
    pub fn id(mut self, id: i64) -> Self {
        self.id = Some(id);
        self
    }

    /// Create/ensure the workflow definition using the provided store.
    pub async fn create(self) -> Result<WorkflowRecord> {
        let store = self
            .store
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Store is required for WorkflowBuilder::create".to_string(),
            })?;
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Workflow name is required for WorkflowBuilder::create".to_string(),
            })?;
        store.workflow(&name).await
    }

    /// Begin building a workflow trigger.
    pub fn trigger<T: Serialize>(self, input: &T) -> Result<WorkflowTriggerBuilder<'a, S>> {
        let input = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        Ok(WorkflowTriggerBuilder {
            store: self.store,
            name: self.name,
            id: self.id,
            input: Some(input),
        })
    }
}

impl<'a, S: Store> Default for WorkflowBuilder<'a, S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for triggering workflow runs.
///
/// Use `.execute()` to enqueue the workflow message.
///
/// Use `.execute()` to enqueue the workflow message.
pub struct WorkflowTriggerBuilder<'a, S: Store> {
    store: Option<&'a S>,
    name: Option<String>,
    id: Option<i64>,
    input: Option<serde_json::Value>,
}

impl<'a, S: Store> WorkflowTriggerBuilder<'a, S> {
    pub async fn execute(self) -> Result<QueueMessage> {
        let store = self
            .store
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Store is required for WorkflowTriggerBuilder::execute".to_string(),
            })?;
        let name = match (self.name, self.id) {
            (Some(n), _) => n,
            (None, Some(id)) => {
                let rec = store.workflows().get(id).await?;
                rec.name
            }
            (None, None) => {
                return Err(crate::error::Error::ValidationFailed {
                    reason: "Workflow name or ID is required for trigger".to_string(),
                })
            }
        };
        store.trigger(&name, self.input).await
    }
}
