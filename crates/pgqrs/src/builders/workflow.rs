use crate::error::Result;
use crate::store::{StepResult, Store, Workflow};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Builder for creating database-agnostic workflows.
pub struct WorkflowBuilder {
    name: Option<String>,
    input: Option<serde_json::Value>,
}

impl WorkflowBuilder {
    /// Create a new workflow builder.
    pub fn new() -> Self {
        Self {
            name: None,
            input: None,
        }
    }

    /// Set the workflow name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the workflow input argument.
    pub fn arg<T: Serialize>(mut self, input: &T) -> Result<Self> {
        self.input = Some(serde_json::to_value(input).map_err(crate::error::Error::Serialization)?);
        Ok(self)
    }

    /// Create the workflow using the provided store.
    pub async fn create<S: Store>(self, store: &S) -> Result<Box<dyn Workflow>> {
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Workflow name is required".to_string(),
            })?;
        let input = self.input.unwrap_or(serde_json::Value::Null);
        store.create_workflow(&name, &input).await
    }
}

impl Default for WorkflowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for acquiring a step guard.
pub struct StepBuilder {
    workflow_id: i64,
    step_id: String,
}

impl StepBuilder {
    /// Create a new step builder.
    pub fn new(workflow_id: i64, step_id: &str) -> Self {
        Self {
            workflow_id,
            step_id: step_id.to_string(),
        }
    }

    /// Acquire the step using the provided store.
    pub async fn acquire<T: DeserializeOwned, S: Store>(self, store: &S) -> Result<StepResult<T>> {
        let res = store.acquire_step(self.workflow_id, &self.step_id).await?;
        match res {
            StepResult::Execute(guard) => Ok(StepResult::Execute(guard)),
            StepResult::Skipped(val) => {
                let t: T =
                    serde_json::from_value(val).map_err(crate::error::Error::Serialization)?;
                Ok(StepResult::Skipped(t))
            }
        }
    }
}

/// Entry point for creating a workflow.
pub fn workflow() -> WorkflowBuilder {
    WorkflowBuilder::new()
}

/// Entry point for acquiring a step.
pub fn step(workflow_id: i64, step_id: &str) -> StepBuilder {
    StepBuilder::new(workflow_id, step_id)
}
