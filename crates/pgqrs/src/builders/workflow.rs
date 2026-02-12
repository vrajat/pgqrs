use crate::error::Result;
use crate::store::{Run, StepResult, Store};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Workflow definition handle builder.
///
/// This represents a workflow *definition* (template). Use `.trigger(..).execute(..)` to create a run.
pub struct WorkflowBuilder {
    name: Option<String>,
}

impl WorkflowBuilder {
    /// Create a new workflow builder.
    pub fn new() -> Self {
        Self { name: None }
    }

    /// Set the workflow name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Create/ensure the workflow definition using the provided store.
    pub async fn create<S: Store>(self, store: &S) -> Result<()> {
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Workflow name is required".to_string(),
            })?;
        store.create_workflow(&name).await
    }

    /// Begin building a workflow trigger.
    pub fn trigger<T: Serialize>(self, input: &T) -> Result<WorkflowTriggerBuilder> {
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Workflow name is required".to_string(),
            })?;

        let input = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;

        Ok(WorkflowTriggerBuilder {
            name,
            input: Some(input),
        })
    }
}

impl Default for WorkflowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for triggering workflow runs.
pub struct WorkflowTriggerBuilder {
    name: String,
    input: Option<serde_json::Value>,
}

impl WorkflowTriggerBuilder {
    pub async fn execute<S: Store>(self, store: &S) -> Result<i64> {
        store.trigger_workflow(&self.name, self.input).await
    }

    /// Create a local run handle for in-process execution.
    pub async fn run<S: Store>(self, store: &S) -> Result<Box<dyn Run>> {
        let run_id = self.execute(store).await?;
        store.run(run_id).await
    }
}

/// Builder for acquiring a step guard.
pub struct StepBuilder {
    run_id: i64,
    step_id: String,
    current_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl StepBuilder {
    /// Create a new step builder.
    pub fn new(run_id: i64, step_id: &str) -> Self {
        Self {
            run_id,
            step_id: step_id.to_string(),
            current_time: None,
        }
    }

    /// Set a custom current time for testing purposes.
    ///
    /// This method allows tests to control time for deterministic behavior
    /// when testing retry logic with scheduled `retry_at` timestamps.
    ///
    /// # Example
    /// ```no_run
    /// # use pgqrs::{step, StepResult};
    /// # use chrono::Utc;
    /// # async fn example(store: &pgqrs::store::AnyStore) -> Result<(), Box<dyn std::error::Error>> {
    /// let custom_time = Utc::now() + chrono::Duration::seconds(10);
    /// let step_res = step(1, "my_step")
    ///     .with_time(custom_time)
    ///     .acquire::<serde_json::Value, _>(store)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_time(mut self, current_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.current_time = Some(current_time);
        self
    }

    /// Acquire the step using the provided store.
    pub async fn acquire<T: DeserializeOwned, S: Store>(self, store: &S) -> Result<StepResult<T>> {
        let current_time = self.current_time.unwrap_or_else(chrono::Utc::now);
        let res = store
            .acquire_step(self.run_id, &self.step_id, current_time)
            .await?;
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
pub fn step(run_id: i64, step_id: &str) -> StepBuilder {
    StepBuilder::new(run_id, step_id)
}
