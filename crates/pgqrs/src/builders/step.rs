use crate::error::Result;
use crate::store::{Run, Store};
use crate::types::StepRecord;

/// Builder for acquiring and managing workflow steps.
pub struct StepBuilder<'a> {
    run: Option<&'a dyn Run>,
    id: Option<String>,
    current_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'a> StepBuilder<'a> {
    pub fn new() -> Self {
        Self {
            run: None,
            id: None,
            current_time: None,
        }
    }

    /// Set the run handle for the step.
    pub fn run(mut self, run: &'a dyn Run) -> Self {
        self.run = Some(run);
        self
    }

    /// Set the step ID.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_string());
        self
    }

    /// Set a custom current time for testing.
    pub fn with_time(mut self, current_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.current_time = Some(current_time);
        self
    }

    /// Set the store (no-op for StepBuilder as it uses the Run handle).
    pub fn store<S: Store>(self, _store: &S) -> Self {
        self
    }

    /// Acquire the step and return its record.
    pub async fn execute(self) -> Result<StepRecord> {
        let run = self
            .run
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Run handle is required for StepBuilder::execute".to_string(),
            })?;
        let id = self
            .id
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Step ID is required for StepBuilder::execute".to_string(),
            })?;
        let current_time = self.current_time.unwrap_or_else(chrono::Utc::now);

        run.acquire_step(&id, current_time).await
    }
}
