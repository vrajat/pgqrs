use crate::error::Result;
use crate::store::Store;
use crate::workers::{Run, Step};

/// Start a step builder.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let message = pgqrs::workflow()
///     .name("archive_files")
///     .store(&store)
///     .trigger(&serde_json::json!({"path": "/tmp/report.csv"}))?
///     .execute()
///     .await?;
/// let run = pgqrs::run()
///     .message(message)
///     .store(&store)
///     .execute()
///     .await?;
/// let step = pgqrs::step()
///     .run(&run)
///     .name("list_files")
///     .execute()
///     .await?;
/// # Ok(()) }
/// ```
pub fn step() -> StepBuilder<'static> {
    StepBuilder::new()
}

/// Builder for acquiring and managing workflow steps.
///
/// Use `.run()` and `.name()` before calling `.execute()`.
///
/// Use `.run()` and `.name()` before calling `.execute()`.
pub struct StepBuilder<'a> {
    run: Option<&'a Run>,
    name: Option<String>,
    current_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'a> StepBuilder<'a> {
    pub fn new() -> Self {
        Self {
            run: None,
            name: None,
            current_time: None,
        }
    }

    /// Set the run handle for the step.
    pub fn run(mut self, run: &'a Run) -> Self {
        self.run = Some(run);
        self
    }

    /// Set the step name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the step ID (alias for name for backward compatibility).
    pub fn id(self, id: &str) -> Self {
        self.name(id)
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

    /// Acquire the step and return its handle.
    pub async fn execute(self) -> Result<Step> {
        let run = self
            .run
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Run handle is required for StepBuilder::execute".to_string(),
            })?;
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "Step name is required for StepBuilder::execute".to_string(),
            })?;
        let current_time = self.current_time.unwrap_or_else(chrono::Utc::now);

        run.acquire_step(&name, current_time).await
    }
}

impl<'a> Default for StepBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}
