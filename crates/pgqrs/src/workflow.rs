//! Workflow helpers and handler utilities.

use crate::error::{Error, Result};
use crate::store::Store;
use crate::types::QueueMessage;
use crate::workers::Run;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Future returned by macro-generated workflow runners.
pub type WorkflowFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

/// A workflow definition that pairs the canonical workflow name with a runner.
#[derive(Clone, Copy)]
pub struct WorkflowDef<TInput, TOutput> {
    name: &'static str,
    runner: fn(Run, TInput) -> WorkflowFuture<TOutput>,
}

impl<TInput, TOutput> WorkflowDef<TInput, TOutput> {
    /// Create a workflow definition from a name and runner.
    pub const fn new(
        name: &'static str,
        runner: fn(Run, TInput) -> WorkflowFuture<TOutput>,
    ) -> Self {
        Self { name, runner }
    }

    /// Canonical workflow name used for stores/queues.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Runner function that can be used with [`workflow_handler`].
    pub fn runner(&self) -> fn(Run, TInput) -> WorkflowFuture<TOutput> {
        self.runner
    }

    /// Execute the workflow runner directly.
    pub fn run(&self, run: Run, input: TInput) -> WorkflowFuture<TOutput> {
        (self.runner)(run, input)
    }
}

/// Create a pause error for a workflow run.
pub fn pause_error(duration: Duration, message: &str) -> Error {
    Error::Paused {
        message: message.to_string(),
        resume_after: duration,
    }
}

/// Execute or replay a named workflow step.
pub async fn workflow_step<F, Fut, T>(run: &Run, name: &str, f: F) -> Result<T>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T>> + Send,
    T: Serialize + DeserializeOwned + Send + Sync,
{
    let current_time = run.current_time().unwrap_or_else(Utc::now);
    let mut step = run.acquire_step(name, current_time).await?;
    let step_rec = step.record();

    if step_rec.status == crate::types::WorkflowStatus::Success {
        if let Some(output) = &step_rec.output {
            return serde_json::from_value(output.clone()).map_err(Error::Serialization);
        }
    }

    match f().await {
        Ok(output) => {
            let val = serde_json::to_value(&output).map_err(Error::Serialization)?;
            step.complete(val).await?;
            Ok(output)
        }
        Err(e) => {
            let err_val = match &e {
                Error::Transient {
                    code,
                    message,
                    retry_after,
                } => serde_json::json!({
                    "is_transient": true,
                    "code": code,
                    "message": message,
                    "retry_after": retry_after.as_ref().map(|d| d.as_secs()),
                }),
                Error::Paused {
                    message,
                    resume_after,
                } => serde_json::json!({
                    "is_transient": true,
                    "code": "PAUSED",
                    "message": message,
                    "resume_after": resume_after.as_secs(),
                    "retry_after": resume_after.as_secs(),
                }),
                #[cfg(any(test, feature = "test-utils"))]
                Error::TestCrash => {
                    return Err(Error::TestCrash);
                }
                Error::Internal { message } => serde_json::json!({
                    "is_transient": false,
                    "code": "INTERNAL",
                    "message": message,
                }),
                _ => serde_json::json!({
                    "is_transient": false,
                    "code": "NON_RETRYABLE",
                    "message": e.to_string(),
                }),
            };
            step.fail_with_json(err_val, current_time).await?;
            Err(e)
        }
    }
}

/// Build a queue handler that runs a workflow with typed input.
pub fn workflow_handler<S, F, Fut, T, R>(
    store: S,
    handler: F,
) -> impl Fn(QueueMessage) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
       + Send
       + Sync
       + Clone
       + 'static
where
    S: Store + Clone + 'static,
    F: Fn(Run, T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R>> + Send,
    T: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
{
    workflow_handler_impl(store, handler, None)
}

/// Build a workflow handler with a fixed current time (tests only).
#[cfg(any(test, feature = "test-utils"))]
pub fn workflow_handler_with_time<S, F, Fut, T, R>(
    store: S,
    handler: F,
    current_time: chrono::DateTime<chrono::Utc>,
) -> impl Fn(QueueMessage) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
       + Send
       + Sync
       + Clone
       + 'static
where
    S: Store + Clone + 'static,
    F: Fn(Run, T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R>> + Send,
    T: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
{
    workflow_handler_impl(store, handler, Some(current_time))
}

/// Internal helper to avoid duplication between workflow_handler and workflow_handler_with_time.
fn workflow_handler_impl<S, F, Fut, T, R>(
    store: S,
    handler: F,
    current_time: Option<chrono::DateTime<chrono::Utc>>,
) -> impl Fn(QueueMessage) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
       + Send
       + Sync
       + Clone
       + 'static
where
    S: Store + Clone + 'static,
    F: Fn(Run, T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R>> + Send,
    T: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
{
    move |msg| {
        let store = store.clone();
        let handler = handler.clone();
        Box::pin(async move {
            let mut run = store.run(msg.clone()).await?;
            if run.record().status == crate::types::WorkflowStatus::Cancelled {
                return Ok(());
            }
            if let Some(time) = current_time {
                run = run.with_time(time);
            }
            run = run.start().await?;

            // Workflow triggers enqueue payloads in the shape `{ "input": <T> }`.
            // For replay/resume messages we may also see `{ "input": <T>, "run_id": <id> }`.
            // Prefer deserializing from the `input` field when present, and fall back to the full
            // payload only for backward-compat / alternate producer shapes.
            let input: T = if let Some(input) = msg.payload.get("input") {
                serde_json::from_value(input.clone())
                    .or_else(|_| serde_json::from_value(msg.payload.clone()))?
            } else {
                serde_json::from_value(msg.payload.clone())?
            };

            match handler(run.clone(), input).await {
                Ok(output) => {
                    let val = serde_json::to_value(output)?;
                    let _ = run.complete(val).await?;
                }
                Err(e) => match e {
                    Error::Paused {
                        message,
                        resume_after,
                    } => {
                        let _ = run.pause(message.clone(), resume_after).await?;
                        return Err(Error::Paused {
                            message,
                            resume_after,
                        });
                    }
                    Error::Transient { .. } | Error::StepNotReady { .. } => {
                        return Err(e);
                    }
                    Error::Cancelled { .. } => {
                        return Ok(());
                    }
                    #[cfg(any(test, feature = "test-utils"))]
                    Error::TestCrash => {
                        return Err(Error::TestCrash);
                    }
                    Error::Internal { message } => {
                        let err_val = serde_json::json!(message);
                        let _ = run.fail_with_json(err_val).await?;
                        return Ok(());
                    }
                    _ => {
                        let err_val = serde_json::json!(e.to_string());
                        let _ = run.fail_with_json(err_val).await?;
                        return Ok(());
                    }
                },
            }
            Ok(())
        })
    }
}
