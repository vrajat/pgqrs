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

pub fn pause_error(duration: Duration, message: &str) -> Error {
    Error::Paused {
        message: message.to_string(),
        resume_after: duration,
    }
}

pub async fn workflow_step<F, Fut, T, E>(run: &Run, name: &str, f: F) -> Result<T>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = std::result::Result<T, E>> + Send,
    T: Serialize + DeserializeOwned + Send + Sync,
    E: std::fmt::Display + Send,
{
    let step_rec = run.acquire_step(name, Utc::now()).await?;

    if step_rec.status == crate::types::WorkflowStatus::Success {
        if let Some(output) = step_rec.output {
            return serde_json::from_value(output).map_err(Error::Serialization);
        }
    }

    match f().await {
        Ok(output) => {
            let val = serde_json::to_value(&output).map_err(Error::Serialization)?;
            run.complete_step(name, val).await?;
            Ok(output)
        }
        Err(e) => {
            let err_msg = e.to_string();
            let err_val = serde_json::json!(err_msg);
            run.fail_step(name, err_val, Utc::now()).await?;
            Err(Error::Internal { message: err_msg })
        }
    }
}

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
    move |msg| {
        let store = store.clone();
        let handler = handler.clone();
        Box::pin(async move {
            let run = store.run(msg.clone()).await?;
            run.start().await?;

            let input: T = if let Some(input) = msg.payload.get("input") {
                if msg.payload.get("run_id").is_some()
                    && msg
                        .payload
                        .as_object()
                        .map(|o| o.len() == 2)
                        .unwrap_or(false)
                {
                    serde_json::from_value(input.clone())
                        .or_else(|_| serde_json::from_value(msg.payload.clone()))?
                } else {
                    serde_json::from_value(msg.payload)?
                }
            } else {
                serde_json::from_value(msg.payload)?
            };

            match handler(run.clone(), input).await {
                Ok(output) => {
                    let val = serde_json::to_value(output)?;
                    run.complete(val).await?;
                }
                Err(e) => match e {
                    Error::Paused {
                        message,
                        resume_after,
                    } => {
                        run.pause(message.clone(), resume_after).await?;
                        return Err(Error::Paused {
                            message,
                            resume_after,
                        });
                    }
                    _ => {
                        let err_val = serde_json::json!(e.to_string());
                        run.fail_with_json(err_val).await?;
                        return Err(e);
                    }
                },
            }
            Ok(())
        })
    }
}
