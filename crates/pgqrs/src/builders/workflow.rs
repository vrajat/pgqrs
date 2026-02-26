use crate::store::Store;
use crate::types::{QueueMessage, WorkflowRecord};
use crate::workers::{Consumer, Producer};
use crate::WorkflowDef;
use serde::Serialize;
use std::marker::PhantomData;

/// Start a workflow builder.
///
/// This API is definition-driven: `.name(...)` accepts a macro-defined workflow definition,
/// not a string. The generic types `TInput` and `TOutput` are inferred from the workflow definition.
pub fn workflow<TInput, TOutput>() -> WorkflowBuilder<TInput, TOutput>
where
    TInput: serde::de::DeserializeOwned + Send + 'static,
    TOutput: serde::Serialize + Send + 'static,
{
    WorkflowBuilder::new()
}

pub struct WorkflowBuilder<TInput = serde_json::Value, TOutput = serde_json::Value> {
    workflow: Option<WorkflowDef<TInput, TOutput>>,
    consumer: Option<Consumer>,
    producer: Option<Producer>,
    poll_interval: std::time::Duration,
    batch_size: usize,
    input: Option<serde_json::Value>,
    _marker: PhantomData<(TInput, TOutput)>,
}

impl<TInput, TOutput> WorkflowBuilder<TInput, TOutput>
where
    TInput: serde::de::DeserializeOwned + Send + 'static,
    TOutput: serde::Serialize + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            workflow: None,
            consumer: None,
            producer: None,
            poll_interval: std::time::Duration::from_millis(50),
            batch_size: 1,
            input: None,
            _marker: PhantomData,
        }
    }

    /// Select the workflow definition (macro-generated `WorkflowDef`).
    pub fn name(mut self, workflow: WorkflowDef<TInput, TOutput>) -> Self {
        self.workflow = Some(workflow);
        self
    }

    pub fn consumer(mut self, consumer: &Consumer) -> Self {
        self.consumer = Some(consumer.clone());
        self
    }

    pub fn producer(mut self, producer: &Producer) -> Self {
        self.producer = Some(producer.clone());
        self
    }

    pub fn poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn batch(mut self, batch_size: usize) -> Self {
        self.batch_size = std::cmp::max(1, batch_size);
        self
    }

    pub async fn create<S: Store>(self, store: &S) -> crate::error::Result<WorkflowRecord> {
        let workflow = self
            .workflow
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "workflow definition is required".to_string(),
            })?;

        store.workflow(workflow.name()).await
    }

    pub fn trigger<T: Serialize>(mut self, input: &T) -> crate::error::Result<Self> {
        let input = serde_json::to_value(input).map_err(crate::error::Error::Serialization)?;
        self.input = Some(input);
        Ok(self)
    }

    pub async fn execute<S: Store>(self, store: &S) -> crate::error::Result<QueueMessage> {
        let workflow = self
            .workflow
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "workflow definition is required".to_string(),
            })?;

        let input = self
            .input
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "workflow input is required".to_string(),
            })?;

        let payload = serde_json::json!({ "input": input });

        let ids = if let Some(producer) = self.producer {
            let msgs = producer.batch_enqueue(&[payload]).await?;
            msgs.into_iter().map(|m| m.id).collect::<Vec<_>>()
        } else {
            crate::enqueue()
                .to(workflow.name())
                .message(&payload)
                .execute(store)
                .await?
        };

        store.messages().get(ids[0]).await
    }

    pub async fn poll<S: Store + Clone + 'static>(self, store: &S) -> crate::error::Result<()> {
        let workflow = self
            .workflow
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "workflow definition is required".to_string(),
            })?;
        let consumer = self
            .consumer
            .ok_or_else(|| crate::error::Error::ValidationFailed {
                reason: "consumer is required".to_string(),
            })?;

        let handler = crate::workflow_handler(store.clone(), workflow.runner());

        crate::dequeue()
            .worker(&consumer)
            .batch(self.batch_size)
            .poll_interval(self.poll_interval)
            .handle(move |msg| {
                let handler = handler.clone();
                Box::pin(async move { handler(msg).await })
            })
            .poll(store)
            .await
    }
}

impl<TInput, TOutput> Default for WorkflowBuilder<TInput, TOutput>
where
    TInput: serde::de::DeserializeOwned + Send + 'static,
    TOutput: serde::Serialize + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
