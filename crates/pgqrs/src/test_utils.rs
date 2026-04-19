//! Test-only workflow lifecycle helpers.

use crate::error::Result;
use crate::store::{AnyStore, Store};
use crate::types::QueueMessage;
use crate::workers::{Consumer, Run};
use std::future::Future;

/// Role-oriented test harness for workflow execution attempts.
///
/// This is intentionally test-only. It exposes the consumer and external-actor
/// phases directly so integration tests can model workflow lifecycle checkpoints
/// without relying entirely on timing-based orchestration.
#[derive(Clone)]
pub struct WorkflowTestRig {
    store: AnyStore,
    consumer: Consumer,
}

/// Backward-compatible alias for the more role-oriented harness name.
pub type WorkflowAttemptHarness = WorkflowTestRig;

/// A single workflow delivery attempt consisting of the dequeued trigger
/// message and the materialized run handle tied to that message.
#[derive(Clone)]
pub struct WorkflowAttempt {
    pub message: QueueMessage,
    pub run: Run,
}

impl WorkflowTestRig {
    /// Create a test rig from a store and consumer representing the actor roles.
    pub fn new(store: AnyStore, consumer: Consumer) -> Self {
        Self { store, consumer }
    }

    /// Access the consumer actor used by this rig.
    pub fn consumer(&self) -> &Consumer {
        &self.consumer
    }

    /// Access the store backing this rig.
    pub fn store(&self) -> &AnyStore {
        &self.store
    }

    /// Dequeue the next workflow trigger message as the consumer actor.
    pub async fn as_consumer_dequeue(&self) -> Result<Option<QueueMessage>> {
        let mut messages = self.consumer.dequeue().await?;
        Ok(messages.pop())
    }

    /// Materialize a workflow attempt from a dequeued trigger message.
    pub async fn as_consumer_open_attempt(&self, msg: QueueMessage) -> Result<WorkflowAttempt> {
        let run = self.store.run(msg.clone()).await?;
        Ok(WorkflowAttempt { message: msg, run })
    }

    /// Resolve the run associated with a workflow message as an external actor.
    pub async fn as_external_actor_get_run(&self, message: &QueueMessage) -> Result<Run> {
        self.store.run(message.clone()).await
    }
}

impl WorkflowAttempt {
    /// Refresh the persisted run record backing this attempt.
    pub async fn refresh(&mut self) -> Result<Run> {
        self.run = self.run.refresh().await?;
        Ok(self.run.clone())
    }

    /// Start the materialized run.
    pub async fn start(&mut self) -> Result<Run> {
        self.run = self.run.start().await?;
        Ok(self.run.clone())
    }

    /// Invoke workflow logic against the current run handle.
    pub async fn invoke<T, F, Fut>(&mut self, handler: F) -> Result<T>
    where
        F: FnOnce(Run) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let result = handler(self.run.clone()).await;
        self.run = self.run.refresh().await?;
        result
    }

    /// Archive the attempt message as the consumer actor.
    pub async fn archive(&self, consumer: &Consumer) -> Result<()> {
        let _ = consumer.archive(self.message.id).await?;
        Ok(())
    }

    /// Release the attempt message back to the queue as the consumer actor.
    pub async fn release(&self, consumer: &Consumer) -> Result<()> {
        let _ = consumer.release_messages(&[self.message.id]).await?;
        Ok(())
    }
}
