//! High-level actor interfaces for workers, producers, and consumers.

use crate::rate_limit::RateLimitStatus;
pub use crate::types::{
    ArchivedMessage, QueueMessage, QueueRecord, StepRecord, WorkerRecord, WorkerStatus,
    WorkflowRecord,
};
use crate::validation::ValidationConfig;
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

/// Trait defining the interface for all worker types.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Get the unique identifier for this worker.
    fn worker_record(&self) -> &WorkerRecord;

    /// Get the unique identifier for this worker.
    fn worker_id(&self) -> i64 {
        self.worker_record().id
    }

    async fn status(&self) -> crate::error::Result<WorkerStatus>;
    async fn suspend(&self) -> crate::error::Result<()>;
    async fn resume(&self) -> crate::error::Result<()>;
    async fn shutdown(&self) -> crate::error::Result<()>;
    async fn heartbeat(&self) -> crate::error::Result<()>;
    async fn is_healthy(&self, max_age: Duration) -> crate::error::Result<bool>;
}

/// Admin interface for managing pgqrs infrastructure.
#[async_trait]
pub trait Admin: Worker {
    /// Verify the pgqrs schema is correctly installed.
    async fn verify(&self) -> crate::error::Result<()>;

    /// Delete a queue.
    async fn delete_queue(&self, queue_info: &QueueRecord) -> crate::error::Result<()>;

    /// Purge all messages and workers from a queue.
    async fn purge_queue(&self, name: &str) -> crate::error::Result<()>;

    /// Get IDs of messages in the dead letter queue.
    async fn dlq(&self) -> crate::error::Result<Vec<i64>>;

    /// Get metrics for a specific queue.
    async fn queue_metrics(&self, name: &str) -> crate::error::Result<crate::stats::QueueMetrics>;

    /// Get metrics for all queues.
    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<crate::stats::QueueMetrics>>;

    /// Get system-wide statistics.
    async fn system_stats(&self) -> crate::error::Result<crate::stats::SystemStats>;

    /// Get worker health statistics.
    async fn worker_health_stats(
        &self,
        heartbeat_timeout: Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<crate::stats::WorkerHealthStats>>;

    /// Get worker statistics for a queue.
    async fn worker_stats(
        &self,
        queue_name: &str,
    ) -> crate::error::Result<crate::stats::WorkerStats>;

    /// Delete a worker by ID.
    async fn delete_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    /// Get messages currently held by a worker.
    async fn get_worker_messages(&self, worker_id: i64) -> crate::error::Result<Vec<QueueMessage>>;

    /// Reclaim messages that have exceeded their visibility timeout.
    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<Duration>,
    ) -> crate::error::Result<u64>;

    /// Purge workers that haven't sent a heartbeat recently.
    async fn purge_old_workers(&self, older_than: chrono::Duration) -> crate::error::Result<u64>;

    /// Release all messages held by a worker.
    async fn release_worker_messages(&self, worker_id: i64) -> crate::error::Result<u64>;
}

/// Producer interface for enqueueing messages.
#[async_trait]
pub trait Producer: Worker {
    async fn get_message_by_id(&self, msg_id: i64) -> crate::error::Result<QueueMessage>;
    async fn enqueue(&self, payload: &Value) -> crate::error::Result<QueueMessage>;
    async fn enqueue_delayed(
        &self,
        payload: &Value,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage>;
    async fn batch_enqueue(&self, payloads: &[Value]) -> crate::error::Result<Vec<QueueMessage>>;
    async fn batch_enqueue_delayed(
        &self,
        payloads: &[serde_json::Value],
        delay_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn enqueue_at(
        &self,
        payload: &Value,
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage>;

    async fn batch_enqueue_at(
        &self,
        payloads: &[Value],
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn insert_message(
        &self,
        payload: &Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<i64>;

    async fn replay_dlq(&self, archived_msg_id: i64) -> crate::error::Result<Option<QueueMessage>>;

    fn validation_config(&self) -> &ValidationConfig;
    fn rate_limit_status(&self) -> Option<RateLimitStatus>;
}

/// Consumer interface for processing messages.
#[async_trait]
pub trait Consumer: Worker {
    async fn dequeue(&self) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_many(&self, limit: usize) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_delay(&self, vt: u32) -> crate::error::Result<Vec<QueueMessage>>;
    async fn dequeue_many_with_delay(
        &self,
        limit: usize,
        vt: u32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<bool>;

    async fn delete(&self, message_id: i64) -> crate::error::Result<bool>;
    async fn delete_many(&self, message_ids: Vec<i64>) -> crate::error::Result<Vec<bool>>;

    async fn archive(&self, msg_id: i64) -> crate::error::Result<Option<ArchivedMessage>>;
    async fn archive_many(&self, msg_ids: Vec<i64>) -> crate::error::Result<Vec<bool>>;

    async fn release_messages(&self, message_ids: &[i64]) -> crate::error::Result<u64>;
}

/// Interface for a workflow definition.
#[async_trait]
pub trait Workflow: Send + Sync {
    fn workflow_record(&self) -> &WorkflowRecord;
}

/// Interface for a workflow execution run.
#[async_trait]
pub trait Run: Send + Sync {
    fn id(&self) -> i64;
    async fn start(&mut self) -> crate::error::Result<()>;
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;
    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()>;
    async fn acquire_step(
        &self,
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::types::StepRecord>;

    async fn complete_step(
        &self,
        step_name: &str,
        output: serde_json::Value,
    ) -> crate::error::Result<()>;

    async fn fail_step(
        &self,
        step_name: &str,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()>;
}

/// Extension trait for Run to provide generic convenience methods.
#[async_trait]
pub trait RunExt: Run {
    async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value).await
    }
}
impl<T: ?Sized + Run> RunExt for T {}

#[async_trait]
impl<T: ?Sized + Run> Run for Box<T> {
    fn id(&self) -> i64 {
        (**self).id()
    }

    async fn start(&mut self) -> crate::error::Result<()> {
        (**self).start().await
    }

    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        (**self).complete(output).await
    }

    async fn fail_with_json(&mut self, error: serde_json::Value) -> crate::error::Result<()> {
        (**self).fail_with_json(error).await
    }

    async fn acquire_step(
        &self,
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::types::StepRecord> {
        (**self).acquire_step(step_name, current_time).await
    }

    async fn complete_step(
        &self,
        step_name: &str,
        output: serde_json::Value,
    ) -> crate::error::Result<()> {
        (**self).complete_step(step_name, output).await
    }

    async fn fail_step(
        &self,
        step_name: &str,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()> {
        (**self).fail_step(step_name, error, current_time).await
    }
}

/// A guard for a workflow step execution.
#[async_trait]
pub trait StepGuard: Send + Sync {
    async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()>;
    async fn fail_with_json(
        &mut self,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()>;
}

/// Extension trait for StepGuard to provide generic convenience methods.
#[async_trait]
pub trait StepGuardExt: StepGuard {
    async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value, chrono::Utc::now()).await
    }
}
impl<T: ?Sized + StepGuard> StepGuardExt for T {}
