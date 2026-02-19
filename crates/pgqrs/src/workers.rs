//! High-level actor interfaces for workers, producers, and consumers.

use crate::rate_limit::RateLimitStatus;
use crate::store::{AnyStore, Store};
pub use crate::types::{
    ArchivedMessage, QueueMessage, QueueRecord, RunRecord, StepRecord, WorkerRecord, WorkerStatus,
    WorkflowRecord,
};
use crate::validation::ValidationConfig;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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

    async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<bool>;
}

/// Workflow execution run handle.
#[derive(Clone, Debug)]
pub struct Run {
    store: AnyStore,
    record: RunRecord,
    current_time: Option<DateTime<Utc>>,
}

impl Run {
    pub fn new(store: AnyStore, record: RunRecord) -> Self {
        Self {
            store,
            record,
            current_time: None,
        }
    }

    pub fn with_time(mut self, time: DateTime<Utc>) -> Self {
        self.current_time = Some(time);
        self
    }

    pub fn current_time(&self) -> Option<DateTime<Utc>> {
        self.current_time
    }

    pub fn id(&self) -> i64 {
        self.record.id
    }

    pub fn record(&self) -> &RunRecord {
        &self.record
    }

    fn with_record(&self, record: RunRecord) -> Self {
        Self {
            store: self.store.clone(),
            record,
            current_time: self.current_time,
        }
    }

    pub async fn refresh(&self) -> crate::error::Result<Run> {
        let record = self.store.workflow_runs().get(self.record.id).await?;
        Ok(self.with_record(record))
    }

    pub async fn start(&self) -> crate::error::Result<Run> {
        let record = self.store.workflow_runs().start_run(self.record.id).await?;
        Ok(self.with_record(record))
    }

    pub async fn complete(&self, output: serde_json::Value) -> crate::error::Result<Run> {
        let record = self
            .store
            .workflow_runs()
            .complete_run(self.record.id, output)
            .await?;
        Ok(self.with_record(record))
    }

    pub async fn pause(
        &self,
        message: String,
        resume_after: std::time::Duration,
    ) -> crate::error::Result<Run> {
        let record = self
            .store
            .workflow_runs()
            .pause_run(self.record.id, message, resume_after)
            .await?;
        Ok(self.with_record(record))
    }

    pub async fn fail_with_json(&self, error: serde_json::Value) -> crate::error::Result<Run> {
        let record = self
            .store
            .workflow_runs()
            .fail_run(self.record.id, error)
            .await?;
        Ok(self.with_record(record))
    }

    pub async fn success<T: serde::Serialize + Send + Sync>(
        &self,
        output: &T,
    ) -> crate::error::Result<Run> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    pub async fn fail<T: serde::Serialize + Send + Sync>(
        &self,
        error: &T,
    ) -> crate::error::Result<Run> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value).await
    }

    pub async fn acquire_step(
        &self,
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<Step> {
        let record = self
            .store
            .acquire_step(self.record.id, step_name, current_time)
            .await?;
        Ok(Step::new(self.store.clone(), record))
    }

    pub async fn complete_step(
        &self,
        step_name: &str,
        output: serde_json::Value,
    ) -> crate::error::Result<()> {
        let current_time = self.current_time().unwrap_or_else(chrono::Utc::now);
        let mut step = self.acquire_step(step_name, current_time).await?;
        step.complete(output).await
    }

    pub async fn fail_step(
        &self,
        step_name: &str,
        error: serde_json::Value,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<()> {
        let mut step = self.acquire_step(step_name, current_time).await?;
        step.fail_with_json(error, current_time).await
    }
}

/// A handle for a workflow step execution.
#[derive(Clone, Debug)]
pub struct Step {
    store: AnyStore,
    record: StepRecord,
    current_time: Option<DateTime<Utc>>,
}

impl Step {
    pub fn new(store: AnyStore, record: StepRecord) -> Self {
        Self {
            store,
            record,
            current_time: None,
        }
    }

    pub fn with_time(mut self, time: DateTime<Utc>) -> Self {
        self.current_time = Some(time);
        self
    }

    pub fn id(&self) -> i64 {
        self.record.id
    }

    pub fn record(&self) -> &StepRecord {
        &self.record
    }

    pub fn status(&self) -> crate::types::WorkflowStatus {
        self.record.status
    }

    pub fn output(&self) -> Option<&serde_json::Value> {
        self.record.output.as_ref()
    }

    pub async fn complete(&mut self, output: serde_json::Value) -> crate::error::Result<()> {
        self.store
            .workflow_steps()
            .complete_step(self.record.id, output)
            .await
    }

    pub async fn fail_with_json(
        &mut self,
        error: serde_json::Value,
        current_time: DateTime<Utc>,
    ) -> crate::error::Result<()> {
        let error_record = if error.get("is_transient").is_some() {
            error
        } else {
            serde_json::json!({
                "is_transient": false,
                "code": "NON_RETRYABLE",
                "message": error.to_string(),
            })
        };

        let is_transient = error_record
            .get("is_transient")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_transient {
            let policy = crate::StepRetryPolicy::default();
            if !policy.should_retry(self.record.retry_count as u32) {
                return self
                    .store
                    .workflow_steps()
                    .fail_step(self.record.id, error_record, None, self.record.retry_count)
                    .await;
            }

            let delay_seconds = policy.extract_retry_delay(&error_record, self.record.retry_count);
            let delay_i64 =
                delay_seconds
                    .try_into()
                    .map_err(|_| crate::error::Error::Internal {
                        message: format!(
                            "Retry delay {} seconds exceeds maximum allowed value (i64::MAX)",
                            delay_seconds
                        ),
                    })?;

            let retry_at = current_time + chrono::Duration::seconds(delay_i64);
            let new_retry_count = self.record.retry_count + 1;

            return self
                .store
                .workflow_steps()
                .fail_step(
                    self.record.id,
                    error_record,
                    Some(retry_at),
                    new_retry_count,
                )
                .await;
        }

        self.store
            .workflow_steps()
            .fail_step(self.record.id, error_record, None, self.record.retry_count)
            .await
    }

    pub async fn success<T: serde::Serialize + Send + Sync>(
        &mut self,
        output: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(output).map_err(crate::error::Error::Serialization)?;
        self.complete(value).await
    }

    pub async fn fail<T: serde::Serialize + Send + Sync>(
        &mut self,
        error: &T,
    ) -> crate::error::Result<()> {
        let value = serde_json::to_value(error).map_err(crate::error::Error::Serialization)?;
        self.fail_with_json(value, chrono::Utc::now()).await
    }
}
