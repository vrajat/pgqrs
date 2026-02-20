//! High-level actor interfaces for workers, producers, and consumers.

use crate::rate_limit::RateLimitStatus;
use crate::store::{AnyStore, Store};
pub use crate::types::{
    QueueMessage, QueueRecord, RunRecord, StepRecord, WorkerRecord, WorkerStatus, WorkflowRecord,
};
use crate::validation::{PayloadValidator, ValidationConfig};
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

/// Producer interface for enqueueing messages to a specific queue.
#[derive(Clone, Debug)]
pub struct Producer {
    store: AnyStore,
    queue_info: QueueRecord,
    worker_record: WorkerRecord,
    validator: PayloadValidator,
    current_time: Option<DateTime<Utc>>,
}

impl Producer {
    pub fn new(
        store: AnyStore,
        queue_info: QueueRecord,
        worker_record: WorkerRecord,
        validation_config: ValidationConfig,
    ) -> Self {
        Self {
            store,
            queue_info,
            worker_record,
            validator: PayloadValidator::new(validation_config),
            current_time: None,
        }
    }

    pub fn with_time(mut self, time: DateTime<Utc>) -> Self {
        self.current_time = Some(time);
        self
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.current_time.unwrap_or_else(Utc::now)
    }

    pub fn worker_id(&self) -> i64 {
        self.worker_record.id
    }

    pub fn worker_record(&self) -> &WorkerRecord {
        &self.worker_record
    }

    pub async fn status(&self) -> crate::error::Result<WorkerStatus> {
        self.store.workers().get_status(self.worker_record.id).await
    }

    pub async fn suspend(&self) -> crate::error::Result<()> {
        self.store.workers().suspend(self.worker_record.id).await
    }

    pub async fn resume(&self) -> crate::error::Result<()> {
        self.store.workers().resume(self.worker_record.id).await
    }

    pub async fn shutdown(&self) -> crate::error::Result<()> {
        self.store.workers().shutdown(self.worker_record.id).await
    }

    pub async fn heartbeat(&self) -> crate::error::Result<()> {
        self.store.workers().heartbeat(self.worker_record.id).await
    }

    pub async fn is_healthy(&self, max_age: Duration) -> crate::error::Result<bool> {
        self.store
            .workers()
            .is_healthy(self.worker_record.id, max_age)
            .await
    }

    pub async fn get_message_by_id(&self, msg_id: i64) -> crate::error::Result<QueueMessage> {
        self.store.messages().get(msg_id).await
    }

    pub async fn enqueue(&self, payload: &Value) -> crate::error::Result<QueueMessage> {
        self.enqueue_delayed(payload, 0).await
    }

    pub async fn enqueue_delayed(
        &self,
        payload: &Value,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage> {
        self.validator.validate(payload)?;

        let now = self.current_time();
        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));

        let new_message = crate::types::NewQueueMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
            producer_worker_id: Some(self.worker_record.id),
            consumer_worker_id: None,
        };

        self.store.messages().insert(new_message).await
    }

    pub async fn batch_enqueue(
        &self,
        payloads: &[Value],
    ) -> crate::error::Result<Vec<QueueMessage>> {
        self.batch_enqueue_delayed(payloads, 0).await
    }

    pub async fn batch_enqueue_delayed(
        &self,
        payloads: &[Value],
        delay_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>> {
        self.batch_enqueue_at(payloads, self.current_time(), delay_seconds)
            .await
    }

    pub async fn enqueue_at(
        &self,
        payload: &Value,
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> crate::error::Result<QueueMessage> {
        self.validator.validate(payload)?;

        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));

        let new_message = crate::types::NewQueueMessage {
            queue_id: self.queue_info.id,
            payload: payload.clone(),
            read_ct: 0,
            enqueued_at: now,
            vt,
            producer_worker_id: Some(self.worker_record.id),
            consumer_worker_id: None,
        };

        self.store.messages().insert(new_message).await
    }

    pub async fn batch_enqueue_at(
        &self,
        payloads: &[Value],
        now: chrono::DateTime<chrono::Utc>,
        delay_seconds: u32,
    ) -> crate::error::Result<Vec<QueueMessage>> {
        self.validator.validate_batch(payloads)?;

        let vt = now + chrono::Duration::seconds(i64::from(delay_seconds));

        let ids = self
            .store
            .messages()
            .batch_insert(
                self.queue_info.id,
                payloads,
                crate::types::BatchInsertParams {
                    read_ct: 0,
                    enqueued_at: now,
                    vt,
                    producer_worker_id: Some(self.worker_record.id),
                    consumer_worker_id: None,
                },
            )
            .await?;

        self.store.messages().get_by_ids(&ids).await
    }

    pub async fn replay_dlq(
        &self,
        archived_msg_id: i64,
    ) -> crate::error::Result<Option<QueueMessage>> {
        self.store.messages().replay_dlq(archived_msg_id).await
    }

    pub fn validation_config(&self) -> &ValidationConfig {
        self.validator.config()
    }

    pub fn rate_limit_status(&self) -> Option<RateLimitStatus> {
        self.validator.rate_limit_status()
    }
}

/// Consumer interface for processing messages.
#[derive(Clone, Debug)]
pub struct Consumer {
    store: AnyStore,
    queue_info: QueueRecord,
    worker_record: WorkerRecord,
    current_time: Option<DateTime<Utc>>,
}

impl Consumer {
    pub fn new(store: AnyStore, queue_info: QueueRecord, worker_record: WorkerRecord) -> Self {
        Self {
            store,
            queue_info,
            worker_record,
            current_time: None,
        }
    }

    pub fn with_time(mut self, time: DateTime<Utc>) -> Self {
        self.current_time = Some(time);
        self
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.current_time.unwrap_or_else(Utc::now)
    }

    pub fn worker_id(&self) -> i64 {
        self.worker_record.id
    }

    pub fn worker_record(&self) -> &WorkerRecord {
        &self.worker_record
    }

    pub async fn status(&self) -> crate::error::Result<WorkerStatus> {
        self.store.workers().get_status(self.worker_record.id).await
    }

    pub async fn suspend(&self) -> crate::error::Result<()> {
        self.store.workers().suspend(self.worker_record.id).await
    }

    pub async fn resume(&self) -> crate::error::Result<()> {
        self.store.workers().resume(self.worker_record.id).await
    }

    pub async fn shutdown(&self) -> crate::error::Result<()> {
        let pending = self
            .store
            .messages()
            .count_pending_for_queue_and_worker(self.queue_info.id, self.worker_record.id)
            .await?;

        if pending > 0 {
            return Err(crate::error::Error::WorkerHasPendingMessages {
                count: pending as u64,
                reason: format!("Consumer has {} pending messages", pending),
            });
        }
        self.store.workers().shutdown(self.worker_record.id).await
    }

    pub async fn heartbeat(&self) -> crate::error::Result<()> {
        self.store.workers().heartbeat(self.worker_record.id).await
    }

    pub async fn is_healthy(&self, max_age: Duration) -> crate::error::Result<bool> {
        self.store
            .workers()
            .is_healthy(self.worker_record.id, max_age)
            .await
    }

    pub async fn dequeue(&self) -> crate::error::Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    pub async fn dequeue_many(&self, limit: usize) -> crate::error::Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, 30).await
    }

    pub async fn dequeue_delay(&self, vt: u32) -> crate::error::Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    pub async fn dequeue_many_with_delay(
        &self,
        limit: usize,
        vt: u32,
    ) -> crate::error::Result<Vec<QueueMessage>> {
        self.dequeue_at(limit, vt, self.current_time()).await
    }

    pub async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<Vec<QueueMessage>> {
        self.store
            .messages()
            .dequeue_at(
                self.queue_info.id,
                limit,
                vt,
                self.worker_record.id,
                now,
                self.store.config().max_read_ct,
            )
            .await
    }

    pub async fn extend_vt(&self, message_id: i64, seconds: u32) -> crate::error::Result<bool> {
        let count = self
            .store
            .messages()
            .extend_visibility(message_id, self.worker_record.id, seconds)
            .await?;
        Ok(count > 0)
    }

    pub async fn delete(&self, message_id: i64) -> crate::error::Result<bool> {
        let count = self
            .store
            .messages()
            .delete_owned(message_id, self.worker_record.id)
            .await?;
        Ok(count > 0)
    }

    pub async fn delete_many(&self, message_ids: Vec<i64>) -> crate::error::Result<Vec<bool>> {
        self.store
            .messages()
            .delete_many_owned(&message_ids, self.worker_record.id)
            .await
    }

    pub async fn archive(&self, msg_id: i64) -> crate::error::Result<Option<QueueMessage>> {
        self.store
            .messages()
            .archive(msg_id, self.worker_record.id)
            .await
    }

    pub async fn archive_many(&self, msg_ids: Vec<i64>) -> crate::error::Result<Vec<bool>> {
        self.store
            .messages()
            .archive_many(&msg_ids, self.worker_record.id)
            .await
    }

    pub async fn release_messages(&self, message_ids: &[i64]) -> crate::error::Result<u64> {
        let res = self
            .store
            .messages()
            .release_messages_by_ids(message_ids, self.worker_record.id)
            .await?;
        Ok(res.iter().filter(|&&x| x).count() as u64)
    }

    pub async fn release_with_visibility(
        &self,
        message_id: i64,
        visible_at: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<bool> {
        let count = self
            .store
            .messages()
            .release_with_visibility(message_id, self.worker_record.id, visible_at)
            .await?;
        Ok(count > 0)
    }
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
        let step_name_string = step_name.to_string();
        let row = self
            .store
            .workflow_steps()
            .execute(
                crate::store::query::QueryBuilder::new(
                    self.store.workflow_steps().sql_acquire_step(),
                )
                .bind_i64(self.record.id)
                .bind_string(step_name_string.clone()),
            )
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "SQL_ACQUIRE_STEP".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to acquire step {} for run {}",
                    step_name_string, self.record.id
                ),
            })?;

        let mut status = row.status;
        let retry_count = row.retry_count;
        let retry_at = row.retry_at;

        if status == crate::types::WorkflowStatus::Error {
            if let Some(retry_at) = retry_at {
                if current_time < retry_at {
                    return Err(crate::error::Error::StepNotReady {
                        retry_at,
                        retry_count: retry_count as u32,
                    });
                }

                self.store
                    .workflow_steps()
                    .execute(
                        crate::store::query::QueryBuilder::new(
                            self.store.workflow_steps().sql_clear_retry(),
                        )
                        .bind_i64(row.id),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|e| crate::error::Error::QueryFailed {
                        query: "SQL_CLEAR_RETRY".into(),
                        source: Box::new(e),
                        context: format!("Failed to clear retry_at for step {}", row.id),
                    })?;

                status = crate::types::WorkflowStatus::Running;
            } else {
                let error_val = row.error.unwrap_or_else(|| {
                    serde_json::json!({
                        "is_transient": false,
                        "message": "Unknown error"
                    })
                });

                return Err(crate::error::Error::RetriesExhausted {
                    error: error_val,
                    attempts: retry_count as u32,
                });
            }
        }

        let record = StepRecord { status, ..row };
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
        let query =
            crate::store::query::QueryBuilder::new(self.store.workflow_steps().sql_complete_step())
                .bind_i64(self.record.id)
                .bind_json(output);
        self.store.workflow_steps().execute(query).await.map(|_| ())
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
                let query = crate::store::query::QueryBuilder::new(
                    self.store.workflow_steps().sql_fail_step(),
                )
                .bind_i64(self.record.id)
                .bind_json(error_record)
                .bind_datetime(None)
                .bind_i32(self.record.retry_count);
                return self.store.workflow_steps().execute(query).await.map(|_| ());
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

            let query =
                crate::store::query::QueryBuilder::new(self.store.workflow_steps().sql_fail_step())
                    .bind_i64(self.record.id)
                    .bind_json(error_record)
                    .bind_datetime(Some(retry_at))
                    .bind_i32(new_retry_count);
            return self.store.workflow_steps().execute(query).await.map(|_| ());
        }

        let query =
            crate::store::query::QueryBuilder::new(self.store.workflow_steps().sql_fail_step())
                .bind_i64(self.record.id)
                .bind_json(error_record)
                .bind_datetime(None)
                .bind_i32(self.record.retry_count);
        self.store.workflow_steps().execute(query).await.map(|_| ())
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
