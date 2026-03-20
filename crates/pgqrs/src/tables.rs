//! Repository traits for low-level storage operations.

use crate::types::{
    NewQueueRecord, NewRunRecord, NewStepRecord, NewWorkflowRecord, QueueMessage, QueueRecord,
    RunRecord, StepRecord, WorkerRecord, WorkerStatus, WorkflowRecord,
};
use crate::{QueueMetrics, SystemStats, WorkerHealthStats};

use async_trait::async_trait;

/// Queue persistence operations.
#[async_trait]
pub trait QueueTable: Send + Sync {
    async fn insert(&self, data: NewQueueRecord) -> crate::error::Result<QueueRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<QueueRecord>;
    async fn list(&self) -> crate::error::Result<Vec<QueueRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn get_by_name(&self, name: &str) -> crate::error::Result<QueueRecord>;
    async fn exists(&self, name: &str) -> crate::error::Result<bool>;
    async fn delete_by_name(&self, name: &str) -> crate::error::Result<u64>;
}

/// Message persistence operations.
#[async_trait]
pub trait MessageTable: Send + Sync {
    async fn insert(
        &self,
        data: crate::types::NewQueueMessage,
    ) -> crate::error::Result<QueueMessage>;
    async fn get(&self, id: i64) -> crate::error::Result<QueueMessage>;
    async fn list(&self) -> crate::error::Result<Vec<QueueMessage>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<QueueMessage>>;
    async fn list_by_consumer_worker(
        &self,
        worker_id: i64,
    ) -> crate::error::Result<Vec<QueueMessage>>;
    async fn count_by_consumer_worker(&self, worker_id: i64) -> crate::error::Result<i64>;
    async fn count_worker_references(&self, worker_id: i64) -> crate::error::Result<i64>;
    async fn move_to_dlq(&self, max_read_ct: i32) -> crate::error::Result<Vec<i64>>;
    async fn release_by_consumer_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> crate::error::Result<Vec<i64>>;

    async fn get_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<QueueMessage>>;

    async fn update_payload(
        &self,
        id: i64,
        payload: serde_json::Value,
    ) -> crate::error::Result<u64>;

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<u64>;

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> crate::error::Result<Vec<bool>>;

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> crate::error::Result<Vec<bool>>;

    async fn release_with_visibility(
        &self,
        id: i64,
        worker_id: i64,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<u64>;

    async fn count_pending_for_queue(&self, queue_id: i64) -> crate::error::Result<i64>;

    async fn count_pending_for_queue_and_worker(
        &self,
        queue_id: i64,
        worker_id: i64,
    ) -> crate::error::Result<i64>;

    async fn dequeue_at(
        &self,
        queue_id: i64,
        limit: usize,
        vt: u32,
        worker_id: i64,
        now: chrono::DateTime<chrono::Utc>,
        max_read_ct: i32,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn archive(&self, id: i64, worker_id: i64) -> crate::error::Result<Option<QueueMessage>>;

    async fn archive_many(&self, ids: &[i64], worker_id: i64) -> crate::error::Result<Vec<bool>>;

    async fn replay_dlq(&self, id: i64) -> crate::error::Result<Option<QueueMessage>>;

    async fn delete_owned(&self, id: i64, worker_id: i64) -> crate::error::Result<u64>;

    async fn delete_many_owned(
        &self,
        ids: &[i64],
        worker_id: i64,
    ) -> crate::error::Result<Vec<bool>>;

    async fn list_archived_by_queue(
        &self,
        queue_id: i64,
    ) -> crate::error::Result<Vec<QueueMessage>>;

    async fn count_by_fk(&self, queue_id: i64) -> crate::error::Result<i64>;

    async fn delete_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<bool>>;
}

/// Worker persistence operations.
#[async_trait]
pub trait WorkerTable: Send + Sync {
    async fn insert(
        &self,
        data: crate::types::NewWorkerRecord,
    ) -> crate::error::Result<WorkerRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkerRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkerRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<WorkerRecord>>;
    async fn count_by_fk(&self, queue_id: i64) -> crate::error::Result<i64>;
    async fn mark_stopped(&self, id: i64) -> crate::error::Result<()>;

    async fn count_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
    ) -> crate::error::Result<i64>;

    async fn count_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> crate::error::Result<i64>;

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
    ) -> crate::error::Result<Vec<WorkerRecord>>;

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: chrono::Duration,
    ) -> crate::error::Result<Vec<WorkerRecord>>;

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> crate::error::Result<WorkerRecord>;

    async fn register_ephemeral(&self, queue_id: Option<i64>)
        -> crate::error::Result<WorkerRecord>;

    async fn get_status(&self, id: i64) -> crate::error::Result<WorkerStatus>;
    async fn suspend(&self, id: i64) -> crate::error::Result<()>;
    async fn resume(&self, id: i64) -> crate::error::Result<()>;
    async fn shutdown(&self, id: i64) -> crate::error::Result<()>;
    async fn poll(&self, id: i64) -> crate::error::Result<()>;
    async fn interrupt(&self, id: i64) -> crate::error::Result<()>;
    async fn heartbeat(&self, id: i64) -> crate::error::Result<()>;
    async fn is_healthy(&self, id: i64, max_age: chrono::Duration) -> crate::error::Result<bool>;
}

/// Cross-table repository operations used by admin/reporting paths.
#[async_trait]
pub trait DbStateTable: Send + Sync {
    async fn verify(&self) -> crate::error::Result<()>;
    async fn purge_queue(&self, queue_id: i64) -> crate::error::Result<()>;
    async fn queue_metrics(&self, queue_id: i64) -> crate::error::Result<QueueMetrics>;
    async fn all_queues_metrics(&self) -> crate::error::Result<Vec<QueueMetrics>>;
    async fn system_stats(&self) -> crate::error::Result<SystemStats>;
    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> crate::error::Result<Vec<WorkerHealthStats>>;
    async fn purge_old_workers(&self, older_than: chrono::Duration) -> crate::error::Result<u64>;
}

/// Workflow definition persistence operations.
#[async_trait]
pub trait WorkflowTable: Send + Sync {
    async fn insert(&self, data: NewWorkflowRecord) -> crate::error::Result<WorkflowRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkflowRecord>;
    async fn get_by_name(&self, name: &str) -> crate::error::Result<WorkflowRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkflowRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}

/// Workflow run persistence operations.
#[async_trait]
pub trait RunRecordTable: Send + Sync {
    async fn insert(&self, data: NewRunRecord) -> crate::error::Result<RunRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<RunRecord>;
    async fn list(&self) -> crate::error::Result<Vec<RunRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn start_run(&self, id: i64) -> crate::error::Result<RunRecord>;
    async fn complete_run(
        &self,
        id: i64,
        output: serde_json::Value,
    ) -> crate::error::Result<RunRecord>;
    async fn pause_run(
        &self,
        id: i64,
        message: String,
        resume_after: std::time::Duration,
    ) -> crate::error::Result<RunRecord>;
    async fn fail_run(&self, id: i64, error: serde_json::Value) -> crate::error::Result<RunRecord>;
    async fn get_by_message_id(&self, message_id: i64) -> crate::error::Result<RunRecord>;
}

/// Workflow step persistence operations.
#[async_trait]
pub trait StepRecordTable: Send + Sync {
    async fn insert(&self, data: NewStepRecord) -> crate::error::Result<StepRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<StepRecord>;
    async fn list(&self) -> crate::error::Result<Vec<StepRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn acquire_step(&self, run_id: i64, step_name: &str) -> crate::error::Result<StepRecord>;
    async fn clear_retry(&self, id: i64) -> crate::error::Result<StepRecord>;
    async fn complete_step(
        &self,
        id: i64,
        output: serde_json::Value,
    ) -> crate::error::Result<StepRecord>;
    async fn fail_step(
        &self,
        id: i64,
        error: serde_json::Value,
        retry_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_count: i32,
    ) -> crate::error::Result<StepRecord>;
    async fn execute(
        &self,
        query: crate::store::query::QueryBuilder,
    ) -> crate::error::Result<StepRecord>;
}
