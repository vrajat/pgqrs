//! Repository traits for low-level database operations.

use crate::types::{
    ArchivedMessage, NewArchivedMessage, NewQueueRecord, NewRunRecord, NewStepRecord,
    NewWorkflowRecord, QueueMessage, QueueRecord, RunRecord, StepRecord, WorkerRecord,
    WorkerStatus, WorkflowRecord,
};

use async_trait::async_trait;

/// Repository for managing queues.
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

/// Repository for managing messages.
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

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> crate::error::Result<Vec<i64>>;

    async fn get_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<QueueMessage>>;

    async fn update_visibility_timeout(
        &self,
        id: i64,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<u64>;

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

    async fn count_pending(&self, queue_id: i64) -> crate::error::Result<i64>;

    async fn count_pending_filtered(
        &self,
        queue_id: i64,
        worker_id: Option<i64>,
    ) -> crate::error::Result<i64>;

    async fn delete_by_ids(&self, ids: &[i64]) -> crate::error::Result<Vec<bool>>;
}

/// Repository for managing workers.
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
}

/// Repository for managing archived messages.
#[async_trait]
pub trait ArchiveTable: Send + Sync {
    async fn insert(&self, data: NewArchivedMessage) -> crate::error::Result<ArchivedMessage>;
    async fn get(&self, id: i64) -> crate::error::Result<ArchivedMessage>;
    async fn list(&self) -> crate::error::Result<Vec<ArchivedMessage>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
    async fn filter_by_fk(&self, queue_id: i64) -> crate::error::Result<Vec<ArchivedMessage>>;

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    async fn dlq_count(&self, max_attempts: i32) -> crate::error::Result<i64>;

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> crate::error::Result<Vec<ArchivedMessage>>;

    async fn count_by_worker(&self, worker_id: i64) -> crate::error::Result<i64>;

    async fn delete_by_worker(&self, worker_id: i64) -> crate::error::Result<u64>;

    async fn replay_message(&self, msg_id: i64) -> crate::error::Result<Option<QueueMessage>>;

    async fn count_for_queue(&self, queue_id: i64) -> crate::error::Result<i64>;
}

/// Repository for managing workflows.
#[async_trait]
pub trait WorkflowTable: Send + Sync {
    async fn insert(&self, data: NewWorkflowRecord) -> crate::error::Result<WorkflowRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<WorkflowRecord>;
    async fn get_by_name(&self, name: &str) -> crate::error::Result<WorkflowRecord>;
    async fn list(&self) -> crate::error::Result<Vec<WorkflowRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}

/// Repository for managing workflow runs.
#[async_trait]
pub trait RunRecordTable: Send + Sync {
    async fn insert(&self, data: NewRunRecord) -> crate::error::Result<RunRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<RunRecord>;
    async fn list(&self) -> crate::error::Result<Vec<RunRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}

/// Repository for managing workflow steps.
#[async_trait]
pub trait StepRecordTable: Send + Sync {
    async fn insert(&self, data: NewStepRecord) -> crate::error::Result<StepRecord>;
    async fn get(&self, id: i64) -> crate::error::Result<StepRecord>;
    async fn list(&self) -> crate::error::Result<Vec<StepRecord>>;
    async fn count(&self) -> crate::error::Result<i64>;
    async fn delete(&self, id: i64) -> crate::error::Result<u64>;
}
