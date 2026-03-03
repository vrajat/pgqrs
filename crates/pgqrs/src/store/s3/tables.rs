use crate::error::Result;
use crate::store::s3::S3MutationGate;
use crate::store::sqlite::SqliteStore;
use crate::store::{
    MessageTable, QueueTable, RunRecordTable, StepRecordTable, Store, WorkerTable, WorkflowTable,
};
use crate::types::{
    BatchInsertParams, NewQueueMessage, NewQueueRecord, NewRunRecord, NewStepRecord,
    NewWorkerRecord, NewWorkflowRecord, QueueMessage, QueueRecord, RunRecord, StepRecord,
    WorkerRecord, WorkerStatus, WorkflowRecord,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

#[derive(Clone)]
pub struct S3MessageTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3MessageTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl MessageTable for S3MessageTable {
    async fn insert(&self, data: NewQueueMessage) -> Result<QueueMessage> {
        self.after_write(self.sqlite.messages().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        self.sqlite.messages().get(id).await
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        self.sqlite.messages().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.messages().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.messages().delete(id).await)
            .await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        self.sqlite.messages().filter_by_fk(queue_id).await
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[Value],
        params: BatchInsertParams,
    ) -> Result<Vec<i64>> {
        self.after_write(
            self.sqlite
                .messages()
                .batch_insert(queue_id, payloads, params)
                .await,
        )
        .await
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        self.sqlite.messages().get_by_ids(ids).await
    }

    async fn update_payload(&self, id: i64, payload: Value) -> Result<u64> {
        self.after_write(self.sqlite.messages().update_payload(id, payload).await)
            .await
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        self.after_write(
            self.sqlite
                .messages()
                .extend_visibility(id, worker_id, additional_seconds)
                .await,
        )
        .await
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        self.after_write(
            self.sqlite
                .messages()
                .extend_visibility_batch(message_ids, worker_id, additional_seconds)
                .await,
        )
        .await
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        self.after_write(
            self.sqlite
                .messages()
                .release_messages_by_ids(message_ids, worker_id)
                .await,
        )
        .await
    }

    async fn release_with_visibility(
        &self,
        id: i64,
        worker_id: i64,
        vt: DateTime<Utc>,
    ) -> Result<u64> {
        self.after_write(
            self.sqlite
                .messages()
                .release_with_visibility(id, worker_id, vt)
                .await,
        )
        .await
    }

    async fn count_pending_for_queue(&self, queue_id: i64) -> Result<i64> {
        self.sqlite
            .messages()
            .count_pending_for_queue(queue_id)
            .await
    }

    async fn count_pending_for_queue_and_worker(
        &self,
        queue_id: i64,
        worker_id: i64,
    ) -> Result<i64> {
        self.sqlite
            .messages()
            .count_pending_for_queue_and_worker(queue_id, worker_id)
            .await
    }

    async fn dequeue_at(
        &self,
        queue_id: i64,
        limit: usize,
        vt: u32,
        worker_id: i64,
        now: DateTime<Utc>,
        max_read_ct: i32,
    ) -> Result<Vec<QueueMessage>> {
        self.after_write(
            self.sqlite
                .messages()
                .dequeue_at(queue_id, limit, vt, worker_id, now, max_read_ct)
                .await,
        )
        .await
    }

    async fn archive(&self, id: i64, worker_id: i64) -> Result<Option<QueueMessage>> {
        self.after_write(self.sqlite.messages().archive(id, worker_id).await)
            .await
    }

    async fn archive_many(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        self.after_write(self.sqlite.messages().archive_many(ids, worker_id).await)
            .await
    }

    async fn replay_dlq(&self, id: i64) -> Result<Option<QueueMessage>> {
        self.after_write(self.sqlite.messages().replay_dlq(id).await)
            .await
    }

    async fn delete_owned(&self, id: i64, worker_id: i64) -> Result<u64> {
        self.after_write(self.sqlite.messages().delete_owned(id, worker_id).await)
            .await
    }

    async fn delete_many_owned(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        self.after_write(
            self.sqlite
                .messages()
                .delete_many_owned(ids, worker_id)
                .await,
        )
        .await
    }

    async fn list_archived_by_queue(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        self.sqlite
            .messages()
            .list_archived_by_queue(queue_id)
            .await
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        self.sqlite.messages().count_by_fk(queue_id).await
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        self.after_write(self.sqlite.messages().delete_by_ids(ids).await)
            .await
    }
}

#[derive(Clone)]
pub struct S3WorkerTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3WorkerTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl WorkerTable for S3WorkerTable {
    async fn insert(&self, data: NewWorkerRecord) -> Result<WorkerRecord> {
        self.after_write(self.sqlite.workers().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<WorkerRecord> {
        self.sqlite.workers().get(id).await
    }

    async fn list(&self) -> Result<Vec<WorkerRecord>> {
        self.sqlite.workers().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.workers().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.workers().delete(id).await)
            .await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerRecord>> {
        self.sqlite.workers().filter_by_fk(queue_id).await
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        self.sqlite.workers().count_by_fk(queue_id).await
    }

    async fn count_for_queue(&self, queue_id: i64, state: WorkerStatus) -> Result<i64> {
        self.sqlite.workers().count_for_queue(queue_id, state).await
    }

    async fn count_zombies_for_queue(&self, queue_id: i64, older_than: Duration) -> Result<i64> {
        self.sqlite
            .workers()
            .count_zombies_for_queue(queue_id, older_than)
            .await
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
    ) -> Result<Vec<WorkerRecord>> {
        self.sqlite.workers().list_for_queue(queue_id, state).await
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: Duration,
    ) -> Result<Vec<WorkerRecord>> {
        self.sqlite
            .workers()
            .list_zombies_for_queue(queue_id, older_than)
            .await
    }

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerRecord> {
        self.after_write(
            self.sqlite
                .workers()
                .register(queue_id, hostname, port)
                .await,
        )
        .await
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerRecord> {
        self.after_write(self.sqlite.workers().register_ephemeral(queue_id).await)
            .await
    }

    async fn get_status(&self, id: i64) -> Result<WorkerStatus> {
        self.sqlite.workers().get_status(id).await
    }

    async fn suspend(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().suspend(id).await)
            .await
    }

    async fn resume(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().resume(id).await)
            .await
    }

    async fn shutdown(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().shutdown(id).await)
            .await
    }

    async fn poll(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().poll(id).await).await
    }

    async fn interrupt(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().interrupt(id).await)
            .await
    }

    async fn heartbeat(&self, id: i64) -> Result<()> {
        self.after_write(self.sqlite.workers().heartbeat(id).await)
            .await
    }

    async fn is_healthy(&self, id: i64, max_age: Duration) -> Result<bool> {
        self.sqlite.workers().is_healthy(id, max_age).await
    }
}

#[derive(Clone)]
pub struct S3QueueTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3QueueTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl QueueTable for S3QueueTable {
    async fn insert(&self, data: NewQueueRecord) -> Result<QueueRecord> {
        self.after_write(self.sqlite.queues().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<QueueRecord> {
        self.sqlite.queues().get(id).await
    }

    async fn list(&self) -> Result<Vec<QueueRecord>> {
        self.sqlite.queues().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.queues().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.queues().delete(id).await)
            .await
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueRecord> {
        self.sqlite.queues().get_by_name(name).await
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        self.sqlite.queues().exists(name).await
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        self.after_write(self.sqlite.queues().delete_by_name(name).await)
            .await
    }
}

#[derive(Clone)]
pub struct S3WorkflowTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3WorkflowTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl WorkflowTable for S3WorkflowTable {
    async fn insert(&self, data: NewWorkflowRecord) -> Result<WorkflowRecord> {
        self.after_write(self.sqlite.workflows().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        self.sqlite.workflows().get(id).await
    }

    async fn get_by_name(&self, name: &str) -> Result<WorkflowRecord> {
        self.sqlite.workflows().get_by_name(name).await
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        self.sqlite.workflows().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.workflows().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.workflows().delete(id).await)
            .await
    }
}

#[derive(Clone)]
pub struct S3RunRecordTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3RunRecordTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl RunRecordTable for S3RunRecordTable {
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        self.after_write(self.sqlite.workflow_runs().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<RunRecord> {
        self.sqlite.workflow_runs().get(id).await
    }

    async fn list(&self) -> Result<Vec<RunRecord>> {
        self.sqlite.workflow_runs().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.workflow_runs().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.workflow_runs().delete(id).await)
            .await
    }

    async fn start_run(&self, id: i64) -> Result<RunRecord> {
        self.after_write(self.sqlite.workflow_runs().start_run(id).await)
            .await
    }

    async fn complete_run(&self, id: i64, output: Value) -> Result<RunRecord> {
        self.after_write(self.sqlite.workflow_runs().complete_run(id, output).await)
            .await
    }

    async fn pause_run(
        &self,
        id: i64,
        message: String,
        resume_after: std::time::Duration,
    ) -> Result<RunRecord> {
        self.after_write(
            self.sqlite
                .workflow_runs()
                .pause_run(id, message, resume_after)
                .await,
        )
        .await
    }

    async fn fail_run(&self, id: i64, error: Value) -> Result<RunRecord> {
        self.after_write(self.sqlite.workflow_runs().fail_run(id, error).await)
            .await
    }

    async fn get_by_message_id(&self, message_id: i64) -> Result<RunRecord> {
        self.sqlite
            .workflow_runs()
            .get_by_message_id(message_id)
            .await
    }
}

#[derive(Clone)]
pub struct S3StepRecordTable {
    sqlite: SqliteStore,
    gate: S3MutationGate,
}

impl S3StepRecordTable {
    pub fn new(sqlite: SqliteStore, gate: S3MutationGate) -> Self {
        Self { sqlite, gate }
    }

    async fn after_write<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        self.gate.commit().await?;
        Ok(value)
    }
}

#[async_trait]
impl StepRecordTable for S3StepRecordTable {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        self.after_write(self.sqlite.workflow_steps().insert(data).await)
            .await
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        self.sqlite.workflow_steps().get(id).await
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        self.sqlite.workflow_steps().list().await
    }

    async fn count(&self) -> Result<i64> {
        self.sqlite.workflow_steps().count().await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.after_write(self.sqlite.workflow_steps().delete(id).await)
            .await
    }

    async fn execute(&self, query: crate::store::query::QueryBuilder) -> Result<StepRecord> {
        self.after_write(self.sqlite.workflow_steps().execute(query).await)
            .await
    }

    fn sql_acquire_step(&self) -> &'static str {
        self.sqlite.workflow_steps().sql_acquire_step()
    }

    fn sql_clear_retry(&self) -> &'static str {
        self.sqlite.workflow_steps().sql_clear_retry()
    }

    fn sql_complete_step(&self) -> &'static str {
        self.sqlite.workflow_steps().sql_complete_step()
    }

    fn sql_fail_step(&self) -> &'static str {
        self.sqlite.workflow_steps().sql_fail_step()
    }
}
