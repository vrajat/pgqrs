use crate::error::Result;
use crate::store::s3::SyncDb;
use crate::store::{
    DbStateTable, MessageTable, QueueTable, RunRecordTable, StepRecordTable, WorkerTable,
    WorkflowTable,
};
use crate::types::{
    BatchInsertParams, NewQueueMessage, NewQueueRecord, NewRunRecord, NewStepRecord,
    NewWorkerRecord, NewWorkflowRecord, QueueMessage, QueueRecord, RunRecord, StepRecord,
    WorkerRecord, WorkerStatus, WorkflowRecord,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

/// Basic table facade: direct delegation to read/write stores.
#[derive(Clone)]
pub struct Tables<DB>
where
    DB: SyncDb,
{
    db: DB,
}

impl<DB> Tables<DB>
where
    DB: SyncDb,
{
    pub fn new(db: DB) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn db_mut(&mut self) -> &mut DB {
        &mut self.db
    }
}

#[async_trait]
impl<DB> MessageTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewQueueMessage) -> Result<QueueMessage> {
        self.db
            .with_write(|store| Box::pin(async move { store.messages().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        self.db
            .with_read(|store| Box::pin(async move { store.messages().get(id).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        self.db
            .with_read(|store| Box::pin(async move { store.messages().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.messages().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.messages().delete(id).await }))
            .await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().filter_by_fk(queue_id).await })
            })
            .await
    }

    async fn list_by_consumer_worker(&self, worker_id: i64) -> Result<Vec<QueueMessage>> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().list_by_consumer_worker(worker_id).await })
            })
            .await
    }

    async fn count_by_consumer_worker(&self, worker_id: i64) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().count_by_consumer_worker(worker_id).await })
            })
            .await
    }

    async fn count_worker_references(&self, worker_id: i64) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().count_worker_references(worker_id).await })
            })
            .await
    }

    async fn move_to_dlq(&self, max_read_ct: i32) -> Result<Vec<i64>> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().move_to_dlq(max_read_ct).await })
            })
            .await
    }

    async fn release_by_consumer_worker(&self, worker_id: i64) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(
                    async move { store.messages().release_by_consumer_worker(worker_id).await },
                )
            })
            .await
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[Value],
        params: BatchInsertParams,
    ) -> Result<Vec<i64>> {
        let payloads = payloads.to_vec();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .batch_insert(queue_id, &payloads, params)
                        .await
                })
            })
            .await
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        let ids = ids.to_vec();
        self.db
            .with_read(|store| Box::pin(async move { store.messages().get_by_ids(&ids).await }))
            .await
    }

    async fn update_payload(&self, id: i64, payload: Value) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().update_payload(id, payload).await })
            })
            .await
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .extend_visibility(id, worker_id, additional_seconds)
                        .await
                })
            })
            .await
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        let message_ids = message_ids.to_vec();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .extend_visibility_batch(&message_ids, worker_id, additional_seconds)
                        .await
                })
            })
            .await
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        let message_ids = message_ids.to_vec();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .release_messages_by_ids(&message_ids, worker_id)
                        .await
                })
            })
            .await
    }

    async fn release_with_visibility(
        &self,
        id: i64,
        worker_id: i64,
        vt: DateTime<Utc>,
    ) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .release_with_visibility(id, worker_id, vt)
                        .await
                })
            })
            .await
    }

    async fn count_pending_for_queue(&self, queue_id: i64) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().count_pending_for_queue(queue_id).await })
            })
            .await
    }

    async fn count_pending_for_queue_and_worker(
        &self,
        queue_id: i64,
        worker_id: i64,
    ) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .count_pending_for_queue_and_worker(queue_id, worker_id)
                        .await
                })
            })
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
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .messages()
                        .dequeue_at(queue_id, limit, vt, worker_id, now, max_read_ct)
                        .await
                })
            })
            .await
    }

    async fn archive(&self, id: i64, worker_id: i64) -> Result<Option<QueueMessage>> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().archive(id, worker_id).await })
            })
            .await
    }

    async fn archive_many(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        let ids = ids.to_vec();
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().archive_many(&ids, worker_id).await })
            })
            .await
    }

    async fn replay_dlq(&self, id: i64) -> Result<Option<QueueMessage>> {
        self.db
            .with_write(|store| Box::pin(async move { store.messages().replay_dlq(id).await }))
            .await
    }

    async fn delete_owned(&self, id: i64, worker_id: i64) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().delete_owned(id, worker_id).await })
            })
            .await
    }

    async fn delete_many_owned(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        let ids = ids.to_vec();
        self.db
            .with_write(|store| {
                Box::pin(async move { store.messages().delete_many_owned(&ids, worker_id).await })
            })
            .await
    }

    async fn list_archived_by_queue(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().list_archived_by_queue(queue_id).await })
            })
            .await
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.messages().count_by_fk(queue_id).await })
            })
            .await
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let ids = ids.to_vec();
        self.db
            .with_write(|store| Box::pin(async move { store.messages().delete_by_ids(&ids).await }))
            .await
    }
}

#[async_trait]
impl<DB> WorkerTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewWorkerRecord) -> Result<WorkerRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<WorkerRecord> {
        self.db
            .with_read(|store| Box::pin(async move { store.workers().get(id).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<WorkerRecord>> {
        self.db
            .with_read(|store| Box::pin(async move { store.workers().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.workers().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().delete(id).await }))
            .await
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<WorkerRecord>> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workers().filter_by_fk(queue_id).await })
            })
            .await
    }

    async fn count_by_fk(&self, queue_id: i64) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.workers().count_by_fk(queue_id).await }))
            .await
    }

    async fn mark_stopped(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().mark_stopped(id).await }))
            .await
    }

    async fn count_for_queue(&self, queue_id: i64, state: WorkerStatus) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workers().count_for_queue(queue_id, state).await })
            })
            .await
    }

    async fn count_zombies_for_queue(&self, queue_id: i64, older_than: Duration) -> Result<i64> {
        self.db
            .with_read(|store| {
                Box::pin(async move {
                    store
                        .workers()
                        .count_zombies_for_queue(queue_id, older_than)
                        .await
                })
            })
            .await
    }

    async fn list_for_queue(
        &self,
        queue_id: i64,
        state: WorkerStatus,
    ) -> Result<Vec<WorkerRecord>> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workers().list_for_queue(queue_id, state).await })
            })
            .await
    }

    async fn list_zombies_for_queue(
        &self,
        queue_id: i64,
        older_than: Duration,
    ) -> Result<Vec<WorkerRecord>> {
        self.db
            .with_read(|store| {
                Box::pin(async move {
                    store
                        .workers()
                        .list_zombies_for_queue(queue_id, older_than)
                        .await
                })
            })
            .await
    }

    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerRecord> {
        let hostname = hostname.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workers().register(queue_id, &hostname, port).await })
            })
            .await
    }

    async fn register_ephemeral(&self, queue_id: Option<i64>) -> Result<WorkerRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workers().register_ephemeral(queue_id).await })
            })
            .await
    }

    async fn get_status(&self, id: i64) -> Result<WorkerStatus> {
        self.db
            .with_read(|store| Box::pin(async move { store.workers().get_status(id).await }))
            .await
    }

    async fn suspend(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().suspend(id).await }))
            .await
    }

    async fn resume(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().resume(id).await }))
            .await
    }

    async fn shutdown(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().shutdown(id).await }))
            .await
    }

    async fn poll(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().poll(id).await }))
            .await
    }

    async fn interrupt(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().interrupt(id).await }))
            .await
    }

    async fn heartbeat(&self, id: i64) -> Result<()> {
        self.db
            .with_write(|store| Box::pin(async move { store.workers().heartbeat(id).await }))
            .await
    }

    async fn is_healthy(&self, id: i64, max_age: Duration) -> Result<bool> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workers().is_healthy(id, max_age).await })
            })
            .await
    }
}

#[async_trait]
impl<DB> DbStateTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn verify(&self) -> Result<()> {
        self.db
            .with_read(|store| Box::pin(async move { store.db_state().verify().await }))
            .await
    }

    async fn purge_queue(&self, queue_id: i64) -> Result<()> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.db_state().purge_queue(queue_id).await })
            })
            .await
    }

    async fn queue_metrics(&self, queue_id: i64) -> Result<crate::QueueMetrics> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.db_state().queue_metrics(queue_id).await })
            })
            .await
    }

    async fn all_queues_metrics(&self) -> Result<Vec<crate::QueueMetrics>> {
        self.db
            .with_read(|store| Box::pin(async move { store.db_state().all_queues_metrics().await }))
            .await
    }

    async fn system_stats(&self) -> Result<crate::SystemStats> {
        self.db
            .with_read(|store| Box::pin(async move { store.db_state().system_stats().await }))
            .await
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: Duration,
        group_by_queue: bool,
    ) -> Result<Vec<crate::WorkerHealthStats>> {
        self.db
            .with_read(|store| {
                Box::pin(async move {
                    store
                        .db_state()
                        .worker_health_stats(heartbeat_timeout, group_by_queue)
                        .await
                })
            })
            .await
    }

    async fn purge_old_workers(&self, older_than: Duration) -> Result<u64> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.db_state().purge_old_workers(older_than).await })
            })
            .await
    }
}

#[async_trait]
impl<DB> QueueTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewQueueRecord) -> Result<QueueRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.queues().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<QueueRecord> {
        self.db
            .with_read(|store| Box::pin(async move { store.queues().get(id).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<QueueRecord>> {
        self.db
            .with_read(|store| Box::pin(async move { store.queues().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.queues().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.queues().delete(id).await }))
            .await
    }

    async fn get_by_name(&self, name: &str) -> Result<QueueRecord> {
        let name = name.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.queues().get_by_name(&name).await }))
            .await
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.queues().exists(&name).await }))
            .await
    }

    async fn delete_by_name(&self, name: &str) -> Result<u64> {
        let name = name.to_string();
        self.db
            .with_write(|store| Box::pin(async move { store.queues().delete_by_name(&name).await }))
            .await
    }
}

#[async_trait]
impl<DB> WorkflowTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewWorkflowRecord) -> Result<WorkflowRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflows().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<WorkflowRecord> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflows().get(id).await }))
            .await
    }

    async fn get_by_name(&self, name: &str) -> Result<WorkflowRecord> {
        let name = name.to_string();
        self.db
            .with_read(|store| Box::pin(async move { store.workflows().get_by_name(&name).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<WorkflowRecord>> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflows().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflows().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflows().delete(id).await }))
            .await
    }
}

#[async_trait]
impl<DB> RunRecordTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewRunRecord) -> Result<RunRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflow_runs().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<RunRecord> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_runs().get(id).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<RunRecord>> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_runs().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_runs().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflow_runs().delete(id).await }))
            .await
    }

    async fn start_run(&self, id: i64) -> Result<RunRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflow_runs().start_run(id).await }))
            .await
    }

    async fn complete_run(&self, id: i64, output: Value) -> Result<RunRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workflow_runs().complete_run(id, output).await })
            })
            .await
    }

    async fn pause_run(
        &self,
        id: i64,
        message: String,
        resume_after: std::time::Duration,
    ) -> Result<RunRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .workflow_runs()
                        .pause_run(id, message, resume_after)
                        .await
                })
            })
            .await
    }

    async fn fail_run(&self, id: i64, error: Value) -> Result<RunRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workflow_runs().fail_run(id, error).await })
            })
            .await
    }

    async fn get_by_message_id(&self, message_id: i64) -> Result<RunRecord> {
        self.db
            .with_read(|store| {
                Box::pin(async move { store.workflow_runs().get_by_message_id(message_id).await })
            })
            .await
    }
}

#[async_trait]
impl<DB> StepRecordTable for Tables<DB>
where
    DB: SyncDb,
{
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflow_steps().insert(data).await }))
            .await
    }

    async fn get(&self, id: i64) -> Result<StepRecord> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_steps().get(id).await }))
            .await
    }

    async fn list(&self) -> Result<Vec<StepRecord>> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_steps().list().await }))
            .await
    }

    async fn count(&self) -> Result<i64> {
        self.db
            .with_read(|store| Box::pin(async move { store.workflow_steps().count().await }))
            .await
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        self.db
            .with_write(|store| Box::pin(async move { store.workflow_steps().delete(id).await }))
            .await
    }

    async fn acquire_step(&self, run_id: i64, step_name: &str) -> Result<StepRecord> {
        let step_name = step_name.to_string();
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .workflow_steps()
                        .acquire_step(run_id, &step_name)
                        .await
                })
            })
            .await
    }

    async fn clear_retry(&self, id: i64) -> Result<StepRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workflow_steps().clear_retry(id).await })
            })
            .await
    }

    async fn complete_step(&self, id: i64, output: Value) -> Result<StepRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workflow_steps().complete_step(id, output).await })
            })
            .await
    }

    async fn fail_step(
        &self,
        id: i64,
        error: Value,
        retry_at: Option<DateTime<Utc>>,
        retry_count: i32,
    ) -> Result<StepRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move {
                    store
                        .workflow_steps()
                        .fail_step(id, error, retry_at, retry_count)
                        .await
                })
            })
            .await
    }

    async fn execute(&self, query: crate::store::query::QueryBuilder) -> Result<StepRecord> {
        self.db
            .with_write(|store| {
                Box::pin(async move { store.workflow_steps().execute(query).await })
            })
            .await
    }
}
