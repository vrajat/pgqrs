use crate::error::Result;
use crate::store::turso::tables::queues::TursoQueueTable;
use crate::store::turso::tables::workers::TursoWorkerTable;
use crate::store::{Admin, QueueTable, Worker, WorkerTable};
use crate::types::{
    QueueInfo, QueueMessage, QueueMetrics, SystemStats, WorkerHealthStats, WorkerInfo, WorkerStats,
    WorkerStatus,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;
use turso::Database;

#[derive(Debug)]
pub struct TursoAdmin {
    worker_id: i64,
    db: Arc<Database>,
}

impl TursoAdmin {
    pub fn new(db: Arc<Database>, worker_id: i64) -> Self {
        Self { worker_id, db }
    }
}

// ... Implement Worker trait ...
#[async_trait]
impl Worker for TursoAdmin {
    fn worker_id(&self) -> i64 {
        self.worker_id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.get_status(self.worker_id).await
    }

    async fn suspend(&self) -> Result<()> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.suspend(self.worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.resume(self.worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.shutdown(self.worker_id).await
    }

    async fn heartbeat(&self) -> Result<()> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.heartbeat(self.worker_id).await
    }

    async fn is_healthy(&self, max_age: Duration) -> Result<bool> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.is_healthy(self.worker_id, max_age).await
    }
}

#[async_trait]
impl Admin for TursoAdmin {
    async fn install(&self) -> Result<()> {
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        let schema = crate::store::turso::schema::SCHEMA;

        for stmt in schema {
            conn.execute(stmt, ())
                .await
                .map_err(|e| crate::error::Error::TursoQueryFailed {
                    query: "Schema Init".into(),
                    context: "Installing schema".into(),
                    source: e,
                })?;
        }

        Ok(())
    }

    async fn verify(&self) -> Result<()> {
        // Simplified check: try to select from queues
        crate::store::turso::query("SELECT 1 FROM pgqrs_queues LIMIT 1")
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn register(&mut self, hostname: String, port: i32) -> Result<WorkerInfo> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        let worker = worker_table.register(None, &hostname, port).await?;
        self.worker_id = worker.id;
        Ok(worker)
    }

    async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
        let queue_table = TursoQueueTable::new(self.db.clone());
        queue_table
            .insert(crate::types::NewQueue {
                queue_name: name.to_string(),
            })
            .await
    }

    async fn get_queue(&self, name: &str) -> Result<QueueInfo> {
        let queue_table = TursoQueueTable::new(self.db.clone());
        queue_table.get_by_name(name).await
    }

    async fn delete_queue(&self, queue_info: &QueueInfo) -> Result<()> {
        let queue_table = TursoQueueTable::new(self.db.clone());
        queue_table.delete(queue_info.id).await.map(|_| ())
    }

    async fn purge_queue(&self, name: &str) -> Result<()> {
        let queue_table = TursoQueueTable::new(self.db.clone());
        let queue = queue_table.get_by_name(name).await?;

        // Delete messages, workers, archive logic would be here.
        // For now simpler impl:
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("DELETE FROM pgqrs_messages WHERE queue_id = ?", (queue.id,))
            .await
            .map_err(|e| crate::error::Error::TursoQueryFailed {
                query: "DELETE messages".into(),
                source: e,
                context: "Purge queue".into(),
            })?;
        conn.execute("DELETE FROM pgqrs_workers WHERE queue_id = ?", (queue.id,))
            .await
            .map_err(|e| crate::error::Error::TursoQueryFailed {
                query: "DELETE workers".into(),
                source: e,
                context: "Purge queue".into(),
            })?;

        Ok(())
    }

    async fn dlq(&self) -> Result<Vec<i64>> {
        // Not fully implemented in Turso backend logic yet (need max_attempts logic in query)
        // Assuming this returns list of IDs that are "dead" or moved to DLQ (which is archive with high read_ct sometimes?)
        // This is ambiguous in current spec without dlq table.
        // Assuming using archive table with read_ct logic.
        Ok(vec![])
    }

    async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        // Implement proper metrics query
        let queue_table = TursoQueueTable::new(self.db.clone());
        let queue = queue_table.get_by_name(name).await?;

        let pending: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1 AND (vt IS NULL OR vt <= datetime('now'))")
            .bind(queue.id)
            .fetch_one(&self.db).await?;

        Ok(QueueMetrics {
            name: name.to_string(),
            total_messages: 0, // TODO
            pending_messages: pending,
            locked_messages: 0,
            archived_messages: 0,
            oldest_pending_message: None, // TODO
            newest_message: None,         // TODO
        })
    }

    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        Ok(vec![]) // Todo
    }

    async fn system_stats(&self) -> Result<SystemStats> {
        Ok(SystemStats {
            total_queues: 0,
            total_workers: 0,
            active_workers: 0,
            total_messages: 0,
            pending_messages: 0,
            locked_messages: 0,
            archived_messages: 0,
            schema_version: "1".into(),
        }) // Todo
    }

    async fn worker_health_stats(
        &self,
        _heartbeat_timeout: Duration,
        _group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        Ok(vec![]) // Todo
    }

    async fn worker_stats(&self, _queue_name: &str) -> Result<WorkerStats> {
        Ok(WorkerStats {
            // queue_name was removed from struct? Or not present.
            total_workers: 0,
            ready_workers: 0,
            suspended_workers: 0,
            stopped_workers: 0,
            average_messages_per_worker: 0.0,
            oldest_worker_age: chrono::Duration::seconds(0),
            newest_heartbeat_age: chrono::Duration::seconds(0),
        })
    }

    async fn delete_worker(&self, worker_id: i64) -> Result<u64> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.delete(worker_id).await
    }

    async fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        let worker_table = TursoWorkerTable::new(self.db.clone());
        worker_table.list().await
    }

    async fn get_worker_messages(&self, _worker_id: i64) -> Result<Vec<QueueMessage>> {
        // Implement using messages table
        Ok(vec![])
    }

    async fn reclaim_messages(&self, _queue_id: i64, _older_than: Option<Duration>) -> Result<u64> {
        Ok(0)
    }

    async fn purge_old_workers(&self, _older_than: chrono::Duration) -> Result<u64> {
        Ok(0)
    }

    async fn release_worker_messages(&self, _worker_id: i64) -> Result<u64> {
        Ok(0)
    }
}
