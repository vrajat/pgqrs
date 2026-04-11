use crate::error::Result;
use crate::store::dialect::SqlDialect;
use crate::store::query::QueryBuilder;
use crate::types::WorkerStatus;
use async_trait::async_trait;
use chrono::Utc;

#[async_trait]
pub(crate) trait DialectWorkerTable: crate::store::WorkerTable + Sync {
    type Dialect: SqlDialect;

    async fn execute_worker_update(&self, query: QueryBuilder) -> Result<u64>;
    async fn query_worker_status(&self, worker_id: i64) -> Result<WorkerStatus>;

    async fn ensure_shutdown_allowed(&self, _worker_id: i64) -> Result<()> {
        Ok(())
    }

    async fn dialect_heartbeat(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();

        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.heartbeat)
                .bind_datetime(Some(now))
                .bind_i64(worker_id),
            worker_id,
            None,
        )
        .await
    }

    async fn dialect_suspend(&self, worker_id: i64) -> Result<()> {
        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.suspend).bind_i64(worker_id),
            worker_id,
            Some((
                "suspended",
                "Worker must be Ready, Polling, or Interrupted to suspend",
            )),
        )
        .await
    }

    async fn dialect_complete_poll(&self, worker_id: i64) -> Result<()> {
        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.complete_poll).bind_i64(worker_id),
            worker_id,
            Some((
                "ready",
                "Worker must be in Polling state to complete polling",
            )),
        )
        .await
    }

    async fn dialect_resume(&self, worker_id: i64) -> Result<()> {
        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.resume).bind_i64(worker_id),
            worker_id,
            Some(("ready", "Worker must be in Suspended state to resume")),
        )
        .await
    }

    async fn dialect_poll(&self, worker_id: i64) -> Result<()> {
        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.poll).bind_i64(worker_id),
            worker_id,
            Some(("polling", "Worker must be Ready to start polling")),
        )
        .await
    }

    async fn dialect_interrupt(&self, worker_id: i64) -> Result<()> {
        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.interrupt).bind_i64(worker_id),
            worker_id,
            Some((
                "interrupted",
                "Worker must be in Polling state to be interrupted",
            )),
        )
        .await
    }

    async fn dialect_shutdown(&self, worker_id: i64) -> Result<()> {
        self.ensure_shutdown_allowed(worker_id).await?;
        let now = Utc::now();

        self.ensure_transition(
            QueryBuilder::new(Self::Dialect::WORKER.shutdown)
                .bind_i64(worker_id)
                .bind_datetime(Some(now)),
            worker_id,
            Some(("stopped", "Worker must be in Suspended state to shutdown")),
        )
        .await
    }

    async fn ensure_transition(
        &self,
        query: QueryBuilder,
        worker_id: i64,
        invalid_transition: Option<(&'static str, &'static str)>,
    ) -> Result<()> {
        let count = self.execute_worker_update(query).await?;

        if count > 0 {
            return Ok(());
        }

        if let Some((to, reason)) = invalid_transition {
            let current_status = self.query_worker_status(worker_id).await?;
            return Err(crate::error::Error::InvalidStateTransition {
                from: current_status.to_string(),
                to: to.to_string(),
                reason: reason.to_string(),
            });
        }

        Err(crate::error::Error::WorkerNotFound { id: worker_id })
    }
}
