//! AdminBuilder for admin operations

use crate::error::Result;
use crate::store::Store;
use crate::types::WorkerInfo;

/// Builder for admin operations.
///
/// Provides a fluent API for administrative tasks:
/// - Queue management: create_queue, delete_queue, purge_queue
/// - Worker management: delete_worker
/// - System operations: install, verify
pub struct AdminBuilder<'a, S: Store> {
    store: &'a S,
}

impl<'a, S: Store> AdminBuilder<'a, S> {
    /// Create a new AdminBuilder with the given store
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    /// Get access to the underlying store
    pub fn store(&self) -> &S {
        self.store
    }

    /// Install the pgqrs schema
    pub async fn install(self) -> Result<()> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.install().await
    }

    /// Verify the pgqrs installation
    pub async fn verify(self) -> Result<()> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.verify().await
    }

    /// Create a new queue
    pub async fn create_queue(self, name: &str) -> crate::error::Result<crate::types::QueueInfo> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.create_queue(name).await
    }

    pub async fn get_queue(self, name: &str) -> crate::error::Result<crate::types::QueueInfo> {
        // Use the store's queue table directly as it's the standard way to get queue info
        self.store.queues().get_by_name(name).await
    }

    /// Delete a queue
    pub async fn delete_queue(
        self,
        queue_info: &crate::types::QueueInfo,
    ) -> crate::error::Result<()> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.delete_queue(queue_info).await
    }

    /// Delete a queue by name
    pub async fn delete_queue_by_name(self, name: &str) -> crate::error::Result<()> {
        let queue_info = self.store.queues().get_by_name(name).await?;
        self.delete_queue(&queue_info).await
    }

    /// Purge all messages from a queue
    pub async fn purge_queue(self, queue_name: &str) -> Result<()> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.purge_queue(queue_name).await
    }

    /// Delete a worker
    pub async fn delete_worker(self, worker_id: i64) -> Result<u64> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.delete_worker(worker_id).await
    }

    /// List all workers
    pub async fn list_workers(self) -> Result<Vec<WorkerInfo>> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.list_workers().await
    }

    /// Move messages to dead letter queue
    pub async fn dlq(self) -> Result<Vec<i64>> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.dlq().await
    }

    /// Get messages held by a specific worker
    pub async fn get_worker_messages(
        self,
        worker_id: i64,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.get_worker_messages(worker_id).await
    }

    /// Reclaim messages from zombie consumers
    pub async fn reclaim_messages(
        self,
        queue_id: i64,
        older_than: Option<chrono::Duration>,
    ) -> Result<u64> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.reclaim_messages(queue_id, older_than).await
    }

    /// Get worker statistics for a queue
    pub async fn worker_stats(self, queue_name: &str) -> Result<crate::types::WorkerStats> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.worker_stats(queue_name).await
    }

    /// Purge old workers
    pub async fn purge_old_workers(self, older_than: chrono::Duration) -> Result<u64> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.purge_old_workers(older_than).await
    }

    /// Get metrics for a specific queue
    pub async fn queue_metrics(self, queue_name: &str) -> Result<crate::types::QueueMetrics> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.queue_metrics(queue_name).await
    }

    /// Get metrics for all queues
    pub async fn all_queues_metrics(self) -> Result<Vec<crate::types::QueueMetrics>> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.all_queues_metrics().await
    }

    /// Get system stats
    pub async fn system_stats(self) -> Result<crate::types::SystemStats> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.system_stats().await
    }

    /// Get worker health stats
    pub async fn worker_health_stats(
        self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<crate::types::WorkerHealthStats>> {
        let admin = self.store.admin(self.store.config()).await?;
        admin
            .worker_health_stats(heartbeat_timeout, group_by_queue)
            .await
    }

    /// Release messages locked by a worker
    pub async fn release_worker_messages(self, worker_id: i64) -> Result<u64> {
        let admin = self.store.admin(self.store.config()).await?;
        admin.release_worker_messages(worker_id).await
    }
}

/// Create an admin builder for administrative operations
///
/// # Example
/// ```no_run
/// # use pgqrs::store::AnyStore;
/// # async fn example(store: AnyStore) -> pgqrs::error::Result<()> {
/// // Create a queue
/// let queue = pgqrs::admin(&store)
///     .create_queue("my_queue")
///     .await?;
///
/// // Purge a queue
/// pgqrs::admin(&store)
///     .purge_queue("my_queue")
///     .await?;
///
/// // Delete a queue
/// pgqrs::admin(&store)
///     .delete_queue(&queue)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn admin<S: Store>(store: &S) -> AdminBuilder<'_, S> {
    AdminBuilder::new(store)
}
