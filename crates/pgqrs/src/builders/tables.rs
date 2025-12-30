//! TablesBuilder for accessing store tables through a high-level API

use crate::store::Store;

/// Builder for accessing store tables.
///
/// Provides a fluent API for table operations:
/// - `messages()` - Access message table operations
/// - `queues()` - Access queue table operations
/// - `workers()` - Access worker table operations
/// - `archive()` - Access archive table operations
/// - `workflows()` - Access workflow table operations
///
/// # Example
/// ```no_run
/// # use pgqrs::store::AnyStore;
/// # async fn example(store: AnyStore) -> pgqrs::error::Result<()> {
/// // Count pending messages
/// # let queue_id = 1;
/// let count = pgqrs::tables(&store)
///     .messages()
///     .count_pending(queue_id)
///     .await?;
///
/// // List all workers
/// let workers = pgqrs::tables(&store)
///     .workers()
///     .list()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct TablesBuilder<'a, S: Store> {
    store: &'a S,
}

impl<'a, S: Store> TablesBuilder<'a, S> {
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    /// Access message table operations
    pub fn messages(self) -> &'a dyn crate::store::MessageTable {
        self.store.messages()
    }

    /// Access queue table operations
    pub fn queues(self) -> &'a dyn crate::store::QueueTable {
        self.store.queues()
    }

    /// Access worker table operations
    pub fn workers(self) -> &'a dyn crate::store::WorkerTable {
        self.store.workers()
    }

    /// Access archive table operations
    pub fn archive(self) -> &'a dyn crate::store::ArchiveTable {
        self.store.archive()
    }

    /// Access workflow table operations
    pub fn workflows(self) -> &'a dyn crate::store::WorkflowTable {
        self.store.workflows()
    }
}

/// Create a TablesBuilder
pub fn tables<S: Store>(store: &S) -> TablesBuilder<'_, S> {
    TablesBuilder::new(store)
}
