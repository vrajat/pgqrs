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
    /// Create a new TablesBuilder with the given store
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    /// Access the messages table
    pub fn messages(&self) -> &dyn crate::store::MessageTable {
        self.store.messages()
    }

    /// Access the queues table
    pub fn queues(&self) -> &dyn crate::store::QueueTable {
        self.store.queues()
    }

    /// Access the workers table
    pub fn workers(&self) -> &dyn crate::store::WorkerTable {
        self.store.workers()
    }

    /// Access the archive table
    pub fn archive(&self) -> &dyn crate::store::ArchiveTable {
        self.store.archive()
    }

    /// Access the workflows table
    pub fn workflows(&self) -> &dyn crate::store::WorkflowTable {
        self.store.workflows()
    }
}

/// Create a tables builder for accessing store tables
///
/// # Example
/// ```no_run
/// # use pgqrs::store::AnyStore;
/// # async fn example(store: AnyStore, queue_id: i64) -> pgqrs::error::Result<()> {
/// // Count pending messages
/// let count = pgqrs::tables(&store)
///     .messages()
///     .count_pending(queue_id)
///     .await?;
///
/// // List all queues
/// let queues = pgqrs::tables(&store)
///     .queues()
///     .list()
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
pub fn tables<S: Store>(store: &S) -> TablesBuilder<'_, S> {
    TablesBuilder::new(store)
}
