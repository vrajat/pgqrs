//! Tables builder for accessing store tables.

use crate::store::Store;

/// Start a tables builder.
///
/// ```rust,no_run
/// # use pgqrs::store::AnyStore;
/// # async fn example(store: AnyStore) -> pgqrs::error::Result<()> {
/// let workers = pgqrs::tables(&store).workers().list().await?;
/// # Ok(()) }
/// ```
pub fn tables<S: Store>(store: &S) -> TablesBuilder<'_, S> {
    TablesBuilder::new(store)
}

/// Builder for accessing store tables.
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

    /// Access workflow table operations
    pub fn workflows(self) -> &'a dyn crate::store::WorkflowTable {
        self.store.workflows()
    }

    /// Access workflow run table operations
    pub fn workflow_runs(self) -> &'a dyn crate::store::RunRecordTable {
        self.store.workflow_runs()
    }
}
