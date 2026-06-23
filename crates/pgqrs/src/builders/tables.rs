//! Tables builder for accessing store tables.

use crate::store::Store;

/// Start a tables builder.
///
/// ```rust,no_run
/// # use pgqrs::store::Store;
/// # async fn example(store: Store) -> pgqrs::error::Result<()> {
/// let workers = pgqrs::tables(&store).workers().list().await?;
/// # Ok(()) }
/// ```
pub fn tables(store: &Store) -> TablesBuilder<'_> {
    TablesBuilder::new(store)
}

/// Builder for accessing store tables.
pub struct TablesBuilder<'a> {
    store: &'a Store,
}

impl<'a> TablesBuilder<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Access message table operations
    pub fn messages(self) -> &'a crate::store::Messages {
        self.store.messages()
    }

    /// Access queue table operations
    pub fn queues(self) -> &'a crate::store::Queues {
        self.store.queues()
    }

    /// Access worker table operations
    pub fn workers(self) -> &'a crate::store::Workers {
        self.store.workers()
    }

    /// Access workflow table operations
    pub fn workflows(self) -> &'a crate::store::Workflows {
        self.store.workflows()
    }

    /// Access workflow run table operations
    pub fn workflow_runs(self) -> &'a crate::store::RunRecords {
        self.store.workflow_runs()
    }
}
