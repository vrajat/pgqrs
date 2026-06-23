//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait for the Postgres-backed runtime.

use crate::Config;
use async_trait::async_trait;

/// Concurrency model supported by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyModel {
    /// Backend supports multiple processes accessing the store concurrently.
    MultiProcess,
    /// Backend supports only a single process accessing the store.
    SingleProcess,
}

pub mod any;
pub mod dblock;
pub(crate) mod dialect;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tables;

pub use crate::tables::*;
pub use crate::workers::*;

pub use any::AnyStore;
pub use dblock::{DbLock, DbOpFuture, DbTables, SerializedLock, Tables};

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    type Workers: WorkerTable + ?Sized;

    /// Execute raw SQL without parameters.
    async fn execute_raw(&self, sql: &str) -> crate::error::Result<()>;

    /// Execute raw SQL with a single i64 parameter.
    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> crate::error::Result<()>;

    /// Execute raw SQL with two i64 parameters.
    async fn execute_raw_with_two_i64(
        &self,
        sql: &str,
        param1: i64,
        param2: i64,
    ) -> crate::error::Result<()>;

    /// Query a single i64 value using raw SQL.
    async fn query_int(&self, sql: &str) -> crate::error::Result<i64>;

    /// Query a single string value using raw SQL.
    async fn query_string(&self, sql: &str) -> crate::error::Result<String>;

    /// Query a single boolean value using raw SQL.
    async fn query_bool(&self, sql: &str) -> crate::error::Result<bool>;

    /// Get the configuration for this store
    fn config(&self) -> &Config;

    /// Get access to the repositories.
    fn queues(&self) -> &dyn QueueTable;
    fn messages(&self) -> &dyn MessageTable;
    fn workers(&self) -> &Self::Workers;
    fn db_state(&self) -> &dyn DbStateTable;
    fn workflows(&self) -> &dyn WorkflowTable;
    fn workflow_runs(&self) -> &dyn RunRecordTable;
    fn workflow_steps(&self) -> &dyn StepRecordTable;

    /// Initialize the pgqrs schema in the database.
    async fn bootstrap(&self) -> crate::error::Result<()>;

    /// Get an admin worker interface.
    async fn admin(&self, name: &str) -> crate::error::Result<crate::workers::Admin>;

    /// Get an ephemeral admin worker interface.
    async fn admin_ephemeral(&self) -> crate::error::Result<crate::workers::Admin>;

    /// Get a producer interface for a specific queue with worker identity.
    async fn producer(
        &self,
        queue: &str,
        name: &str,
        config: &Config,
    ) -> crate::error::Result<Producer>;

    /// Get a consumer interface for a specific queue with worker identity.
    async fn consumer(&self, queue: &str, name: &str) -> crate::error::Result<Consumer>;

    /// Create a new queue.
    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord>;

    /// Get a workflow definition handle.
    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord>;

    /// Create a local run handle from a message.
    ///
    /// This should parse the message payload and either create a new RunRecord
    /// (for new triggers) or fetch an existing one (for resumptions).
    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<Run>;

    /// Returns the concurrency model supported by this backend.
    fn concurrency_model(&self) -> ConcurrencyModel;

    /// Returns the backend name.
    fn backend_name(&self) -> &'static str;

    /// Create an ephemeral producer (auto-cleanup).
    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Producer>;

    /// Create an ephemeral consumer (auto-cleanup).
    async fn consumer_ephemeral(&self, queue: &str) -> crate::error::Result<Consumer>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Postgres,
}

impl BackendType {
    const POSTGRES_PREFIXES: &'static [&'static str] =
        &["postgres://", "postgresql://", "postgres", "pg"];

    pub fn detect(dsn: &str) -> crate::error::Result<Self> {
        if Self::POSTGRES_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            return Ok(Self::Postgres);
        }
        Err(crate::error::Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Unsupported DSN format: {}", dsn),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BackendType;

    #[test]
    fn detect_rejects_unsupported_dsn() {
        let err = BackendType::detect("invalid://dsn").unwrap_err();
        assert!(err.to_string().contains("Unsupported DSN format"));
    }
}
