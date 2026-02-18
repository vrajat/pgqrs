//! Core database abstraction for pgqrs.
//!
//! This module defines the [`Store`] trait which enables pgqrs to support
//! multiple database backends (Postgres, SQLite, Turso).

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
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "turso")]
pub mod turso;

pub use crate::tables::*;
pub use crate::workers::*;

pub use any::AnyStore;

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) mod sqlite_utils {
    use crate::error::{Error, Result};
    use chrono::{DateTime, Utc};

    /// Parse SQLite/Turso TEXT timestamp to DateTime<Utc>
    pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
        // SQLite datetime() returns "YYYY-MM-DD HH:MM:SS" format
        // We append +0000 to parse it as UTC
        DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| Error::Internal {
                message: format!("Invalid timestamp: {}", e),
            })
    }

    /// Format DateTime<Utc> for SQLite/Turso TEXT storage
    pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

/// Main store trait that provides access to entity-specific repositories
/// and transaction management.
#[async_trait]
pub trait Store: Send + Sync + 'static {
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
    fn workers(&self) -> &dyn WorkerTable;
    fn archive(&self) -> &dyn ArchiveTable;
    fn workflows(&self) -> &dyn WorkflowTable;
    fn workflow_runs(&self) -> &dyn RunRecordTable;
    fn workflow_steps(&self) -> &dyn StepRecordTable;

    /// Attempt to acquire a step lock.
    async fn acquire_step(
        &self,
        run_id: i64,
        step_name: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<crate::types::StepRecord>;

    /// Initialize the pgqrs schema in the database.
    async fn bootstrap(&self) -> crate::error::Result<()>;

    /// Create a step guard for manual management.
    fn step_guard(&self, id: i64) -> Box<dyn StepGuard>;

    /// Get an admin worker interface.
    async fn admin(
        &self,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Admin>>;

    /// Get an ephemeral admin worker interface.
    async fn admin_ephemeral(&self, config: &Config) -> crate::error::Result<Box<dyn Admin>>;

    /// Get a producer interface for a specific queue with worker identity.
    async fn producer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>>;

    /// Get a consumer interface for a specific queue with worker identity.
    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>>;

    /// Create a new queue.
    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord>;

    /// Get a workflow definition handle.
    async fn workflow(&self, name: &str) -> crate::error::Result<Box<dyn Workflow>>;

    /// Trigger a workflow run.
    ///
    /// This enqueues a message with the workflow input. The run record is created
    /// when the worker starts processing the message.
    async fn trigger(
        &self,
        name: &str,
        input: Option<serde_json::Value>,
    ) -> crate::error::Result<crate::types::QueueMessage>;

    /// Create a local run handle from a message.
    ///
    /// This should parse the message payload and either create a new RunRecord
    /// (for new triggers) or fetch an existing one (for resumptions).
    async fn run(&self, message: crate::types::QueueMessage) -> crate::error::Result<Run>;

    /// Get a generic worker handle by ID.
    async fn worker(&self, id: i64) -> crate::error::Result<Box<dyn Worker>>;

    /// Returns the concurrency model supported by this backend.
    fn concurrency_model(&self) -> ConcurrencyModel;

    /// Returns the backend name (e.g., "postgres", "sqlite", "turso")
    fn backend_name(&self) -> &'static str;

    /// Create an ephemeral producer (NULL hostname/port, auto-cleanup).
    async fn producer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Producer>>;

    /// Create an ephemeral consumer (NULL hostname/port, auto-cleanup).
    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Box<dyn Consumer>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl BackendType {
    const POSTGRES_PREFIXES: &'static [&'static str] =
        &["postgres://", "postgresql://", "postgres", "pg"];
    const SQLITE_PREFIXES: &'static [&'static str] = &["sqlite://", "sqlite:", "sqlite"];
    const TURSO_PREFIXES: &'static [&'static str] = &["turso://", "turso:", "turso"];

    pub fn detect(dsn: &str) -> crate::error::Result<Self> {
        if Self::POSTGRES_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "postgres")]
            return Ok(Self::Postgres);
            #[cfg(not(feature = "postgres"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Postgres backend is not enabled".to_string(),
            });
        }
        if Self::SQLITE_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "sqlite")]
            return Ok(Self::Sqlite);
            #[cfg(not(feature = "sqlite"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Sqlite backend is not enabled".to_string(),
            });
        }
        if Self::TURSO_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            #[cfg(feature = "turso")]
            return Ok(Self::Turso);
            #[cfg(not(feature = "turso"))]
            return Err(crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Turso backend is not enabled".to_string(),
            });
        }
        Err(crate::error::Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Unsupported DSN format: {}", dsn),
        })
    }
}
