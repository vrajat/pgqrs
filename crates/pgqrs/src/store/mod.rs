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
pub(crate) mod query;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "turso")]
pub mod turso;

pub use crate::tables::*;
pub use crate::workers::*;

pub use any::AnyStore;

// S3 store uses SQLite locally, so sqlite_utils are needed for `s3` too.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "s3"))]
pub(crate) mod sqlite_utils {
    use crate::error::{Error, Result};
    use chrono::{DateTime, NaiveDateTime, Utc};

    /// Parse SQLite/Turso TEXT timestamp to DateTime<Utc>
    pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
        const TIMESTAMP_FORMATS: [&str; 2] = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"];

        TIMESTAMP_FORMATS
            .iter()
            .find_map(|fmt| NaiveDateTime::parse_from_str(s, fmt).ok())
            .map(|dt| dt.and_utc())
            .ok_or_else(|| Error::Internal {
                message: format!("Invalid timestamp: {s}"),
            })
    }

    /// Format DateTime<Utc> for SQLite/Turso TEXT storage
    pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
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
    fn workflows(&self) -> &dyn WorkflowTable;
    fn workflow_runs(&self) -> &dyn RunRecordTable;
    fn workflow_steps(&self) -> &dyn StepRecordTable;

    /// Initialize the pgqrs schema in the database.
    async fn bootstrap(&self) -> crate::error::Result<()>;

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
    ) -> crate::error::Result<Producer>;

    /// Get a consumer interface for a specific queue with worker identity.
    async fn consumer(
        &self,
        queue: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> crate::error::Result<Consumer>;

    /// Create a new queue.
    async fn queue(&self, name: &str) -> crate::error::Result<crate::types::QueueRecord>;

    /// Get a workflow definition handle.
    async fn workflow(&self, name: &str) -> crate::error::Result<crate::types::WorkflowRecord>;

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
    ) -> crate::error::Result<Producer>;

    /// Create an ephemeral consumer (NULL hostname/port, auto-cleanup).
    async fn consumer_ephemeral(
        &self,
        queue: &str,
        config: &Config,
    ) -> crate::error::Result<Consumer>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "s3")]
    S3,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl BackendType {
    const POSTGRES_PREFIXES: &'static [&'static str] =
        &["postgres://", "postgresql://", "postgres", "pg"];
    const SQLITE_PREFIXES: &'static [&'static str] = &["sqlite://", "sqlite:", "sqlite"];
    #[cfg(feature = "s3")]
    const S3_PREFIXES: &'static [&'static str] = &["s3://", "s3:", "s3"];
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
        #[cfg(feature = "s3")]
        if Self::S3_PREFIXES.iter().any(|p| dsn.starts_with(p)) {
            return Ok(Self::S3);
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

#[cfg(test)]
mod tests {
    use super::BackendType;

    #[test]
    fn detect_rejects_unsupported_dsn() {
        let err = BackendType::detect("invalid://dsn").unwrap_err();
        assert!(err.to_string().contains("Unsupported DSN format"));
    }

    #[cfg(feature = "s3")]
    #[test]
    fn detect_s3_dsn_returns_s3_backend() {
        assert_eq!(
            BackendType::detect("s3://bucket/queue.sqlite").unwrap(),
            BackendType::S3
        );
        assert_eq!(BackendType::detect("s3:").unwrap(), BackendType::S3);
        assert_eq!(BackendType::detect("s3").unwrap(), BackendType::S3);
    }
}
