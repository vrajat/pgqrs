//! Configuration types for pgqrs.
//!
//! This module defines the [`Config`] struct and related types for configuring pgqrs, including database connection, queue, and performance options.
//!
//! ## What
//!
//! - [`Config`] holds all settings for connecting to PostgreSQL and tuning queue behavior.
//! - [`QueueConfig`] and [`PerformanceConfig`] provide granular control over queue and performance parameters.
//!
//! ## How
//!
//! Create a [`Config`] using defaults or customize fields as needed. Pass it to pgqrs APIs to initialize the system.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::config::Config;
//!
//! let config = Config {
//!     dsn: "postgresql://user:pass@localhost/db".to_string(),
//!     max_connections: 32,
//!     ..Config::default()
//! };
//! ```
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Configuration for pgqrs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// PostgreSQL connection string (DSN)
    pub dsn: String,
    /// Maximum number of database connections in the pool
    pub max_connections: u32,
    /// Timeout (seconds) for acquiring a database connection
    pub connection_timeout_seconds: u64,
    /// Queue-specific configuration options
    pub queue: QueueConfig,
    /// Performance tuning configuration options
    pub performance: PerformanceConfig,
}

/// Queue-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Default lock time (seconds) for jobs fetched from the queue
    pub default_lock_time_seconds: u32,
    /// Maximum number of jobs to fetch in a single batch
    pub max_batch_size: usize,
    /// Enable PostgreSQL LISTEN/NOTIFY for job arrival notifications
    pub enable_listen_notify: bool,
    /// Interval (seconds) for periodic cleanup of old jobs
    pub cleanup_interval_seconds: u64,
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub prefetch_count: usize,
    pub connection_pool_size: u32,
    pub query_timeout_seconds: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dsn: "postgresql://postgres:postgres@localhost:5432/postgres".to_string(),
            max_connections: 16,
            connection_timeout_seconds: 30,
            queue: QueueConfig {
                default_lock_time_seconds: 5,
                max_batch_size: 100,
                enable_listen_notify: true,
                cleanup_interval_seconds: 300,
            },
            performance: PerformanceConfig {
                prefetch_count: 10,
                connection_pool_size: 10,
                query_timeout_seconds: 30,
            },
        }
    }
}

impl Config {
    /// Create config from environment variables
    pub fn from_env() -> Result<Self> {
        todo!("Implement Config::from_env")
    }

    /// Create config from YAML file
    pub fn from_file<P: AsRef<Path>>(_path: P) -> Result<Self> {
        todo!("Implement Config::from_file")
    }

    /// Create config from multiple sources (env, file, defaults)
    pub fn load() -> Result<Self> {
        todo!("Implement Config::load")
    }

    /// Get database connection URL
    pub fn database_url(&self) -> &String {
        &self.dsn
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        todo!("Implement Config::validate")
    }
}
