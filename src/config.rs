//! Configuration types for pgqrs.
//!
//! This module defines the [`Config`] struct and related types for configuring pgqrs, including database connection, queue, and performance options.
//!
//! ## What
//!
//! - [`Config`] holds all settings for connecting to PostgreSQL and tuning queue behavior.
//! - The DSN (database connection string) is required and must be provided.
//! - Configuration can be loaded from environment variables, files, or created directly.
//!
//! ## How
//!
//! Create a [`Config`] using one of the provided methods. The DSN is always required.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::config::Config;
//!
//! // Create from DSN directly
//! let config = Config::from_dsn("postgresql://user:pass@localhost/db");
//!
//! // Load from environment variables
//! let config = Config::from_env().expect("PGQRS_DSN environment variable required");
//!
//! // Load from file
//! let config = Config::from_file("config.yaml").expect("Failed to load config");
//! ```
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

// Environment variable names
const ENV_DSN: &str = "PGQRS_DSN";
const ENV_MAX_CONNECTIONS: &str = "PGQRS_MAX_CONNECTIONS";
const ENV_CONNECTION_TIMEOUT: &str = "PGQRS_CONNECTION_TIMEOUT";
const ENV_DEFAULT_LOCK_TIME: &str = "PGQRS_DEFAULT_LOCK_TIME";
const ENV_DEFAULT_BATCH_SIZE: &str = "PGQRS_DEFAULT_BATCH_SIZE";
const ENV_CONFIG_FILE: &str = "PGQRS_CONFIG_FILE";

// Default configuration values
const DEFAULT_MAX_CONNECTIONS: u32 = 16;
const DEFAULT_CONNECTION_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_LOCK_TIME_SECONDS: u32 = 5;
const DEFAULT_BATCH_SIZE: usize = 100;

/// Configuration for pgqrs
///
/// The DSN (database connection string) is required and must be provided
/// when creating a Config instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// PostgreSQL connection string (DSN) - REQUIRED
    pub dsn: String,
    /// Maximum number of database connections in the pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Timeout (seconds) for acquiring a database connection
    #[serde(default = "default_connection_timeout_seconds")]
    pub connection_timeout_seconds: u64,
    /// Default lock time (seconds) for jobs fetched from the queue
    #[serde(default = "default_lock_time_seconds")]
    pub default_lock_time_seconds: u32,
    /// Maximum number of jobs to fetch in a single batch
    #[serde(default = "default_max_batch_size")]
    pub default_max_batch_size: usize,
}

// Default functions for serde
fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

fn default_connection_timeout_seconds() -> u64 {
    DEFAULT_CONNECTION_TIMEOUT_SECONDS
}

fn default_lock_time_seconds() -> u32 {
    DEFAULT_LOCK_TIME_SECONDS
}

fn default_max_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl Config {
    /// Create a new Config with the provided DSN and default values for other fields.
    ///
    /// This is the simplest way to create a Config when you have a database connection string.
    /// All other configuration fields will use their default values, ignoring environment variables.
    ///
    /// # Arguments
    /// * `dsn` - PostgreSQL connection string (e.g., "postgresql://user:pass@localhost/db")
    ///
    /// # Example
    /// ```
    /// # use pgqrs::config::Config;
    /// let config = Config::from_dsn("postgresql://user:pass@localhost/db");
    /// assert_eq!(config.max_connections, 16); // default value
    /// ```
    pub fn from_dsn<S: Into<String>>(dsn: S) -> Self {
        Self {
            dsn: dsn.into(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connection_timeout_seconds: DEFAULT_CONNECTION_TIMEOUT_SECONDS,
            default_lock_time_seconds: DEFAULT_LOCK_TIME_SECONDS,
            default_max_batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Create config from environment variables
    ///
    /// Environment variables supported:
    /// - PGQRS_DSN (required): PostgreSQL connection string
    /// - PGQRS_MAX_CONNECTIONS: Maximum database connections (default: 16)
    /// - PGQRS_CONNECTION_TIMEOUT: Connection timeout in seconds (default: 30)
    /// - PGQRS_DEFAULT_LOCK_TIME: Default lock time in seconds (default: 5)
    /// - PGQRS_DEFAULT_BATCH_SIZE: Default batch size (default: 100)
    pub fn from_env() -> Result<Self> {
        use std::env;

        // DSN is required
        let dsn = env::var(ENV_DSN).map_err(|_| crate::error::PgqrsError::MissingConfig {
            field: ENV_DSN.to_string(),
        })?;

        Ok(Self::with_dsn_and_env_fallback(dsn))
    }

    /// Internal helper to create Config with a DSN and environment variable fallbacks.
    ///
    /// This method consolidates the logic for reading environment variables and applying
    /// default values, reducing code duplication across different Config creation methods.
    ///
    /// # Arguments
    /// * `dsn` - PostgreSQL connection string
    ///
    /// # Returns
    /// Config instance with DSN set and other fields from environment or defaults
    fn with_dsn_and_env_fallback(dsn: String) -> Self {
        use std::env;

        // Parse optional environment variables with defaults
        let max_connections = env::var(ENV_MAX_CONNECTIONS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);

        let connection_timeout_seconds = env::var(ENV_CONNECTION_TIMEOUT)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_SECONDS);

        let default_lock_time_seconds = env::var(ENV_DEFAULT_LOCK_TIME)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_LOCK_TIME_SECONDS);

        let default_max_batch_size = env::var(ENV_DEFAULT_BATCH_SIZE)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BATCH_SIZE);

        Self {
            dsn,
            max_connections,
            connection_timeout_seconds,
            default_lock_time_seconds,
            default_max_batch_size,
        }
    }

    /// Create config from YAML file
    ///
    /// The file must contain at least a 'dsn' field. Other fields are optional
    /// and will use default values if not specified.
    ///
    /// Example YAML file:
    /// ```yaml
    /// dsn: "postgresql://user:pass@localhost/db"
    /// max_connections: 32
    /// connection_timeout_seconds: 60
    /// ```
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pgqrs::config::Config;
    /// let config = Config::from_file("config.yaml").expect("Failed to load config");
    /// assert!(!config.dsn.is_empty());
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::PgqrsError::InvalidConfig {
                field: "file".to_string(),
                message: format!("Failed to read config file '{}': {}", path.display(), e),
            })?;

        let config: Config = serde_yaml::from_str(&content).map_err(|e| {
            crate::error::PgqrsError::InvalidConfig {
                field: "yaml".to_string(),
                message: format!("Failed to parse YAML config: {}", e),
            }
        })?;

        Ok(config)
    }

    /// Create config from multiple sources with priority order
    ///
    /// This method tries to load configuration from multiple sources in the following priority:
    /// 1. Config file specified by PGQRS_CONFIG_FILE environment variable
    /// 2. Environment variables (PGQRS_DSN, etc.)
    /// 3. Default config file locations (pgqrs.yaml, pgqrs.yml)
    ///
    /// At least one source must provide a DSN, or an error will be returned.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pgqrs::config::Config;
    /// // Try to load from env vars, then config file
    /// let config = Config::load().expect("Failed to load configuration");
    /// ```
    pub fn load() -> Result<Self> {
        Self::load_with_options(None::<String>, None::<String>)
    }

    /// Create config from multiple sources with explicit options
    ///
    /// This method allows overriding the DSN and config file path explicitly.
    /// Priority order:
    /// 1. Explicit DSN parameter (if provided)
    /// 2. Explicit config file path (if provided)
    /// 3. Config file specified by PGQRS_CONFIG_FILE environment variable
    /// 4. Environment variables (PGQRS_DSN, etc.)
    /// 5. Default config file locations (pgqrs.yaml, pgqrs.yml)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pgqrs::config::Config;
    /// // Load with explicit DSN
    /// let config = Config::load_with_options(
    ///     Some("postgresql://user:pass@localhost/db"),
    ///     None
    /// ).expect("Failed to load configuration");
    ///
    /// // Load with explicit config file
    /// let config = Config::load_with_options(
    ///     None,
    ///     Some("custom-config.yaml")
    /// ).expect("Failed to load configuration");
    /// ```
    pub fn load_with_options<D, P>(
        explicit_dsn: Option<D>,
        explicit_config_path: Option<P>,
    ) -> Result<Self>
    where
        D: Into<String>,
        P: AsRef<Path>,
    {
        // If explicit DSN is provided, use it with env vars for other settings
        if let Some(dsn) = explicit_dsn {
            return Ok(Self::with_dsn_and_env_fallback(dsn.into()));
        }

        // If explicit config path is provided, try that
        if let Some(config_path) = explicit_config_path {
            return Self::from_file(config_path);
        }

        // Use standard fallback logic
        Self::load_from_standard_sources()
    }

    /// Internal helper for loading config from standard sources with fallback logic.
    ///
    /// This method encapsulates the common fallback sequence used by both `load()`
    /// and `load_with_options()` methods.
    ///
    /// Priority order:
    /// 1. Config file specified by PGQRS_CONFIG_FILE environment variable
    /// 2. Environment variables (PGQRS_DSN, etc.)
    /// 3. Default config file locations (pgqrs.yaml, pgqrs.yml)
    ///
    /// # Returns
    /// Config loaded from the first available source, or error if none found.
    fn load_from_standard_sources() -> Result<Self> {
        use std::env;

        // Try to load from config file specified by environment variable
        if let Ok(config_path) = env::var(ENV_CONFIG_FILE) {
            return Self::from_file(config_path);
        }

        // Try to load from environment variables
        if let Ok(config) = Self::from_env() {
            return Ok(config);
        }

        // Try default config file locations
        let default_paths = ["pgqrs.yaml", "pgqrs.yml"];
        for path in &default_paths {
            if std::path::Path::new(path).exists() {
                return Self::from_file(path);
            }
        }

        // No configuration source found
        Err(crate::error::PgqrsError::MissingConfig {
            field: "configuration".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    // Helper function to create temporary config files for testing
    fn create_test_config_file(content: &str, suffix: &str) -> String {
        let temp_dir = env::temp_dir();
        let file_path = temp_dir.join(format!("test_config_{}.yaml", suffix));
        fs::write(&file_path, content).expect("Failed to write test config");
        file_path.to_string_lossy().to_string()
    }

    // Helper function to clean up test files
    fn cleanup_test_file(path: &str) {
        fs::remove_file(path).ok();
    }

    // Helper function to clear test environment variables
    fn clear_test_env_vars() {
        env::remove_var(ENV_DSN);
        env::remove_var(ENV_MAX_CONNECTIONS);
        env::remove_var(ENV_CONNECTION_TIMEOUT);
        env::remove_var(ENV_DEFAULT_LOCK_TIME);
        env::remove_var(ENV_DEFAULT_BATCH_SIZE);
        env::remove_var(ENV_CONFIG_FILE);
    }

    #[test]
    fn test_from_dsn_basic() {
        let dsn = "postgresql://user:pass@localhost/testdb";
        let config = Config::from_dsn(dsn);

        assert_eq!(config.dsn, dsn);
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.connection_timeout_seconds,
            DEFAULT_CONNECTION_TIMEOUT_SECONDS
        );
        assert_eq!(config.default_lock_time_seconds, DEFAULT_LOCK_TIME_SECONDS);
        assert_eq!(config.default_max_batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_from_dsn_with_string() {
        let dsn = "postgresql://user:pass@localhost/testdb".to_string();
        let config = Config::from_dsn(dsn.clone());
        assert_eq!(config.dsn, dsn);
    }

    #[test]
    fn test_from_env_complete() {
        // Use a nested scope to ensure cleanup happens
        {
            clear_test_env_vars();

            env::set_var(ENV_DSN, "postgresql://env:test@localhost/envdb");
            env::set_var(ENV_MAX_CONNECTIONS, "32");
            env::set_var(ENV_CONNECTION_TIMEOUT, "60");
            env::set_var(ENV_DEFAULT_LOCK_TIME, "10");
            env::set_var(ENV_DEFAULT_BATCH_SIZE, "200");

            let config = Config::from_env().expect("Should load from env");

            assert_eq!(config.dsn, "postgresql://env:test@localhost/envdb");
            assert_eq!(config.max_connections, 32);
            assert_eq!(config.connection_timeout_seconds, 60);
            assert_eq!(config.default_lock_time_seconds, 10);
            assert_eq!(config.default_max_batch_size, 200);
        }
        clear_test_env_vars();
    }

    #[test]
    fn test_from_env_minimal() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://minimal:test@localhost/minimaldb");

        let config = Config::from_env().expect("Should load from env");

        assert_eq!(config.dsn, "postgresql://minimal:test@localhost/minimaldb");
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.connection_timeout_seconds,
            DEFAULT_CONNECTION_TIMEOUT_SECONDS
        );
        assert_eq!(config.default_lock_time_seconds, DEFAULT_LOCK_TIME_SECONDS);
        assert_eq!(config.default_max_batch_size, DEFAULT_BATCH_SIZE);

        clear_test_env_vars();
    }

    #[test]
    fn test_from_env_missing_dsn() {
        clear_test_env_vars();

        let result = Config::from_env();
        assert!(result.is_err());

        if let Err(crate::error::PgqrsError::MissingConfig { field }) = result {
            assert_eq!(field, ENV_DSN);
        } else {
            panic!("Expected MissingConfig error for DSN");
        }
    }

    #[test]
    fn test_from_env_invalid_numbers() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://test:test@localhost/testdb");
        env::set_var(ENV_MAX_CONNECTIONS, "invalid");
        env::set_var(ENV_CONNECTION_TIMEOUT, "not_a_number");

        let config =
            Config::from_env().expect("Should load from env with defaults for invalid numbers");

        assert_eq!(config.dsn, "postgresql://test:test@localhost/testdb");
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.connection_timeout_seconds,
            DEFAULT_CONNECTION_TIMEOUT_SECONDS
        );

        clear_test_env_vars();
    }

    #[test]
    fn test_from_file_complete() {
        let config_content = r#"
dsn: "postgresql://file:test@localhost/filedb"
max_connections: 64
connection_timeout_seconds: 120
default_lock_time_seconds: 15
default_max_batch_size: 500
"#;
        let config_path = create_test_config_file(config_content, "complete");

        let config = Config::from_file(&config_path).expect("Should load from file");

        assert_eq!(config.dsn, "postgresql://file:test@localhost/filedb");
        assert_eq!(config.max_connections, 64);
        assert_eq!(config.connection_timeout_seconds, 120);
        assert_eq!(config.default_lock_time_seconds, 15);
        assert_eq!(config.default_max_batch_size, 500);

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_from_file_minimal() {
        let config_content = r#"
dsn: "postgresql://minimal:test@localhost/minimaldb"
"#;
        let config_path = create_test_config_file(config_content, "minimal");

        let config = Config::from_file(&config_path).expect("Should load from file");

        assert_eq!(config.dsn, "postgresql://minimal:test@localhost/minimaldb");
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.connection_timeout_seconds,
            DEFAULT_CONNECTION_TIMEOUT_SECONDS
        );
        assert_eq!(config.default_lock_time_seconds, DEFAULT_LOCK_TIME_SECONDS);
        assert_eq!(config.default_max_batch_size, DEFAULT_BATCH_SIZE);

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_from_file_missing_dsn() {
        let config_content = r#"
max_connections: 32
connection_timeout_seconds: 60
"#;
        let config_path = create_test_config_file(config_content, "missing_dsn");

        let result = Config::from_file(&config_path);
        assert!(result.is_err());

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_from_file_invalid_yaml() {
        let config_content = r#"
dsn: "postgresql://test:test@localhost/testdb
max_connections: [invalid yaml structure
"#;
        let config_path = create_test_config_file(config_content, "invalid_yaml");

        let result = Config::from_file(&config_path);
        assert!(result.is_err());

        if let Err(crate::error::PgqrsError::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "yaml");
        } else {
            panic!("Expected InvalidConfig error for yaml");
        }

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_from_file_nonexistent() {
        let result = Config::from_file("/nonexistent/path/config.yaml");
        assert!(result.is_err());

        if let Err(crate::error::PgqrsError::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "file");
        } else {
            panic!("Expected InvalidConfig error for file");
        }
    }

    #[test]
    fn test_load_with_explicit_dsn() {
        clear_test_env_vars();

        let dsn = "postgresql://explicit:test@localhost/explicitdb";
        let config = Config::load_with_options(Some(dsn), None::<&str>)
            .expect("Should load with explicit DSN");

        assert_eq!(config.dsn, dsn);
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn test_load_with_explicit_config_file() {
        clear_test_env_vars();

        let config_content = r#"
dsn: "postgresql://explicit_file:test@localhost/explicitfiledb"
max_connections: 128
"#;
        let config_path = create_test_config_file(config_content, "explicit");

        let config = Config::load_with_options(None::<&str>, Some(&config_path))
            .expect("Should load with explicit config file");

        assert_eq!(
            config.dsn,
            "postgresql://explicit_file:test@localhost/explicitfiledb"
        );
        assert_eq!(config.max_connections, 128);

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_load_with_dsn_priority_over_file() {
        clear_test_env_vars();

        let config_content = r#"
dsn: "postgresql://file:test@localhost/filedb"
max_connections: 128
"#;
        let config_path = create_test_config_file(config_content, "priority");

        let explicit_dsn = "postgresql://explicit:test@localhost/explicitdb";
        let config = Config::load_with_options(Some(explicit_dsn), Some(&config_path))
            .expect("Should load with DSN priority");

        // DSN should come from explicit parameter, other settings can come from env
        assert_eq!(config.dsn, explicit_dsn);

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_load_env_file_priority() {
        clear_test_env_vars();

        let config_content = r#"
dsn: "postgresql://envfile:test@localhost/envfiledb"
max_connections: 256
"#;
        let config_path = create_test_config_file(config_content, "env_file");

        env::set_var(ENV_CONFIG_FILE, &config_path);

        let config = Config::load().expect("Should load from env config file");

        assert_eq!(config.dsn, "postgresql://envfile:test@localhost/envfiledb");
        assert_eq!(config.max_connections, 256);

        cleanup_test_file(&config_path);
        clear_test_env_vars();
    }

    #[test]
    fn test_load_fallback_to_env_vars() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://fallback:test@localhost/fallbackdb");
        env::set_var(ENV_MAX_CONNECTIONS, "512");

        let config = Config::load().expect("Should load from env vars");

        assert_eq!(
            config.dsn,
            "postgresql://fallback:test@localhost/fallbackdb"
        );
        assert_eq!(config.max_connections, 512);

        clear_test_env_vars();
    }

    #[test]
    fn test_load_no_config_source() {
        clear_test_env_vars();

        let result = Config::load();
        assert!(result.is_err());

        if let Err(crate::error::PgqrsError::MissingConfig { field }) = result {
            assert_eq!(field, "configuration");
        } else {
            panic!("Expected MissingConfig error for configuration");
        }
    }
}
