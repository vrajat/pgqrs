//! Configuration for pgqrs connections and defaults.
//!
//! Use [`Config`] to set the DSN, schema, and queue defaults. Load from explicit values,
//! environment variables, or a YAML file.
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Validate a SQL identifier for schema names.
///
/// # Errors
/// Returns `InvalidConfig` when the identifier is empty, too long, or contains invalid characters.
fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(crate::error::Error::InvalidConfig {
            field: "schema".to_string(),
            message: "Schema name cannot be empty".to_string(),
        });
    }

    if identifier.len() > 63 {
        return Err(crate::error::Error::InvalidConfig {
            field: "schema".to_string(),
            message: format!(
                "Schema name '{}' exceeds maximum length of 63 bytes",
                identifier
            ),
        });
    }

    // Check first character
    let first_char = identifier.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(crate::error::Error::InvalidConfig {
            field: "schema".to_string(),
            message: format!(
                "Schema name '{}' must start with a letter or underscore",
                identifier
            ),
        });
    }

    // Check remaining characters
    for c in identifier.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '$' {
            return Err(crate::error::Error::InvalidConfig {
                field: "schema".to_string(),
                message: format!(
                    "Schema name '{}' contains invalid character '{}'. Only letters, digits, underscores, and dollar signs are allowed",
                    identifier, c
                ),
            });
        }
    }

    Ok(())
}

// Environment variable names
const ENV_DSN: &str = "PGQRS_DSN";
const ENV_MAX_CONNECTIONS: &str = "PGQRS_MAX_CONNECTIONS";
const ENV_CONNECTION_TIMEOUT: &str = "PGQRS_CONNECTION_TIMEOUT";
const ENV_DEFAULT_LOCK_TIME: &str = "PGQRS_DEFAULT_LOCK_TIME";
const ENV_DEFAULT_BATCH_SIZE: &str = "PGQRS_DEFAULT_BATCH_SIZE";
const ENV_CONFIG_FILE: &str = "PGQRS_CONFIG_FILE";
const ENV_SCHEMA: &str = "PGQRS_SCHEMA";
const ENV_VALIDATION_CONFIG: &str = "PGQRS_VALIDATION_CONFIG";
const ENV_MAX_READ_CT: &str = "PGQRS_MAX_READ_CT";
const ENV_HEARTBEAT_INTERVAL: &str = "PGQRS_HEARTBEAT_INTERVAL";
const ENV_POLL_INTERVAL_MS: &str = "PGQRS_POLL_INTERVAL_MS";
const ENV_SQLITE_USE_WAL: &str = "PGQRS_SQLITE_USE_WAL";
#[cfg(feature = "s3")]
const ENV_S3_MODE: &str = "PGQRS_S3_MODE";

// Default configuration values
const DEFAULT_MAX_CONNECTIONS: u32 = 16;
const DEFAULT_CONNECTION_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_LOCK_TIME_SECONDS: u32 = 5;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_MAX_READ_CT: i32 = 5;
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5;
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

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

fn default_schema() -> String {
    DEFAULT_SCHEMA.to_string()
}

fn default_max_read_ct() -> i32 {
    DEFAULT_MAX_READ_CT
}

fn default_heartbeat_interval() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL
}

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

fn default_sqlite_use_wal() -> bool {
    true
}

fn default_sqlite_config() -> SqliteConfig {
    SqliteConfig::default()
}

#[cfg(feature = "s3")]
fn default_s3_mode() -> crate::store::s3::DurabilityMode {
    crate::store::s3::DurabilityMode::Durable
}

#[cfg(feature = "s3")]
fn default_s3_config() -> S3Config {
    S3Config::default()
}

/// SQLite-specific runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// Enable WAL journal mode for SQLite connections.
    #[serde(default = "default_sqlite_use_wal")]
    pub use_wal: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            use_wal: default_sqlite_use_wal(),
        }
    }
}

/// S3-specific runtime settings.
#[cfg(feature = "s3")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3-backed durability mode (only used for s3:// DSNs)
    #[serde(default = "default_s3_mode")]
    pub mode: crate::store::s3::DurabilityMode,
}

#[cfg(feature = "s3")]
impl Default for S3Config {
    fn default() -> Self {
        Self {
            mode: default_s3_mode(),
        }
    }
}

/// Connection and queue defaults for pgqrs.
///
/// The DSN is required. The schema must exist before installing pgqrs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// PostgreSQL connection string (DSN) - REQUIRED
    pub dsn: String,
    /// Schema name for pgqrs tables and objects (must exist before install)
    #[serde(default = "default_schema")]
    pub schema: String,
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
    /// Maximum read count for messages before moving to dead-letter queue
    #[serde(default = "default_max_read_ct")]
    pub max_read_ct: i32,
    /// Heartbeat interval (seconds) for workers
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,

    /// Poll interval (milliseconds) used by consumer polling loops.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Validation configuration for payload checking and rate limiting
    #[serde(default)]
    pub validation_config: crate::validation::ValidationConfig,
    #[serde(default = "default_sqlite_config")]
    pub sqlite: SqliteConfig,
    #[cfg(feature = "s3")]
    #[serde(default = "default_s3_config")]
    pub s3: S3Config,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            schema: default_schema(),
            max_connections: default_max_connections(),
            connection_timeout_seconds: default_connection_timeout_seconds(),
            default_lock_time_seconds: default_lock_time_seconds(),
            default_max_batch_size: default_max_batch_size(),
            max_read_ct: default_max_read_ct(),
            heartbeat_interval: default_heartbeat_interval(),
            poll_interval_ms: default_poll_interval_ms(),
            validation_config: Default::default(),
            sqlite: default_sqlite_config(),
            #[cfg(feature = "s3")]
            s3: default_s3_config(),
        }
    }
}

impl Config {
    /// Build a config from a DSN with default values.
    ///
    /// This ignores environment overrides.
    pub fn from_dsn<S: Into<String>>(dsn: S) -> Self {
        Self {
            dsn: dsn.into(),
            schema: default_schema(),
            max_connections: default_max_connections(),
            connection_timeout_seconds: default_connection_timeout_seconds(),
            default_lock_time_seconds: default_lock_time_seconds(),
            default_max_batch_size: default_max_batch_size(),
            max_read_ct: default_max_read_ct(),
            heartbeat_interval: default_heartbeat_interval(),
            poll_interval_ms: default_poll_interval_ms(),
            validation_config: Default::default(),
            sqlite: default_sqlite_config(),
            #[cfg(feature = "s3")]
            s3: default_s3_config(),
        }
    }

    /// Build a config from a DSN and schema name.
    ///
    /// # Errors
    /// Returns `InvalidConfig` if the schema name is invalid.
    pub fn from_dsn_with_schema<D, S>(dsn: D, schema: S) -> Result<Self>
    where
        D: Into<String>,
        S: Into<String>,
    {
        let schema_str = schema.into();
        validate_identifier(&schema_str)?;

        Ok(Self {
            dsn: dsn.into(),
            schema: schema_str,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connection_timeout_seconds: DEFAULT_CONNECTION_TIMEOUT_SECONDS,
            default_lock_time_seconds: DEFAULT_LOCK_TIME_SECONDS,
            default_max_batch_size: DEFAULT_BATCH_SIZE,
            max_read_ct: DEFAULT_MAX_READ_CT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            validation_config: Default::default(),
            sqlite: default_sqlite_config(),
            #[cfg(feature = "s3")]
            s3: default_s3_config(),
        })
    }

    /// Set the schema name.
    pub fn with_schema<S: Into<String>>(mut self, schema: S) -> Self {
        self.schema = schema.into();
        self
    }

    /// Set the maximum number of database connections.
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Build a config from environment variables.
    ///
    /// # Errors
    /// Returns `MissingConfig` when `PGQRS_DSN` is not found,
    /// or other DSN-specific loading errors.
    pub fn from_env() -> Result<Self> {
        use std::env;

        // DSN is required
        let dsn = env::var(ENV_DSN).map_err(|_| crate::error::Error::MissingConfig {
            field: ENV_DSN.to_string(),
        })?;

        Self::with_dsn_and_env_fallback(dsn)
    }

    /// Build a config using a DSN plus environment fallback for the schema.
    fn with_dsn_and_env_fallback(dsn: String) -> Result<Self> {
        use std::env;

        // Parse schema from environment variable with validation
        let schema = env::var(ENV_SCHEMA).unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        validate_identifier(&schema)?;

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

        let max_read_ct = env::var(ENV_MAX_READ_CT)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_READ_CT);

        let heartbeat_interval = env::var(ENV_HEARTBEAT_INTERVAL)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

        let poll_interval_ms = env::var(ENV_POLL_INTERVAL_MS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_MS);

        let validation_config = env::var(ENV_VALIDATION_CONFIG)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let sqlite_use_wal = env::var(ENV_SQLITE_USE_WAL)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_sqlite_use_wal);

        let sqlite = SqliteConfig {
            use_wal: sqlite_use_wal,
        };

        #[cfg(feature = "s3")]
        let s3_mode = env::var(ENV_S3_MODE)
            .ok()
            .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
                "local" => Some(crate::store::s3::DurabilityMode::Local),
                "durable" => Some(crate::store::s3::DurabilityMode::Durable),
                _ => None,
            })
            .unwrap_or_else(default_s3_mode);

        #[cfg(feature = "s3")]
        let s3 = S3Config { mode: s3_mode };

        Ok(Self {
            dsn,
            schema,
            max_connections,
            connection_timeout_seconds,
            default_lock_time_seconds,
            default_max_batch_size,
            max_read_ct,
            heartbeat_interval,
            poll_interval_ms,
            validation_config,
            sqlite,
            #[cfg(feature = "s3")]
            s3,
        })
    }

    /// Build a config from a YAML file.
    ///
    /// # Errors
    /// Returns `InvalidConfig` if the file cannot be read or parsed.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::Error::InvalidConfig {
                field: "file".to_string(),
                message: format!("Failed to read config file '{}': {}", path.display(), e),
            })?;

        let config: Config =
            serde_yaml::from_str(&content).map_err(|e| crate::error::Error::InvalidConfig {
                field: "yaml".to_string(),
                message: format!("Failed to parse YAML config: {}", e),
            })?;

        // Validate schema name
        validate_identifier(&config.schema)?;

        Ok(config)
    }

    /// Load config from file, environment, or defaults.
    ///
    /// # Errors
    /// Returns `MissingConfig` when no DSN is provided by any source.
    pub fn load() -> Result<Self> {
        Self::load_with_options(None::<String>, None::<String>)
    }

    /// Load config with explicit DSN or config file overrides.
    ///
    /// # Errors
    /// Returns `MissingConfig` when no DSN is provided and none can be found in environment.
    pub fn load_with_options<D, P>(
        explicit_dsn: Option<D>,
        explicit_config_path: Option<P>,
    ) -> Result<Self>
    where
        D: Into<String>,
        P: AsRef<Path>,
    {
        Self::load_with_schema_options(explicit_dsn, None::<String>, explicit_config_path)
    }

    /// Load config with explicit DSN, schema, or file overrides.
    ///
    /// # Errors
    /// Returns `InvalidConfig` for invalid schema names.
    pub fn load_with_schema_options<D, S, P>(
        explicit_dsn: Option<D>,
        explicit_schema: Option<S>,
        explicit_config_path: Option<P>,
    ) -> Result<Self>
    where
        D: Into<String>,
        S: Into<String>,
        P: AsRef<Path>,
    {
        // First, get base config using existing logic
        let mut config = if let Some(dsn) = explicit_dsn {
            Self::with_dsn_and_env_fallback(dsn.into())?
        } else if let Some(config_path) = explicit_config_path {
            Self::from_file(config_path)?
        } else {
            Self::load_from_standard_sources()?
        };

        // Override schema if explicitly provided
        if let Some(schema) = explicit_schema {
            let schema_str = schema.into();
            validate_identifier(&schema_str)?;
            config.schema = schema_str;
        }

        Ok(config)
    }

    /// Load config from standard sources in priority order.
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
        Err(crate::error::Error::MissingConfig {
            field: "configuration".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
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
        env::remove_var(ENV_SCHEMA);
        env::remove_var(ENV_VALIDATION_CONFIG);
        env::remove_var(ENV_SQLITE_USE_WAL);
        #[cfg(feature = "s3")]
        {
            env::remove_var(ENV_S3_MODE);
        }
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
        assert!(config.sqlite.use_wal);
    }

    #[test]
    fn test_from_dsn_with_string() {
        let dsn = "postgresql://user:pass@localhost/testdb".to_string();
        let config = Config::from_dsn(dsn.clone());
        assert_eq!(config.dsn, dsn);
    }

    #[test]
    #[serial]
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
            assert!(config.sqlite.use_wal);
        }
        clear_test_env_vars();
    }

    #[test]
    #[serial]
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
        assert!(config.sqlite.use_wal);

        clear_test_env_vars();
    }

    #[test]
    #[serial]
    fn test_from_env_missing_dsn() {
        clear_test_env_vars();

        let result = Config::from_env();
        assert!(result.is_err());

        if let Err(crate::error::Error::MissingConfig { field }) = result {
            assert_eq!(field, ENV_DSN);
        } else {
            panic!("Expected MissingConfig error for DSN");
        }
    }

    #[test]
    #[serial]
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
        assert!(config.sqlite.use_wal);

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
        assert!(config.sqlite.use_wal);

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
        assert!(config.sqlite.use_wal);

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

        if let Err(crate::error::Error::InvalidConfig { field, .. }) = result {
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

        if let Err(crate::error::Error::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "file");
        } else {
            panic!("Expected InvalidConfig error for file");
        }
    }

    #[test]
    #[serial]
    fn test_load_with_explicit_dsn() {
        clear_test_env_vars();

        let dsn = "postgresql://explicit:test@localhost/explicitdb";
        let config = Config::load_with_options(Some(dsn), None::<&str>)
            .expect("Should load with explicit DSN");

        assert_eq!(config.dsn, dsn);
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
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
    #[serial]
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
    #[serial]
    fn test_load_no_config_source() {
        clear_test_env_vars();

        let result = Config::load();
        assert!(result.is_err());

        if let Err(crate::error::Error::MissingConfig { field }) = result {
            assert_eq!(field, "configuration");
        } else {
            panic!("Expected MissingConfig error for configuration");
        }
    }

    // Schema validation tests
    #[test]
    fn test_validate_schema_name_valid() {
        assert!(validate_identifier("public").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("schema123").is_ok());
        assert!(validate_identifier("my_schema").is_ok());
        assert!(validate_identifier("schema$name").is_ok());
        assert!(validate_identifier("a").is_ok());
        assert!(validate_identifier("A").is_ok());
    }

    #[test]
    fn test_validate_schema_name_invalid() {
        // Empty schema
        assert!(validate_identifier("").is_err());

        // Starts with digit
        assert!(validate_identifier("1schema").is_err());

        // Contains invalid characters
        assert!(validate_identifier("schema-name").is_err());
        assert!(validate_identifier("schema.name").is_err());
        assert!(validate_identifier("schema name").is_err());
        assert!(validate_identifier("schema@name").is_err());

        // Too long (64+ characters)
        let long_name = "a".repeat(64);
        assert!(validate_identifier(&long_name).is_err());
    }

    #[test]
    fn test_from_dsn_with_schema_valid() {
        let config = Config::from_dsn_with_schema("postgresql://test@localhost/db", "my_schema")
            .expect("Valid schema should work");
        assert_eq!(config.schema, "my_schema");
        assert_eq!(config.dsn, "postgresql://test@localhost/db");
    }

    #[test]
    fn test_from_dsn_with_schema_invalid() {
        let result = Config::from_dsn_with_schema("postgresql://test@localhost/db", "123invalid");
        assert!(result.is_err());

        if let Err(crate::error::Error::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "schema");
        } else {
            panic!("Expected InvalidConfig error for schema");
        }
    }

    #[test]
    #[serial]
    fn test_from_env_with_schema() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://test:test@localhost/testdb");
        env::set_var(ENV_SCHEMA, "test_schema");

        let config = Config::from_env().expect("Should load from env with schema");

        assert_eq!(config.dsn, "postgresql://test:test@localhost/testdb");
        assert_eq!(config.schema, "test_schema");

        clear_test_env_vars();
    }

    #[test]
    #[serial]
    fn test_from_env_with_invalid_schema() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://test:test@localhost/testdb");
        env::set_var(ENV_SCHEMA, "invalid-schema");

        let result = Config::from_env();
        assert!(result.is_err());

        if let Err(crate::error::Error::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "schema");
        } else {
            panic!("Expected InvalidConfig error for schema");
        }

        clear_test_env_vars();
    }

    #[test]
    fn test_from_file_with_schema() {
        let config_content = r#"
dsn: "postgresql://file:test@localhost/filedb"
schema: "file_schema"
max_connections: 64
"#;
        let config_path = create_test_config_file(config_content, "with_schema");

        let config = Config::from_file(&config_path).expect("Should load from file with schema");

        assert_eq!(config.dsn, "postgresql://file:test@localhost/filedb");
        assert_eq!(config.schema, "file_schema");
        assert_eq!(config.max_connections, 64);

        cleanup_test_file(&config_path);
    }

    #[test]
    fn test_from_file_with_invalid_schema() {
        let config_content = r#"
dsn: "postgresql://file:test@localhost/filedb"
schema: "invalid-schema-name"
"#;
        let config_path = create_test_config_file(config_content, "invalid_schema");

        let result = Config::from_file(&config_path);
        assert!(result.is_err());

        if let Err(crate::error::Error::InvalidConfig { field, .. }) = result {
            assert_eq!(field, "schema");
        } else {
            panic!("Expected InvalidConfig error for schema");
        }

        cleanup_test_file(&config_path);
    }

    #[cfg(feature = "s3")]
    #[test]
    #[serial]
    fn test_from_env_s3_settings() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "s3://bucket/queue.sqlite");
        env::set_var(ENV_S3_MODE, "durable");

        let config = Config::from_env().expect("Should load s3 settings from env");
        assert_eq!(config.s3.mode, crate::store::s3::DurabilityMode::Durable);

        clear_test_env_vars();
    }

    #[test]
    #[serial]
    fn test_from_env_sqlite_use_wal_override() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "sqlite://test.db?mode=rwc");
        env::set_var(ENV_SQLITE_USE_WAL, "false");

        let config = Config::from_env().expect("Should load sqlite settings from env");
        assert!(!config.sqlite.use_wal);

        clear_test_env_vars();
    }
}
