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

const DEFAULT_MAX_CONNECTIONS: u32 = 16;
const DEFAULT_CONNECTION_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_LOCK_TIME_SECONDS: u32 = 5;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_MAX_READ_CT: i32 = 5;
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5;
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

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
            ..Self::default()
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
            ..Self::default()
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
    /// Returns `MissingConfig` when `PGQRS_DSN` is not found.
    pub fn from_env() -> Result<Self> {
        use std::env;

        let dsn = env::var(ENV_DSN).map_err(|_| crate::error::Error::MissingConfig {
            field: ENV_DSN.to_string(),
        })?;

        Self::with_dsn_and_env_fallback(dsn)
    }

    fn with_dsn_and_env_fallback(dsn: String) -> Result<Self> {
        use std::env;

        let schema = env::var(ENV_SCHEMA).unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        validate_identifier(&schema)?;

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
        let mut config = if let Some(dsn) = explicit_dsn {
            Self::with_dsn_and_env_fallback(dsn.into())?
        } else if let Some(config_path) = explicit_config_path {
            Self::from_file(config_path)?
        } else {
            Self::load_from_standard_sources()?
        };

        if let Some(schema) = explicit_schema {
            let schema_str = schema.into();
            validate_identifier(&schema_str)?;
            config.schema = schema_str;
        }

        Ok(config)
    }

    fn load_from_standard_sources() -> Result<Self> {
        use std::env;

        if let Ok(config_path) = env::var(ENV_CONFIG_FILE) {
            return Self::from_file(config_path);
        }

        if let Ok(config) = Self::from_env() {
            return Ok(config);
        }

        for path in ["pgqrs.yaml", "pgqrs.yml"] {
            if std::path::Path::new(path).exists() {
                return Self::from_file(path);
            }
        }

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

    fn create_test_config_file(content: &str, suffix: &str) -> String {
        let temp_dir = env::temp_dir();
        let file_path = temp_dir.join(format!("test_config_{}.yaml", suffix));
        fs::write(&file_path, content).expect("Failed to write test config");
        file_path.to_string_lossy().to_string()
    }

    fn cleanup_test_file(path: &str) {
        fs::remove_file(path).ok();
    }

    fn clear_test_env_vars() {
        env::remove_var(ENV_DSN);
        env::remove_var(ENV_MAX_CONNECTIONS);
        env::remove_var(ENV_CONNECTION_TIMEOUT);
        env::remove_var(ENV_DEFAULT_LOCK_TIME);
        env::remove_var(ENV_DEFAULT_BATCH_SIZE);
        env::remove_var(ENV_CONFIG_FILE);
        env::remove_var(ENV_SCHEMA);
        env::remove_var(ENV_VALIDATION_CONFIG);
        env::remove_var(ENV_MAX_READ_CT);
        env::remove_var(ENV_HEARTBEAT_INTERVAL);
        env::remove_var(ENV_POLL_INTERVAL_MS);
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
        assert_eq!(config.schema, DEFAULT_SCHEMA);
    }

    #[test]
    #[serial]
    fn test_from_env_complete() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://env:test@localhost/envdb");
        env::set_var(ENV_MAX_CONNECTIONS, "32");
        env::set_var(ENV_CONNECTION_TIMEOUT, "60");
        env::set_var(ENV_DEFAULT_LOCK_TIME, "10");
        env::set_var(ENV_DEFAULT_BATCH_SIZE, "200");
        env::set_var(ENV_MAX_READ_CT, "7");
        env::set_var(ENV_HEARTBEAT_INTERVAL, "11");
        env::set_var(ENV_POLL_INTERVAL_MS, "500");

        let config = Config::from_env().expect("Should load from env");

        assert_eq!(config.dsn, "postgresql://env:test@localhost/envdb");
        assert_eq!(config.max_connections, 32);
        assert_eq!(config.connection_timeout_seconds, 60);
        assert_eq!(config.default_lock_time_seconds, 10);
        assert_eq!(config.default_max_batch_size, 200);
        assert_eq!(config.max_read_ct, 7);
        assert_eq!(config.heartbeat_interval, 11);
        assert_eq!(config.poll_interval_ms, 500);

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
    fn test_from_file_minimal() {
        let config_content = r#"
dsn: "postgresql://minimal:test@localhost/minimaldb"
"#;
        let config_path = create_test_config_file(config_content, "minimal");

        let config = Config::from_file(&config_path).expect("Should load from file");
        assert_eq!(config.dsn, "postgresql://minimal:test@localhost/minimaldb");
        assert_eq!(config.schema, DEFAULT_SCHEMA);

        cleanup_test_file(&config_path);
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
    fn test_validate_schema_name_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("1schema").is_err());
        assert!(validate_identifier("schema-name").is_err());
        assert!(validate_identifier("schema.name").is_err());
        assert!(validate_identifier("schema name").is_err());
        assert!(validate_identifier("schema@name").is_err());
        assert!(validate_identifier(&"a".repeat(64)).is_err());
    }

    #[test]
    fn test_from_dsn_with_schema_valid() {
        let config = Config::from_dsn_with_schema("postgresql://test@localhost/db", "my_schema")
            .expect("Valid schema should work");
        assert_eq!(config.schema, "my_schema");
        assert_eq!(config.dsn, "postgresql://test@localhost/db");
    }

    #[test]
    #[serial]
    fn test_from_env_with_schema() {
        clear_test_env_vars();

        env::set_var(ENV_DSN, "postgresql://test:test@localhost/testdb");
        env::set_var(ENV_SCHEMA, "test_schema");

        let config = Config::from_env().expect("Should load from env with schema");
        assert_eq!(config.schema, "test_schema");

        clear_test_env_vars();
    }
}
