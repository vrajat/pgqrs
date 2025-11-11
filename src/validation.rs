//! Input validation and service protection for pgqrs.
//!
//! This module provides configurable validation rules and rate limiting
//! to protect services from malicious or malformed payloads.
//!
//! ## What
//!
//! - [`ValidationConfig`] defines validation rules including size limits, content validation, and rate limiting
//! - [`PayloadValidator`] implements the validation logic with in-memory rate limiting
//!
//! ## How
//!
//! Create a [`ValidationConfig`] with appropriate limits for your service, then use it
//! to create a [`PayloadValidator`] that can validate payloads before enqueueing.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::validation::{ValidationConfig, PayloadValidator};
//! use serde_json::json;
//!
//! let config = ValidationConfig {
//!     max_payload_size_bytes: 64 * 1024,    // 64KB
//!     required_keys: vec!["user_id".to_string()],
//!     max_enqueue_per_second: Some(100),
//!     ..Default::default()
//! };
//!
//! let validator = PayloadValidator::new(config);
//! let payload = json!({"user_id": "123", "data": "test"});
//!
//! match validator.validate(&payload) {
//!     Ok(()) => println!("Payload is valid"),
//!     Err(e) => println!("Validation failed: {}", e),
//! }
//! ```

use crate::error::Result;
use crate::rate_limit::TokenBucket;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for payload validation and rate limiting.
///
/// This struct defines validation rules that services can configure
/// to protect against abuse and malformed payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Maximum size of a JSON payload in bytes (after serialization)
    pub max_payload_size_bytes: usize,
    /// Maximum length for individual string values
    pub max_string_length: usize,
    /// Maximum depth of nested objects/arrays to prevent JSON bombs
    pub max_object_depth: usize,
    /// List of keys that are forbidden in the payload
    pub forbidden_keys: Vec<String>,
    /// List of keys that must be present in the payload
    pub required_keys: Vec<String>,
    /// Maximum number of enqueue operations per second (None = no limit)
    pub max_enqueue_per_second: Option<u32>,
    /// Maximum burst capacity for rate limiting
    pub max_enqueue_burst: Option<u32>,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_payload_size_bytes: 1024 * 1024, // 1MB - reasonable default for most use cases
            max_string_length: 1024,             // 1KB strings max
            max_object_depth: 10,                // Allow reasonable nesting
            forbidden_keys: vec!["__proto__".to_string(), "constructor".to_string()],
            required_keys: vec![],
            max_enqueue_per_second: Some(1000), // 1K/s default rate limit
            max_enqueue_burst: Some(50),        // Allow reasonable bursting
        }
    }
}

impl ValidationConfig {
    /// Validate payload without enqueueing (for testing).
    ///
    /// This method validates a payload against the configuration rules
    /// but does not perform rate limiting checks since it's intended for testing.
    ///
    /// # Arguments
    /// * `payload` - JSON payload to validate
    ///
    /// # Returns
    /// * `Ok(())` if the payload is valid
    /// * `Err(PgqrsError)` if validation fails
    pub fn validate_payload(&self, payload: &serde_json::Value) -> Result<()> {
        // Create a temporary validator without rate limiting for testing
        let validator = PayloadValidator {
            config: self.clone(),
            rate_limiter: None,
        };

        // Validate without rate limiting
        validator.validate_structure(payload, 0)?;
        validator.validate_content(payload)?;
        validator.validate_size(payload)?;

        Ok(())
    }
}

/// Payload validator with configurable rules and rate limiting.
///
/// This struct provides validation capabilities including size checks,
/// structure validation, content filtering, and in-memory rate limiting.
pub struct PayloadValidator {
    config: ValidationConfig,
    rate_limiter: Option<TokenBucket>,
}

impl PayloadValidator {
    /// Create a new PayloadValidator with the given configuration.
    ///
    /// Rate limiting is configured based on the ValidationConfig settings.
    /// If max_enqueue_per_second is None, no rate limiting is applied.
    ///
    /// # Arguments
    /// * `config` - Validation configuration
    pub fn new(config: ValidationConfig) -> Self {
        let rate_limiter = config
            .max_enqueue_per_second
            .map(|rate| TokenBucket::new(rate, config.max_enqueue_burst.unwrap_or(50)));

        Self {
            config,
            rate_limiter,
        }
    }

    /// Validate a payload against all configured rules.
    ///
    /// This method performs comprehensive validation including:
    /// - Rate limiting (if configured) - checked first to prevent CPU abuse
    /// - Size checks (payload and string lengths)
    /// - Structure validation (depth limits)
    /// - Content validation (forbidden/required keys)
    ///
    /// # Arguments
    /// * `payload` - JSON payload to validate
    ///
    /// # Returns
    /// * `Ok(())` if the payload passes all validation rules
    /// * `Err(PgqrsError)` if validation fails
    pub fn validate(&self, payload: &serde_json::Value) -> Result<()> {
        // 1. Rate limit check first (very fast atomic operation) to prevent CPU abuse
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire() {
                return Err(crate::error::PgqrsError::RateLimited {
                    retry_after: Duration::from_secs(1),
                });
            }
        }

        // 2. Fast size check (serialize once)
        self.validate_size(payload)?;

        // 3. Structure validation (depth and content)
        self.validate_structure(payload, 0)?;
        self.validate_content(payload)?;

        Ok(())
    }

    /// Validate multiple payloads atomically for batch operations.
    ///
    /// This method validates all payloads and consumes rate limit tokens for the entire
    /// batch atomically. If any validation fails, no rate limit tokens are consumed.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to validate
    ///
    /// # Returns
    /// * `Ok(())` if all payloads pass validation rules
    /// * `Err(PgqrsError)` if any payload fails validation
    pub fn validate_batch(&self, payloads: &[serde_json::Value]) -> Result<()> {
        // 1. First check rate limit capacity for entire batch without consuming tokens
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire_multiple(payloads.len() as u32) {
                return Err(crate::error::PgqrsError::RateLimited {
                    retry_after: Duration::from_secs(1),
                });
            }
        }

        // 2. Validate all payloads without consuming additional rate limit tokens
        for (index, payload) in payloads.iter().enumerate() {
            // Validate without rate limiting (already checked above)
            self.validate_size(payload).map_err(|e| match e {
                crate::error::PgqrsError::PayloadTooLarge {
                    actual_bytes,
                    max_bytes,
                } => crate::error::PgqrsError::ValidationFailed {
                    reason: format!(
                        "Payload at index {} too large: {} bytes exceeds limit {}",
                        index, actual_bytes, max_bytes
                    ),
                },
                other => other,
            })?;

            self.validate_structure(payload, 0).map_err(|e| match e {
                crate::error::PgqrsError::ValidationFailed { reason } => {
                    crate::error::PgqrsError::ValidationFailed {
                        reason: format!("Payload at index {}: {}", index, reason),
                    }
                }
                other => other,
            })?;

            self.validate_content(payload).map_err(|e| match e {
                crate::error::PgqrsError::ValidationFailed { reason } => {
                    crate::error::PgqrsError::ValidationFailed {
                        reason: format!("Payload at index {}: {}", index, reason),
                    }
                }
                other => other,
            })?;
        }

        Ok(())
    }

    /// Validate the serialized size of the payload.
    fn validate_size(&self, payload: &serde_json::Value) -> Result<()> {
        let serialized = serde_json::to_string(payload)?;
        let size = serialized.len();

        if size > self.config.max_payload_size_bytes {
            return Err(crate::error::PgqrsError::PayloadTooLarge {
                actual_bytes: size,
                max_bytes: self.config.max_payload_size_bytes,
            });
        }

        Ok(())
    }

    /// Validate the structure of the payload (depth and string lengths).
    fn validate_structure(&self, payload: &serde_json::Value, depth: usize) -> Result<()> {
        if depth > self.config.max_object_depth {
            return Err(crate::error::PgqrsError::ValidationFailed {
                reason: format!(
                    "Object depth {} exceeds limit {}",
                    depth, self.config.max_object_depth
                ),
            });
        }

        match payload {
            serde_json::Value::Object(obj) => {
                for (key, value) in obj {
                    // Check string length
                    if key.len() > self.config.max_string_length {
                        return Err(crate::error::PgqrsError::ValidationFailed {
                            reason: format!("Key '{}' length exceeds limit", key),
                        });
                    }
                    // Recurse into nested objects
                    self.validate_structure(value, depth + 1)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    self.validate_structure(item, depth + 1)?;
                }
            }
            serde_json::Value::String(s) => {
                if s.len() > self.config.max_string_length {
                    return Err(crate::error::PgqrsError::ValidationFailed {
                        reason: format!(
                            "String length {} exceeds limit {}",
                            s.len(),
                            self.config.max_string_length
                        ),
                    });
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Validate the content of the payload (forbidden/required keys).
    fn validate_content(&self, payload: &serde_json::Value) -> Result<()> {
        if let serde_json::Value::Object(obj) = payload {
            // Check forbidden keys
            for forbidden in &self.config.forbidden_keys {
                if obj.contains_key(forbidden) {
                    return Err(crate::error::PgqrsError::ValidationFailed {
                        reason: format!("Forbidden key '{}' found in payload", forbidden),
                    });
                }
            }

            // Check required keys
            for required in &self.config.required_keys {
                if !obj.contains_key(required) {
                    return Err(crate::error::PgqrsError::ValidationFailed {
                        reason: format!("Required key '{}' missing from payload", required),
                    });
                }
            }
        }

        Ok(())
    }

    /// Get current rate limit status for debugging.
    ///
    /// Returns None if rate limiting is disabled.
    pub fn rate_limit_status(&self) -> Option<crate::rate_limit::RateLimitStatus> {
        self.rate_limiter.as_ref().map(|limiter| limiter.status())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_config() {
        let config = ValidationConfig::default();
        assert_eq!(config.max_payload_size_bytes, 1024 * 1024); // 1MB
        assert_eq!(config.max_string_length, 1024);
        assert_eq!(config.max_object_depth, 10);
        assert_eq!(config.max_enqueue_per_second, Some(1000));
        assert_eq!(config.max_enqueue_burst, Some(50));
    }

    #[test]
    fn test_validate_payload_size() {
        let config = ValidationConfig {
            max_payload_size_bytes: 50,
            ..Default::default()
        };

        let small_payload = json!({"key": "value"});
        assert!(config.validate_payload(&small_payload).is_ok());

        let large_payload = json!({
            "very_long_key_that_exceeds_limit": "very_long_value_that_definitely_exceeds_the_50_byte_limit_we_set_for_testing_purposes"
        });
        assert!(config.validate_payload(&large_payload).is_err());
    }

    #[test]
    fn test_validate_string_length() {
        let config = ValidationConfig {
            max_string_length: 10,
            ..Default::default()
        };

        let valid_payload = json!({"key": "short"});
        assert!(config.validate_payload(&valid_payload).is_ok());

        let invalid_payload = json!({"key": "this_is_a_very_long_string"});
        assert!(config.validate_payload(&invalid_payload).is_err());
    }

    #[test]
    fn test_validate_object_depth() {
        let config = ValidationConfig {
            max_object_depth: 2,
            ..Default::default()
        };

        let valid_payload = json!({"level1": {"level2": "value"}});
        assert!(config.validate_payload(&valid_payload).is_ok());

        let invalid_payload = json!({"level1": {"level2": {"level3": {"level4": "value"}}}});
        assert!(config.validate_payload(&invalid_payload).is_err());
    }

    #[test]
    fn test_forbidden_keys() {
        let config = ValidationConfig {
            forbidden_keys: vec!["__proto__".to_string()],
            ..Default::default()
        };

        let valid_payload = json!({"data": "value"});
        assert!(config.validate_payload(&valid_payload).is_ok());

        let invalid_payload = json!({"__proto__": "malicious"});
        assert!(config.validate_payload(&invalid_payload).is_err());
    }

    #[test]
    fn test_required_keys() {
        let config = ValidationConfig {
            required_keys: vec!["user_id".to_string()],
            ..Default::default()
        };

        let valid_payload = json!({"user_id": "123", "data": "value"});
        assert!(config.validate_payload(&valid_payload).is_ok());

        let invalid_payload = json!({"data": "value"});
        assert!(config.validate_payload(&invalid_payload).is_err());
    }
}
