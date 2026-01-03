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
//! Configure validation through the main [`Config`] struct when creating queues.
//! Validation is automatically applied to all enqueue operations.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::{Config, Admin, Producer, ValidationConfig};
//! use serde_json::json;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = Config::from_dsn("postgresql://localhost/test");
//! config.validation_config = ValidationConfig {
//!     max_payload_size_bytes: 64 * 1024,    // 64KB
//!     required_keys: vec!["user_id".to_string()],
//!     max_enqueue_per_second: Some(100),
//!     ..Default::default()
//! };
//!
//! let store = pgqrs::connect_with_config(&config).await?;
//! pgqrs::admin(&store).install().await?;
//! pgqrs::admin(&store).create_queue("my_queue").await?;
//!
//! let producer = pgqrs::producer("localhost", 8080, "my_queue")
//!     .create(&store)
//!     .await?;
//!
//! // This will be validated automatically
//! let payload = json!({"user_id": "123", "data": "test"});
//! let message = producer.enqueue(&payload).await?;
//! # Ok(())
//! # }
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
    /// Maximum depth of nested objects/arrays to prevent JSON bombs.
    ///
    /// The depth limit controls how many levels of nesting are allowed. Each time
    /// we recurse into an object or array, depth increases by 1. The depth check
    /// is applied before processing each value, not just at each object level.
    ///
    /// Examples with `max_object_depth = 2`:
    /// - ✅ Allowed: `{"key": "value"}` - value at depth 1
    /// - ✅ Allowed: `{"outer": {"inner": "value"}}` - value at depth 2
    /// - ❌ Rejected: `{"l1": {"l2": {"l3": "value"}}}` - value at depth 3
    ///
    /// Default of 5 allows reasonable API nesting while protecting against deeply nested JSON bombs.
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
            max_object_depth: 5,                 // Allow reasonable nesting depth for most APIs
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

        // Single-pass validation without rate limiting
        validator.validate_single_pass(payload, 0, true)?;
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
    /// - Quick structure and content validation first (prevent CPU abuse)
    /// - Rate limiting (after basic validation to prevent resource waste)
    /// - Size checks (expensive serialization) done last
    ///
    /// # Arguments
    /// * `payload` - JSON payload to validate
    ///
    /// # Returns
    /// * `Ok(())` if the payload passes all validation rules
    /// * `Err(PgqrsError)` if validation fails
    pub fn validate(&self, payload: &serde_json::Value) -> Result<()> {
        // 1. Fast structure and content validation first (prevents CPU abuse from malformed payloads)
        self.validate_single_pass(payload, 0, true)?;

        // 2. Rate limit check (after we know the payload structure is reasonable)
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire() {
                return Err(crate::error::Error::RateLimited {
                    retry_after: Duration::from_secs(1),
                });
            }
        }

        // 3. Size check last (expensive serialization, only if structure is valid and rate limit passed)
        self.validate_size(payload)?;

        Ok(())
    }

    /// Validate multiple payloads atomically for batch operations.
    ///
    /// This method validates all payloads with atomic rate limit consumption.
    /// Validation order:
    /// 1. Structure/content validation for all payloads (fast checks)
    /// 2. Rate limit tokens consumed atomically for entire batch
    /// 3. Size validation for all payloads (expensive serialization)
    ///
    /// If structure/content validation fails, no tokens are consumed.
    /// If rate limit is exceeded, no tokens are consumed.
    /// If size validation fails after rate limit consumption, tokens are lost.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to validate
    ///
    /// # Returns
    /// * `Ok(())` if all payloads pass validation rules
    /// * `Err(PgqrsError)` if any payload fails validation
    pub fn validate_batch(&self, payloads: &[serde_json::Value]) -> Result<()> {
        // 1. First validate all payload structures (fast validation to prevent CPU abuse)
        for (index, payload) in payloads.iter().enumerate() {
            let res = self.validate_single_pass(payload, 0, true);
            if payloads.len() > 1 {
                res.map_err(|e| match e {
                    crate::error::Error::ValidationFailed { reason } => {
                        crate::error::Error::ValidationFailed {
                            reason: format!("Payload at index {}: {}", index, reason),
                        }
                    }
                    other => other,
                })?;
            } else {
                res?;
            }
        }

        // 2. Then check rate limit capacity for entire batch (atomic consumption)
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire_multiple(payloads.len() as u32) {
                return Err(crate::error::Error::RateLimited {
                    retry_after: Duration::from_secs(1),
                });
            }
        }

        // 3. Finally do expensive size validation (only if structure valid and rate limit passed)
        for (index, payload) in payloads.iter().enumerate() {
            let res = self.validate_size(payload);
            if payloads.len() > 1 {
                res.map_err(|e| match e {
                    crate::error::Error::PayloadTooLarge {
                        actual_bytes,
                        max_bytes,
                    } => crate::error::Error::ValidationFailed {
                        reason: format!(
                            "Payload at index {} too large: {} bytes exceeds limit {}",
                            index, actual_bytes, max_bytes
                        ),
                    },
                    other => other,
                })?;
            } else {
                res?;
            }
        }

        Ok(())
    }

    /// Validate the serialized size of the payload.
    fn validate_size(&self, payload: &serde_json::Value) -> Result<()> {
        let serialized = serde_json::to_string(payload)?;
        let size = serialized.len();

        if size > self.config.max_payload_size_bytes {
            return Err(crate::error::Error::PayloadTooLarge {
                actual_bytes: size,
                max_bytes: self.config.max_payload_size_bytes,
            });
        }

        Ok(())
    }

    /// Optimized single-pass validation of structure and content.
    ///
    /// This method combines depth checking, string length validation, and forbidden/required
    /// key checking in a single tree traversal for optimal performance.
    ///
    /// # Arguments
    /// * `payload` - JSON value to validate
    /// * `depth` - Current nesting depth
    /// * `is_top_level` - Whether this is the root level (for required key checking)
    fn validate_single_pass(
        &self,
        payload: &serde_json::Value,
        depth: usize,
        is_top_level: bool,
    ) -> Result<()> {
        // Check depth limit first
        if depth > self.config.max_object_depth {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Object depth {} exceeds limit {}",
                    depth, self.config.max_object_depth
                ),
            });
        }

        match payload {
            serde_json::Value::Object(obj) => {
                // Check forbidden keys at every object level (security fix)
                if !self.config.forbidden_keys.is_empty() {
                    for forbidden in &self.config.forbidden_keys {
                        if obj.contains_key(forbidden) {
                            return Err(crate::error::Error::ValidationFailed {
                                reason: format!("Forbidden key '{}' found in payload", forbidden),
                            });
                        }
                    }
                }

                // Check required keys only at the top level
                if is_top_level && !self.config.required_keys.is_empty() {
                    for required in &self.config.required_keys {
                        if !obj.contains_key(required) {
                            return Err(crate::error::Error::ValidationFailed {
                                reason: format!("Required key '{}' missing from payload", required),
                            });
                        }
                    }
                }

                // Check key lengths and recurse into values
                for (key, value) in obj {
                    if key.len() > self.config.max_string_length {
                        return Err(crate::error::Error::ValidationFailed {
                            reason: format!(
                                "Key '{}' length {} exceeds limit {}",
                                key,
                                key.len(),
                                self.config.max_string_length
                            ),
                        });
                    }
                    // Recurse into nested structures
                    self.validate_single_pass(value, depth + 1, false)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    self.validate_single_pass(item, depth + 1, false)?;
                }
            }
            serde_json::Value::String(s) => {
                if s.len() > self.config.max_string_length {
                    return Err(crate::error::Error::ValidationFailed {
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
        assert_eq!(config.max_object_depth, 5);
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

        // Test top-level forbidden key
        let invalid_payload = json!({"__proto__": "malicious"});
        assert!(config.validate_payload(&invalid_payload).is_err());

        // Test nested forbidden key (security fix)
        let nested_invalid_payload = json!({"data": {"__proto__": "malicious"}});
        assert!(config.validate_payload(&nested_invalid_payload).is_err());

        // Test deeply nested forbidden key
        let deep_nested_invalid = json!({"level1": {"level2": {"__proto__": "malicious"}}});
        assert!(config.validate_payload(&deep_nested_invalid).is_err());
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
