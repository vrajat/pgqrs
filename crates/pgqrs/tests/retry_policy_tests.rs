//! Unit tests for retry policy logic
//!
//! Tests the `StepRetryPolicy` and `BackoffStrategy` types to ensure
//! correct retry behavior and backoff calculations.

use pgqrs::types::{BackoffStrategy, StepRetryPolicy};

#[test]
fn test_fixed_backoff() {
    let policy = StepRetryPolicy {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed { delay_seconds: 5 },
    };

    // Fixed backoff should return the same delay for all attempts
    assert_eq!(policy.calculate_delay(0), 5);
    assert_eq!(policy.calculate_delay(1), 5);
    assert_eq!(policy.calculate_delay(2), 5);
    assert_eq!(policy.calculate_delay(10), 5);
}

#[test]
fn test_exponential_backoff() {
    let policy = StepRetryPolicy {
        max_attempts: 5,
        backoff: BackoffStrategy::Exponential {
            base_seconds: 2,
            max_seconds: 60,
        },
    };

    // Exponential: delay = base * 2^attempt
    assert_eq!(policy.calculate_delay(0), 2); // 2 * 2^0 = 2
    assert_eq!(policy.calculate_delay(1), 4); // 2 * 2^1 = 4
    assert_eq!(policy.calculate_delay(2), 8); // 2 * 2^2 = 8
    assert_eq!(policy.calculate_delay(3), 16); // 2 * 2^3 = 16
    assert_eq!(policy.calculate_delay(4), 32); // 2 * 2^4 = 32
}

#[test]
fn test_exponential_backoff_caps_at_max() {
    let policy = StepRetryPolicy {
        max_attempts: 10,
        backoff: BackoffStrategy::Exponential {
            base_seconds: 2,
            max_seconds: 30,
        },
    };

    // Should cap at max_seconds
    assert_eq!(policy.calculate_delay(0), 2); // 2
    assert_eq!(policy.calculate_delay(1), 4); // 4
    assert_eq!(policy.calculate_delay(2), 8); // 8
    assert_eq!(policy.calculate_delay(3), 16); // 16
    assert_eq!(policy.calculate_delay(4), 30); // 32 capped to 30
    assert_eq!(policy.calculate_delay(5), 30); // 64 capped to 30
    assert_eq!(policy.calculate_delay(10), 30); // Still capped
}

#[test]
fn test_exponential_with_jitter() {
    let policy = StepRetryPolicy {
        max_attempts: 5,
        backoff: BackoffStrategy::ExponentialWithJitter {
            base_seconds: 2,
            max_seconds: 60,
        },
    };

    // With jitter, delays should vary but stay within bounds
    for attempt in 0..5 {
        let delay = policy.calculate_delay(attempt);
        let base_delay = 2u32.saturating_mul(2u32.pow(attempt));
        let capped = base_delay.min(60);

        // Jitter is ±25%, so delay should be within [75% to 125%] of base
        let min_expected = capped.saturating_sub(capped / 4);
        let max_expected = capped.saturating_add(capped / 4);

        assert!(
            delay >= min_expected && delay <= max_expected,
            "Attempt {}: delay {} not in range [{}, {}]",
            attempt,
            delay,
            min_expected,
            max_expected
        );
    }
}

#[test]
fn test_should_retry() {
    let policy = StepRetryPolicy {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed { delay_seconds: 1 },
    };

    // With max_attempts = 3, we can retry on attempts 0, 1, 2
    assert!(policy.should_retry(0)); // First retry
    assert!(policy.should_retry(1)); // Second retry
    assert!(policy.should_retry(2)); // Third retry
    assert!(!policy.should_retry(3)); // Exhausted
    assert!(!policy.should_retry(4)); // Still exhausted
}

#[test]
fn test_should_retry_zero_attempts() {
    let policy = StepRetryPolicy {
        max_attempts: 0,
        backoff: BackoffStrategy::Fixed { delay_seconds: 1 },
    };

    // No retries allowed
    assert!(!policy.should_retry(0));
    assert!(!policy.should_retry(1));
}

#[test]
fn test_default_policy() {
    let policy = StepRetryPolicy::default();

    // Default: 3 attempts, exponential with jitter
    assert_eq!(policy.max_attempts, 3);
    assert!(policy.should_retry(0));
    assert!(policy.should_retry(1));
    assert!(policy.should_retry(2));
    assert!(!policy.should_retry(3));

    // Default backoff should be ExponentialWithJitter
    match policy.backoff {
        BackoffStrategy::ExponentialWithJitter {
            base_seconds,
            max_seconds,
        } => {
            assert_eq!(base_seconds, 1);
            assert_eq!(max_seconds, 60);
        }
        _ => panic!("Default backoff should be ExponentialWithJitter"),
    }
}

#[test]
fn test_transient_error_creation() {
    use pgqrs::error::TransientStepError;
    use std::time::Duration;

    // Basic creation
    let err = TransientStepError::new("TIMEOUT", "Connection timeout");
    assert_eq!(err.code, "TIMEOUT");
    assert_eq!(err.message, "Connection timeout");
    assert!(err.source.is_none());
    assert!(err.retry_after.is_none());

    // With custom delay
    let err = TransientStepError::new("RATE_LIMITED", "Too many requests")
        .with_delay(Duration::from_secs(60));
    assert_eq!(err.code, "RATE_LIMITED");
    assert_eq!(err.retry_after, Some(Duration::from_secs(60)));
}

#[test]
fn test_transient_error_display() {
    use pgqrs::error::TransientStepError;

    let err = TransientStepError::new("CONNECTION_FAILED", "Network unreachable");
    assert_eq!(err.to_string(), "CONNECTION_FAILED: Network unreachable");
}

#[test]
fn test_backoff_overflow_protection() {
    let policy = StepRetryPolicy {
        max_attempts: 100,
        backoff: BackoffStrategy::Exponential {
            base_seconds: u32::MAX / 2,
            max_seconds: 3600,
        },
    };

    // Should not panic on overflow, should saturate at max_seconds
    let delay = policy.calculate_delay(50);
    assert_eq!(delay, 3600); // Should cap at max_seconds
}
