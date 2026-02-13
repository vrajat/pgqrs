use serde::{Deserialize, Serialize};

/// Backoff strategy for workflow step retries.
///
/// Determines how long to wait between retry attempts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed {
        /// Delay in seconds
        delay_seconds: u32,
    },
    /// Exponential backoff: delay = base * 2^attempt
    Exponential {
        /// Base delay in seconds
        base_seconds: u32,
        /// Maximum delay in seconds
        max_seconds: u32,
    },
    /// Exponential backoff with jitter (±25%)
    ExponentialWithJitter {
        /// Base delay in seconds
        base_seconds: u32,
        /// Maximum delay in seconds
        max_seconds: u32,
    },
}

/// Retry policy for workflow steps.
///
/// Configures automatic retry behavior when steps fail with transient errors.
///
/// ## What
///
/// Controls step-level retry with:
/// - `max_attempts`: Maximum number of retry attempts
/// - `backoff`: Delay strategy between attempts
///
/// ## How
///
/// ```rust
/// use pgqrs::policy::{StepRetryPolicy, BackoffStrategy};
///
/// // Default: 3 attempts with exponential backoff + jitter
/// let policy = StepRetryPolicy::default();
///
/// // Custom: 5 attempts with fixed 10-second delay
/// let policy = StepRetryPolicy {
///     max_attempts: 5,
///     backoff: BackoffStrategy::Fixed { delay_seconds: 10 },
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRetryPolicy {
    /// Maximum number of retry attempts (0 = no retry, 1 = one retry after initial attempt)
    pub max_attempts: u32,
    /// Backoff strategy for calculating delay between attempts
    pub backoff: BackoffStrategy,
}

impl Default for StepRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::ExponentialWithJitter {
                base_seconds: 1,
                max_seconds: 60,
            },
        }
    }
}

impl StepRetryPolicy {
    /// Calculate delay in seconds for the given retry attempt (0-indexed).
    ///
    /// # Arguments
    ///
    /// * `attempt` - The retry count (0 = before first retry, 1 = after first retry, etc.)
    ///
    /// # Returns
    ///
    /// Delay in seconds before the next retry attempt.
    pub fn calculate_delay(&self, attempt: u32) -> u32 {
        match &self.backoff {
            BackoffStrategy::Fixed { delay_seconds } => *delay_seconds,
            BackoffStrategy::Exponential {
                base_seconds,
                max_seconds,
            } => {
                let delay = base_seconds.saturating_mul(2u32.saturating_pow(attempt));
                delay.min(*max_seconds)
            }
            BackoffStrategy::ExponentialWithJitter {
                base_seconds,
                max_seconds,
            } => {
                let base_delay = base_seconds.saturating_mul(2u32.saturating_pow(attempt));
                let capped_delay = base_delay.min(*max_seconds);

                // Add jitter: ±25% using true randomness to prevent thundering herd
                let jitter_range = capped_delay / 4; // 25% of delay
                if jitter_range == 0 {
                    // No room for jitter on very small delays
                    return capped_delay;
                }

                // Cap jitter_range to i32::MAX to prevent overflow when casting
                // This ensures safe conversion from i64 random value to i32
                let jitter_range_i32 = jitter_range.min(i32::MAX as u32);

                // Use true randomness for jitter to prevent multiple workflows
                // retrying at exactly the same time (thundering herd problem)
                // Use signed arithmetic for true ±25% jitter distribution
                use rand::Rng;
                let jitter = rand::thread_rng()
                    .gen_range(-(jitter_range_i32 as i64)..=(jitter_range_i32 as i64))
                    as i32; // Safe cast: range is within i32::MIN..=i32::MAX

                let with_jitter = capped_delay.saturating_add_signed(jitter);

                // Ensure we don't accidentally schedule an immediate retry when the
                // policy is configured for a non-zero delay. This is specifically
                // important for the default policy where base_seconds = 1.
                with_jitter.max(1)
            }
        }
    }

    /// Check if retry should be attempted for the given attempt number.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The current attempt number (0 = initial attempt, 1 = first retry, etc.)
    ///
    /// # Returns
    ///
    /// `true` if retry should be attempted, `false` if retries are exhausted.
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    /// Extract retry delay from error or calculate from policy.
    ///
    /// Checks the error for a `retry_after` field and uses that if present.
    /// Otherwise, calculates delay using the retry policy's backoff strategy.
    ///
    /// # Arguments
    ///
    /// * `error` - The error value that may contain a `retry_after` field
    /// * `retry_count` - The current retry count (0-indexed)
    ///
    /// # Returns
    ///
    /// Delay in seconds before the next retry attempt.
    pub fn extract_retry_delay(&self, error: &serde_json::Value, retry_count: i32) -> u64 {
        if let Some(retry_after_val) = error.get("retry_after") {
            if let Some(secs) = retry_after_val.as_u64() {
                // Use custom delay from error as plain seconds (e.g., Retry-After header)
                return secs;
            } else if let Some(secs) = retry_after_val.get("secs").and_then(|v| v.as_u64()) {
                // Use custom delay from error when serialized as a Duration { secs, nanos }
                return secs;
            }
        }
        // Use policy backoff
        self.calculate_delay(retry_count as u32) as u64
    }
}

/// Workflow configuration (future use).
///
/// Will be used to configure workflow-level settings including default retry policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowConfig {
    /// Default retry policy for all steps in the workflow
    pub default_step_retry_policy: Option<StepRetryPolicy>,
}
