//! In-memory rate limiting for service protection.
//!
//! This module provides a token bucket implementation for rate limiting
//! enqueue operations to protect services from abuse.
//!
//! ## What
//!
//! - [`TokenBucket`] implements a thread-safe token bucket algorithm
//! - [`RateLimitStatus`] provides debugging information about rate limit state
//!
//! ## How
//!
//! Rate limiting is configured through the [`ValidationConfig`] in the main [`Config`] struct.
//! It's automatically applied to all enqueue operations.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::{Config, Admin, Producer, ValidationConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = Config::from_dsn("postgresql://localhost/test");
//! config.validation_config.max_enqueue_per_second = Some(100); // 100/second
//! config.validation_config.max_enqueue_burst = Some(10);       // 10 burst capacity
//!
//! let store = pgqrs::connect_with_config(&config).await?;
//! pgqrs::admin(&store).install().await?;
//! pgqrs::admin(&store).create_queue("my_queue").await?;
//!
//! let producer = pgqrs::producer("localhost", 8080, "my_queue")
//!     .create(&store)
//!     .await?;
//!
//! // Rate limiting is automatically applied
//! for i in 0..200 {
//!     match producer.enqueue(&serde_json::json!({"id": i})).await {
//!         Ok(_) => println!("Enqueued message {}", i),
//!         Err(e) => println!("Rate limited: {}", e),
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Thread-safe token bucket for rate limiting.
///
/// Implements a token bucket algorithm where tokens are refilled at a constant rate
/// up to a maximum burst capacity. Operations consume tokens and are blocked when
/// no tokens are available.
pub struct TokenBucket {
    /// Maximum tokens per second to add
    max_per_second: u32,
    /// Maximum number of tokens that can be stored
    burst_capacity: u32,
    /// Current number of available tokens
    tokens: AtomicU32,
    /// Last refill timestamp (nanoseconds since UNIX epoch)
    last_refill: AtomicU64,
}

impl TokenBucket {
    /// Create a new TokenBucket with the specified rate and burst capacity.
    ///
    /// # Arguments
    /// * `max_per_second` - Maximum number of tokens to add per second
    /// * `burst_capacity` - Maximum number of tokens that can be stored (must be at least 1)
    ///
    /// # Panics
    /// Panics if `burst_capacity` is 0, which would cause division by zero in rate limit calculations.
    ///
    /// # Example
    /// ```rust
    /// // This is an internal implementation detail.
    /// // Configure rate limiting through ValidationConfig instead:
    /// use pgqrs::{Config, ValidationConfig};
    ///
    /// let mut config = Config::from_dsn("postgresql://localhost/test");
    /// config.validation_config.max_enqueue_per_second = Some(1000);
    /// config.validation_config.max_enqueue_burst = Some(50);
    /// ```
    pub fn new(max_per_second: u32, burst_capacity: u32) -> Self {
        if burst_capacity == 0 {
            panic!("burst_capacity must be at least 1 to prevent division by zero");
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            max_per_second,
            burst_capacity,
            tokens: AtomicU32::new(burst_capacity),
            last_refill: AtomicU64::new(now),
        }
    }

    /// Try to acquire a token (non-blocking).
    ///
    /// This method attempts to consume one token from the bucket. If no tokens
    /// are available, it returns false immediately without blocking.
    ///
    /// # Returns
    /// * `true` if a token was successfully acquired
    /// * `false` if no tokens are available (rate limited)
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_multiple(1)
    }

    /// Try to acquire multiple tokens atomically (non-blocking).
    ///
    /// This method attempts to consume the specified number of tokens from the bucket.
    /// If insufficient tokens are available, it returns false without consuming any.
    ///
    /// # Arguments
    /// * `count` - Number of tokens to acquire
    ///
    /// # Returns
    /// * `true` if all tokens were successfully acquired
    /// * `false` if insufficient tokens are available (rate limited)
    pub fn try_acquire_multiple(&self, count: u32) -> bool {
        self.refill_tokens();

        // Try to atomically decrement the token count
        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            if current_tokens < count {
                return false;
            }

            // Try to decrement atomically
            match self.tokens.compare_exchange_weak(
                current_tokens,
                current_tokens - count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => {
                    // Another thread modified the count, retry
                    continue;
                }
            }
        }
    }

    /// Refill tokens based on elapsed time.
    ///
    /// This method calculates how many tokens should be added based on the
    /// time elapsed since the last refill and updates the token count atomically.
    fn refill_tokens(&self) {
        // Guard against division by zero
        if self.max_per_second == 0 {
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let last_refill = self.last_refill.load(Ordering::Acquire);
        let elapsed_nanos = now.saturating_sub(last_refill);
        let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;

        // Calculate minimum time between token additions
        let min_interval = 1.0 / self.max_per_second as f64;

        if elapsed_secs >= min_interval {
            let tokens_to_add = (elapsed_secs * self.max_per_second as f64) as u32;

            if tokens_to_add > 0 {
                // Update timestamp first to prevent race conditions
                // This ensures other threads see the updated time before token changes
                self.last_refill.store(now, Ordering::Release);

                // Then try to update tokens atomically
                loop {
                    let current_tokens = self.tokens.load(Ordering::Acquire);
                    let new_tokens = (current_tokens + tokens_to_add).min(self.burst_capacity);

                    // Only update if we would actually add tokens
                    if new_tokens > current_tokens {
                        match self.tokens.compare_exchange_weak(
                            current_tokens,
                            new_tokens,
                            Ordering::Release,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                // Successfully updated tokens
                                break;
                            }
                            Err(_) => {
                                // Another thread updated tokens, retry
                                continue;
                            }
                        }
                    } else {
                        // No tokens to add (already at capacity)
                        break;
                    }
                }
            } else {
                // No tokens to add this cycle, but update timestamp to prevent drift
                self.last_refill.store(now, Ordering::Relaxed);
            }
        }
    }

    /// Get current rate limit status for debugging.
    ///
    /// This method provides a snapshot of the current token bucket state,
    /// useful for monitoring and debugging rate limiting behavior.
    ///
    /// # Returns
    /// Current rate limiting status
    pub fn status(&self) -> RateLimitStatus {
        self.refill_tokens();

        RateLimitStatus {
            available_tokens: self.tokens.load(Ordering::Relaxed),
            max_per_second: self.max_per_second,
            burst_capacity: self.burst_capacity,
        }
    }
}

/// Current status of a token bucket for debugging and monitoring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitStatus {
    /// Number of tokens currently available
    pub available_tokens: u32,
    /// Maximum tokens per second
    pub max_per_second: u32,
    /// Maximum burst capacity
    pub burst_capacity: u32,
}

impl RateLimitStatus {
    /// Calculate the percentage of burst capacity currently available.
    ///
    /// # Returns
    /// Percentage (0-100) of burst capacity available
    pub fn utilization_percentage(&self) -> f64 {
        (self.available_tokens as f64 / self.burst_capacity as f64) * 100.0
    }

    /// Check if the bucket is near empty (10% or less capacity).
    pub fn is_near_empty(&self) -> bool {
        self.utilization_percentage() <= 10.0
    }

    /// Check if the bucket is full.
    pub fn is_full(&self) -> bool {
        self.available_tokens == self.burst_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_bucket_has_full_capacity() {
        let bucket = TokenBucket::new(100, 10);
        let status = bucket.status();
        assert_eq!(status.available_tokens, 10);
        assert_eq!(status.max_per_second, 100);
        assert_eq!(status.burst_capacity, 10);
    }

    #[test]
    fn test_try_acquire_depletes_tokens() {
        let bucket = TokenBucket::new(100, 3);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.status().available_tokens, 2);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.status().available_tokens, 1);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.status().available_tokens, 0);

        // Should be rate limited now
        assert!(!bucket.try_acquire());
        assert_eq!(bucket.status().available_tokens, 0);
    }

    #[test]
    fn test_tokens_refill_over_time() {
        let bucket = TokenBucket::new(10, 5); // 10 tokens/second, 5 capacity

        // Deplete all tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert_eq!(bucket.status().available_tokens, 0);

        // Wait for refill (at 10 tokens/second, we should get 1 token every 100ms)
        thread::sleep(Duration::from_millis(150));

        // Should have at least 1 token now
        let status = bucket.status();
        assert!(status.available_tokens >= 1);
    }

    #[test]
    fn test_tokens_dont_exceed_capacity() {
        let bucket = TokenBucket::new(1000, 5); // Very high rate, low capacity

        // Wait longer than needed to fill capacity
        thread::sleep(Duration::from_millis(100));

        let status = bucket.status();
        assert_eq!(status.available_tokens, 5); // Should not exceed capacity
    }

    #[test]
    fn test_concurrent_access() {
        let bucket = std::sync::Arc::new(TokenBucket::new(100, 10));
        let mut handles = vec![];

        // Start multiple threads trying to acquire tokens
        for _ in 0..5 {
            let bucket_clone = bucket.clone();
            let handle = thread::spawn(move || {
                let mut acquired = 0;
                for _ in 0..3 {
                    if bucket_clone.try_acquire() {
                        acquired += 1;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                acquired
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        let mut total_acquired = 0;
        for handle in handles {
            total_acquired += handle.join().unwrap();
        }

        // Should have acquired exactly the initial capacity (10 tokens)
        assert_eq!(total_acquired, 10);
    }

    #[test]
    fn test_rate_limit_status_utilization() {
        let bucket = TokenBucket::new(100, 10);

        // Full capacity
        let status = bucket.status();
        assert!(status.is_full());
        assert_eq!(status.utilization_percentage(), 100.0);
        assert!(!status.is_near_empty());

        // Acquire some tokens
        for _ in 0..9 {
            bucket.try_acquire();
        }

        let status = bucket.status();
        assert!(!status.is_full());
        // Should be exactly 10% (1 out of 10 tokens remaining)
        assert_eq!(status.utilization_percentage(), 10.0);
        assert!(status.is_near_empty());
    }

    #[test]
    fn test_zero_rate_limit() {
        let bucket = TokenBucket::new(0, 5);

        // Should start with full capacity
        assert_eq!(bucket.status().available_tokens, 5);

        // Deplete tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }

        // Wait and ensure no tokens are refilled
        thread::sleep(Duration::from_millis(100));
        assert_eq!(bucket.status().available_tokens, 0);
    }

    #[test]
    #[should_panic(expected = "burst_capacity must be at least 1")]
    fn test_zero_burst_capacity_panics() {
        // Creating a token bucket with zero burst capacity should panic
        TokenBucket::new(100, 0);
    }
}
