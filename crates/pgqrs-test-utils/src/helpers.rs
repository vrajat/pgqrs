use tokio::time::Duration;
use std::net::SocketAddr;

/// Common test timeouts and constants
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
pub const PERFORMANCE_TEST_TIMEOUT: Duration = Duration::from_millis(500);
pub const PERFORMANCE_TEST_ITERATIONS: usize = 10;

/// Helper to create a test client endpoint URL
pub fn test_endpoint(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

/// Performance test helper that measures execution time
pub async fn measure_performance<F, Fut, T>(iterations: usize, operation: F) -> (Duration, Vec<T>)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let start = std::time::Instant::now();
    let mut results = Vec::with_capacity(iterations);
    
    for _ in 0..iterations {
        let result = operation().await;
        results.push(result);
    }
    
    let duration = start.elapsed();
    (duration, results)
}

/// Assert that a duration is within acceptable performance bounds
pub fn assert_performance(duration: Duration, max_duration: Duration, iterations: usize) {
    assert!(
        duration < max_duration,
        "Performance test failed: {} iterations took {:?}, expected less than {:?} (avg: {:?} per call)",
        iterations,
        duration,
        max_duration,
        duration / iterations as u32
    );
}