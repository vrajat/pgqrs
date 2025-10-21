use pgqrs_client::{LivenessRequest, PgqrsClient, ReadinessRequest};
use pgqrs_test_utils::{
    assert_performance, start_test_server, test_endpoint, Duration, SocketAddr,
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_RPC_TIMEOUT, PERFORMANCE_TEST_ITERATIONS,
    PERFORMANCE_TEST_TIMEOUT,
};

/// Helper to create a client connected to the test server
async fn create_test_client(addr: SocketAddr) -> PgqrsClient {
    PgqrsClient::builder()
        .endpoint(test_endpoint(addr))
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .rpc_timeout(DEFAULT_RPC_TIMEOUT)
        .build()
        .await
        .expect("Failed to create test client")
}

/// Test proto message creation (basic smoke test)
#[tokio::test]
async fn test_health_check_proto_messages() {
    let _liveness_req = LivenessRequest {};
    let _readiness_req = ReadinessRequest {};
    // Proto messages can be created successfully
}

/// Integration test: Liveness probe with healthy dependencies using pgqrs-client
#[tokio::test]
async fn test_liveness_probe_integration() {
    let (addr, _server_handle) = start_test_server(true).await;

    // Connect using our client
    let mut client = create_test_client(addr).await;

    // Test liveness probe using client
    let response = client
        .liveness()
        .await
        .expect("Liveness call should succeed");

    assert_eq!(response.status, "OK");
}

/// Integration test: Liveness probe with failing dependencies (should still return OK)
#[tokio::test]
async fn test_liveness_probe_with_failing_db() {
    let (addr, _server_handle) = start_test_server(false).await; // Unhealthy DB

    let mut client = create_test_client(addr).await;

    // Liveness should return OK even with failing database
    let response = client
        .liveness()
        .await
        .expect("Liveness call should succeed even with failing DB");

    assert_eq!(response.status, "OK");
}

/// Integration test: Readiness probe with healthy dependencies
#[tokio::test]
async fn test_readiness_probe_healthy() {
    let (addr, _server_handle) = start_test_server(true).await; // Healthy DB

    let mut client = create_test_client(addr).await;

    // Test readiness probe
    let response = client
        .readiness()
        .await
        .expect("Readiness call should succeed");

    assert_eq!(response.status, "OK");
    assert!(response.failing_services.is_empty());
}

/// Integration test: Readiness probe with failing dependencies
#[tokio::test]
async fn test_readiness_probe_unhealthy() {
    let (addr, _server_handle) = start_test_server(false).await; // Unhealthy DB

    let mut client = create_test_client(addr).await;

    // Test readiness probe with failing database
    let response = client
        .readiness()
        .await
        .expect("Readiness call should succeed");

    assert_eq!(response.status, "FAIL");
    assert!(!response.failing_services.is_empty());
    assert!(response.failing_services.contains(&"database".to_string()));
}

/// Performance test: Liveness should be fast (< 50ms per call)
#[tokio::test]
async fn test_liveness_performance() {
    let (addr, _server_handle) = start_test_server(true).await;

    let mut client = create_test_client(addr).await;

    let start = std::time::Instant::now();

    // Make multiple rapid liveness calls
    for _ in 0..PERFORMANCE_TEST_ITERATIONS {
        let response = client
            .liveness()
            .await
            .expect("Liveness call should succeed");

        assert_eq!(response.status, "OK");
    }

    let duration = start.elapsed();
    assert_performance(
        duration,
        PERFORMANCE_TEST_TIMEOUT,
        PERFORMANCE_TEST_ITERATIONS,
    );
}

/// Test client builder configuration
#[tokio::test]
async fn test_client_builder_configuration() {
    let (addr, _server_handle) = start_test_server(true).await;

    // Test builder with various configurations
    let mut client = PgqrsClient::builder()
        .endpoint(test_endpoint(addr))
        .api_key("test-key")
        .connect_timeout(Duration::from_secs(1))
        .rpc_timeout(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to build client with configuration");

    // Verify client works
    let response = client.liveness().await.expect("Liveness should work");
    assert_eq!(response.status, "OK");
}

/// Test client error handling
#[tokio::test]
async fn test_client_error_handling() {
    // Test connection to non-existent server
    let result = PgqrsClient::builder()
        .endpoint("http://127.0.0.1:9999") // Non-existent port
        .connect_timeout(Duration::from_millis(100))
        .build()
        .await;

    assert!(
        result.is_err(),
        "Should fail to connect to non-existent server"
    );
}

/// Test client timeout configuration
#[tokio::test]
async fn test_client_timeout_configuration() {
    let (addr, _server_handle) = start_test_server(true).await;

    // Test with very short timeout
    let mut client = PgqrsClient::builder()
        .endpoint(test_endpoint(addr))
        .rpc_timeout(Duration::from_nanos(1)) // Very short timeout
        .build()
        .await
        .expect("Client should build successfully");

    // This might fail due to timeout, but shouldn't panic
    let _result = client.liveness().await;
    // We don't assert success here as the timeout might be too aggressive
}
