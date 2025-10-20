use pgqrs_server::api::{LivenessRequest, ReadinessRequest, queue_service_client::QueueServiceClient};
use pgqrs_test_utils::{start_test_server, test_endpoint, assert_performance, PERFORMANCE_TEST_ITERATIONS, PERFORMANCE_TEST_TIMEOUT};
use tonic::Request;

/// Test proto message creation (basic smoke test)
#[tokio::test]
async fn test_health_check_proto_messages() {
    let _liveness_req = LivenessRequest {};
    let _readiness_req = ReadinessRequest {};
    // Proto messages can be created successfully
}

/// Integration test: Liveness probe with healthy dependencies
#[tokio::test]
async fn test_liveness_probe_integration() {
    let (addr, _server_handle) = start_test_server(true).await;
    
    // Connect to the server
    let mut client = QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server");
    
    // Test liveness probe
    let response = client.liveness(Request::new(LivenessRequest {}))
        .await
        .expect("Liveness call should succeed");
    
    let liveness_response = response.into_inner();
    assert_eq!(liveness_response.status, "OK");
}

/// Integration test: Liveness probe with failing dependencies (should still return OK)
#[tokio::test]
async fn test_liveness_probe_with_failing_db() {
    let (addr, _server_handle) = start_test_server(false).await; // Unhealthy DB
    
    let mut client = QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server");
    
    // Liveness should return OK even with failing database
    let response = client.liveness(Request::new(LivenessRequest {}))
        .await
        .expect("Liveness call should succeed even with failing DB");
    
    let liveness_response = response.into_inner();
    assert_eq!(liveness_response.status, "OK");
}

/// Integration test: Readiness probe with healthy dependencies
#[tokio::test]
async fn test_readiness_probe_healthy() {
    let (addr, _server_handle) = start_test_server(true).await; // Healthy DB
    
    let mut client = QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server");
    
    // Test readiness probe
    let response = client.readiness(Request::new(ReadinessRequest {}))
        .await
        .expect("Readiness call should succeed");
    
    let readiness_response = response.into_inner();
    assert_eq!(readiness_response.status, "OK");
    assert!(readiness_response.failing_services.is_empty());
}

/// Integration test: Readiness probe with failing dependencies
#[tokio::test]
async fn test_readiness_probe_unhealthy() {
    let (addr, _server_handle) = start_test_server(false).await; // Unhealthy DB
    
    let mut client = QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server");
    
    // Test readiness probe with failing database
    let response = client.readiness(Request::new(ReadinessRequest {}))
        .await
        .expect("Readiness call should succeed");
    
    let readiness_response = response.into_inner();
    assert_eq!(readiness_response.status, "FAIL");
    assert!(!readiness_response.failing_services.is_empty());
    assert!(readiness_response.failing_services.contains(&"database".to_string()));
}

/// Performance test: Liveness should be fast (< 50ms)
#[tokio::test]
async fn test_liveness_performance() {
    let (addr, _server_handle) = start_test_server(true).await;
    
    let mut client = QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server");
    
    let start = std::time::Instant::now();
    
    // Make multiple rapid liveness calls
    for _ in 0..PERFORMANCE_TEST_ITERATIONS {
        let response = client.liveness(Request::new(LivenessRequest {}))
            .await
            .expect("Liveness call should succeed");
        
        assert_eq!(response.into_inner().status, "OK");
    }
    
    let duration = start.elapsed();
    assert_performance(duration, PERFORMANCE_TEST_TIMEOUT, PERFORMANCE_TEST_ITERATIONS);
}