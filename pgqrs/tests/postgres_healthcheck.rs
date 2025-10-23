use pgqrs::{LivenessRequest, PgqrsClient, ReadinessRequest};
use pgqrs_test_utils::{
    start_test_server_with_postgres, test_endpoint, Duration, SocketAddr, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_RPC_TIMEOUT,
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

/// Integration test: Liveness probe with real PostgreSQL backend
#[tokio::test]
async fn test_liveness_probe_with_postgres() {
    let (addr, _server_handle) = start_test_server_with_postgres().await;

    // Connect using our client
    let mut client = create_test_client(addr).await;

    // Test liveness probe using client
    let response = client
        .liveness()
        .await
        .expect("Liveness call should succeed");

    assert_eq!(response.status, "OK");
}

/// Integration test: Readiness probe with real PostgreSQL backend
#[tokio::test]
async fn test_readiness_probe_with_postgres() {
    let (addr, _server_handle) = start_test_server_with_postgres().await;

    let mut client = create_test_client(addr).await;

    // Test readiness probe - should be healthy since we have real DB
    let response = client
        .readiness()
        .await
        .expect("Readiness call should succeed");

    assert_eq!(response.status, "OK");
    assert!(response.failing_services.is_empty());
}

/// Integration test: Multiple clients against real PostgreSQL
#[tokio::test]
async fn test_multiple_clients_with_postgres() {
    let (addr, _server_handle) = start_test_server_with_postgres().await;

    // Create multiple clients
    let mut client1 = create_test_client(addr).await;
    let mut client2 = create_test_client(addr).await;
    let mut client3 = create_test_client(addr).await;

    // All clients should work simultaneously
    let (response1, response2, response3) =
        tokio::join!(client1.liveness(), client2.liveness(), client3.readiness());

    assert_eq!(response1.expect("Client 1 should succeed").status, "OK");
    assert_eq!(response2.expect("Client 2 should succeed").status, "OK");
    let r3 = response3.expect("Client 3 should succeed");
    assert_eq!(r3.status, "OK");
    assert!(r3.failing_services.is_empty());
}

/// Test proto message creation with PostgreSQL context (smoke test)
#[tokio::test]
async fn test_health_check_proto_messages_with_postgres() {
    let (_addr, _server_handle) = start_test_server_with_postgres().await;

    let _liveness_req = LivenessRequest {};
    let _readiness_req = ReadinessRequest {};
    // Proto messages can be created successfully in PostgreSQL context
}

/// Performance test: Ensure PostgreSQL backend doesn't significantly slow down health checks
#[tokio::test]
async fn test_postgres_performance() {
    let (addr, _server_handle) = start_test_server_with_postgres().await;

    let mut client = create_test_client(addr).await;

    let start = std::time::Instant::now();

    // Make several health check calls
    for _ in 0..10 {
        let response = client
            .liveness()
            .await
            .expect("Liveness call should succeed");

        assert_eq!(response.status, "OK");
    }

    let duration = start.elapsed();

    // Health checks should complete within reasonable time (allowing for DB overhead)
    assert!(
        duration < Duration::from_secs(5),
        "Health checks took too long with PostgreSQL backend: {:?}",
        duration
    );
}
