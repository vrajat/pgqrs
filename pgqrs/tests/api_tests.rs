use pgqrs::{LivenessRequest, PgqrsClient, ReadinessRequest};
use pgqrs_test_utils::{
    start_test_server_with_postgres, test_endpoint, SocketAddr, DEFAULT_CONNECT_TIMEOUT,
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

/// Create, list, get and delete queues with real PostgreSQL backend
#[tokio::test]
async fn test_queue_crud_with_postgres() {
    let (addr, _server_handle) = start_test_server_with_postgres().await;
    let mut client = create_test_client(addr).await;
    let queue_name = "test_queue_crud_with_postgres".to_string();
    // Create queue
    let queue_result = client.create_queue(&queue_name, false).await;
    assert!(queue_result.is_ok());
    let queue = queue_result.unwrap();
    assert_eq!(queue.name, queue_name);
    assert!(queue.id > 0);
    assert!(queue.created_at_unix <= chrono::Utc::now().timestamp());
    assert!(!queue.unlogged);

    // Get queue
    let get_result = client.get_queue(&queue_name).await;
    assert!(get_result.is_ok());
    let get_queue = get_result.unwrap();
    assert_eq!(get_queue.name, queue_name);
    assert_eq!(get_queue.id, queue.id); // IDs should match
    assert_eq!(get_queue.created_at_unix, queue.created_at_unix);
    assert_eq!(get_queue.unlogged, queue.unlogged);

    // List queues
    let list_result = client.list_queues().await;
    assert!(list_result.is_ok());
    let queues = list_result.unwrap();
    let found = queues.iter().find(|q| q.name == queue_name);
    assert!(found.is_some());
    let listed_queue = found.unwrap();
    assert_eq!(listed_queue.id, queue.id);
    assert_eq!(listed_queue.created_at_unix, queue.created_at_unix);
    assert_eq!(listed_queue.unlogged, queue.unlogged);
    // Delete queue
    let delete_result = client.delete_queue(&queue_name).await;
    assert!(delete_result.is_ok());
    let listed_after_delete = client.list_queues().await.unwrap();
    let found_after_delete = listed_after_delete.iter().find(|q| q.name == queue_name);
    assert!(found_after_delete.is_none());
}
