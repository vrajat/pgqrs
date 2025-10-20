use pgqrs_server::api::{LivenessRequest, ReadinessRequest, queue_service_server::QueueServiceServer, queue_service_client::QueueServiceClient};
use pgqrs_server::service::QueueServiceImpl;
use pgqrs_core::traits::{QueueRepo, MessageRepo, Queue, QueueMessage, QueueStats};
use pgqrs_core::error::PgqrsError;
use tonic::transport::Server;
use tonic::Request;
use std::sync::Arc;
use async_trait::async_trait;
use sqlx::types::JsonValue;
use tokio::time::{Duration, sleep};
use std::net::SocketAddr;

// Mock repository implementations for testing
#[derive(Clone)]
struct MockQueueRepo {
    should_fail: bool,
}

#[derive(Clone)]
struct MockMessageRepo;

#[async_trait]
impl QueueRepo for MockQueueRepo {
    async fn list_queues(&self) -> Result<Vec<Queue>, PgqrsError> {
        if self.should_fail {
            Err(PgqrsError::Other("Database connection failed".to_string()))
        } else {
            Ok(vec![])
        }
    }
    
    async fn create_queue(&self, _name: &str, _unlogged: bool) -> Result<Queue, PgqrsError> {
        unimplemented!()
    }
    
    async fn get_queue(&self, _name: &str) -> Result<Queue, PgqrsError> {
        unimplemented!()
    }
    
    async fn delete_queue(&self, _name: &str) -> Result<(), PgqrsError> {
        unimplemented!()
    }
    
    async fn purge_queue(&self, _name: &str) -> Result<(), PgqrsError> {
        unimplemented!()
    }
}

#[async_trait]
impl MessageRepo for MockMessageRepo {
    async fn enqueue(&self, _queue: &str, _payload: &JsonValue) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }
    
    async fn enqueue_delayed(&self, _queue: &str, _payload: &JsonValue, _delay_seconds: u32) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }
    
    async fn batch_enqueue(&self, _queue: &str, _payloads: &[JsonValue]) -> Result<Vec<QueueMessage>, PgqrsError> {
        unimplemented!()
    }
    
    async fn dequeue(&self, _queue: &str, _message_id: i64) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }
    
    async fn ack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        unimplemented!()
    }
    
    async fn nack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        unimplemented!()
    }
    
    async fn peek(&self, _queue: &str, _limit: usize) -> Result<Vec<QueueMessage>, PgqrsError> {
        unimplemented!()
    }
    
    async fn stats(&self, _queue: &str) -> Result<QueueStats, PgqrsError> {
        unimplemented!()
    }
    
    async fn get_message_by_id(&self, _queue: &str, _message_id: i64) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }
    
    async fn heartbeat(&self, _queue: &str, _message_id: i64, _additional_seconds: u32) -> Result<(), PgqrsError> {
        unimplemented!()
    }
}

// Helper to start an in-process gRPC server with mock dependencies
async fn start_test_server(healthy_db: bool) -> (SocketAddr, tokio::task::JoinHandle<Result<(), tonic::transport::Error>>) {
    let queue_repo = Arc::new(MockQueueRepo { should_fail: !healthy_db });
    let message_repo = Arc::new(MockMessageRepo);
    
    let service = QueueServiceImpl {
        queue_repo,
        message_repo,
    };
    
    let svc = QueueServiceServer::new(service);
    
    // Bind to any available port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();
    
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    
    // Wait for server to be ready by attempting to connect
    wait_for_server_ready(actual_addr).await;
    
    (actual_addr, server_handle)
}

/// Wait for the server to be ready by attempting connections with exponential backoff
async fn wait_for_server_ready(addr: SocketAddr) {
    let max_attempts = 10;
    let mut attempt = 0;
    let base_delay = Duration::from_millis(10);
    
    while attempt < max_attempts {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => return, // Server is ready
            Err(_) => {
                attempt += 1;
                let delay = base_delay * (2_u32.pow(attempt.min(4))); // Exponential backoff, cap at 16x
                sleep(Duration::from_millis(delay.as_millis().min(200) as u64)).await;
            }
        }
    }
    
    panic!("Server failed to start within reasonable time");
}

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
    let mut client = QueueServiceClient::connect(format!("http://{}", addr))
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
    
    let mut client = QueueServiceClient::connect(format!("http://{}", addr))
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
    
    let mut client = QueueServiceClient::connect(format!("http://{}", addr))
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
    
    let mut client = QueueServiceClient::connect(format!("http://{}", addr))
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
    
    let mut client = QueueServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to test server");
    
    let start = std::time::Instant::now();
    
    // Make multiple rapid liveness calls
    for _ in 0..10 {
        let response = client.liveness(Request::new(LivenessRequest {}))
            .await
            .expect("Liveness call should succeed");
        
        assert_eq!(response.into_inner().status, "OK");
    }
    
    let duration = start.elapsed();
    // 10 calls should complete in under 500ms (50ms per call average)
    assert!(duration < Duration::from_millis(500), "Liveness calls took too long: {:?}", duration);
}