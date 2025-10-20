use crate::mocks::{MockMessageRepo, MockQueueRepo};
use pgqrs_server::api::queue_service_server::QueueServiceServer;
use pgqrs_server::service::QueueServiceImpl;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tonic::transport::Server;

/// Start a test gRPC server with mock dependencies
///
/// # Arguments
/// * `healthy_db` - Whether the database mock should be healthy or failing
///
/// # Returns
/// * `SocketAddr` - The address the server is listening on
/// * `JoinHandle` - Handle to the server task
pub async fn start_test_server(
    healthy_db: bool,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let queue_repo = if healthy_db {
        Arc::new(MockQueueRepo::healthy())
    } else {
        Arc::new(MockQueueRepo::failing())
    };
    let message_repo = Arc::new(MockMessageRepo::new());

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

    // Wait for server to be ready
    wait_for_server_ready(actual_addr).await;

    (actual_addr, server_handle)
}

/// Wait for the server to be ready by attempting connections with exponential backoff
///
/// # Arguments
/// * `addr` - The server address to check
///
/// # Panics
/// Panics if the server doesn't become ready within a reasonable time
pub async fn wait_for_server_ready(addr: SocketAddr) {
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
