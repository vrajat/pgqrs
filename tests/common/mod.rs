// Common utilities for tests
// Note: Functionality not yet implemented in pgqrs-server
// This is a placeholder for future implementation

use pgqrs_server::api::queue_service_client::QueueServiceClient;
use pgqrs_test_utils::{start_test_server, test_endpoint};
use std::fs;
use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::sleep;

pub async fn get_test_client() -> QueueServiceClient<tonic::transport::Channel> {
    let (addr, _server_handle) = start_test_server(true).await;

    QueueServiceClient::connect(test_endpoint(addr))
        .await
        .expect("Failed to connect to test server")
}

/// Connect to a pgqrs-server running on a specific port
pub async fn get_client_for_port(
    port: u16,
) -> Result<QueueServiceClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    let endpoint = format!("http://127.0.0.1:{}", port);

    // Wait a bit for the server to be ready and try to connect
    for _attempt in 0..10 {
        match QueueServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return Ok(client),
            Err(_) => {
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err("Failed to connect to server after multiple attempts".into())
}

/// Find an available port by binding to port 0 and letting the OS choose
fn find_available_port() -> Result<u16, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Helper function to start the pgqrs-server with a custom database URL
///
/// This function:
/// 1. Finds an available port
/// 2. Writes a temporary config file with the provided DSN
/// 3. Starts the server using `cargo run` with the chosen port
/// 4. Waits for the server to be ready
/// 5. Returns the process handle and port number for cleanup
///
/// # Example
/// ```rust
/// use tests::common;
///
/// #[tokio::test]
/// async fn example_usage() {
///     let dsn = "postgresql://user:pass@localhost/testdb";
///
///     // Start server and get both process and port
///     let (server_process, port) = common::start_server(dsn).await.unwrap();
///     println!("Server started on port: {}", port);
///
///     // Connect to the server using the port
///     let mut client = common::get_client_for_port(port).await.unwrap();
///
///     // Use the client...
///
///     // Clean up
///     common::stop_server(server_process).unwrap();
/// }
/// ```
pub async fn start_server(dsn: &str) -> Result<(Child, u16), Box<dyn std::error::Error>> {
    // Find an available port
    let port = find_available_port()?;
    // Create a temporary config file
    let config_content = format!(
        r#"database:
  database_url: "{}"
  max_connections: 10
  visibility_timeout: 30
  dead_letter_after: 5
"#,
        dsn
    );

    let config_path = "/tmp/pgqrs_test_config.yaml";
    fs::write(config_path, config_content)?;

    // Start the server process with the chosen port
    let server_addr = format!("127.0.0.1:{}", port);
    let mut child = Command::new("cargo")
        .args([
            "run",
            "--package",
            "pgqrs-server",
            "--bin",
            "pgqrs-server",
            "--",
            "--config-path",
            config_path,
            "start",
        ])
        .env("PGQRS_ADDR", &server_addr)
        .spawn()?;

    // Give the server a moment to start up
    // Note: In a real implementation, you might want to poll the health endpoint
    sleep(Duration::from_secs(2)).await;

    // Check if process is still running (didn't exit immediately with error)
    match child.try_wait()? {
        Some(exit_status) => {
            return Err(format!("Server exited immediately with status: {}", exit_status).into());
        }
        None => {
            // Process is still running, which is good
        }
    }

    Ok((child, port))
}

/// Helper function to stop the server and clean up resources
pub fn stop_server(mut child: Child) -> Result<(), Box<dyn std::error::Error>> {
    // Terminate the server process
    child.kill()?;
    child.wait()?;

    // Clean up the temporary config file
    let config_path = "/tmp/pgqrs_test_config.yaml";
    if std::path::Path::new(config_path).exists() {
        fs::remove_file(config_path)?;
    }

    Ok(())
}
