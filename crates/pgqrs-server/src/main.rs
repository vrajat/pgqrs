use tonic::transport::Server;
use tracing_subscriber::{fmt, EnvFilter};
use std::net::SocketAddr;
use pgqrs_core::pool::create_pool;
use pgqrs_core::config::Config;
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config (could be from env, file, etc.)
    let config = Config {
        database_url: std::env::var("DATABASE_URL")?,
        max_connections: 10,
        visibility_timeout: 30,
        dead_letter_after: 5,
    };
    let pool = create_pool(&config).await?;
    let pool = Arc::new(pool);

    // Setup tracing subscriber for JSON logs
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // TODO: Setup OpenTelemetry metrics, TLS, Auth, etc.

    // Server address
    let addr: SocketAddr = std::env::var("PGQRS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    // TODO: Construct QueueServiceImpl and inject pool
    // let svc = ...;

    // Graceful shutdown
    let shutdown = async {
        signal::ctrl_c().await.expect("failed to install CTRL+C handler");
    };

    // Start server (replace `svc` with actual service)
    // Server::builder()
    //     .add_service(svc)
    //     .serve_with_shutdown(addr, shutdown)
    //     .await?;

    Ok(())
}
