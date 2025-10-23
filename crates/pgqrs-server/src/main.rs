use std::net::SocketAddr;
use tonic::transport::Server;
use clap::Parser;
mod api;
mod cli;
mod config;
mod db;
mod service;
use cli::{get_config_path, Cli, Commands};
use config::AppConfig;
use db::pgqrs_impl::{PgMessageRepo, PgQueueRepo};
use db::pool::create_pool;
use db::init;
use std::sync::Arc;
use tokio::signal;

async fn start_server(config_path: &str) -> anyhow::Result<()> {
    // Load config from YAML file
    let app_config = AppConfig::from_yaml_file(config_path)?;
    let db_cfg = &app_config.database;
    let pool = create_pool(db_cfg).await?;
    let pool = Arc::new(pool);
    // Create repos
    let queue_repo = std::sync::Arc::new(PgQueueRepo {
        pool: pool.clone().as_ref().clone(),
    });
    let message_repo = std::sync::Arc::new(PgMessageRepo {
        pool: pool.clone().as_ref().clone(),
    });
    // Create gRPC service
    let svc = api::queue_service_server::QueueServiceServer::new(service::QueueServiceImpl {
        queue_repo,
        message_repo,
    });

    // Setup tracing subscriber for logs
    tracing_subscriber::fmt().init();

    // TODO: Setup OpenTelemetry metrics, TLS, Auth, etc.

    // Server address
    let addr: SocketAddr = std::env::var("PGQRS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    // Graceful shutdown
    let shutdown = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    // Start tonic server
    println!("Starting gRPC server on {}", addr);
    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config_path = get_config_path(cli.config_path);

    match cli.command {
        Commands::Install => {
            let app_config = AppConfig::from_yaml_file(&config_path)?;
            let db_cfg = &app_config.database;
            let pool = create_pool(db_cfg).await?;

            init::init_db(&pool).await.map_err(anyhow::Error::from)?;
            println!("Database initialized successfully");
        }
        Commands::Uninstall => {
            let app_config = AppConfig::from_yaml_file(&config_path)?;
            let db_cfg = &app_config.database;

            init::uninstall(&db_cfg.database_url).map_err(anyhow::Error::from)?;
            println!("Database uninstalled successfully");
        }
        Commands::IsInitialized => {
            let app_config = AppConfig::from_yaml_file(&config_path)?;
            let db_cfg = &app_config.database;
            let pool = create_pool(db_cfg).await?;

            let initialized = init::is_db_initialized(&pool).await.map_err(anyhow::Error::from)?;
            if initialized {
                println!("Database is initialized");
            } else {
                println!("Database is not initialized");
            }
        }
        Commands::Start => {
            start_server(&config_path).await?;
        }
    }

    Ok(())
}
