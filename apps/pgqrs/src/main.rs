use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Generator, Shell};
use std::io;
use std::time::Duration;

mod commands;
mod error;
mod output;

use commands::*;
use output::OutputFormat;

#[derive(Parser)]
#[command(name = "pgqrs")]
#[command(about = "A CLI for interacting with pgqrs queue service")]
#[command(version, long_about = None)]
pub struct Cli {
    /// Server endpoint (can also be set via PGQRS_ENDPOINT)
    #[arg(long, env = "PGQRS_ENDPOINT", default_value = "http://127.0.0.1:50051")]
    pub endpoint: String,

    /// API key for authentication (can also be set via PGQRS_API_KEY)
    #[arg(long, env = "PGQRS_API_KEY")]
    pub api_key: Option<String>,

    /// Connection timeout in seconds
    #[arg(long, default_value = "10")]
    pub connect_timeout: u64,

    /// RPC timeout in seconds
    #[arg(long, default_value = "30")]
    pub rpc_timeout: u64,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,

    /// Suppress output except for errors
    #[arg(long, short = 'q')]
    pub quiet: bool,

    /// Enable verbose logging
    #[arg(long, short = 'v')]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Queue management commands
    #[command(subcommand)]
    Queue(QueueCommands),

    /// Message operations
    #[command(subcommand)]
    Message(MessageCommands),

    /// Health check commands
    #[command(subcommand)]
    Health(HealthCommands),

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
}

#[derive(Subcommand)]
pub enum QueueCommands {
    /// Create a new queue
    Create {
        /// Queue name
        name: String,
        /// Create as unlogged table (faster but less durable)
        #[arg(long)]
        unlogged: bool,
    },
    /// List all queues
    List,
    /// Get queue information
    Get {
        /// Queue name
        name: String,
    },
    /// Delete a queue
    Delete {
        /// Queue name
        name: String,
    },
    /// Get queue statistics
    Stats {
        /// Queue name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum MessageCommands {
    /// Add a message to the queue
    Enqueue {
        /// Queue name
        queue: String,
        /// Message payload (JSON string or @filename to read from file)
        payload: String,
        /// Delay in seconds before message becomes visible
        #[arg(long, default_value = "0")]
        delay: u64,
    },
    /// Retrieve messages from the queue
    Dequeue {
        /// Queue name
        queue: String,
        /// Maximum number of messages to retrieve
        #[arg(long, default_value = "1")]
        max_messages: u32,
        /// Lease duration in seconds
        #[arg(long, default_value = "30")]
        lease_seconds: u64,
    },
    /// Acknowledge message processing
    Ack {
        /// Message ID to acknowledge
        message_id: String,
    },
    /// Reject message and optionally dead letter it
    Nack {
        /// Message ID to reject
        message_id: String,
        /// Reason for rejection
        #[arg(long)]
        reason: Option<String>,
        /// Send to dead letter queue
        #[arg(long)]
        dead_letter: bool,
    },
    /// Requeue a message with optional delay
    Requeue {
        /// Message ID to requeue
        message_id: String,
        /// Delay in seconds before message becomes visible again
        #[arg(long, default_value = "0")]
        delay: u64,
    },
    /// Extend message lease
    ExtendLease {
        /// Message ID
        message_id: String,
        /// Additional seconds to extend lease
        #[arg(long, default_value = "30")]
        additional_seconds: u64,
    },
    /// Peek at messages without removing them
    Peek {
        /// Queue name
        queue: String,
        /// Maximum number of messages to peek
        #[arg(long, default_value = "10")]
        limit: u32,
    },
    /// List in-flight messages
    ListInFlight {
        /// Queue name
        queue: String,
        /// Maximum number of messages to list
        #[arg(long, default_value = "10")]
        limit: u32,
    },
    /// List dead letter messages
    ListDeadLetters {
        /// Queue name
        queue: String,
        /// Maximum number of messages to list
        #[arg(long, default_value = "10")]
        limit: u32,
    },
    /// Purge dead letter messages
    PurgeDeadLetters {
        /// Queue name
        queue: String,
    },
}

#[derive(Subcommand)]
pub enum HealthCommands {
    /// Check if server is alive (liveness probe)
    Liveness,
    /// Check if server is ready to serve requests (readiness probe)
    Readiness,
    /// General health check
    Check,
}

fn print_completions<G: Generator>(gen: G, cmd: &mut clap::Command) {
    generate(gen, cmd, cmd.get_name().to_string(), &mut io::stdout());
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    if cli.verbose {
        tracing_subscriber::fmt().with_env_filter("debug").init();
    } else if !cli.quiet {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    }

    // Handle shell completion generation
    if let Commands::Completions { shell } = &cli.command {
        let mut cmd = Cli::command();
        print_completions(*shell, &mut cmd);
        return Ok(());
    }

    // Create client from global flags
    let client = create_client(&cli).await?;

    // Execute command
    match &cli.command {
        Commands::Queue(cmd) => handle_queue_command(client, cmd, &cli).await,
        Commands::Message(cmd) => handle_message_command(client, cmd, &cli).await,
        Commands::Health(cmd) => handle_health_command(client, cmd, &cli).await,
        Commands::Completions { .. } => unreachable!(), // Already handled above
    }
}

async fn create_client(cli: &Cli) -> Result<pgqrs_client::PgqrsClient> {
    let mut builder = pgqrs_client::PgqrsClient::builder()
        .endpoint(cli.endpoint.clone())
        .connect_timeout(Duration::from_secs(cli.connect_timeout))
        .rpc_timeout(Duration::from_secs(cli.rpc_timeout));

    if let Some(api_key) = &cli.api_key {
        builder = builder.api_key(api_key);
    }

    builder
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create client: {}", e))
}
