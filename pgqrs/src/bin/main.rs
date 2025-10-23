use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::{generate, Generator};
use std::io;
use std::time::Duration;

use pgqrs::cli::{
    handle_health_command, handle_message_command, handle_queue_command, Cli, Commands,
};

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

async fn create_client(cli: &Cli) -> Result<pgqrs::PgqrsClient> {
    let mut builder = pgqrs::PgqrsClient::builder()
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
