//! Command-line interface for pgqrs: manage queues and messages.
//!
//! This file implements the CLI entry point for pgqrs, allowing users to install, uninstall, verify, and manage queues and messages via command-line commands.
//!
//! ## What
//!
//! - Provides commands for schema management, queue operations, and message operations.
//! - Supports output in JSON, CSV, and YAML formats.
//!
//! ## How
//!
//! Run the CLI with various subcommands to interact with pgqrs. See `--help` for usage details.
//!
//! ### Example
//!
//! ```sh
//! pgqrs install
//! pgqrs queue create jobs
//! pgqrs message send --queue jobs --payload '{"foo": "bar"}'
//! ```
use clap::{Parser, Subcommand};
use pgqrs::config::Config;
use pgqrs::types::{QueueInfo, QueueMessage};

use std::fs::File;
use std::process;

mod output;

use crate::output::{JsonOutputWriter, OutputWriter, TableOutputWriter};

#[derive(Parser)]
#[command(name = "pgqrs")]
#[command(about = "A PostgreSQL-backed job queue CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Database URL (highest priority, overrides all other config sources)
    #[arg(long, short = 'd')]
    dsn: Option<String>,

    /// Schema name for pgqrs tables and objects (default: public, must exist before install)
    #[arg(long, short = 's')]
    schema: Option<String>,

    /// Config file path (overrides environment variables and defaults)
    #[arg(long, short = 'c')]
    config: Option<String>,

    /// Log destination: stderr or file path
    #[arg(long, default_value = "stderr")]
    log_dest: String,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Output format: json, table
    #[arg(long, default_value = "table")]
    format: String,

    /// Output destination: stdout or file path
    #[arg(long, default_value = "stdout")]
    out: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Admin Commands
    Admin {
        #[command(subcommand)]
        admin_command: AdminCommands,
    },
    /// Queue Commands
    Queue {
        #[command(subcommand)]
        queue_command: QueueCommands,
    },
    /// Worker Commands
    Worker {
        #[command(subcommand)]
        worker_command: WorkerCommands,
    },
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Install pgqrs schema (schema must be pre-created)
    Install,
    /// Verify pgqrs installation
    Verify,
    /// Get system-wide statistics
    Stats,
    /// Reclaim messages from zombie workers
    Reclaim {
        /// Name of the queue
        #[arg(long, short = 'q')]
        queue: String,

        /// Reclaim messages from workers with heartbeat older than this duration (e.g., '5s', '1m')
        /// If not provided, uses configured heartbeat_interval
        #[arg(long)]
        older_than: Option<String>,
    },
}

#[derive(Subcommand)]
enum QueueCommands {
    /// Create a new queue
    Create {
        /// Name of the queue
        name: String,
    },
    /// List all queues
    List,
    /// Get
    Get {
        /// Name of the queue to get
        name: String,
    },
    /// List messages in queue
    Messages {
        /// Name of the queue
        name: String,
    },
    /// Move dead letter queue messages to archive
    ArchiveDlq,
    /// Delete a queue
    Delete {
        /// Name of the queue to delete
        name: String,
    },
    /// Purge all messages from a queue
    Purge {
        /// Name of the queue to purge
        name: String,
    },
    /// Show queue metrics
    Metrics {
        /// Name of the queue (if not provided, shows all queues)
        name: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorkerCommands {
    /// List all workers
    List {
        /// Name of the queue to filter workers by
        #[arg(long, short = 'q')]
        queue: Option<String>,
    },
    /// Get a worker by ID
    Get {
        /// Worker ID
        id: i64,
    },
    /// Get worker messages
    Messages {
        /// Worker ID
        id: i64,
    },
    /// Release messages from a worker
    ReleaseMessages {
        /// Worker ID
        id: i64,
    },
    /// Suspend a worker (Ready -> Suspended)
    Suspend {
        /// Worker ID
        id: i64,
    },
    /// Resume a worker (Suspended -> Ready)
    Resume {
        /// Worker ID
        id: i64,
    },
    /// Shutdown a worker (must be Suspended first)
    Shutdown {
        /// Worker ID
        id: i64,
    },
    /// Purge old stopped workers
    Purge {
        /// Remove workers older than this duration (e.g., '7d', '24h', '30m')
        #[arg(long, default_value = "7d")]
        older_than: String,
    },
    /// Delete a worker
    Delete {
        /// Worker ID
        id: i64,
    },
    /// Update worker heartbeat
    Heartbeat {
        /// Worker ID
        id: i64,
    },
    /// Get Worker Stats
    Stats {
        /// Name of the queue
        queue: String,
    },
    /// Check worker health
    Health {
        /// Stale threshold in seconds (default: 60)
        #[arg(long, default_value = "60")]
        timeout: u64,
        /// Group stats by queue
        #[arg(long)]
        group_by_queue: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize tracing
    let level = match cli.log_level.to_lowercase().as_str() {
        "error" => tracing::Level::ERROR,
        "warn" => tracing::Level::WARN,
        "info" => tracing::Level::INFO,
        "debug" => tracing::Level::DEBUG,
        "trace" => tracing::Level::TRACE,
        other => {
            eprintln!("Unknown log level '{}', defaulting to INFO", other);
            tracing::Level::INFO
        }
    };

    let writer: Box<dyn Fn() -> Box<dyn std::io::Write + Send> + Send + Sync> =
        if cli.log_dest == "stderr" {
            Box::new(|| Box::new(std::io::stderr()))
        } else {
            let file = std::fs::File::create(&cli.log_dest).expect("Failed to create log file");
            Box::new(move || Box::new(file.try_clone().expect("Failed to clone log file")))
        };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .with_writer(writer)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    if let Err(e) = run_cli(cli).await {
        tracing::error!("Error: {}", e);
        process::exit(1);
    }
}

/// Run the CLI with the provided arguments and configuration.
///
/// This function handles loading configuration from multiple sources,
/// initializing the admin interface, and dispatching to the appropriate
/// command handlers.
///
/// # Arguments
/// * `cli` - Parsed CLI arguments and options
///
/// # Returns
/// Ok if command executed successfully, error otherwise.
async fn run_cli(cli: Cli) -> anyhow::Result<()> {
    // Load configuration using the new prioritized loading system
    // Priority order:
    // 1. --dsn CLI argument (if provided)
    // 2. --schema CLI argument (if provided)
    // 3. --config CLI argument (if provided)
    // 4. PGQRS_CONFIG_FILE environment variable
    // 5. PGQRS_DSN, PGQRS_SCHEMA and other environment variables
    // 6. Default config files (pgqrs.yaml, pgqrs.yml)
    let config = Config::load_with_schema_options(cli.dsn, cli.schema, cli.config)
        .map_err(|e| anyhow::anyhow!("Failed to load configuration: {}", e))?;

    let store = pgqrs::connect_with_config(&config).await?;

    let writer = match cli.format.to_lowercase().as_str() {
        "json" => OutputWriter::Json(JsonOutputWriter),
        _ => OutputWriter::Table(TableOutputWriter),
    };
    // Use an owned boxed writer so the underlying writer lives long enough for borrows
    let mut out_writer: Box<dyn std::io::Write> = match cli.out.as_str() {
        "stdout" => Box::new(std::io::stdout()),
        _ => Box::new(File::create(&cli.out)?),
    };
    let out: &mut dyn std::io::Write = out_writer.as_mut();

    match cli.command {
        Commands::Admin { admin_command } => {
            handle_admin_commands(&store, admin_command, writer, out).await?
        }

        Commands::Worker { worker_command } => {
            handle_worker_commands(&store, worker_command, writer, out).await?
        }

        Commands::Queue { queue_command } => {
            handle_queue_commands(&store, queue_command, writer, out).await?
        }
    }
    Ok(())
}

async fn handle_admin_commands(
    store: &impl pgqrs::store::Store,
    command: AdminCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match command {
        AdminCommands::Install => {
            tracing::info!("Installing pgqrs schema ...");
            pgqrs::admin(store).install().await?;
            tracing::info!("Installation completed successfully");
        }

        AdminCommands::Verify => {
            tracing::info!("Verifying pgqrs installation...");
            pgqrs::admin(store).verify().await?;
            tracing::info!("Verification completed successfully");
        }

        AdminCommands::Stats => {
            tracing::info!("Getting system statistics...");
            let stats = pgqrs::admin(store).system_stats().await?;
            writer.write_item(&stats, out)?;
        }

        AdminCommands::Reclaim { queue, older_than } => {
            tracing::info!("Reclaiming messages for queue '{}'...", queue);
            let queue_info = pgqrs::tables(store).queues().get_by_name(&queue).await?;

            let duration = match older_than {
                Some(s) => Some(
                    chrono::Duration::from_std(
                        s.parse::<humantime::Duration>()
                            .map_err(|e| anyhow::anyhow!("Invalid duration format '{}': {}", s, e))?
                            .into(),
                    )
                    .map_err(|e| anyhow::anyhow!("Duration too large: {}", e))?,
                ),
                None => None,
            };

            let count = pgqrs::admin(store)
                .reclaim_messages(queue_info.id, duration)
                .await?;
            tracing::info!("Reclaimed {} messages from zombie workers", count);
            writeln!(out, "Reclaimed {} messages from zombie workers", count)?;
        }
    }
    Ok(())
}

async fn handle_queue_commands(
    store: &impl pgqrs::store::Store,
    command: QueueCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match command {
        QueueCommands::Create { name } => {
            tracing::info!("Creating queue '{}' ...", &name);
            let queue = pgqrs::admin(store).create_queue(&name).await?;
            writer.write_item(&queue, out)?;
        }

        QueueCommands::List => {
            tracing::info!("Listing all queues...");
            let queue_list: Vec<QueueInfo> = pgqrs::tables(store).queues().list().await?;
            writer.write_list(&queue_list, out)?;
        }

        QueueCommands::Get { name } => {
            tracing::info!("Getting queue '{}'...", name);
            let queue_info = pgqrs::admin(store).get_queue(&name).await?;
            writer.write_item(&queue_info, out)?;
        }

        QueueCommands::Messages { name } => {
            tracing::info!("Listing messages for queue '{}'...", name);
            let queue_info = pgqrs::admin(store).get_queue(&name).await?;
            let messages_list: Vec<QueueMessage> = pgqrs::tables(store)
                .messages()
                .filter_by_fk(queue_info.id)
                .await?;
            writer.write_list(&messages_list, out)?;
        }

        QueueCommands::ArchiveDlq => {
            tracing::info!("Moving dead letter queue messages to archive");
            let moved_ids = pgqrs::admin(store).dlq().await?;
            tracing::info!("Moved {} messages from DLQ to archive", moved_ids.len());
            writer.write_list(&moved_ids, out)?;
        }

        QueueCommands::Delete { name } => {
            tracing::info!("Deleting queue '{}'...", name);
            let queue_info = pgqrs::admin(store).get_queue(&name).await?;
            pgqrs::admin(store).delete_queue(&queue_info).await?;
            tracing::info!("Queue '{}' deleted successfully", name);
        }

        QueueCommands::Purge { name } => {
            tracing::info!("Purging queue '{}'...", name);
            pgqrs::admin(store).purge_queue(&name).await?;
            tracing::info!("Queue '{}' purged successfully", name);
        }

        QueueCommands::Metrics { name } => {
            if let Some(queue_name) = name {
                tracing::info!("Getting metrics for queue '{}'...", queue_name);
                let metrics = pgqrs::admin(store).queue_metrics(&queue_name).await?;
                writer.write_item(&metrics, out)?;
            } else {
                tracing::info!("Getting metrics for all queues...");
                let metrics = pgqrs::admin(store).all_queues_metrics().await?;
                writer.write_list(&metrics, out)?;
            }
        }
    }
    Ok(())
}

async fn handle_worker_commands(
    store: &impl pgqrs::store::Store,
    command: WorkerCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match command {
        WorkerCommands::List { queue } => {
            let workers = match queue {
                Some(queue_name) => {
                    tracing::info!("Listing workers for queue '{}'...", queue_name);
                    let queue_id = pgqrs::tables(store)
                        .queues()
                        .get_by_name(&queue_name)
                        .await?
                        .id;
                    pgqrs::tables(store)
                        .workers()
                        .filter_by_fk(queue_id)
                        .await?
                }
                None => {
                    tracing::info!("Listing all workers...");
                    pgqrs::tables(store).workers().list().await?
                }
            };
            tracing::info!("Found {} workers", workers.len());
            writer.write_list(&workers, out)?;
        }
        WorkerCommands::Get { id } => {
            let worker = pgqrs::tables(store)
                .workers()
                .get(id)
                .await
                .map_err(|_| anyhow::anyhow!("Worker with ID {} not found", id))?;
            writer.write_item(&worker, out)?;
        }

        WorkerCommands::Messages { id } => {
            // Find the worker and get its messages
            let worker_info = pgqrs::tables(store)
                .workers()
                .get(id)
                .await
                .map_err(|_| anyhow::anyhow!("Worker with ID {} not found", id))?;

            tracing::info!("Getting messages for worker {}...", id);
            let messages = pgqrs::admin(store)
                .get_worker_messages(worker_info.id)
                .await?;
            tracing::info!("Found {} messages", messages.len());
            writer.write_list(&messages, out)?;
        }

        WorkerCommands::ReleaseMessages { id } => {
            tracing::info!("Releasing messages from worker {}...", id);
            let released_count = pgqrs::admin(store).release_worker_messages(id).await?;
            tracing::info!("Released {} messages", released_count);
            writeln!(
                out,
                "Released {} messages from worker {}",
                released_count, id
            )?;
        }

        WorkerCommands::Suspend { id } => {
            tracing::info!("Suspending worker {}...", id);
            let worker_handler = pgqrs::worker_handle(store, id).await?;
            worker_handler.suspend().await?;
            tracing::info!("Worker {} suspended", id);
            writeln!(out, "Worker {} suspended", id)?;
        }

        WorkerCommands::Resume { id } => {
            tracing::info!("Resuming worker {}...", id);
            let worker_handler = pgqrs::worker_handle(store, id).await?;
            worker_handler.resume().await?;
            tracing::info!("Worker {} resumed", id);
            writeln!(out, "Worker {} resumed", id)?;
        }

        WorkerCommands::Shutdown { id } => {
            tracing::info!("Shutting down worker {}...", id);
            let worker_handler = pgqrs::worker_handle(store, id).await?;
            worker_handler.shutdown().await?;
            tracing::info!("Worker {} shut down successfully", id);
            writeln!(out, "Worker {} shut down successfully", id)?;
        }

        WorkerCommands::Heartbeat { id } => {
            tracing::info!("Updating heartbeat for worker {}...", id);
            let worker = pgqrs::worker_handle(store, id).await?;
            worker.heartbeat().await?;
            tracing::info!("Heartbeat updated for worker {}", id);
            writeln!(out, "Heartbeat updated for worker {}", id)?;
        }

        WorkerCommands::Purge { older_than } => {
            // Parse duration string using humantime (supports "7d", "24h", "30m", "1s", etc.)
            let duration = chrono::Duration::from_std(
                older_than
                    .parse::<humantime::Duration>()
                    .map_err(|e| {
                        anyhow::anyhow!("Invalid duration format '{}': {}", older_than, e)
                    })?
                    .into(),
            )
            .map_err(|e| anyhow::anyhow!("Duration too large: {}", e))?;
            tracing::info!("Purging workers older than {:?}...", duration);
            let purged_count = pgqrs::admin(store).purge_old_workers(duration).await?;
            tracing::info!("Purged {} old workers", purged_count);
            writeln!(out, "Purged {} old workers", purged_count)?;
        }

        WorkerCommands::Delete { id } => {
            tracing::info!("Deleting worker {}...", id);
            match pgqrs::admin(store).delete_worker(id).await {
                Ok(deleted_count) => {
                    if deleted_count > 0 {
                        tracing::info!("Deleted worker {}", id);
                        writeln!(out, "Worker {} deleted successfully", id)?;
                    } else {
                        tracing::warn!("Worker {} not found", id);
                        writeln!(out, "Worker {} not found", id)?;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to delete worker {}: {}", id, e);
                    writeln!(out, "Error: {}", e)?;
                }
            }
        }

        WorkerCommands::Stats { queue } => {
            tracing::info!("Getting worker statistics for queue '{}'...", queue);
            let stats = pgqrs::admin(store).worker_stats(&queue).await?;
            tracing::info!("Worker statistics retrieved");

            // Print stats in a readable format
            writeln!(out, "Worker Statistics for Queue '{}':", queue)?;
            writeln!(out, "  Total Workers: {}", stats.total_workers)?;
            writeln!(out, "  Ready Workers: {}", stats.ready_workers)?;
            writeln!(out, "  Suspended Workers: {}", stats.suspended_workers)?;
            writeln!(out, "  Stopped Workers: {}", stats.stopped_workers)?;
            writeln!(
                out,
                "  Average Messages per Worker: {:.2}",
                stats.average_messages_per_worker
            )?;
            writeln!(out, "  Oldest Worker Age: {:?}", stats.oldest_worker_age)?;
            writeln!(
                out,
                "  Newest Heartbeat Age: {:?}",
                stats.newest_heartbeat_age
            )?;
        }

        WorkerCommands::Health {
            timeout,
            group_by_queue,
        } => {
            tracing::info!(
                "Checking worker health (timeout: {}s, grouped: {})...",
                timeout,
                group_by_queue
            );
            let stats = pgqrs::admin(store)
                .worker_health_stats(chrono::Duration::seconds(timeout as i64), group_by_queue)
                .await?;
            writer.write_list(&stats, out)?;
        }
    }
    Ok(())
}
