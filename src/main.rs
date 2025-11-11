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
use pgqrs::archive::Archive;
use pgqrs::config::Config;
use pgqrs::types::QueueInfo;
use pgqrs::{admin::PgqrsAdmin, tables::PgqrsQueues};
use pgqrs::{Queue, Table};
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
    /// Install pgqrs schema (schema must be pre-created)
    Install,
    /// Uninstall pgqrs schema
    Uninstall,
    /// Verify pgqrs installation
    Verify,
    /// Queue management commands
    Queue {
        #[command(subcommand)]
        action: QueueCommands,
    },
    /// Message management commands
    Message {
        #[command(subcommand)]
        action: MessageCommands,
    },
    /// Worker management commands
    Worker {
        #[command(subcommand)]
        action: WorkerCommands,
    },
    /// Archive management commands
    Archive {
        #[command(subcommand)]
        action: ArchiveCommands,
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
enum MessageCommands {
    /// Enqueue a message to a queue
    Enqueue {
        /// Name of the queue
        queue: String,
        /// JSON message payload
        payload: String,
        /// Delay in seconds before message becomes available
        #[arg(long, short = 'd')]
        delay: Option<u32>,
    },
    /// Dequeue a message from a queue
    Dequeue {
        /// Name of the queue
        queue: String,
        /// Worker ID
        worker: i64,
        /// Lock time in seconds
        lock_time: Option<u32>,
    },
    /// Archive a message by ID
    Archive {
        /// Name of the queue
        queue: String,
        /// Message ID to archive
        id: i64,
    },
    /// Delete a message from the queue
    Delete {
        /// Name of the queue
        queue: String,
        /// Message ID to delete
        id: String,
    },
    /// Show pending message count
    Count {
        /// Name of the queue
        queue: String,
    },
    /// Get message by ID
    Get {
        /// Name of the queue
        queue: String,
        /// Message ID to show
        id: i64,
    },
}

#[derive(Subcommand)]
enum WorkerCommands {
    /// Create a new worker for a queue
    Create {
        /// Name of the queue
        queue: String,
        /// Worker identifier (e.g., hostname or unique name)
        host: String,
        /// Heartbeat interval in seconds
        port: i32,
    },
    /// List all workers
    List {
        /// Name of the queue to filter workers by
        #[arg(long, short = 'q')]
        queue: Option<String>,
    },
    /// Get detailed worker information
    Get {
        /// Worker ID
        id: i64,
    },
    /// Show worker statistics
    Stats {
        /// Name of the queue
        #[arg(long, short = 'q')]
        queue: String,
    },
    /// Stop a worker (mark as stopped)
    Stop {
        /// Worker ID
        id: i64,
    },
    /// Show messages assigned to a worker
    Messages {
        /// Worker ID
        id: i64,
    },
    /// Release messages from a worker
    Release {
        /// Worker ID
        id: i64,
    },
    /// Delete a specific worker (only if no associated messages)
    Delete {
        /// Worker ID
        id: i64,
    },
    /// Remove old stopped workers
    Purge {
        /// Remove workers older than this duration (e.g., '7d', '24h', '30m')
        #[arg(long, default_value = "7d")]
        older_than: String,
    },
    /// Check worker health
    Health {
        /// Name of the queue
        #[arg(long, short = 'q')]
        queue: String,
        /// Maximum heartbeat age in seconds
        #[arg(long, default_value = "300")]
        max_age: u64,
    },
}

#[derive(Subcommand)]
enum ArchiveCommands {
    /// List archived messages
    List {
        queue: String,
        /// Filter by worker ID
        #[arg(long, short = 'w')]
        worker: Option<i64>,
    },
    /// Delete archived messages
    Delete {
        queue: String,
        /// Filter by worker ID
        #[arg(long, short = 'w')]
        worker: Option<i64>,
    },
    /// Count archived messages
    Count {
        queue: String,
        /// Filter by worker ID
        #[arg(long, short = 'w')]
        worker: Option<i64>,
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

    let admin = PgqrsAdmin::new(&config).await?;
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
        Commands::Install => {
            tracing::info!("Installing pgqrs schema ...");
            admin.install().await?;
            tracing::info!("Installation completed successfully");
        }

        Commands::Uninstall => {
            tracing::info!("Uninstalling pgqrs schema ...");
            admin.uninstall().await?;
            tracing::info!("Uninstall completed successfully");
        }

        Commands::Verify => {
            tracing::info!("Verifying pgqrs installation...");
            admin.verify().await?;
            tracing::info!("Verification completed successfully");
        }

        Commands::Queue { action } => {
            handle_queue_commands(&admin, action, writer, out).await?;
        }

        Commands::Message { action } => {
            handle_message_commands(&admin, action, writer, out).await?;
        }

        Commands::Worker { action } => {
            handle_worker_commands(&admin, action, writer, out).await?;
        }

        Commands::Archive { action } => {
            handle_archive_commands(&admin, action, writer, out).await?;
        }
    }

    Ok(())
}

/// Handle queue-related CLI commands.
///
/// This function processes all queue management commands including create,
/// list, delete, purge, and metrics operations.
///
/// # Arguments
/// * `admin` - Admin interface for queue operations
/// * `action` - Specific queue command to execute
/// * `writer` - Output formatter for results
/// * `out` - Output destination for formatted results
///
/// # Returns
/// Ok if command executed successfully, error otherwise.
async fn handle_queue_commands(
    admin: &PgqrsAdmin,
    action: QueueCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match action {
        QueueCommands::Create { name } => {
            tracing::info!("Creating queue '{}' ...", &name);
            let queue = admin.create_queue(&name).await?;
            writer.write_item(&queue, out)?;
        }

        QueueCommands::List => {
            tracing::info!("Listing all queues...");
            let queue = PgqrsQueues::new(admin.pool.clone());
            let queue_list: Vec<QueueInfo> = queue.list(None).await?;
            writer.write_list(&queue_list, out)?;
        }

        QueueCommands::Get { name } => {
            tracing::info!("Getting queue '{}'...", name);
            let queue_info = admin.get_queue(&name).await?;
            writer.write_item(&queue_info, out)?;
        }

        QueueCommands::Delete { name } => {
            tracing::info!("Deleting queue '{}'...", name);
            let queue_info = admin.get_queue(&name).await?;
            admin.delete_queue(&queue_info).await?;
            tracing::info!("Queue '{}' deleted successfully", name);
        }

        QueueCommands::Purge { name } => {
            tracing::info!("Purging queue '{}'...", name);
            admin.purge_queue(&name).await?;
            tracing::info!("Queue '{}' purged successfully", name);
        }

        QueueCommands::Metrics { name } => {
            if let Some(queue_name) = name {
                tracing::info!("Getting metrics for queue '{}'...", queue_name);
                let metrics = admin.queue_metrics(&queue_name).await?;
                tracing::info!("Queue: {}", metrics.name);
                tracing::info!("  Total Messages: {}", metrics.total_messages);
                tracing::info!("  Pending Messages: {}", metrics.pending_messages);
                tracing::info!("  Locked Messages: {}", metrics.locked_messages);
                if let Some(oldest) = metrics.oldest_pending_message {
                    tracing::info!(
                        "  Oldest Pending: {}",
                        oldest.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                }
                if let Some(newest) = metrics.newest_message {
                    tracing::info!(
                        "  Newest Message: {}",
                        newest.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                }
            } else {
                tracing::info!("Getting metrics for all queues...");
                let metrics = admin.all_queues_metrics().await?;
                if metrics.is_empty() {
                    tracing::info!("No queues found");
                } else {
                    for metric in metrics {
                        tracing::info!("Queue: {}", metric.name);
                        tracing::info!("  Total Messages: {}", metric.total_messages);
                        tracing::info!("  Pending Messages: {}", metric.pending_messages);
                        tracing::info!("  Locked Messages: {}", metric.locked_messages);
                        tracing::info!("  Archived Messages: {}", metric.archived_messages);
                        if let Some(oldest) = metric.oldest_pending_message {
                            tracing::info!(
                                "  Oldest Pending: {}",
                                oldest.format("%Y-%m-%d %H:%M:%S UTC")
                            );
                        }
                        if let Some(newest) = metric.newest_message {
                            tracing::info!(
                                "  Newest Message: {}",
                                newest.format("%Y-%m-%d %H:%M:%S UTC")
                            );
                        }
                        println!();
                    }
                }
            }
        }
    }
    Ok(())
}

/// Handle message-related CLI commands.
///
/// This function processes all message management commands including send,
/// read, dequeue, delete, and count operations.
///
/// # Arguments
/// * `admin` - Admin interface for accessing queues
/// * `action` - Specific message command to execute
/// * `writer` - Output formatter for results
/// * `out` - Output destination for formatted results
///
/// # Returns
/// Ok if command executed successfully, error otherwise.
async fn handle_message_commands(
    admin: &PgqrsAdmin,
    action: MessageCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match action {
        MessageCommands::Enqueue {
            queue,
            payload,
            delay,
        } => {
            let queue_info = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue_info, &admin.config);
            tracing::info!("Sending message to queue '{}'...", queue);
            let payload_json: serde_json::Value = serde_json::from_str(&payload)?;
            let msg_id = if let Some(delay_secs) = delay {
                tracing::info!("Sending delayed message (delay: {}s)...", delay_secs);
                queue_obj.enqueue_delayed(&payload_json, delay_secs).await?
            } else {
                queue_obj.enqueue(&payload_json).await?
            };
            tracing::info!("Message sent successfully with ID: {}", msg_id);
            writer.write_item(&msg_id, out)?;
            Ok(())
        }
        MessageCommands::Dequeue {
            queue,
            worker,
            lock_time,
        } => {
            let queue = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue, &admin.config);
            let worker_info = admin.get_worker_by_id(worker).await?;
            tracing::info!(
                "Dequeue message from queue '{}', worker: {})...",
                queue,
                worker_info
            );
            let messages = match lock_time {
                Some(lt) => queue_obj.dequeue_delay(&worker_info, lt).await?,
                None => queue_obj.dequeue(&worker_info).await?,
            };
            writer.write_list(&messages, out)?;
            Ok(())
        }
        MessageCommands::Archive { queue, id } => {
            let queue = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue, &admin.config);
            tracing::info!("Archiving message {} from queue '{}'...", id, queue);
            let archived_message = queue_obj.archive(id).await?;
            tracing::info!("Message archived successfully");
            writer.write_item(&archived_message, out)?;
            Ok(())
        }
        MessageCommands::Delete { queue, id } => {
            let queue = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue, &admin.config);
            let msg_id = id.parse::<i64>()?;
            tracing::info!("Deleting message {} from queue '{}'...", msg_id, queue);
            let deleted = queue_obj.delete_many(vec![msg_id]).await?;
            if deleted.first().copied().unwrap_or(false) {
                tracing::info!("Message deleted successfully");
            } else {
                tracing::info!("Message not found or could not be deleted");
            }
            Ok(())
        }
        MessageCommands::Count { queue } => {
            let queue = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue, &admin.config);
            tracing::info!("Getting pending message count for queue '{}'...", queue);
            let count = queue_obj.pending_count().await?;
            tracing::info!("Pending messages: {}", count);
            Ok(())
        }

        MessageCommands::Get { queue, id } => {
            let queue = admin.get_queue(&queue).await?;
            let queue_obj = Queue::new(admin.pool.clone(), &queue, &admin.config);
            tracing::debug!("Retrieving message {} from queue '{}'...", id, queue);
            let message = queue_obj.get_message_by_id(id).await?;
            writer.write_item(&message, out)?;
            Ok(())
        }
    }
}

/// Handle worker-related CLI commands.
///
/// This function processes all worker management commands including list,
/// show, stats, stop, messages, release, purge, and health operations.
///
/// # Arguments
/// * `admin` - Admin interface for worker operations
/// * `action` - Specific worker command to execute
/// * `writer` - Output formatter for results
/// * `out` - Output destination for formatted results
///
/// # Returns
/// Ok if command executed successfully, error otherwise.
async fn handle_worker_commands(
    admin: &PgqrsAdmin,
    action: WorkerCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match action {
        WorkerCommands::Create { queue, host, port } => {
            let worker = admin.register(queue, host, port).await?;
            writer.write_item(&worker, out)?;
            Ok(())
        }
        WorkerCommands::List { queue } => {
            let workers = match queue {
                Some(queue_name) => {
                    tracing::info!("Listing workers for queue '{}'...", queue_name);
                    admin.list_queue_workers(&queue_name).await?
                }
                None => {
                    tracing::info!("Listing all workers...");
                    admin.list_all_workers().await?
                }
            };
            tracing::info!("Found {} workers", workers.len());
            writer.write_list(&workers, out)?;
            Ok(())
        }

        WorkerCommands::Get { id } => {
            let worker = admin.get_worker_by_id(id).await?;
            writer.write_item(&worker, out)?;
            Ok(())
        }

        WorkerCommands::Stats { queue } => {
            tracing::info!("Getting worker statistics for queue '{}'...", queue);
            let stats = admin.worker_stats(&queue).await?;
            tracing::info!("Worker statistics retrieved");

            // Print stats in a readable format
            writeln!(out, "Worker Statistics for Queue '{}':", queue)?;
            writeln!(out, "  Total Workers: {}", stats.total_workers)?;
            writeln!(out, "  Ready Workers: {}", stats.ready_workers)?;
            writeln!(
                out,
                "  Shutting Down Workers: {}",
                stats.shutting_down_workers
            )?;
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
            Ok(())
        }

        WorkerCommands::Stop { id } => {
            tracing::info!("Stopping worker {}...", id);
            admin.mark_stopped(id).await?;
            tracing::info!("Worker stopped successfully");
            Ok(())
        }

        WorkerCommands::Messages { id } => {
            // Find the worker and get its messages
            let worker = admin
                .get_worker_by_id(id)
                .await
                .map_err(|_| anyhow::anyhow!("Worker with ID {} not found", id))?;

            tracing::info!("Getting messages for worker {}...", id);
            let messages = admin.get_worker_messages(worker.id).await?;
            tracing::info!("Found {} messages", messages.len());
            writer.write_list(&messages, out)?;
            Ok(())
        }

        WorkerCommands::Release { id } => {
            tracing::info!("Releasing messages from worker {}...", id);
            let released_count = admin.release_worker_messages(id).await?;
            tracing::info!("Released {} messages", released_count);
            writeln!(
                out,
                "Released {} messages from worker {}",
                released_count, id
            )?;
            Ok(())
        }

        WorkerCommands::Delete { id } => {
            tracing::info!("Deleting worker {}...", id);
            match admin.delete_worker(id).await {
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
            Ok(())
        }

        WorkerCommands::Purge { older_than } => {
            // Parse duration string using humantime (supports "7d", "24h", "30m", "1s", etc.)
            let duration = older_than
                .parse::<humantime::Duration>()
                .map_err(|e| anyhow::anyhow!("Invalid duration format '{}': {}", older_than, e))?
                .into();
            tracing::info!("Purging workers older than {:?}...", duration);
            let purged_count = admin.purge_old_workers(duration).await?;
            tracing::info!("Purged {} old workers", purged_count);
            writeln!(out, "Purged {} old workers", purged_count)?;
            Ok(())
        }

        WorkerCommands::Health { queue, max_age } => {
            tracing::info!("Checking worker health for queue '{}'...", queue);
            let workers = admin.list_queue_workers(&queue).await?;
            // admin.is_healthy expects a chrono::Duration (the same kind produced by
            // now.signed_duration_since(...)), so create a chrono::Duration here.
            let max_age_duration = chrono::Duration::seconds(max_age as i64);

            let mut healthy = 0;
            let mut unhealthy = 0;

            writeln!(out, "Worker Health Report for Queue '{}':", queue)?;
            for worker in &workers {
                let is_healthy = admin.is_healthy(worker.id, max_age_duration).await?;
                if is_healthy {
                    healthy += 1;
                } else {
                    unhealthy += 1;
                }
                writeln!(
                    out,
                    "  Worker {}: {} ({}:{})",
                    worker.id,
                    if is_healthy { "HEALTHY" } else { "UNHEALTHY" },
                    worker.hostname,
                    worker.port
                )?;
            }

            writeln!(out)?;
            writeln!(out, "Summary: {} healthy, {} unhealthy", healthy, unhealthy)?;
            Ok(())
        }
    }
}

async fn handle_archive_commands(
    admin: &PgqrsAdmin,
    action: ArchiveCommands,
    writer: OutputWriter,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    match action {
        ArchiveCommands::List { queue, worker } => {
            // Get the queue object to obtain queue_id
            let queue_obj = admin.get_queue(&queue).await?;
            let archive = Archive::new(admin.pool.clone(), &queue_obj);

            tracing::info!("Listing archived messages for queue '{}'...", queue);
            let messages = archive.list(worker, 100, 0).await?;
            tracing::info!("Found {} archived messages", messages.len());
            writer.write_list(&messages, out)?;

            Ok(())
        }

        ArchiveCommands::Delete { queue, worker } => {
            // Get the queue object to obtain queue_id
            let queue_obj = admin.get_queue(&queue).await?;
            let archive = Archive::new(admin.pool.clone(), &queue_obj);

            tracing::info!("Deleting archived messages for queue '{}'...", queue);
            let deleted_count = archive.delete(worker).await?;

            writeln!(out, "Deleted {} archived messages", deleted_count)?;
            Ok(())
        }

        ArchiveCommands::Count { queue, worker } => {
            // Get the queue object to obtain queue_id
            let queue_obj = admin.get_queue(&queue).await?;
            let archive = Archive::new(admin.pool.clone(), &queue_obj);

            tracing::info!("Counting archived messages for queue '{}'...", queue);
            let count = archive.count(worker).await?;

            writeln!(out, "Archived messages: {}", count)?;
            Ok(())
        }
    }
}
