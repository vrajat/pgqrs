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
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::error::PgqrsError;
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
    /// Install pgqrs schema
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
}

#[derive(Subcommand)]
enum QueueCommands {
    /// Create a new queue
    Create {
        /// Name of the queue
        name: String,
        /// Create as UNLOGGED table
        #[arg(long, default_value = "false")]
        unlogged: bool,
    },
    /// List all queues
    List,
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
    /// Purge all archived messages from a queue's archive table
    PurgeArchive {
        /// Name of the queue whose archive to purge
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
    /// Send a message to a queue
    Send {
        /// Name of the queue
        queue: String,
        /// JSON message payload
        payload: String,
        /// Delay in seconds before message becomes available
        #[arg(long, short = 'd')]
        delay: Option<u32>,
    },
    /// Read messages from a queue
    Read {
        /// Name of the queue
        queue: String,
        /// Number of messages to read
        #[arg(long, short = 'n', default_value = "1")]
        count: usize,
        /// Lock time in seconds
        #[arg(long, default_value = "5")]
        lock_time: u32,
        /// Filter by message type
        #[arg(long, short = 't')]
        message_type: Option<String>,
        /// Query archive table instead of active queue
        #[arg(long, help = "Query archive table instead of active queue")]
        archive: bool,
    },
    /// Dequeue a message from a queue (reads and returns one message)
    Dequeue {
        /// Name of the queue
        queue: String,
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
        /// Query archive table instead of active queue
        #[arg(long, help = "Query archive table instead of active queue")]
        archive: bool,
    },
    /// Show message details by ID
    Show {
        /// Name of the queue
        queue: String,
        /// Message ID to show
        id: String,
        /// Query archive table instead of active queue
        #[arg(long, help = "Query archive table instead of active queue")]
        archive: bool,
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
    /// Show detailed worker information
    Show {
        /// Worker ID
        id: String,
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
        id: String,
    },
    /// Show messages assigned to a worker
    Messages {
        /// Worker ID
        id: String,
    },
    /// Release messages from a worker
    Release {
        /// Worker ID
        id: String,
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
    // 2. --config CLI argument (if provided)
    // 3. PGQRS_CONFIG_FILE environment variable
    // 4. PGQRS_DSN and other environment variables
    // 5. Default config files (pgqrs.yaml, pgqrs.yml)
    let config = Config::load_with_options(cli.dsn, cli.config)
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
        QueueCommands::Create { name, unlogged } => {
            tracing::info!("Creating queue '{}' (unlogged: {})...", &name, unlogged);
            admin.create_queue(&name, unlogged).await?;
            tracing::info!("Queue '{}' created successfully", &name);
        }

        QueueCommands::List => {
            tracing::info!("Listing all queues...");
            let meta_results = admin.list_queues().await?;
            writer.write_list(&meta_results, out)?;
        }

        QueueCommands::Delete { name } => {
            tracing::info!("Deleting queue '{}'...", name);
            admin.delete_queue(&name).await?;
            tracing::info!("Queue '{}' deleted successfully", name);
        }

        QueueCommands::Purge { name } => {
            tracing::info!("Purging queue '{}'...", name);
            admin.purge_queue(&name).await?;
            tracing::info!("Queue '{}' purged successfully", name);
        }

        QueueCommands::PurgeArchive { name } => {
            tracing::info!("Purging archive for queue '{}'...", name);
            admin.purge_archive(&name).await?;
            tracing::info!("Archive for queue '{}' purged successfully", name);
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
        MessageCommands::Send {
            queue,
            payload,
            delay,
        } => {
            let queue_obj = admin.get_queue(&queue).await?;
            tracing::info!("Sending message to queue '{}'...", queue);
            let payload_json: serde_json::Value = serde_json::from_str(&payload)?;
            let msg_id = if let Some(delay_secs) = delay {
                tracing::info!("Sending delayed message (delay: {}s)...", delay_secs);
                queue_obj.enqueue_delayed(&payload_json, delay_secs).await?
            } else {
                queue_obj.enqueue(&payload_json).await?
            };
            tracing::info!("Message sent successfully with ID: {}", msg_id);
            Ok(())
        }
        MessageCommands::Read {
            queue,
            count,
            lock_time,
            message_type,
            archive,
        } => {
            let queue_obj = admin.get_queue(&queue).await?;

            if archive {
                tracing::info!(
                    "Reading {} archived messages from queue '{}'...",
                    count,
                    queue
                );
                let messages = queue_obj.archive_list(count as i64, 0).await?;
                tracing::info!("Found {} archived messages", messages.len());
                writer.write_list(&messages, out)?;
                return Ok(());
            }

            tracing::info!(
                "Reading {} messages from queue '{}' (lock_time: {}s)...",
                count,
                queue,
                lock_time
            );
            if let Some(ref msg_type) = message_type {
                tracing::info!("Filtering by message type: '{}'", msg_type);
            }
            let messages = queue_obj.read(count).await?;
            if messages.is_empty() {
                tracing::info!("No messages available");
                return Ok(());
            }
            writer.write_list(&messages, out)?;
            Ok(())
        }
        MessageCommands::Dequeue { queue } => {
            let queue_obj = admin.get_queue(&queue).await?;
            tracing::info!("Dequeuing one message from queue '{}'...", queue);
            let messages = queue_obj.read(1).await?;
            if messages.is_empty() {
                tracing::info!("No messages available");
                return Ok(());
            }
            writer.write_list(&messages, out)?;
            Ok(())
        }
        MessageCommands::Delete { queue, id } => {
            let queue_obj = admin.get_queue(&queue).await?;
            let msg_id = id.parse::<i64>()?;
            tracing::info!("Deleting message {} from queue '{}'...", msg_id, queue);
            let deleted = queue_obj.delete_batch(vec![msg_id]).await?;
            if deleted.first().copied().unwrap_or(false) {
                tracing::info!("Message deleted successfully");
            } else {
                tracing::info!("Message not found or could not be deleted");
            }
            Ok(())
        }
        MessageCommands::Count { queue, archive } => {
            let queue_obj = admin.get_queue(&queue).await?;

            if archive {
                tracing::info!("Getting archived message count for queue '{}'...", queue);
                // Get archive count using archive_list to avoid SQL injection
                let archived_messages = queue_obj.archive_list(i64::MAX, 0).await?;
                let count = archived_messages.len();
                tracing::info!("Archived messages: {}", count);
            } else {
                tracing::info!("Getting pending message count for queue '{}'...", queue);
                let count = queue_obj.pending_count().await?;
                tracing::info!("Pending messages: {}", count);
            }
            Ok(())
        }

        MessageCommands::Show { queue, id, archive } => {
            let queue_obj = admin.get_queue(&queue).await?;
            let msg_id: i64 = id.parse().map_err(|_| PgqrsError::InvalidMessage {
                message: "Invalid message ID format - must be a number".to_string(),
            })?;

            if archive {
                tracing::debug!(
                    "Retrieving archived message {} from queue '{}'...",
                    msg_id,
                    queue
                );
                match queue_obj.get_archived_message_by_id(msg_id).await {
                    Ok(message) => {
                        tracing::info!("Archived message found");
                        writer.write_list(&[message], out)?;
                    }
                    Err(e) => {
                        tracing::error!("Error retrieving archived message: {:?}", e);
                        tracing::debug!("Archived message not found");
                    }
                }
            } else {
                tracing::debug!("Retrieving message {} from queue '{}'...", msg_id, queue);
                match queue_obj.get_message_by_id(msg_id).await {
                    Ok(message) => {
                        tracing::debug!("Message found");
                        writer.write_list(&[message], out)?;
                    }
                    Err(e) => {
                        tracing::error!("Error retrieving message: {:?}", e);
                        tracing::debug!("Message not found");
                    }
                }
            }
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

        WorkerCommands::Show { id } => {
            let worker_id = id.parse::<i64>().map_err(|_| PgqrsError::InvalidMessage {
                message: "Invalid worker ID format - must be a valid integer".to_string(),
            })?;

            let workers = admin.list_all_workers().await?;
            if let Some(worker) = workers.iter().find(|w| w.id == worker_id) {
                tracing::info!("Worker found");
                writer.write_list(&[worker.clone()], out)?;
            } else {
                tracing::error!("Worker not found");
                return Err(anyhow::anyhow!("Worker with ID {} not found", id));
            }
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
            let worker_id = id.parse::<i64>().map_err(|_| PgqrsError::InvalidMessage {
                message: "Invalid worker ID format - must be a valid integer".to_string(),
            })?;

            // Find the worker to get queue context
            let workers = admin.list_all_workers().await?;
            if let Some(worker) = workers.iter().find(|w| w.id == worker_id) {
                let queue = admin.get_queue(&worker.queue_id).await?;
                let worker_instance = pgqrs::Worker {
                    id: worker.id,
                    hostname: worker.hostname.clone(),
                    port: worker.port,
                    queue_id: worker.queue_id.clone(),
                    started_at: worker.started_at,
                    heartbeat_at: worker.heartbeat_at,
                    shutdown_at: worker.shutdown_at,
                    status: worker.status.clone(),
                };

                tracing::info!("Stopping worker {}...", id);
                worker_instance.mark_stopped(&queue).await?;
                tracing::info!("Worker stopped successfully");
            } else {
                return Err(anyhow::anyhow!("Worker with ID {} not found", id));
            }
            Ok(())
        }

        WorkerCommands::Messages { id } => {
            let worker_id = id.parse::<i64>().map_err(|_| PgqrsError::InvalidMessage {
                message: "Invalid worker ID format - must be a valid integer".to_string(),
            })?;

            // Find the worker to get queue context
            let workers = admin.list_all_workers().await?;
            if let Some(worker) = workers.iter().find(|w| w.id == worker_id) {
                let queue = admin.get_queue(&worker.queue_id).await?;
                tracing::info!("Getting messages for worker {}...", id);
                let messages = queue.get_worker_messages(worker_id).await?;
                tracing::info!("Found {} messages", messages.len());
                writer.write_list(&messages, out)?;
            } else {
                return Err(anyhow::anyhow!("Worker with ID {} not found", id));
            }
            Ok(())
        }

        WorkerCommands::Release { id } => {
            let worker_id = id.parse::<i64>().map_err(|_| PgqrsError::InvalidMessage {
                message: "Invalid worker ID format - must be a valid integer".to_string(),
            })?;

            // Find the worker to get queue context
            let workers = admin.list_all_workers().await?;
            if let Some(worker) = workers.iter().find(|w| w.id == worker_id) {
                let queue = admin.get_queue(&worker.queue_id).await?;
                tracing::info!("Releasing messages from worker {}...", id);
                let released_count = queue.release_worker_messages(worker_id).await?;
                tracing::info!("Released {} messages", released_count);
                writeln!(
                    out,
                    "Released {} messages from worker {}",
                    released_count, id
                )?;
            } else {
                return Err(anyhow::anyhow!("Worker with ID {} not found", id));
            }
            Ok(())
        }

        WorkerCommands::Purge { older_than } => {
            // Parse duration string (e.g., "7d", "24h", "30m")
            let duration = parse_duration(&older_than)?;
            tracing::info!("Purging workers older than {:?}...", duration);
            let purged_count = admin.purge_old_workers(duration).await?;
            tracing::info!("Purged {} old workers", purged_count);
            writeln!(out, "Purged {} old workers", purged_count)?;
            Ok(())
        }

        WorkerCommands::Health { queue, max_age } => {
            tracing::info!("Checking worker health for queue '{}'...", queue);
            let workers = admin.list_queue_workers(&queue).await?;
            let max_age_duration = std::time::Duration::from_secs(max_age);

            let mut healthy = 0;
            let mut unhealthy = 0;

            writeln!(out, "Worker Health Report for Queue '{}':", queue)?;
            for worker in &workers {
                let is_healthy = worker.is_healthy(max_age_duration);
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

/// Parse duration string into std::time::Duration
/// Supports formats like "7d", "24h", "30m", "60s"
fn parse_duration(duration_str: &str) -> anyhow::Result<std::time::Duration> {
    let duration_str = duration_str.trim();
    let (number, unit) = if duration_str.ends_with('d') {
        (
            duration_str[..duration_str.len() - 1].parse::<u64>()?,
            "days",
        )
    } else if duration_str.ends_with('h') {
        (
            duration_str[..duration_str.len() - 1].parse::<u64>()?,
            "hours",
        )
    } else if duration_str.ends_with('m') {
        (
            duration_str[..duration_str.len() - 1].parse::<u64>()?,
            "minutes",
        )
    } else if duration_str.ends_with('s') {
        (
            duration_str[..duration_str.len() - 1].parse::<u64>()?,
            "seconds",
        )
    } else {
        return Err(anyhow::anyhow!("Invalid duration format. Use 'd' for days, 'h' for hours, 'm' for minutes, 's' for seconds"));
    };

    let duration = match unit {
        "days" => std::time::Duration::from_secs(number * 24 * 60 * 60),
        "hours" => std::time::Duration::from_secs(number * 60 * 60),
        "minutes" => std::time::Duration::from_secs(number * 60),
        "seconds" => std::time::Duration::from_secs(number),
        _ => return Err(anyhow::anyhow!("Invalid duration unit")),
    };

    Ok(duration)
}
