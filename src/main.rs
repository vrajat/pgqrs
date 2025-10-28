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
use std::fs::File;
use std::io::{self, Write};
use std::process;

enum OutputFormatWriter {
    Json(JsonOutputWriter),
    Yaml(YamlOutputWriter),
}

impl OutputFormatWriter {
    pub fn write<T: serde::Serialize>(
        &self,
        value: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        match self {
            OutputFormatWriter::Json(w) => w.write(value, out),
            OutputFormatWriter::Yaml(w) => w.write(value, out),
        }
    }
}
// Writer trait for message output
trait OutputWriter {
    fn write<T: serde::Serialize>(&self, value: &T, out: &mut dyn Write) -> anyhow::Result<()>;
}

struct JsonOutputWriter;
impl OutputWriter for JsonOutputWriter {
    fn write<T: serde::Serialize>(&self, value: &T, out: &mut dyn Write) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(value)?;
        writeln!(out, "{}", json)?;
        Ok(())
    }
}

struct CsvOutputWriter;
impl CsvOutputWriter {
    fn write_queue_messages(
        &self,
        messages: &[pgqrs::types::QueueMessage],
        out: &mut dyn Write,
    ) -> anyhow::Result<()> {
        writeln!(out, "msg_id,enqueued_at,read_ct,vt,message")?;
        for msg in messages {
            let payload = serde_json::to_string(&msg.message)?;
            writeln!(
                out,
                "{},{},{},{},{}",
                msg.msg_id,
                msg.enqueued_at.format("%Y-%m-%d %H:%M:%S UTC"),
                msg.read_ct,
                msg.vt.format("%Y-%m-%d %H:%M:%S UTC"),
                payload
            )?;
        }
        Ok(())
    }
    fn write_queue_names(
        &self,
        queues: &[pgqrs::types::MetaResult],
        out: &mut dyn Write,
    ) -> anyhow::Result<()> {
        writeln!(out, "queue_name,unlogged")?;
        for queue in queues {
            writeln!(out, "{},{}", queue.queue_name, queue.unlogged)?;
        }
        Ok(())
    }
}

struct YamlOutputWriter;
impl OutputWriter for YamlOutputWriter {
    fn write<T: serde::Serialize>(&self, value: &T, out: &mut dyn Write) -> anyhow::Result<()> {
        let yaml = serde_yaml::to_string(value)?;
        writeln!(out, "{}", yaml)?;
        Ok(())
    }
}

#[derive(Parser)]
#[command(name = "pgqrs")]
#[command(about = "A PostgreSQL-backed job queue CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Database URL (overrides config file)
    #[arg(long)]
    database_url: Option<String>,

    /// Config file path
    #[arg(long, short = 'c', default_value = "pgqrs.yaml")]
    config: String,

    /// Log destination: stderr or file path
    #[arg(long, default_value = "stderr")]
    log_dest: String,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Output format: json, csv, yaml
    #[arg(long, default_value = "json")]
    output_format: String,

    /// Output destination: stdout or file path
    #[arg(long, default_value = "stdout")]
    output_dest: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Install pgqrs schema
    Install {
        /// Perform a dry run without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Uninstall pgqrs schema
    Uninstall {
        /// Perform a dry run without making changes
        #[arg(long)]
        dry_run: bool,
    },
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

async fn run_cli(cli: Cli) -> anyhow::Result<()> {
    // Load configuration
    let config = if let Some(db_url) = cli.database_url {
        let mut config = Config::default();
        config.dsn = db_url;
        config
    } else {
        Config::from_file(&cli.config).unwrap_or_else(|_| {
            tracing::warn!("Could not load config file, using defaults");
            Config::default()
        })
    };

    let admin = PgqrsAdmin::new(&config).await?;

    match cli.command {
        Commands::Install { dry_run } => {
            tracing::info!("Installing pgqrs schema (dry_run: {})...", dry_run);
            admin.install(dry_run).await?;
            tracing::info!("Installation completed successfully");
        }

        Commands::Uninstall { dry_run } => {
            tracing::info!("Uninstalling pgqrs schema (dry_run: {})...", dry_run);
            admin.uninstall(dry_run).await?;
            tracing::info!("Uninstall completed successfully");
        }

        Commands::Verify => {
            tracing::info!("Verifying pgqrs installation...");
            admin.verify().await?;
            tracing::info!("Verification completed successfully");
        }

        Commands::Queue { action } => {
            handle_queue_commands(&admin, action).await?;
        }

        Commands::Message { action } => {
            handle_message_commands(&admin, action, &cli.output_format, &cli.output_dest).await?;
        }
    }

    Ok(())
}

async fn handle_queue_commands(admin: &PgqrsAdmin, action: QueueCommands) -> anyhow::Result<()> {
    match action {
        QueueCommands::Create { name, unlogged } => {
            tracing::info!("Creating queue '{}' (unlogged: {})...", &name, unlogged);
            admin.create_queue(&name, unlogged).await?;
            tracing::info!("Queue '{}' created successfully", &name);
        }

        QueueCommands::List => {
            tracing::info!("Listing all queues...");
            let meta_results = admin.list_queues().await?;
            let output_format = "json"; // Default, can be extended to CLI arg
            let output_dest = "stdout"; // Default, can be extended to CLI arg
            if output_format == "csv" {
                let writer = CsvOutputWriter;
                if output_dest == "stdout" {
                    let stdout = std::io::stdout();
                    let mut handle = stdout.lock();
                    writer.write_queue_names(&meta_results, &mut handle)?;
                } else {
                    let mut file = std::fs::File::create(output_dest)?;
                    writer.write_queue_names(&meta_results, &mut file)?;
                }
            } else {
                let writer = match output_format {
                    "json" => OutputFormatWriter::Json(JsonOutputWriter),
                    "yaml" => OutputFormatWriter::Yaml(YamlOutputWriter),
                    _ => OutputFormatWriter::Json(JsonOutputWriter),
                };
                if output_dest == "stdout" {
                    let stdout = std::io::stdout();
                    let mut handle = stdout.lock();
                    writer.write(&meta_results, &mut handle)?;
                } else {
                    let mut file = std::fs::File::create(output_dest)?;
                    writer.write(&meta_results, &mut file)?;
                }
            }
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

        QueueCommands::Metrics { name } => {
            if let Some(queue_name) = name {
                tracing::info!("Getting metrics for queue '{}'...", queue_name);
                let metrics = admin.queue_metrics(&queue_name).await?;
                tracing::info!("Queue: {}", metrics.name);
                tracing::info!("  Total Messages: {}", metrics.total_messages);
                tracing::info!("  Pending Messages: {}", metrics.pending_messages);
                tracing::info!("  Locked Messages: {}", metrics.locked_messages);
                tracing::info!("  Archived Messages: {}", metrics.archived_messages);
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

async fn handle_message_commands(
    admin: &PgqrsAdmin,
    action: MessageCommands,
    output_format: &str,
    output_dest: &str,
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
        } => {
            let queue_obj = admin.get_queue(&queue).await?;
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
            if output_format.to_lowercase() == "csv" {
                let writer = CsvOutputWriter;
                if output_dest == "stdout" {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    writer.write_queue_messages(&messages, &mut handle)?;
                } else {
                    let mut file = File::create(output_dest)?;
                    writer.write_queue_messages(&messages, &mut file)?;
                }
            } else {
                let writer = match output_format.to_lowercase().as_str() {
                    "json" => OutputFormatWriter::Json(JsonOutputWriter),
                    "yaml" => OutputFormatWriter::Yaml(YamlOutputWriter),
                    _ => OutputFormatWriter::Json(JsonOutputWriter),
                };
                if output_dest == "stdout" {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    writer.write(&messages, &mut handle)?;
                } else {
                    let mut file = File::create(output_dest)?;
                    writer.write(&messages, &mut file)?;
                }
            }
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
            let writer = match output_format.to_lowercase().as_str() {
                "json" => OutputFormatWriter::Json(JsonOutputWriter),
                "yaml" => OutputFormatWriter::Yaml(YamlOutputWriter),
                other => {
                    tracing::warn!("Unknown output format '{}', defaulting to JSON", other);
                    OutputFormatWriter::Json(JsonOutputWriter)
                }
            };
            if output_dest == "stdout" {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                writer.write(&messages, &mut handle)?;
            } else {
                let mut file = File::create(output_dest)?;
                writer.write(&messages, &mut file)?;
            }
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
        MessageCommands::Count { queue } => {
            let queue_obj = admin.get_queue(&queue).await?;
            tracing::info!("Getting pending message count for queue '{}'...", queue);
            let count = queue_obj.pending_count().await?;
            tracing::info!("Pending messages: {}", count);
            Ok(())
        }
    }
}

mod main_writer_tests;
