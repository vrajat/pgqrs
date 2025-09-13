use clap::{Parser, Subcommand};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use pgqrs::{create_pool, Config, Queue};
use std::process;

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

    /// Enable verbose output
    #[arg(long, short = 'v')]
    verbose: bool,
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
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(if cli.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    if let Err(e) = run_cli(cli).await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

async fn run_cli(cli: Cli) -> anyhow::Result<()> {
    // Load configuration
    let config = if let Some(db_url) = cli.database_url {
        let mut config = Config::default();
        config.dsn = db_url;
        // TODO: Parse database URL and update config
        config
    } else {
        Config::from_file(&cli.config).unwrap_or_else(|_| {
            tracing::warn!("Could not load config file, using defaults");
            Config::default()
        })
    };

    let pool = create_pool(&config)?;
    let admin = pgqrs::Admin::new(&pool);

    match cli.command {
        Commands::Install { dry_run } => {
            println!("Installing pgqrs schema (dry_run: {})...", dry_run);
            admin.install(dry_run)?;
            println!("Installation completed successfully");
        }

        Commands::Uninstall { dry_run } => {
            println!("Uninstalling pgqrs schema (dry_run: {})...", dry_run);
            admin.uninstall(dry_run)?;
            println!("Uninstall completed successfully");
        }

        Commands::Verify => {
            println!("Verifying pgqrs installation...");
            admin.verify()?;
            println!("Verification completed successfully");
        }

        Commands::Queue { action } => {
            handle_queue_commands(&pool, action).await?;
        }

        Commands::Message { action } => {
            handle_message_commands(&pool, action).await?;
        }
    }

    Ok(())
}

async fn handle_queue_commands(
    pool: &Pool<ConnectionManager<PgConnection>>,
    action: QueueCommands,
) -> anyhow::Result<()> {
    let admin = pgqrs::Admin::new(pool);
    match action {
        QueueCommands::Create { name } => {
            println!("Creating queue '{}'...", &name);
            admin.create_queue(&name).await?;
            println!("Queue '{}' created successfully", &name);
        }

        QueueCommands::List => {
            println!("Listing all queues...");
            let queues = admin.list_queues().await?;
            if queues.is_empty() {
                println!("No queues found");
            } else {
                println!("Queues:");
                for queue in queues {
                    println!("  {}", queue);
                }
            }
        }

        QueueCommands::Delete { name } => {
            println!("Deleting queue '{}'...", name);
            admin.delete_queue(&name).await?;
            println!("Queue '{}' deleted successfully", name);
        }

        QueueCommands::Purge { name } => {
            println!("Purging queue '{}'...", name);
            admin.purge_queue(&name).await?;
            println!("Queue '{}' purged successfully", name);
        }

        QueueCommands::Metrics { name } => {
            if let Some(queue_name) = name {
                println!("Getting metrics for queue '{}'...", queue_name);
                let metrics = admin.queue_metrics(&queue_name).await?;
                print_queue_metrics(&metrics);
            } else {
                println!("Getting metrics for all queues...");
                let metrics = admin.all_queues_metrics().await?;
                if metrics.is_empty() {
                    println!("No queues found");
                } else {
                    for metric in metrics {
                        print_queue_metrics(&metric);
                        println!();
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_message_commands(
    pool: &'_ Pool<ConnectionManager<PgConnection>>,
    action: MessageCommands,
) -> anyhow::Result<()> {
    match action {
        MessageCommands::Send {
            queue,
            payload,
            delay,
        } => {
            let queue_obj = Queue::new(&pool, &queue);
            println!("Sending message to queue '{}'...", queue);
            let payload_json: serde_json::Value = serde_json::from_str(&payload)?;
            // Parse JSON message
            let msg_id = if let Some(delay_secs) = delay {
                println!("Sending delayed message (delay: {}s)...", delay_secs);
                queue_obj.enqueue_delayed(&payload_json, delay_secs).await?
            } else {
                queue_obj.enqueue(&payload_json).await?
            };

            println!("Message sent successfully with ID: {}", msg_id);
        }

        MessageCommands::Read {
            queue,
            count,
            lock_time,
            message_type,
        } => {
            let queue_obj = Queue::new(&pool, &queue);

            println!(
                "Reading {} messages from queue '{}' (lock_time: {}s)...",
                count, queue, lock_time
            );
            if let Some(ref msg_type) = message_type {
                println!("Filtering by message type: '{}'", msg_type);
            }

            let messages = queue_obj.read(lock_time, count).await?;
            if messages.is_empty() {
                println!("No messages available");
            } else {
                println!("Found {} messages:", messages.len());
                println!();

                for (i, msg) in messages.iter().enumerate() {
                    println!("Message {} of {}:", i + 1, messages.len());
                    println!("  ID: {}", msg.msg_id);
                    println!(
                        "  Enqueued: {}",
                        msg.enqueued_at.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                    println!("  Read Count: {}", msg.read_ct);
                    println!(
                        "  Visible Until: {}",
                        msg.vt.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                    println!("  Payload:");
                    println!("{}", serde_json::to_string_pretty(&msg.message)?);

                    if i < messages.len() - 1 {
                        println!("  ---");
                    }
                }
            }
        }

        MessageCommands::Delete { queue, id } => {
            let queue_obj = Queue::new(&pool, &queue);

            let msg_id = id.parse::<i64>()?;
            println!("Deleting message {} from queue '{}'...", msg_id, queue);

            let deleted = queue_obj.delete_batch(vec![msg_id]).await?;
            if deleted.first().copied().unwrap_or(false) {
                println!("Message deleted successfully");
            } else {
                println!("Message not found or could not be deleted");
            }
        }

        MessageCommands::Count { queue } => {
            let queue_obj = Queue::new(&pool, &queue);

            println!("Getting pending message count for queue '{}'...", queue);
            let count = queue_obj.pending_count().await?;
            println!("Pending messages: {}", count);
        }
    }
    Ok(())
}

fn print_queue_metrics(metrics: &pgqrs::QueueMetrics) {
    println!("Queue: {}", metrics.name);
    println!("  Total Messages: {}", metrics.total_messages);
    println!("  Pending Messages: {}", metrics.pending_messages);
    println!("  Locked Messages: {}", metrics.locked_messages);
    println!("  Archived Messages: {}", metrics.archived_messages);

    if let Some(oldest) = metrics.oldest_pending_message {
        println!(
            "  Oldest Pending: {}",
            oldest.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }

    if let Some(newest) = metrics.newest_message {
        println!(
            "  Newest Message: {}",
            newest.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }
}
