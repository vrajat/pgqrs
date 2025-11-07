
# pgqrs

A PostgreSQL-backed job queue for Rust applications.

## Features
- **Lightweight**: Use `pgqrs` as a library in your Rust applications.
- **Compatible with Connection Poolers**: Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
- **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching
- **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by time limit
- **Message Archiving**: Built-in archiving system for audit trails and historical data retention

## Architecture

pgqrs is a distributed job queue system built around PostgreSQL. The architecture consists of several components that work together to provide reliable, scalable background job processing.

### System Overview

```mermaid
graph TB
    subgraph "Application Layer"
        P["Producer Services<br/>Your Rust Apps"]
        W["Worker Services<br/>Your Rust Apps"]
        A["Admin Server<br/>Optional Monitoring"]
    end

    subgraph "pgqrs APIs"
        PQ["Queue API<br/>enqueue()<br/>batch_enqueue()"]
        WQ["Queue API<br/>read()<br/>delete_batch()<br/>fail_message()"]
        AA["PgqrsAdmin API<br/>queue_metrics()<br/>create_queue()"]
    end

    CLI["pgqrs CLI<br/>install<br/>queue create<br/>message send<br/>metrics"]

    subgraph "Connection Layer"
        CP["Connection Pooler<br/>PgBouncer/pgcat<br/>Optional"]
    end

    subgraph "PostgreSQL Database"
        subgraph "pgqrs Schema"
            QT["Queue Tables<br/>queue_email<br/>queue_tasks<br/>queue_reports"]
            AT["Archive Tables<br/>archive_email<br/>archive_tasks<br/>archive_reports"]
            META["Metadata Tables<br/>pgqrs.meta"]
        end
    end

    %% Connections
    P --> PQ
    W --> WQ
    A --> AA

    PQ --> CP
    WQ --> CP
    AA --> CP
    CLI --> CP

    CP --> QT
    CP --> AT
    CP --> META

    %% Alternative direct connection (without pooler)
    PQ -.-> QT
    WQ -.-> QT
    WQ -.-> AT
    AA -.-> QT
    AA -.-> AT
    CLI -.-> QT
    CLI -.-> AT

    %% Styling
    classDef userApp fill:#e1f5fe
    classDef pgqrsLib fill:#f3e5f5
    classDef database fill:#e8f5e8
    classDef optional fill:#fff3e0

    class P,W,A userApp
    class PQ,WQ,AA,CLI pgqrsLib
    class QT,AT,META database
    class CP optional
```

### Component Details

#### 1. **PostgreSQL Database**
- **Central storage** for all queue data, archived messages, and metadata
- **ACID compliance** ensures message durability and exactly-once processing within a visibility timeout.
- **SKIP LOCKED** feature enables efficient concurrent message processing
- **Schema isolation** via dedicated `pgqrs` schema
- **Archive system** maintains processed message history in separate archive tables

#### 2. **Connection Pooler (Optional)**
- **PgBouncer or pgcat** for connection management and scaling
- **Connection multiplexing** allows more workers than database connections

#### 3. **Producer Services**
- **Your Rust applications** that create and enqueue jobs
- **Uses pgqrs library** to interact with queues programmatically
- **Key operations**:
  - `queue.enqueue(payload)` - Add single job
  - `queue.batch_enqueue(payloads)` - Add multiple jobs efficiently
  - `queue.enqueue_delayed(payload, delay)` - Schedule future jobs

#### 4. **Worker Services**
- **Your Rust applications** that process jobs from queues
- **Consumes messages** using pgqrs library APIs
- **Key operations**:
  - `queue.read(batch_size)` - Fetch jobs for processing
  - `queue.delete_batch(msg_ids)` - Mark jobs as completed
  - `queue.archive(msg_id, worker_id)` - Archive processed messages for audit trail

#### 5. **Admin Server (Optional)**
- **Your monitoring/admin service** using `PgqrsAdmin` APIs
- **Operational management** and metrics collection
- **Key operations**:
  - `admin.queue_metrics(name)` - Get queue health metrics
  - `admin.all_queues_metrics()` - System-wide monitoring
  - `admin.create_queue(name)` - Queue lifecycle management
  - `admin.purge_archive(name)` - Archive cleanup operations

#### 6. **pgqrs CLI**
- **Command-line tool** for administrative operations
- **Direct database access** for debugging and management
- **Key commands**:
  - `pgqrs install` - Set up database schema
  - `pgqrs queue create <name>` - Create new queues
  - `pgqrs message send <queue> <payload>` - Manual job creation
  - `pgqrs queue metrics <name>` - Inspect queue health
  - `pgqrs archive list <queue>` - View processed message history

### Data Flow

1. **Job Creation**: Producer services use `queue.enqueue()` to add jobs to PostgreSQL
2. **Job Processing**: Worker services use `queue.read()` to fetch and process jobs
3. **Job Completion**: Workers call `queue.delete_batch()` to mark jobs as done
4. **Job Archiving**: Optionally, workers use `queue.archive()` to preserve processed message history
5. **Error Handling**: Failed jobs automatically retry or move to dead letter queues
6. **Monitoring**: Admin services and CLI provide operational visibility into both active and archived messages

### Scalability Patterns

- **Horizontal Workers**: Run multiple worker instances for increased throughput
- **Queue Partitioning**: Use multiple queues to distribute load
- **Connection Pooling**: PgBouncer enables more workers than database connections
- **Batch Processing**: Process multiple jobs per database transaction for efficiency

### Deployment Considerations

- **Database**: Single PostgreSQL instance or managed service (RDS, Cloud SQL)
- **Workers**: Deploy as separate services/containers, scale independently
- **Producers**: Integrate pgqrs library into existing application services
- **Admin/CLI**: Use for operational management and debugging


## Getting Started

### Install the binary

```
cargo install pgqrs
```

### Start a Postgres DB or get the DSN of an existing db.

You'll need a PostgreSQL database to use pgqrs. Here are your options:

#### Option 1: Using Docker (Recommended for development)
```bash
# Start a PostgreSQL container
docker run --name pgqrs-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15

# Your DSN will be:
# postgresql://postgres:postgres@localhost:5432/postgres
```

#### Option 2: Using an existing PostgreSQL database
Get your database connection string (DSN) in this format:
```
postgresql://username:password@hostname:port/database
```

#### Option 3: Using a cloud PostgreSQL service
- **AWS RDS**: Get the connection string from the RDS console
- **Google Cloud SQL**: Get the connection string from the Cloud Console
- **Azure Database**: Get the connection string from the Azure portal
- **Heroku Postgres**: Use the `DATABASE_URL` from your Heroku config

### Configure pgqrs

Set your database connection using one of these methods (in order of priority):

```bash
# Method 1: Command line argument (highest priority)
pgqrs --dsn "postgresql://postgres:postgres@localhost:5432/postgres"

# Method 2: Environment variable
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
pgqrs ...
```

Create a `pgqrs.yaml` file:
```yaml
dsn: "postgresql://postgres:postgres@localhost:5432/postgres"
```

Then run:
```bash
# Method 3: Use a yaml config file.
pgqrs ...
```

### Install the pgqrs schema

pgqrs requires a few tables to store metadata. It creates these tables as well as
queue tables in the schema `pgqrs`.

Once you have your database configured, install the pgqrs schema:

```bash
pgqrs install
# Verify the state
pgqrs verify
```

### Test queue commands from the CLI

Items can be enqueued or dequeued using the CLI. This option is only available for testing
or experiments.

```bash
# Create a test queue
pgqrs queue create test_queue

# Send a message to the queue
pgqrs message send test_queue '{"message": "Hello, World!", "priority": 1}'

# Send a delayed message (available after 30 seconds)
pgqrs message send test_queue '{"task": "delayed_task"}' --delay 30


# Read and immediately consume one message
pgqrs message dequeue test_queue

# Delete a specific message by ID
pgqrs message delete test_queue 12345
```

## Queue API

Add to your `Cargo.toml`:

```toml
[dependencies]
pgqrs = "0.1.0"
```

See `examples/basic_usage.rs` for a full example. Typical usage:

```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration - choose one of these approaches:

    // Option 1: Load from multiple sources automatically (recommended)
    let config = Config::load().expect("Failed to load configuration");

    // Option 2: Load from environment variables
    // let config = Config::from_env().expect("PGQRS_DSN environment variable required");

    // Option 3: Load from a specific file
    // let config = Config::from_file("pgqrs.yaml").expect("Failed to load config");

    // Option 4: Create with explicit DSN
    // let config = Config::from_dsn("postgresql://postgres:postgres@localhost:5432/postgres");

    let admin = PgqrsAdmin::new(&config).await?;

    // Create queues
    admin.create_queue("email_queue", false).await?;
    admin.create_queue("task_queue", false).await?;

    // Send messages
    let email_payload = json!({
        "to": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    });
    let email_queue = admin.get_queue("email_queue").await?;
    let email_id = email_queue.enqueue(&email_payload).await?;
    println!("Sent email message with ID: {}", email_id);

    // Read messages
    let messages = email_queue.read(10).await?;
    println!("Read {} messages", messages.len());

    // Process and archive messages
    if let Some(msg) = messages.first() {
        // Process the message here...
        println!("Processing message: {}", msg.msg_id);

        // Archive the message to maintain audit trail
        let archived = email_queue.archive(msg.msg_id, Some("worker-01")).await?;
        if archived {
            println!("Archived message {} successfully", msg.msg_id);
        }

        // Or delete the message if archiving is not needed
        // let deleted = email_queue.delete_batch(vec![msg.msg_id]).await?;
        // if deleted.first().copied().unwrap_or(false) {
        //     println!("Deleted message {}", msg.msg_id);
        // }
    }

    Ok(())
}
```

## Configuration

pgqrs uses a prioritized configuration system. Configuration is loaded in the following order (highest priority first):

### 1. Command Line Arguments (Highest Priority)
```bash
# Override DSN via command line
pgqrs --dsn "postgresql://user:pass@localhost/db" verify

# Override config file location
pgqrs --config "custom-config.yaml" verify
```

### 2. Environment Variables
```bash
# Required: Database connection string
export PGQRS_DSN="postgresql://user:pass@localhost/db"

# Optional: Connection pool settings
export PGQRS_MAX_CONNECTIONS=32
export PGQRS_CONNECTION_TIMEOUT=60

# Optional: Default job settings
export PGQRS_DEFAULT_LOCK_TIME=10
export PGQRS_DEFAULT_BATCH_SIZE=200

# Optional: Config file location
export PGQRS_CONFIG_FILE="path/to/config.yaml"
```

### 3. Configuration File
Create a YAML configuration file (default locations: `pgqrs.yaml`, `pgqrs.yml`):

```yaml
# Required: Database connection string
dsn: "postgresql://user:pass@localhost/db"

# Optional: Connection pool settings (defaults shown)
max_connections: 16
connection_timeout_seconds: 30

# Optional: Default job settings (defaults shown)
default_lock_time_seconds: 5
default_max_batch_size: 100
```

### 4. Programmatic Configuration
```rust
use pgqrs::config::Config;

// Create from explicit DSN
let config = Config::from_dsn("postgresql://user:pass@localhost/db");

// Load from environment variables
let config = Config::from_env()?;

// Load from specific file
let config = Config::from_file("config.yaml")?;

// Load automatically with priority order
let config = Config::load()?;

// Load with explicit overrides (for CLI tools)
let config = Config::load_with_options(
    Some("postgresql://explicit:dsn@localhost/db"), // DSN override
    Some("custom-config.yaml")                      // Config file override
)?;
```

### Configuration Reference

| Field | Environment Variable | Description | Default |
|-------|---------------------|-------------|---------|
| `dsn` | `PGQRS_DSN` | PostgreSQL connection string | **Required** |
| `max_connections` | `PGQRS_MAX_CONNECTIONS` | Maximum database connections | 16 |
| `connection_timeout_seconds` | `PGQRS_CONNECTION_TIMEOUT` | Connection timeout in seconds | 30 |
| `default_lock_time_seconds` | `PGQRS_DEFAULT_LOCK_TIME` | Default job lock time | 5 |
| `default_max_batch_size` | `PGQRS_DEFAULT_BATCH_SIZE` | Default batch size for operations | 100 |

## CLI Usage

The CLI is defined in `src/main.rs` and supports the following commands:

### Top-level commands

- `install` — Install pgqrs schema
- `uninstall` — Uninstall pgqrs schema
- `verify` — Verify installation
- `queue <subcommand>` — Queue management
- `message <subcommand>` — Message management

### Queue commands

- `queue create <name>` — Create a new queue
- `queue list` — List all queues
- `queue delete <name>` — Delete a queue
- `queue purge <name>` — Purge all messages from a queue
- `queue metrics [<name>]` — Show metrics for a queue or all queues

### Message commands

- `message send <queue> <payload> [--delay <seconds>]` — Send a message (payload is JSON)
- `message read <queue> [--count <n>] [--lock-time <seconds>] [--message-type <type>]` — Read messages
- `message dequeue <queue>` — Read and return one message
- `message delete <queue> <id>` — Delete a message by ID
- `message count <queue>` — Show pending message count
- `message show <queue> <id>` — Show message details by ID
- `message show <queue> <id> --archive` — Show archived message details by ID

### Archive commands

pgqrs provides message archiving functionality to maintain a historical record of processed messages while keeping the active queue performant.

- `archive <queue> <id>` — Archive a specific message by ID
- `archive list <queue> [--limit <n>] [--offset <n>]` — List archived messages
- `archive purge <queue>` — Delete all archived messages for a queue
- `message count <queue> --archive` — Show archived message count

#### Archive System Overview

The archive system automatically creates archive tables (`archive_<queue_name>`) when queues are created. Archived messages retain all original data plus additional metadata:

- `archived_at` — Timestamp when the message was archived
- `archived_by` — Optional identifier of the archiving process
- `processing_duration` — Time taken to process the message (if applicable)

#### Archive Usage Examples

```bash
# Archive a specific message that has been processed
pgqrs archive email_queue 12345

# List the 10 most recent archived messages
pgqrs archive list email_queue --limit 10

# List archived messages with pagination
pgqrs archive list email_queue --limit 20 --offset 100

# Show details of an archived message
pgqrs message show email_queue 12345 --archive

# Check how many messages are archived
pgqrs message count email_queue --archive

# Clean up old archived messages
pgqrs archive purge email_queue
```

#### Programmatic Archive API

The archive functionality is also available through the Rust API:

```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    let admin = PgqrsAdmin::new(&config).await?;
    let queue = admin.get_queue("email_queue").await?;

    // Archive a message after processing
    let archived = queue.archive(message_id, Some("worker-01")).await?;
    if archived {
        println!("Message {} archived successfully", message_id);
    }

    // List archived messages with pagination
    let archived_messages = queue.archive_list(50, 0).await?;
    println!("Found {} archived messages", archived_messages.len());

    // Get specific archived message by ID
    let archived_msg = queue.get_archived_message_by_id(message_id).await?;
    println!("Archived message: {:?}", archived_msg);

    // Purge old archived messages (admin operation)
    admin.purge_archive("email_queue").await?;
    println!("Archive purged");

    Ok(())
}
```

#### Archive Best Practices

- **Archive after processing**: Archive messages only after successful processing
- **Include worker ID**: Use the `archived_by` parameter to track which worker processed the message
- **Regular cleanup**: Periodically purge old archived messages to manage database size
- **Monitoring**: Track archive growth as part of your queue metrics
- **Retention policy**: Establish how long to keep archived messages based on your compliance needs

### Output and Logging Options

All commands support global flags:

- `-d, --dsn <DSN>` — Database URL (highest priority, overrides all other config sources)
- `-c, --config <CONFIG>` — Config file path (overrides environment variables and defaults)
- `--log-dest <stderr|file>` — Log destination
- `--log-level <error|warn|info|debug|trace>` — Log level
- `--format <json|table>` — Output format
- `--out <stdout|file>` — Output destination

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.