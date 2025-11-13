
# pgqrs

A PostgreSQL-backed job queue for Rust applications.

## Features
- **Lightweight**: Use `pgqrs` as a library in your Rust applications.
- **Compatible with Connection Poolers**: Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
- **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching
- **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by time limit
- **Message Archiving**: Built-in archiving system for audit trails and historical data retention

## Architecture

pgqrs is a distributed job queue system built around PostgreSQL with a clean separation of concerns. The architecture consists of Producer/Consumer roles and a unified table interface for reliable, scalable background job processing.

### System Overview

```mermaid
graph TB
    subgraph "Application Layer"
        P["Producer Services<br/>Your Rust Apps"]
        W["Worker/Consumer Services<br/>Your Rust Apps"]
        A["Admin Server<br/>Optional Monitoring"]
    end

    subgraph "pgqrs APIs"
        PROD["Producer API<br/>enqueue()<br/>batch_enqueue()<br/>extend_visibility()"]
        CONS["Consumer API<br/>dequeue()<br/>archive()<br/>delete()"]
        TABLES["Table APIs<br/>PgqrsQueues<br/>PgqrsWorkers<br/>PgqrsMessages<br/>PgqrsArchiveTable"]
        AA["PgqrsAdmin API<br/>queue_metrics()<br/>create_queue()<br/>worker_management()"]
    end

    CLI["pgqrs CLI<br/>install<br/>queue create<br/>message send<br/>metrics"]

    subgraph "Connection Layer"
        CP["Connection Pooler<br/>PgBouncer/pgcat<br/>Optional"]
    end

    subgraph "PostgreSQL Database"
        subgraph "pgqrs Schema"
            QT["pgqrs_queues<br/>Queue definitions"]
            WT["pgqrs_workers<br/>Worker registrations"]
            MT["pgqrs_messages<br/>Active messages"]
            AT["pgqrs_archive<br/>Processed messages"]
            META["Metadata & Indexes<br/>Foreign keys & constraints"]
        end
    end

    %% Connections
    P --> PROD
    W --> CONS
    A --> AA
    A --> TABLES

    PROD --> CP
    CONS --> CP
    TABLES --> CP
    AA --> CP
    CLI --> CP

    CP --> QT
    CP --> WT
    CP --> MT
    CP --> AT
    CP --> META

    %% Alternative direct connection (without pooler)
    PROD -.-> QT
    PROD -.-> MT
    CONS -.-> MT
    CONS -.-> AT
    TABLES -.-> QT
    TABLES -.-> WT
    TABLES -.-> MT
    TABLES -.-> AT
    AA -.-> QT
    AA -.-> WT
    AA -.-> MT
    AA -.-> AT
    CLI -.-> QT
    CLI -.-> WT
    CLI -.-> MT
    CLI -.-> AT

    %% Styling
    classDef userApp fill:#e1f5fe
    classDef pgqrsLib fill:#f3e5f5
    classDef database fill:#e8f5e8
    classDef optional fill:#fff3e0

    class P,W,A userApp
    class PROD,CONS,TABLES,AA,CLI pgqrsLib
    class QT,WT,MT,AT,META database
    class CP optional
```

### Component Details

#### 1. **PostgreSQL Database**
- **Central storage** for all queue data with proper relational design
- **Four core tables** with foreign key relationships:
  - `pgqrs_queues`: Queue definitions and metadata
  - `pgqrs_workers`: Worker registrations linked to queues
  - `pgqrs_messages`: Active messages awaiting processing
  - `pgqrs_archive`: Processed messages for audit trails
- **ACID compliance** ensures message durability and exactly-once processing
- **SKIP LOCKED** feature enables efficient concurrent message processing
- **Referential integrity** maintains data consistency across tables

#### 2. **Producer Services - Message Creation**
- **Your Rust applications** that create and enqueue jobs
- **Dedicated Producer API** for message creation operations
- **Key operations**:
  - `producer.enqueue(payload)` - Add single job to queue
  - `producer.batch_enqueue(payloads)` - Add multiple jobs efficiently
  - `producer.enqueue_delayed(payload, delay)` - Schedule future jobs
  - `producer.extend_visibility(msg_id, duration)` - Extend job processing time
- **Validation & rate limiting** built-in for message integrity
- **Queue-specific instances** for type safety and performance

#### 3. **Consumer Services - Message Processing**
- **Your Rust applications** that process jobs from queues
- **Dedicated Consumer API** for message consumption operations
- **Key operations**:
  - `consumer.dequeue()` - Fetch single job with automatic locking
  - `consumer.dequeue_batch(size)` - Fetch multiple jobs efficiently
  - `consumer.archive(msg_id)` - Archive processed message for audit trail
  - `consumer.delete(msg_id)` - Remove completed message
  - `consumer.delete_batch(msg_ids)` - Remove multiple completed messages
- **Automatic visibility timeouts** prevent stuck jobs
- **Queue-specific instances** for focused processing

#### 4. **Table APIs - Direct Data Access**
- **Unified Table trait interface** for consistent CRUD operations
- **Four table implementations**:
  - `PgqrsQueues`: Manage queue definitions (`insert`, `get`, `list`, `delete`)
  - `PgqrsWorkers`: Manage worker registrations with queue relationships
  - `PgqrsMessages`: Direct message table access (typically used by Producer/Consumer)
  - `PgqrsArchiveTable`: Archive management with full CRUD support
- **Common interface methods**:
  - `count()` - Count total records
  - `count_by_fk(id)` - Count records by foreign key relationship
  - `filter_by_fk(id)` - List records by foreign key relationship
- **Type-safe operations** with proper error handling

#### 5. **Admin Server - System Management**
- **Your monitoring/admin service** using `PgqrsAdmin` APIs
- **Cross-cutting concerns** for system health and management
- **Key operations**:
  - `admin.queue_metrics(name)` - Get queue health metrics
  - `admin.all_queues_metrics()` - System-wide monitoring
  - `admin.create_queue(name)` - Queue lifecycle management
  - `admin.worker_management()` - Worker registration and health
  - `admin.purge_archive(name)` - Archive cleanup operations
  - `admin.install()` / `admin.uninstall()` - Schema management

#### 6. **Connection Pooler (Optional)**
- **PgBouncer or pgcat** for connection management and scaling
- **Connection multiplexing** allows more workers than database connections
- **Transaction-mode compatibility** for queue operations

#### 7. **pgqrs CLI**
- **Command-line tool** for administrative operations
- **Direct database access** for debugging and management
- **Key commands**:
  - `pgqrs install` - Set up database schema with all tables
  - `pgqrs queue create <name>` - Create new queues
  - `pgqrs message send <queue> <payload>` - Manual job creation
  - `pgqrs queue metrics <name>` - Inspect queue health
  - `pgqrs archive list <queue>` - View processed message history

### Data Flow

1. **Job Creation**: Producer services use dedicated `Producer` instances to add jobs to the `pgqrs_messages` table
2. **Worker Registration**: Worker services register with `pgqrs_workers` table linking to specific queues
3. **Job Processing**: Consumer services use `Consumer` instances to fetch jobs with automatic locking
4. **Job Completion**: Consumers mark jobs as completed by deleting from `pgqrs_messages`
5. **Job Archiving**: Optionally, consumers use `archive()` to move completed jobs to `pgqrs_archive` table
6. **System Monitoring**: Admin services query across all tables for metrics and health monitoring

### Role Separation Benefits

- **Producer Focus**: Dedicated to message creation, validation, and rate limiting
- **Consumer Focus**: Optimized for job fetching, processing, and completion
- **Clear Boundaries**: Producers never read jobs, Consumers never create jobs
- **Independent Scaling**: Scale producers and consumers independently
- **Type Safety**: Queue-specific instances prevent cross-queue contamination

### Scalability Patterns

- **Horizontal Workers**: Run multiple consumer instances for increased throughput
- **Producer Scaling**: Scale producer services independently based on job creation load
- **Queue Partitioning**: Use multiple queues to distribute load and isolate workloads
- **Connection Pooling**: PgBouncer enables more workers than database connections
- **Batch Processing**: Process multiple jobs per database transaction for efficiency
- **Worker Registration**: Track and monitor worker health via `pgqrs_workers` table

### Deployment Considerations

- **Database**: Single PostgreSQL instance or managed service (RDS, Cloud SQL)
- **Workers**: Deploy as separate services/containers, scale independently
- **Producers**: Integrate pgqrs library into existing application services
- **Admin/CLI**: Use for operational management and debugging

### Architecture Benefits

The current pgqrs architecture provides several key advantages:

#### **Clean Separation of Concerns**
- **Producers**: Focus solely on message creation, validation, and rate limiting
- **Consumers**: Optimized for job fetching, processing, and completion tracking
- **Admin**: System-wide management and monitoring operations
- **Tables**: Direct database access for advanced use cases

#### **Unified Data Model**
- **Single schema** with four core tables linked by foreign keys
- **Referential integrity** prevents orphaned records and data corruption
- **Consistent interface** via Table trait across all database operations
- **Predictable scaling** with clear table relationships

#### **Type Safety & Performance**
- **Queue-specific instances** prevent cross-queue contamination
- **Compile-time verification** of message types and operations
- **Optimized queries** with proper indexing and constraints
- **Connection pooling** support for high-concurrency scenarios

#### **Operational Excellence**
- **Built-in monitoring** via metrics and admin APIs
- **Archive system** for compliance and audit requirements
- **Worker tracking** for health monitoring and load balancing
- **CLI tools** for debugging and administrative tasks


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
queue tables in the specified schema.

**Important**: You must create the schema before running `pgqrs install`.

#### Step 1: Create the schema

Connect to your PostgreSQL database and create the schema:

```sql
-- For default 'public' schema (no action needed)
-- For custom schema:
CREATE SCHEMA IF NOT EXISTS pgqrs;
```

#### Step 2: Install pgqrs

Once you have your database configured and schema created, install the pgqrs schema:

```bash
# Install in default 'public' schema
pgqrs install

# Install in custom schema
pgqrs --schema pgqrs install

# Verify the installation
pgqrs verify
# Or verify custom schema
pgqrs --schema pgqrs verify
```

### Test queue commands from the CLI

Items can be enqueued or dequeued using the CLI. This option is only available for testing
or experiments.

```bash
# Create a test queue
pgqrs queue create test_queue

# Send a message to the queue
pgqrs message send test_queue '{"message": "Hello, World!", "timestamp": "2023-01-01T00:00:00Z"}'

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
pgqrs = "0.3.0"
```

### Core APIs

pgqrs provides three main API layers for different use cases:

#### 1. **Producer/Consumer APIs** (Recommended)
High-level, role-specific APIs for most queue operations:
- **Producer**: Message creation, validation, rate limiting
- **Consumer**: Message processing, archiving, deletion

#### 2. **Table APIs** (Advanced)
Low-level, unified CRUD interface for direct database operations:
- **PgqrsQueues**: Queue management
- **PgqrsWorkers**: Worker registration and health
- **PgqrsMessages**: Direct message table access
- **PgqrsArchiveTable**: Archive management

#### 3. **Admin API** (Management)
System-wide operations for monitoring and administration:
- **PgqrsAdmin**: Schema management, metrics, cross-table operations

### Table Trait Interface

All table types implement a unified `Table` trait providing consistent CRUD operations:

```rust
use pgqrs::tables::{PgqrsQueues, PgqrsWorkers, PgqrsMessages, PgqrsArchiveTable, Table};
use pgqrs::config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://postgres:postgres@localhost:5432/postgres");
    let admin = PgqrsAdmin::new(&config).await?;
    let pool = admin.pool.clone();

    // Initialize table interfaces
    let queues = PgqrsQueues::new(pool.clone());
    let workers = PgqrsWorkers::new(pool.clone());
    let messages = PgqrsMessages::new(pool.clone());
    let archives = PgqrsArchiveTable::new(pool.clone());

    // Unified counting operations across all tables
    let total_queues = queues.count().await?;
    let total_workers = workers.count().await?;
    let total_messages = messages.count().await?;
    let total_archives = archives.count().await?;

    println!("System totals: {} queues, {} workers, {} messages, {} archived",
             total_queues, total_workers, total_messages, total_archives);

    // Count by foreign key relationships
    let queue_id = 1;
    let queue_workers = workers.count_by_fk(queue_id).await?;
    let queue_messages = messages.count_by_fk(queue_id).await?;
    let queue_archives = archives.count_by_fk(queue_id).await?;

    println!("Queue {} has: {} workers, {} pending messages, {} archived messages",
             queue_id, queue_workers, queue_messages, queue_archives);

    // Filter operations
    let workers_for_queue = workers.filter_by_fk(queue_id).await?;
    let messages_for_queue = messages.filter_by_fk(queue_id).await?;

    println!("Found {} workers and {} messages for queue {}",
             workers_for_queue.len(), messages_for_queue.len(), queue_id);

    Ok(())
}
```

### Producer/Consumer Usage

See `examples/basic_usage.rs` for a full example. The Producer/Consumer APIs provide clean separation of concerns:

#### Producer Example (Message Creation Service)
```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::Producer;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    let admin = PgqrsAdmin::new(&config).await?;

    // Get queue information
    let queue_info = admin.get_queue("email_queue").await?;

    // Create producer for this queue
    let producer = Producer::new(admin.pool.clone(), &queue_info, &config);

    // Send individual messages
    let welcome_email = json!({
        "to": "user@example.com",
        "template": "welcome",
        "data": {"name": "John Doe"}
    });
    let msg = producer.enqueue(&welcome_email).await?;
    println!("Enqueued welcome email: {}", msg.id);

    // Send delayed message (newsletter after 7 days)
    let newsletter = json!({
        "to": "user@example.com",
        "template": "weekly_newsletter"
    });
    let delayed_msg = producer.enqueue_delayed(&newsletter, std::time::Duration::from_secs(7 * 24 * 3600)).await?;
    println!("Scheduled newsletter: {}", delayed_msg.id);

    // Batch enqueue for efficiency
    let batch_emails = vec![
        json!({"to": "user1@example.com", "template": "promotion"}),
        json!({"to": "user2@example.com", "template": "promotion"}),
        json!({"to": "user3@example.com", "template": "promotion"}),
    ];
    let batch_msgs = producer.batch_enqueue(batch_emails).await?;
    println!("Enqueued {} promotional emails", batch_msgs.len());

    Ok(())
}
```

#### Consumer Example (Message Processing Service)
```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::Consumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    let admin = PgqrsAdmin::new(&config).await?;

    // Get queue information
    let queue_info = admin.get_queue("email_queue").await?;

    // Create consumer for this queue
    let consumer = Consumer::new(admin.pool.clone(), &queue_info);

    // Process single message
    if let Some(message) = consumer.dequeue().await? {
        println!("Processing email: {}", message.id);

        // Parse and send the email
        let email_data = &message.payload;
        // ... send email logic here ...

        // Archive the message for audit trail
        let archived = consumer.archive(message.id).await?;
        if archived {
            println!("Email {} archived successfully", message.id);
        }
    }

    // Batch processing for higher throughput
    let messages = consumer.dequeue_batch(10).await?;
    if !messages.is_empty() {
        println!("Processing batch of {} emails", messages.len());

        let mut processed_ids = Vec::new();
        for message in messages {
            // Process each email
            println!("Sending email to: {}", message.payload["to"]);
            // ... email sending logic ...

            processed_ids.push(message.id);
        }

        // Delete all processed messages
        let results = consumer.delete_batch(processed_ids).await?;
        let success_count = results.iter().filter(|&&success| success).count();
        println!("Successfully processed {} emails", success_count);
    }

    Ok(())
}
```

#### Full Application Example

```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::{Producer, Consumer};
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

    // Set up admin for queue management
    let admin = PgqrsAdmin::new(&config).await?;

    // Create queues
    admin.create_queue("email_queue").await?;
    admin.create_queue("task_queue").await?;

    // Get queue information for creating producers/consumers
    let email_queue_info = admin.get_queue("email_queue").await?;
    let task_queue_info = admin.get_queue("task_queue").await?;

    // Create producers for sending messages
    let email_producer = Producer::new(admin.pool.clone(), &email_queue_info, &config);
    let task_producer = Producer::new(admin.pool.clone(), &task_queue_info, &config);

    // Create consumers for processing messages
    let email_consumer = Consumer::new(admin.pool.clone(), &email_queue_info);
    let task_consumer = Consumer::new(admin.pool.clone(), &task_queue_info);

    // Send messages using producers
    let email_payload = json!({
        "to": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    });
    let email_message = email_producer.enqueue(&email_payload).await?;
    println!("Sent email message with ID: {}", email_message.id);

    let task_payload = json!({
        "task_type": "image_resize",
        "image_url": "https://example.com/image.jpg",
        "dimensions": {"width": 800, "height": 600}
    });
    let task_message = task_producer.enqueue(&task_payload).await?;
    println!("Sent task message with ID: {}", task_message.id);

    // Process messages using consumers

    // Process email messages
    if let Some(email_msg) = email_consumer.dequeue().await? {
        println!("Processing email message: {}", email_msg.id);
        // Process the email here...

        // Archive the message to maintain audit trail
        let archived = email_consumer.archive(email_msg.id).await?;
        if archived {
            println!("Archived email message {} successfully", email_msg.id);
        }
    }

    // Process task messages
    if let Some(task_msg) = task_consumer.dequeue().await? {
        println!("Processing task message: {}", task_msg.id);
        // Process the task here...

        // Delete the message when processing is complete (no archive needed)
        let deleted = task_consumer.delete(task_msg.id).await?;
        if deleted {
            println!("Deleted task message {} successfully", task_msg.id);
        }
    }

    // Batch processing example
    let task_messages = task_consumer.dequeue_batch(5).await?;
    if !task_messages.is_empty() {
        println!("Processing {} task messages in batch", task_messages.len());

        let msg_ids: Vec<i64> = task_messages.iter().map(|m| m.id).collect();
        // Process all tasks...

        // Delete all processed messages in batch
        let deleted_results = task_consumer.delete_batch(msg_ids).await?;
        let success_count = deleted_results.iter().filter(|&&success| success).count();
        println!("Successfully deleted {} task messages", success_count);
    }

    // Admin operations for monitoring
    let email_metrics = admin.queue_metrics("email_queue").await?;
    println!("Email queue metrics: {} pending messages", email_metrics.pending_count);

    let all_metrics = admin.all_queues_metrics().await?;
    println!("Total queues: {}", all_metrics.len());

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

# Optional: Schema name for pgqrs tables (default: public)
export PGQRS_SCHEMA="pgqrs"

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

# Optional: Schema name for pgqrs tables (default: public)
schema: "pgqrs"

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

// Create from explicit DSN (uses 'public' schema by default)
let config = Config::from_dsn("postgresql://user:pass@localhost/db");

// Create with custom schema
let config = Config::from_dsn_with_schema(
    "postgresql://user:pass@localhost/db",
    "my_schema"
)?;

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
| `schema` | `PGQRS_SCHEMA` | Schema name for pgqrs tables | `public` |
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

pgqrs provides message archiving functionality to maintain a historical record of processed messages while keeping the active queue performant. Archive functionality is accessed through existing commands with the `--archive` flag.

- `message show <queue> <id> --archive` — Show archived message details by ID
- `message count <queue> --archive` — Show archived message count
- `message read <queue> --archive` — Read/list archived messages

#### Archive System Overview

The archive system automatically creates archive tables (`archive_<queue_name>`) when queues are created. Archived messages retain all original data plus additional metadata:

- `archived_at` — Timestamp when the message was archived
- `processing_duration` — Time taken to process the message (in milliseconds)

#### Archive Usage Examples

```bash
# Show details of an archived message
pgqrs message show email_queue 12345 --archive

# Check how many messages are archived
pgqrs message count email_queue --archive

# List archived messages (use read command with --archive flag)
pgqrs message read email_queue --archive --count 10

# Note: Message archiving is done programmatically via the Rust API
# CLI does not provide direct archiving commands, only viewing archived messages
```

#### Programmatic Archive API

The archive functionality is available through the Consumer API and Table APIs:

```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::{Consumer, tables::{PgqrsArchiveTable, Table}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    let admin = PgqrsAdmin::new(&config).await?;
    let queue_info = admin.get_queue("email_queue").await?;

    // Consumer API for message processing and archiving
    let consumer = Consumer::new(admin.pool.clone(), &queue_info);

    // Process and archive a message
    if let Some(message) = consumer.dequeue().await? {
        // Process the message...
        println!("Processing message {}", message.id);

        // Archive the message after successful processing
        let archived = consumer.archive(message.id).await?;
        if archived {
            println!("Message {} archived successfully", message.id);
        }
    }

    // Table API for direct archive operations
    let archives = PgqrsArchiveTable::new(admin.pool.clone());

    // Count archived messages for the queue
    let archive_count = archives.count_by_fk(queue_info.id).await?;
    println!("Queue has {} archived messages", archive_count);

    // List archived messages with Table trait
    let archived_messages = archives.filter_by_fk(queue_info.id).await?;
    println!("Found {} archived messages", archived_messages.len());

    // Get specific archived message by ID
    if let Some(first_archive) = archived_messages.first() {
        let archived_msg = archives.get(first_archive.id).await?;
        println!("Archived message details: {:?}", archived_msg);
    }

    // Admin operations for archive cleanup
    admin.purge_archive("email_queue").await?;
    println!("Archive purged");

    Ok(())
}
```

#### Archive Best Practices

- **Archive after processing**: Archive messages only after successful processing
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