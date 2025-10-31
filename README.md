
# pgqrs

A PostgreSQL-backed job queue for Rust applications.

## Features
- **Simple Installation**: Add `pgqrs` library as a dependency in your Rust applications.
- **Compatible with Cxn Poolers** Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
- **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching

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

####
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

    // Delete a message
    if let Some(msg) = messages.first() {
        let deleted = email_queue.delete_batch(vec![msg.msg_id]).await?;
        if deleted.first().copied().unwrap_or(false) {
            println!("Deleted message {}", msg.msg_id);
        }
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