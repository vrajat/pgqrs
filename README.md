# pgqrs

A high-performance PostgreSQL-backed job queue for Rust applications.

## Features

- **High Performance**: Uses PostgreSQL's `SKIP LOCKED` for efficient job fetching
- **Low Latency**: Typically under 3ms from task schedule to execution using `LISTEN/NOTIFY`
- **Type-Safe**: Uses Rust's type system to ensure job payloads match their handlers
- **Exactly-Once Delivery**: Guaranteed within a visibility timeout
- **Message Archiving**: Archive messages instead of deleting for retention and replayability
- **CLI Tools**: Administration and debugging via command-line interface

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pgqrs = "0.1.0"
```

## Quick Start

### Basic Usage

```rust
use pgqrs::{Config, PgqrsClient, Message, CreateQueueOptions, ReadOptions};
use serde::{Deserialize, Serialize};

// Define your message type
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailMessage {
    to: String,
    subject: String,
    body: String,
}

// Implement the Message trait
impl Message for EmailMessage {
    fn message_type(&self) -> &'static str {
        "email"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let config = Config::default();
    let client = PgqrsClient::new(config).await?;
    
    // Install schema and create queue
    client.admin().install(false).await?;
    let queue_opts = CreateQueueOptions::new("email_queue");
    client.admin().create_queue(queue_opts).await?;
    
    // Send a message
    let email = EmailMessage {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
        body: "Welcome to our service!".to_string(),
    };
    
    let message_id = client.producer().enqueue("email_queue", email).await?;
    println!("Sent message with ID: {}", message_id);
    
    // Read and process messages
    let read_opts = ReadOptions::default();
    let messages = client.consumer().read_batch::<EmailMessage>("email_queue", read_opts).await?;
    
    for msg in messages {
        println!("Processing email: {}", msg.payload.subject);
        // Process the message...
        
        // Archive or delete the message
        client.consumer().archive("email_queue", msg.id).await?;
    }
    
    Ok(())
}
```

### Configuration

pgqrs supports multiple configuration methods:

#### Environment Variables

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
export PGQRS_SCHEMA="pgqrs"
export PGQRS_MAX_CONNECTIONS="10"
```

#### Config File (YAML)

```yaml
# pgqrs.yaml
database:
  host: localhost
  port: 5432
  username: postgres
  password: postgres
  database: myapp
  schema: pgqrs
  max_connections: 10
  connection_timeout_seconds: 30

queue:
  default_lock_time_seconds: 5
  max_batch_size: 100
  enable_listen_notify: true
  cleanup_interval_seconds: 300

performance:
  prefetch_count: 10
  connection_pool_size: 10
  query_timeout_seconds: 30
```

#### Programmatic Configuration

```rust
use pgqrs::{Config, DatabaseConfig, QueueConfig, PerformanceConfig};

let config = Config {
    database: DatabaseConfig {
        host: "localhost".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: "postgres".to_string(),
        database: "myapp".to_string(),
        schema: "pgqrs".to_string(),
        max_connections: 10,
        connection_timeout_seconds: 30,
    },
    queue: QueueConfig::default(),
    performance: PerformanceConfig::default(),
};
```

## CLI Usage

pgqrs includes a CLI for administration and debugging:

### Install Schema

```bash
pgqrs install
```

### Create and Manage Queues

```bash
# Create a queue
pgqrs queue create my_queue

# List all queues
pgqrs queue list

# Get queue metrics
pgqrs queue metrics my_queue

# Purge all messages from a queue
pgqrs queue purge my_queue

# Delete a queue
pgqrs queue delete my_queue
```

### Send and Read Messages

```bash
# Send a message
pgqrs message send my_queue '{"task": "process_data", "id": 123}'

# Read messages
pgqrs message read my_queue --count 5 --lock-time 30
```

## API Reference

### Admin Operations

```rust
// Install/uninstall schema
client.admin().install(false).await?;
client.admin().uninstall(false).await?;

// Queue management
client.admin().create_queue(CreateQueueOptions::new("my_queue")).await?;
client.admin().list_queues().await?;
client.admin().delete_queue("my_queue").await?;
client.admin().purge_queue("my_queue").await?;

// Metrics
let metrics = client.admin().queue_metrics("my_queue").await?;
let all_metrics = client.admin().all_queues_metrics().await?;
```

### Producer Operations

```rust
// Send single message
let id = client.producer().enqueue("my_queue", message).await?;

// Send batch of messages
let ids = client.producer().batch_enqueue("my_queue", messages).await?;

// Send delayed message
let id = client.producer().enqueue_delayed("my_queue", message, 60).await?;
```

### Consumer Operations

```rust
// Read single message
let msg = client.consumer().read::<MyMessage>("my_queue", read_opts).await?;

// Read batch of messages
let messages = client.consumer().read_batch::<MyMessage>("my_queue", read_opts).await?;

// Delete messages
client.consumer().dequeue("my_queue", message_id).await?;
client.consumer().dequeue_batch("my_queue", message_ids).await?;

// Archive messages  
client.consumer().archive("my_queue", message_id).await?;
client.consumer().archive_batch("my_queue", message_ids).await?;

// Extend message lock
client.consumer().extend_lock("my_queue", message_id, 30).await?;
```

## Architecture

pgqrs uses PostgreSQL as the backend with the following key features:

- **SKIP LOCKED**: For efficient, concurrent message consumption without blocking
- **LISTEN/NOTIFY**: For low-latency notification of new messages
- **Visibility Timeout**: Messages become invisible to other consumers for a configurable period
- **Archiving**: Move processed messages to archive tables instead of deleting
- **Type Safety**: Rust's type system ensures message payloads match handlers

## Database Schema

pgqrs creates the following tables in the configured schema:

- `queues`: Metadata about each queue
- `q_<queue_name>`: Message storage for each queue  
- `a_<queue_name>`: Archive storage for each queue (if archiving enabled)

## Development Status

⚠️ **This is the initial API design phase**. All functions are currently `todo!()` placeholders.

Next steps:
1. Implement database schema and migrations
2. Implement core queue operations
3. Add LISTEN/NOTIFY support
4. Add CLI functionality
5. Add comprehensive tests
6. Performance optimization

## Contributing

This project is in early development. Contributions welcome!

## License

Licensed under either of

- Apache License, Version 2.0
- MIT license

at your option.