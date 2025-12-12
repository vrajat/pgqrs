# Rust API

This section provides comprehensive documentation for the pgqrs Rust library.

## Overview

pgqrs is a native Rust library that provides a PostgreSQL-backed job queue. It's built with async/await and works with the Tokio runtime.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pgqrs = "0.4"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

## Core Types

### Main Components

| Type | Description |
|------|-------------|
| [`Config`](configuration.md) | Configuration for database connection and settings |
| [`Admin`](admin.md) | Queue and schema management |
| [`Producer`](producer.md) | Message creation and enqueueing |
| [`Consumer`](consumer.md) | Message consumption and processing |
| `Worker` | Trait for worker lifecycle management |
| `WorkerHandle` | Generic handle for managing workers by ID |

### Supporting Types

| Type | Description |
|------|-------------|
| `WorkerInfo` | Worker registration details |
| `WorkerStatus` | Worker state (Ready, Suspended, Stopped) |
| `WorkerStats` | Worker statistics |
| `QueueInfo` | Queue metadata |
| `QueueMessage` | Message with payload and metadata |

### Table APIs

| Type | Description |
|------|-------------|
| `Queues` | CRUD operations on queues |
| `Workers` | CRUD operations on workers |
| `Messages` | CRUD operations on messages |
| `Archive` | CRUD operations on archived messages |

## Quick Reference

### Setup

```rust
use pgqrs::{Admin, Producer, Consumer, Config};

// Create configuration
let config = Config::from_dsn("postgresql://localhost/mydb");

// Create admin for schema/queue management
let admin = Admin::new(&config).await?;

// Install schema (first time only)
admin.install().await?;

// Create a queue
let queue = admin.create_queue("tasks").await?;
```

### Producing Messages

```rust
use serde_json::json;

// Create producer
let producer = Producer::new(
    admin.pool.clone(),
    &queue,
    "localhost",
    3000,
    &config,
).await?;

// Single message
let msg = producer.enqueue(&json!({"task": "process"})).await?;

// Batch enqueue
let payloads = vec![json!({"id": 1}), json!({"id": 2})];
let msgs = producer.batch_enqueue(&payloads).await?;

// Delayed message
let delayed = producer.enqueue_delayed(&json!({"remind": true}), 300).await?;
```

### Consuming Messages

```rust
// Create consumer
let consumer = Consumer::new(
    admin.pool.clone(),
    &queue,
    "localhost",
    3001,
    &config,
).await?;

// Dequeue messages
let messages = consumer.dequeue().await?;

// Process and archive
for message in messages {
    process(&message.payload)?;
    consumer.archive(message.id).await?;
}
```

### Worker Management

```rust
use pgqrs::Worker;

// Check worker status
let status = producer.status().await?;

// Send heartbeat
producer.heartbeat().await?;

// Graceful shutdown
producer.suspend().await?;
producer.shutdown().await?;
```

## Error Handling

pgqrs uses a custom `Error` type with `Result`:

```rust
use pgqrs::{Error, Result};

async fn example() -> Result<()> {
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?; // Returns Result<Admin, Error>
    Ok(())
}
```

Common error variants:

| Error | Description |
|-------|-------------|
| `DatabaseError` | PostgreSQL error |
| `ConfigError` | Invalid configuration |
| `QueueNotFound` | Referenced queue doesn't exist |
| `WorkerNotFound` | Referenced worker doesn't exist |
| `InvalidState` | Invalid worker state transition |

## Async Runtime

pgqrs requires Tokio:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Your pgqrs code here
    Ok(())
}
```

## API Reference

For complete API documentation, see [docs.rs/pgqrs](https://docs.rs/pgqrs/latest/pgqrs/).

## Sections

<div class="grid cards" markdown>

-   :material-arrow-up-bold:{ .lg .middle } **Producer**

    ---

    Creating and sending messages.

    [:octicons-arrow-right-24: Producer API](producer.md)

-   :material-arrow-down-bold:{ .lg .middle } **Consumer**

    ---

    Fetching and processing messages.

    [:octicons-arrow-right-24: Consumer API](consumer.md)

-   :material-shield-account:{ .lg .middle } **Admin**

    ---

    Queue and schema management.

    [:octicons-arrow-right-24: Admin API](admin.md)

-   :material-cog:{ .lg .middle } **Configuration**

    ---

    Setting up pgqrs.

    [:octicons-arrow-right-24: Configuration](configuration.md)

</div>
