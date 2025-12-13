# Admin API

The `Admin` provides queue management, schema administration, and monitoring capabilities.

## Creating an Admin

```rust
use pgqrs::{Admin, Config};

let config = Config::from_dsn("postgresql://localhost/mydb");
let admin = Admin::new(&config).await?;
```

The `Admin` maintains a connection pool that can be shared with Producers and Consumers.

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `pool` | `PgPool` | Database connection pool |
| `config` | `Config` | Configuration settings |
| `queues` | `Queues` | Queue table operations |
| `workers` | `Workers` | Worker table operations |
| `messages` | `Messages` | Message table operations |
| `archive` | `Archive` | Archive table operations |

## Schema Management

### install

Install the pgqrs schema (tables, indexes, constraints).

```rust
admin.install().await?;
println!("Schema installed successfully");
```

Run this once when setting up pgqrs for the first time. It's idempotent—safe to call multiple times.

### verify

Verify that the pgqrs schema is correctly installed.

```rust
admin.verify().await?;
println!("Schema verification passed");
```

Returns an error if the schema is missing or corrupted.

### uninstall

Remove the pgqrs schema (tables and data).

```rust
admin.uninstall().await?;
println!("Schema removed");
```

!!! danger
    This permanently deletes all queues, messages, workers, and archives!

## Queue Management

### create_queue

Create a new queue.

```rust
let queue = admin.create_queue("email-notifications").await?;
println!("Created queue: {} (ID: {})", queue.queue_name, queue.id);
```

**Returns:** `Result<QueueInfo>` with:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `i64` | Unique queue ID |
| `queue_name` | `String` | Queue name |
| `created_at` | `DateTime<Utc>` | Creation timestamp |

### get_queue

Get a queue by name.

```rust
let queue = admin.get_queue("email-notifications").await?;
```

**Returns:** `Result<QueueInfo>` - Error if queue doesn't exist.

### delete_queue

Delete a queue and all its messages.

```rust
admin.delete_queue("old-queue").await?;
```

!!! warning
    This permanently deletes the queue, all pending messages, and all archived messages.

### queue_metrics

Get metrics for a specific queue.

```rust
let metrics = admin.queue_metrics("email-notifications").await?;

println!("Queue: {}", metrics.name);
println!("  Total messages: {}", metrics.total_messages);
println!("  Pending: {}", metrics.pending_messages);
println!("  Locked: {}", metrics.locked_messages);
println!("  Archived: {}", metrics.archived_messages);

if let Some(oldest) = metrics.oldest_pending_message {
    println!("  Oldest pending: {}", oldest);
}
```

**Returns:** `Result<QueueMetrics>` with:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | Queue name |
| `total_messages` | `i64` | Total in messages table |
| `pending_messages` | `i64` | Available for processing |
| `locked_messages` | `i64` | Currently being processed |
| `archived_messages` | `i64` | In archive table |
| `oldest_pending_message` | `Option<DateTime>` | Oldest pending timestamp |
| `newest_message` | `Option<DateTime>` | Newest message timestamp |

### all_queues_metrics

Get metrics for all queues.

```rust
let all_metrics = admin.all_queues_metrics().await?;

for metrics in all_metrics {
    println!("{}: {} pending, {} locked, {} archived",
        metrics.name,
        metrics.pending_messages,
        metrics.locked_messages,
        metrics.archived_messages
    );
}
```

## Archive Management

### purge_archive

Delete old archived messages.

```rust
// Purge archive for a specific queue
admin.purge_archive("email-notifications").await?;
```

## Table APIs

Admin provides direct access to table operations through its fields:

### Queues Table

```rust
// List all queues
let queues = admin.queues.list().await?;

// Get queue by ID
let queue = admin.queues.get(queue_id).await?;

// Get queue by name
let queue = admin.queues.get_by_name("tasks").await?;

// Count queues
let count = admin.queues.count().await?;
```

### Workers Table

```rust
// List all workers
let workers = admin.workers.list().await?;

// List workers for a queue
let tx = &mut admin.pool.begin().await?;
let queue_workers = admin.workers.filter_by_fk(queue_id, tx).await?;

// Count workers
let count = admin.workers.count().await?;
```

### Messages Table

```rust
use pgqrs::tables::Messages;

let messages = Messages::new(admin.pool.clone());

// Count pending messages for a queue
let pending = messages.count_pending(queue_id).await?;

// Count total messages
let total = messages.count().await?;
```

### Archive Table

```rust
use pgqrs::Archive;

let archive = Archive::new(admin.pool.clone());

// Count archived messages for a queue
let tx = &mut admin.pool.begin().await?;
let count = archive.count_for_fk(queue_id, tx).await?;

// List archived messages
let archived = archive.filter_by_fk(queue_id, tx).await?;
```

## Patterns

### Queue Setup Script

```rust
async fn setup_queues(admin: &Admin) -> Result<()> {
    // Install schema if needed
    admin.install().await?;

    // Create application queues
    let queues = ["emails", "notifications", "reports", "dlq"];

    for name in queues {
        match admin.create_queue(name).await {
            Ok(q) => println!("Created queue: {}", q.queue_name),
            Err(e) if e.to_string().contains("already exists") => {
                println!("Queue {} already exists", name);
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
```

### Monitoring Dashboard

```rust
async fn print_dashboard(admin: &Admin) -> Result<()> {
    let metrics = admin.all_queues_metrics().await?;

    println!("\n{:=^60}", " Queue Dashboard ");
    println!("{:<20} {:>8} {:>8} {:>8} {:>8}",
        "Queue", "Total", "Pending", "Locked", "Archived");
    println!("{:-^60}", "");

    for m in metrics {
        println!("{:<20} {:>8} {:>8} {:>8} {:>8}",
            m.name,
            m.total_messages,
            m.pending_messages,
            m.locked_messages,
            m.archived_messages
        );
    }

    Ok(())
}
```

### Health Check Endpoint

```rust
use serde::Serialize;

#[derive(Serialize)]
struct HealthStatus {
    healthy: bool,
    database: bool,
    queues: Vec<QueueHealth>,
}

#[derive(Serialize)]
struct QueueHealth {
    name: String,
    pending: i64,
    oldest_pending_age_seconds: Option<i64>,
}

async fn health_check(admin: &Admin) -> Result<HealthStatus> {
    // Check database connectivity
    let db_healthy = admin.verify().await.is_ok();

    // Check queue health
    let metrics = admin.all_queues_metrics().await?;
    let now = chrono::Utc::now();

    let queues: Vec<QueueHealth> = metrics
        .iter()
        .map(|m| QueueHealth {
            name: m.name.clone(),
            pending: m.pending_messages,
            oldest_pending_age_seconds: m.oldest_pending_message
                .map(|t| (now - t).num_seconds()),
        })
        .collect();

    Ok(HealthStatus {
        healthy: db_healthy,
        database: db_healthy,
        queues,
    })
}
```

### Cleanup Job

```rust
async fn cleanup_old_data(admin: &Admin) -> Result<()> {
    // Purge archives older than 30 days
    let queues = admin.queues.list().await?;

    for queue in queues {
        admin.purge_archive(&queue.queue_name).await?;
        tracing::info!("Purged archive for queue: {}", queue.queue_name);
    }

    // Purge old stopped workers
    // (This would require additional CLI command or direct SQL)

    Ok(())
}
```

## Full Example

```rust
use pgqrs::{Admin, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;

    // Initial setup
    println!("Installing schema...");
    admin.install().await?;
    admin.verify().await?;
    println!("Schema verified");

    // Create queues
    println!("\nCreating queues...");
    let email_queue = admin.create_queue("emails").await?;
    let task_queue = admin.create_queue("tasks").await?;
    println!("Created: emails ({}), tasks ({})", email_queue.id, task_queue.id);

    // List queues
    println!("\nAll queues:");
    let queues = admin.queues.list().await?;
    for q in &queues {
        println!("  - {} (ID: {}, created: {})",
            q.queue_name, q.id, q.created_at);
    }

    // Show metrics
    println!("\nQueue metrics:");
    let all_metrics = admin.all_queues_metrics().await?;
    for m in &all_metrics {
        println!("  {}: {} pending, {} archived",
            m.name, m.pending_messages, m.archived_messages);
    }

    // The pool can be shared with producers/consumers
    println!("\nConnection pool can be shared via: admin.pool.clone()");

    Ok(())
}
```

## See Also

- [Producer API](producer.md) - Creating messages
- [Consumer API](consumer.md) - Processing messages
- [Configuration](configuration.md) - Configuration options
- [CLI Reference](../cli-reference.md) - Command-line administration
