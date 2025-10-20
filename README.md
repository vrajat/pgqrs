
# pgqrs

A PostgreSQL-backed job queue for Rust applications, with a CLI for administration and a type-safe async library API.

## Features

- **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching
- **Type-Safe**: Rust types for message payloads
- **Visibility Timeout**: Exactly-once delivery within a lock period
- **CLI Tools**: Administer and debug queues from the command line

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pgqrs = "0.1.0"
```

## Library Usage

See `examples/basic_usage.rs` for a full example. Typical usage:

```rust
use pgqrs::admin::PgqrsAdmin;
use pgqrs::Config;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
        // Initialize tracing
        tracing_subscriber::fmt::init();

        // Load configuration
        let config = Config::default();
        let admin = PgqrsAdmin::new(&config);

        // Install schema
        admin.install(false)?;

        // Create queues
        admin.create_queue(&"email_queue".to_string()).await?;
        admin.create_queue(&"task_queue".to_string()).await?;

        // Send messages
        let email_payload = json!({ "to": "user@example.com", "subject": "Welcome!", "body": "Welcome to our service!" });
        let email_queue = admin.get_queue("email_queue").await?;
        let email_id = email_queue.enqueue(&email_payload).await?;
        println!("Sent email message with ID: {}", email_id);

        // Read messages
        let messages = email_queue.read_delay(10, 2).await?;
        println!("Read {} messages", messages.len());

        // Delete a message
        if let Some(msg) = messages.first() {
                let deleted = email_queue.delete_batch(vec![msg.msg_id]).await?;
                if deleted.first().copied().unwrap_or(false) {
                        println!("Deleted message {}");
                }
        }

        Ok(())
}
```

## Configuration

You can configure pgqrs via:

- **Environment variables**:
    - `DATABASE_URL`, `PGQRS_SCHEMA`, etc.
- **YAML config file** (default: `pgqrs.yaml`):
    - See example in this repo for all options.
- **Programmatic**:
    - `Config::default()` or build your own config struct.

## CLI Usage

The CLI is defined in `src/main.rs` and supports the following commands:

### Top-level commands

- `install [--dry-run]` — Install pgqrs schema
- `uninstall [--dry-run]` — Uninstall pgqrs schema
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

- `--database-url <url>` — Override database URL
- `--config <path>` — Config file path (default: pgqrs.yaml)
- `--log-dest <stderr|file>` — Log destination
- `--log-level <error|warn|info|debug|trace>` — Log level
- `--output-format <json|csv|yaml>` — Output format
- `--output-dest <stdout|file>` — Output destination

## API Reference

See `src/main.rs` and `examples/basic_usage.rs` for the current API. Key types and methods:

- `PgqrsAdmin::install(dry_run)` — Install schema
- `PgqrsAdmin::create_queue(name)` — Create queue
- `PgqrsAdmin::list_queues()` — List queues
- `PgqrsAdmin::get_queue(name)` — Get queue handle
- `QueueHandle::enqueue(payload)` — Send message
- `QueueHandle::batch_enqueue(payloads)` — Send batch
- `QueueHandle::enqueue_delayed(payload, delay_secs)` — Send delayed message
- `QueueHandle::read(count)` — Read messages
- `QueueHandle::read_delay(count, delay_secs)` — Read messages with delay
- `QueueHandle::delete_batch(ids)` — Delete messages
- `QueueHandle::extend_visibility(id, seconds)` — Extend lock
- `QueueHandle::pending_count()` — Pending message count
- `PgqrsAdmin::queue_metrics(name)` — Queue metrics
- `PgqrsAdmin::all_queues_metrics()` — Metrics for all queues

## Development Status

**Note:** Many functions are currently `todo!()` placeholders. See `src/main.rs` and `examples/basic_usage.rs` for the evolving API.

## Known Issues

### CLI Subprocess Timing Issue
There is a known issue where CLI health check commands (`health liveness` and `health readiness`) fail with gRPC timeout errors when run as subprocesses during integration testing, while the exact same `pgqrs_client::PgqrsClient` code works perfectly when called directly from test code.

**Symptoms:**
- CLI subprocess: `Error: gRPC status error: status: 'The operation was cancelled', self: "Timeout expired"`
- Direct client library: Works perfectly with same endpoint and timeouts
- Both cargo run and direct binary execution show the same timeout behavior

**Investigation completed:**
- ✅ Server starts successfully and accepts connections
- ✅ CLI argument parsing works correctly
- ✅ CLI client creation succeeds
- ❌ CLI gRPC calls timeout in subprocess context
- ✅ Same gRPC calls work from test process context

**Current workaround:** Integration tests use a hybrid approach testing CLI interface validation plus direct client library calls (which use the identical code path as the CLI).

**TODO:** Investigate async runtime or network context differences between CLI subprocess and test process environments.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.