# pgqrs

`pgqrs` is a library-only PostgreSQL-backed job queue for Rust and Python applications.

## Features

- **Lightweight**: No servers to operate. Directly use `pgqrs` as a library in your Rust or Python applications.
- **Compatible with Connection Poolers**: Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
- **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
- **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by visibility timeout.
- **Message Archiving**: Built-in archiving system for audit trails and historical data retention.

## Quick Example

=== "Rust"

    ```rust
    use pgqrs::{Admin, Config, Producer, Consumer};
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        // Connect to PostgreSQL
        let config = Config::from_dsn("postgresql://user:pass@localhost/mydb")?;

        // Set up pgqrs (run once)
        let admin = Admin::new(&config).await?;
        admin.install().await?;
        admin.create_queue("tasks").await?;

        // Producer: enqueue a job
        let producer = Producer::new(&config, "tasks").await?;
        let message = producer.enqueue(&json!({"task": "send_email", "to": "user@example.com"})).await?;
        println!("Enqueued message: {}", message.id);

        // Consumer: process jobs
        let consumer = Consumer::new(&config, "tasks").await?;
        if let Some(msg) = consumer.dequeue().await? {
            println!("Processing: {}", msg.payload);

            // Mark as complete
            consumer.archive(msg.id, "completed").await?;
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    import pgqrs
    import json

    # Connect to PostgreSQL
    config = pgqrs.Config.from_dsn("postgresql://user:pass@localhost/mydb")

    # Set up pgqrs (run once)
    admin = pgqrs.Admin(config)
    admin.install()
    admin.create_queue("tasks")

    # Producer: enqueue a job
    producer = pgqrs.Producer(config, "tasks")
    message = producer.enqueue(json.dumps({"task": "send_email", "to": "user@example.com"}))
    print(f"Enqueued message: {message.id}")

    # Consumer: process jobs
    consumer = pgqrs.Consumer(config, "tasks")
    msg = consumer.dequeue()
    if msg:
        print(f"Processing: {msg.payload}")

        # Mark as complete
        consumer.archive(msg.id, "completed")
    ```

## Next Steps

- [Installation](user-guide/getting-started/installation.md) - Get pgqrs set up
- [Quickstart](user-guide/getting-started/quickstart.md) - Complete walkthrough
- [Architecture](user-guide/concepts/architecture.md) - Understand how pgqrs works
```