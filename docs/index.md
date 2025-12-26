# pgqrs

`pgqrs` is a PostgreSQL-backed durable workflow engine and job queue for Rust and Python.


## Key Features

### Core
- **Library-only**: No servers to operate. Use directly in your Rust or Python applications.
- **Connection Pooler Compatible**: Works with [pgBouncer](https://www.pgbouncer.org) and [pgcat](https://github.com/postgresml/pgcat) for connection scaling.

### Job Queue
- **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
- **Exactly-once Delivery**: Guarantees within visibility timeout window.
- **Message Archiving**: Built-in audit trails and historical data retention.

### Workflows
- **Crash Recovery**: Resume from the last completed step after failures.
- **Exactly-once Steps**: Completed steps are never re-executed.
- **Persistent State**: All workflow progress stored in PostgreSQL.

## Durable Workflows

Orchestrate multi-step processes that survive crashes and resume from where they left off:

=== "Rust"

    ```rust
    use pgqrs::workflow::Workflow;
    use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

    #[pgqrs_step]
    async fn fetch_data(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
        Ok(reqwest::get(url).await?.text().await?)
    }

    #[pgqrs_step]
    async fn process_data(ctx: &Workflow, data: String) -> Result<i32, anyhow::Error> {
        Ok(data.lines().count() as i32)
    }

    #[pgqrs_workflow]
    async fn data_pipeline(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
        let data = fetch_data(ctx, url).await?;
        let count = process_data(ctx, data).await?;
        Ok(format!("Processed {} lines", count))
    }

    // Usage
    let workflow = Workflow::create(pool, "data_pipeline", &url).await?;
    let result = data_pipeline(&workflow, url).await?;
    ```

=== "Python"

    ```python
    from pgqrs import Admin, PyWorkflow
    from pgqrs.decorators import workflow, step

    @step
    async def fetch_data(ctx: PyWorkflow, url: str) -> dict:
        return await http_client.get(url)

    @step
    async def process_data(ctx: PyWorkflow, data: dict) -> dict:
        return {"processed": True, "count": len(data)}

    @workflow
    async def data_pipeline(ctx: PyWorkflow, url: str):
        data = await fetch_data(ctx, url)
        result = await process_data(ctx, data)
        return result

    # Usage
    admin = Admin("postgresql://localhost/mydb", None)
    await admin.install()

    ctx = await admin.create_workflow("data_pipeline", "https://api.example.com")
    result = await data_pipeline(ctx, "https://api.example.com")
    ```

**Key benefits:**

- **Crash recovery**: Automatically resumes from the last completed step
- **Exactly-once semantics**: Completed steps are never re-executed
- **Persistent state**: All progress stored in PostgreSQL

[:octicons-arrow-right-24: Learn more about Durable Workflows](user-guide/concepts/durable-workflows.md)

## Job Queue

Simple, reliable message queue for background processing:

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

[:octicons-arrow-right-24: Learn more about Producer & Consumer](user-guide/concepts/producer-consumer.md)

## Next Steps

- [Installation](user-guide/getting-started/installation.md) - Get pgqrs set up
- [Quickstart](user-guide/getting-started/quickstart.md) - Complete walkthrough
- [Architecture](user-guide/concepts/architecture.md) - Understand how pgqrs works
- [Durable Workflows Guide](user-guide/guides/durable-workflows.md) - Build crash-resistant pipelines
```