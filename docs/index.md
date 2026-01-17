# pgqrs

`pgqrs` is a durable workflow engine and job queue that works with **PostgreSQL, SQLite, and Turso**. Written in Rust with Python bindings.

Use PostgreSQL for production scale. Use SQLite or Turso for CLI tools, testing, and embedded apps.

## Key Features

### Multi-Backend Support
- **PostgreSQL**: Production-ready with unlimited concurrent workers
- **SQLite**: Zero-config embedded option for single-process applications
- **Turso**: SQLite-compatible (libSQL) with enhanced features for local storage
- **Unified API**: Switch backends by changing your connection string

### Core
- **Library-only**: No servers to operate. Use directly in your Rust or Python applications.
- **Connection Pooler Compatible**: Works with [pgBouncer](https://www.pgbouncer.org) and [pgcat](https://github.com/postgresml/pgcat) for connection scaling (PostgreSQL).

### Job Queue
- **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
- **Exactly-once Delivery**: Guarantees within visibility timeout window.
- **Message Archiving**: Built-in audit trails and historical data retention.

### Workflows
- **Crash Recovery**: Resume from the last completed step after failures.
- **Exactly-once Steps**: Completed steps are never re-executed.
- **Persistent State**: All workflow progress stored durably.

## Choose Your Backend

| Scenario | Recommended Backend | Why |
|----------|---------------------|-----|
| Production with multiple workers | **PostgreSQL** | Full concurrency, no writer conflicts |
| CLI tools & scripts | **SQLite / Turso** | Zero-config, embedded, portable |
| Testing & prototyping | **SQLite / Turso** | Fast setup, no external dependencies |
| Embedded applications | **SQLite / Turso** | Single-file database, no server |
| High write throughput | **PostgreSQL** | SQLite/Turso allow only 1 writer at a time |

!!! warning "SQLite/Turso Concurrency Limit"
    SQLite and Turso use database-level locks. With many concurrent writers, you may hit lock contention.
    See [SkyPilot's findings on SQLite concurrency](https://blog.skypilot.co/abusing-sqlite-to-handle-concurrency/).
    pgqrs enables WAL mode and sets a 5s busy timeout to mitigate this, but PostgreSQL is recommended for multi-worker scenarios.

## Job Queue

Simple, reliable message queue for background processing:

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        // Connect to PostgreSQL
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Setup (run once)
        pgqrs::admin(&store).install().await?;
        pgqrs::admin(&store).create_queue("tasks").await?;

        // Producer: enqueue a job
        let ids = pgqrs::enqueue()
            .message(&json!({"task": "send_email", "to": "user@example.com"}))
            .to("tasks")
            .execute(&store)
            .await?;

        // Consumer: process jobs
        pgqrs::dequeue()
            .from("tasks")
            .handle(|msg| async move {
                println!("Processing: {:?}", msg.payload);
                Ok(())
            })
            .execute(&store)
            .await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import pgqrs
    import asyncio

    async def main():
        # Connect to PostgreSQL
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Setup (run once)
        admin = pgqrs.admin(store)
        await admin.install()
        await admin.create_queue("tasks")

        # Producer: enqueue a job
        msg_id = await pgqrs.produce(store, "tasks", {
            "task": "send_email",
            "to": "user@example.com"
        })

        # Consumer: process jobs
        async def handler(msg):
            print(f"Processing: {msg.payload}")
            return True

        await pgqrs.consume(store, "tasks", handler)

    asyncio.run(main())
    ```

[:octicons-arrow-right-24: Learn more about Producer & Consumer](user-guide/concepts/producer-consumer.md)

## Durable Workflows

Orchestrate multi-step processes that survive crashes and resume from where they left off:

=== "Rust"

    ```rust
    use pgqrs;
    use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

    #[pgqrs_step]
    async fn fetch_data(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
        Ok(reqwest::get(url).await?.text().await?)
    }

    #[pgqrs_step]
    async fn process_data(ctx: &pgqrs::Workflow, data: String) -> Result<i32, anyhow::Error> {
        Ok(data.lines().count() as i32)
    }

    #[pgqrs_workflow]
    async fn data_pipeline(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
        let data = fetch_data(ctx, url).await?;
        let count = process_data(ctx, data).await?;
        Ok(format!("Processed {} lines", count))
    }

    // Usage
    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        pgqrs::admin(&store).install().await?;

        let url = "https://example.com/data.txt";
        let workflow = pgqrs::admin(&store)
            .create_workflow("data_pipeline", &url)
            .await?;

        let result = data_pipeline(&workflow, url).await?;
        println!("Result: {}", result);
        Ok(())
    }
    ```

=== "Python"

    ```python
    import pgqrs
    from pgqrs.decorators import workflow, step

    @step
    async def fetch_data(ctx, url: str) -> dict:
        # Fetch data from API
        return {"lines": 100, "data": "..."}

    @step
    async def process_data(ctx, data: dict) -> dict:
        return {"processed": True, "count": data["lines"]}

    @workflow
    async def data_pipeline(ctx, url: str):
        data = await fetch_data(ctx, url)
        result = await process_data(ctx, data)
        return result

    # Usage
    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)
        await admin.install()

        url = "https://example.com/data"
        ctx = await admin.create_workflow("data_pipeline", url)
        result = await data_pipeline(ctx, url)
        print(f"Result: {result}")

    import asyncio
    asyncio.run(main())
    ```

**Key benefits:**

- **Crash recovery**: Automatically resumes from the last completed step
- **Exactly-once semantics**: Completed steps are never re-executed
- **Persistent state**: All progress stored in PostgreSQL

[:octicons-arrow-right-24: Learn more about Durable Workflows](user-guide/concepts/durable-workflows.md)

## Next Steps

- [Installation](user-guide/getting-started/installation.md) - Get pgqrs set up
- [Quickstart](user-guide/getting-started/quickstart.md) - Complete walkthrough
- [Architecture](user-guide/concepts/architecture.md) - Understand how pgqrs works
- [Durable Workflows Guide](user-guide/guides/durable-workflows.md) - Build crash-resistant pipelines