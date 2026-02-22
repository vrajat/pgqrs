# pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust with Python bindings. Built for Postgres. Also supports SQLite and Turso.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres (production), SQLite/Turso (testing, CLI, embedded)
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

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
        store.queue("tasks").await?;

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
    import asyncio
    import pgqrs

    async def main():
        # Connect to PostgreSQL
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Setup (run once)
        admin = pgqrs.admin(store)
        await admin.install()
        await store.queue("tasks")

        # Producer: enqueue a job
        producer = await store.producer("tasks")
        msg_id = await producer.enqueue({
            "task": "send_email",
            "to": "user@example.com"
        })
        print(f"Enqueued: {msg_id}")

        # Consumer: process jobs
        consumer = await store.consumer("tasks")
        messages = await consumer.dequeue(batch_size=1)
        for msg in messages:
            print(f"Processing: {msg.payload}")
            await consumer.archive(msg.id)

    asyncio.run(main())
    ```

[:octicons-arrow-right-24: Learn more about Producer & Consumer](user-guide/concepts/producer-consumer.md)

## Durable Workflows

Orchestrate multi-step processes that survive crashes and resume from where they left off:

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    async fn app_workflow(run: pgqrs::Run, input: serde_json::Value) -> Result<serde_json::Value, pgqrs::Error> {
        let files = pgqrs::workflow_step(&run, "list_files", || async {
            Ok::<_, pgqrs::Error>(vec![input["path"].as_str().unwrap().to_string()])
        })
        .await?;

        let archive = pgqrs::workflow_step(&run, "create_archive", || async {
            Ok::<_, pgqrs::Error>(format!("{}.zip", files[0]))
        })
        .await?;

        Ok(json!({"archive": archive}))
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        pgqrs::admin(&store).install().await?;

        pgqrs::workflow()
            .name("archive_files")
            .store(&store)
            .create()
            .await?;

        let consumer = pgqrs::consumer("worker-1", 8080, "archive_files")
            .create(&store)
            .await?;

        let handler = pgqrs::workflow_handler(store.clone(), move |run, input| async move {
            app_workflow(run, input).await
        });
        let handler = { let handler = handler.clone(); move |msg| (handler)(msg) };

        pgqrs::workflow()
            .name("archive_files")
            .store(&store)
            .trigger(&json!({"path": "/tmp/report.csv"}))?
            .execute()
            .await?;

        pgqrs::dequeue()
            .worker(&consumer)
            .handle(handler)
            .execute(&store)
            .await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)
        await admin.install()

        await pgqrs.workflow().name("archive_files").store(store).create()

        consumer = await pgqrs.consumer("worker-1", 8080, "archive_files").create(store)
        message = await pgqrs.workflow() \
            .name("archive_files") \
            .store(store) \
            .trigger({"path": "/tmp/report.csv"}) \
            .execute()

        messages = await consumer.dequeue(batch_size=1)
        msg = messages[0]

        run = await pgqrs.run().message(msg).store(store).execute()
        step = await run.acquire_step("list_files", current_time=run.current_time)
        if step.status == "EXECUTE":
            await step.guard.success([msg.payload["path"]])

        step = await run.acquire_step("create_archive", current_time=run.current_time)
        if step.status == "EXECUTE":
            await step.guard.success(f"{msg.payload['path']}.zip")

        await run.complete({"archive": f"{msg.payload['path']}.zip"})
        await consumer.archive(msg.id)

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