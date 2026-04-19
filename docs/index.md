# pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust with Python bindings. Built for Postgres. Also supports SQLite, Turso, and S3-backed queues.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres, SQLite, Turso, and S3-backed queues
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

## Choose Your Backend

| Scenario | Recommended Backend | Why |
|----------|---------------------|-----|
| Production with multiple workers | **PostgreSQL** | Full concurrency, no writer conflicts |
| Testing & prototyping | **SQLite / Turso** | Fast setup, no external dependencies |
| Embedded applications | **SQLite / Turso** | Single-file database, no server |
| Remote durable queue without a database server | **S3** | SQLite queue state persisted as an S3 object |
| High write throughput | **PostgreSQL** | SQLite/Turso allow only 1 writer at a time |

### S3 Queue Model

The S3 backend stores queue state as a SQLite database file in object storage.

- `s3://bucket/key.sqlite` selects the S3 backend
- `Durable` mode syncs writes to S3 before returning
- `Local` mode gives Rust applications explicit `snapshot()` / `sync()` control

[:octicons-arrow-right-24: Learn the S3 lifecycle](user-guide/guides/s3-queue.md)

!!! warning "SQLite/Turso Concurrency Limit"
    SQLite and Turso use database-level locks. With many concurrent writers, you may hit lock contention.
    See [SkyPilot's findings on SQLite concurrency](https://blog.skypilot.co/abusing-sqlite-to-handle-concurrency/).
    pgqrs enables WAL mode and sets a 5s busy timeout to mitigate this, but PostgreSQL is recommended for multi-worker scenarios.

### Benchmark Highlights

Current queue benchmark baselines show:

- **PostgreSQL is the gold standard**: strong throughput with one consumer, and close to linear scaling as more consumers are added
- **SQLite has similar single-consumer behavior** for this queue-drain scenario, but it does not scale with more consumers
- **Turso currently behaves like SQLite** in this repo's local-path mode
- **S3 is much slower on the durable object-store path** because per-message latency is much higher

[:octicons-arrow-right-24: See benchmark methodology and scenario writeups](benchmarks/index.md)

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

Workflow cancellation is also durable. When another actor requests cancellation,
the run first moves to `CANCELLING`. Once the workflow observes that request at
its next boundary, the run becomes terminal as `CANCELLED`. This makes it
possible to distinguish "cancel has been requested" from "the workflow has fully
stopped."

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    // A workflow definition is just async code plus durable steps.
    #[pgqrs::pgqrs_workflow(name = "archive_files")]
    async fn archive_files(
        run: &pgqrs::Run,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, pgqrs::Error> {
        // Step results are persisted. If the worker crashes after this step,
        // pgqrs will replay the cached result instead of re-running it.
        let files = pgqrs::workflow_step(run, "list_files", || async {
            Ok::<_, pgqrs::Error>(vec![input["path"].as_str().unwrap().to_string()])
        })
        .await?;

        // The second step sees the output of the first step just like normal async code.
        let archive_path = pgqrs::workflow_step(run, "create_archive", || async {
            Ok::<_, pgqrs::Error>(format!("{}.zip", files[0]))
        })
        .await?;

        Ok(json!({ "archive": archive_path }))
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Install schema once per database.
        pgqrs::admin(&store).install().await?;

        // Register the workflow definition. This is idempotent.
        pgqrs::workflow()
            .name(archive_files)
            .create()
            .await?;

        // Start a worker for this workflow queue.
        //
        // For a quickstart, spawning the worker in the same process keeps the example
        // self-contained. In production, run this polling loop in a dedicated worker
        // service instead.
        let consumer = pgqrs::consumer("workflow-worker", 8080, archive_files.name())
            .create(&store)
            .await?;
        let store_for_worker = store.clone();
        let consumer_for_worker = consumer.clone();
        let worker_task = tokio::spawn(async move {
            pgqrs::workflow()
                .name(archive_files)
                .consumer(&consumer_for_worker)
                .poll(&store_for_worker)
                .await
        });

        // Trigger a new workflow run. This only enqueues work; it does not execute it inline.
        let message = pgqrs::workflow()
            .name(archive_files)
            .trigger(&json!({"path": "/tmp/report.csv"}))?
            .execute()
            .await?;

        // Wait for the workflow result while the worker polls in the background.
        let result: serde_json::Value = pgqrs::run()
            .message(message)
            .store(&store)
            .result()
            .await?;
        println!("Workflow result: {:?}", result);

        // Stop the background worker before exiting the example.
        consumer.interrupt().await?;
        let _ = worker_task.await;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs
    from pgqrs.decorators import step as step_def
    from pgqrs.decorators import workflow as workflow_def


    @workflow_def(name="archive_files")
    async def archive_files_wf(ctx, input_data: dict) -> dict:
        # Decorated steps persist their results automatically.
        @step_def
        async def list_files(step_ctx):
            return [input_data["path"]]

        @step_def
        async def create_archive(step_ctx, files):
            return f"{files[0]}.zip"

        files = await list_files(ctx)
        archive_path = await create_archive(ctx, files)
        return {"archive": archive_path}


    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)

        # Install schema once per database.
        await admin.install()

        # Register the workflow definition. This is idempotent.
        await pgqrs.workflow().name("archive_files").store(store).create()

        # Start a worker for the workflow queue.
        #
        # For a quickstart, running it as a background task keeps the example small.
        # In production, run the polling loop in a separate worker process.
        consumer = await store.consumer("archive_files")
        worker_task = asyncio.create_task(
            pgqrs.dequeue()
            .worker(consumer)
            .handle_workflow(archive_files_wf)
            .poll(store)
        )

        # Trigger a workflow run. This only enqueues the input payload.
        message = await (
            pgqrs.workflow()
            .name("archive_files")
            .store(store)
            .trigger({"path": "/tmp/report.csv"})
            .execute()
        )

        # Wait for the workflow result while the worker polls in the background.
        result = await pgqrs.run().message(message).store(store).result()
        print(f"Workflow result: {result}")

        # Stop the background worker before exiting the example.
        await consumer.interrupt()
        try:
            await worker_task
        except Exception:
            pass

    asyncio.run(main())
    ```

**Key benefits:**

- **Crash recovery**: Automatically resumes from the last completed step
- **Exactly-once semantics**: Completed steps are never re-executed
- **Persistent state**: All progress stored in PostgreSQL

[:octicons-arrow-right-24: Learn more about Durable Workflows](user-guide/concepts/durable-workflows.md)

[:octicons-arrow-right-24: Follow the Durable Workflows Guide](user-guide/guides/durable-workflows.md)

## Next Steps

- [Installation](user-guide/getting-started/installation.md) - Get pgqrs set up
- [Quickstart](user-guide/getting-started/quickstart.md) - Complete walkthrough
- [Architecture](user-guide/concepts/architecture.md) - Understand how pgqrs works
- [Durable Workflows Guide](user-guide/guides/durable-workflows.md) - Build crash-resistant pipelines
