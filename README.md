# pgqrs

[![Rust](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml/badge.svg)](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/pgqrs.svg)](https://badge.fury.io/py/pgqrs)

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust with Python bindings. Built for Postgres. Also supports SQLite, Turso, and S3.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres (production), SQLite/Turso (testing, CLI, embedded), S3 (portable object-store-backed state)
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

## Choose Your Backend

| Scenario | Recommended Backend | Why |
|----------|---------------------|-----|
| Production with multiple workers | **PostgreSQL** | Full concurrency, no writer conflicts |
| CLI tools & scripts | **SQLite / Turso** | Zero-config, embedded, portable |
| Testing & prototyping | **SQLite / Turso** | Fast setup, no external dependencies |
| Embedded applications | **SQLite / Turso** | Single-file database, no server |
| Portable remote queue state | **S3** | Durable queue state in object storage without running PostgreSQL |
| High write throughput | **PostgreSQL** | SQLite/Turso allow only 1 writer at a time |

> ⚠️ **SQLite/Turso Concurrency Limit**: SQLite and Turso use database-level locks. With many concurrent writers, you may hit lock contention. See [SkyPilot's findings on SQLite concurrency](https://blog.skypilot.co/abusing-sqlite-to-handle-concurrency/). pgqrs enables WAL mode and sets a 5s busy timeout to mitigate this, but PostgreSQL is recommended for multi-worker scenarios.

### Benchmark Highlights

Current queue benchmark baselines show:

- **PostgreSQL is the gold standard**: strong throughput with one consumer, and close to linear scaling as more consumers are added
- **SQLite has similar single-consumer behavior** for this queue-drain scenario, but it does not scale with more consumers
- **Turso currently behaves like SQLite** in this repo's local-path mode
- **S3 is much slower on the durable object-store path** because per-message latency is much higher

See:

- [Benchmark overview](docs/benchmarks/index.md)
- [Queue Drain Fixed Backlog](docs/benchmarks/queue-drain-fixed-backlog.md)
- [Backend Selection Guide](docs/user-guide/concepts/backends.md)

## Quick Start

### Job Queue

Simple, reliable message queue for background processing:

#### Rust

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
    println!("Enqueued: {:?}", ids);

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

#### Python

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

### Durable Workflows

Orchestrate multi-step processes that survive crashes:

#### Rust

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

#### Python

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

For a fuller walkthrough, see the [Durable Workflows Guide](docs/user-guide/guides/durable-workflows.md).

## Installation

### Python

```bash
pip install pgqrs
```

### Rust

```toml
[dependencies]
# PostgreSQL only (default)
pgqrs = "0.15.1"

# SQLite only
pgqrs = { version = "0.15.1", default-features = false, features = ["sqlite"] }

# Turso only
pgqrs = { version = "0.15.1", default-features = false, features = ["turso"] }

# All backends
pgqrs = { version = "0.15.1", features = ["full"] }

# Workflow macros (optional)
pgqrs-macros = "0.15.1"
```

## Documentation

- **[Full Documentation](https://pgqrs.vrajat.com)** - Complete guides and API reference
- **[Rust API Docs](https://docs.rs/pgqrs)** - Rust crate documentation
- **[Python Examples](py-pgqrs/tests/test_pgqrs.py)** - Python test suite with examples
- **[Docs Home](docs/index.md)** - Master documentation source

## Development

Prerequisites:
- **Rust**: 1.70+
- **Python**: 3.8+
- **PostgreSQL**: 12+

### Setup

```bash
# Setup environment and install dependencies
make requirements
```

### Build & Test

```bash
# Build both Rust core and Python bindings
make build

# Run all tests (Rust + Python)
make test
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
