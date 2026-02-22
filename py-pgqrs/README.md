# py-pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Python bindings for the Rust core. Built for Postgres. Also supports SQLite and Turso.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres (production), SQLite/Turso (testing, CLI, embedded)
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

## Installation

```bash
pip install pgqrs
```

For local development:

```bash
make requirements
```

## Backend Support

py-pgqrs supports all three backends. Choose the right one for your use case:

```python
# PostgreSQL (production)
store = await pgqrs.connect("postgresql://user:pass@localhost:5432/db")

# SQLite (embedded, testing)
store = await pgqrs.connect("sqlite:///path/to/database.db")

# Turso (SQLite-compatible, embedded)
store = await pgqrs.connect("turso:///path/to/database.db")
```

## Usage

### Producer + Consumer

```python
import asyncio
import pgqrs

async def main():
    store = await pgqrs.connect("postgresql://localhost/mydb")

    admin = pgqrs.admin(store)
    await admin.install()
    await store.queue("tasks")

    producer = await store.producer("tasks")
    msg_id = await producer.enqueue({"task": "process_image", "url": "..."})
    print(f"Enqueued job {msg_id}")

    consumer = await store.consumer("tasks")
    messages = await consumer.dequeue(batch_size=1)
    for msg in messages:
        print(f"Processing {msg.id}: {msg.payload}")
        await consumer.archive(msg.id)

asyncio.run(main())
```

### Durable Workflow (Python)

```python
import asyncio
import pgqrs

async def main():
    store = await pgqrs.connect("postgresql://localhost/mydb")
    admin = pgqrs.admin(store)
    await admin.install()

    await pgqrs.workflow().name("archive_files").store(store).create()
    consumer = await pgqrs.consumer("worker-1", 8080, "archive_files").create(store)

    await pgqrs.workflow() \
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

## Testing

```bash
make test-py PGQRS_TEST_BACKEND=postgres
```

## Documentation

- **[Full Documentation](https://pgqrs.vrajat.com)** - Complete guides and API reference
- **[Docs Home](../docs/index.md)** - Master documentation source
- **[Python Examples](tests/test_pgqrs.py)** - Python test suite with examples
- **[API Reference](../docs/user-guide/api/consumer.md)** - Consumer/producer API details
