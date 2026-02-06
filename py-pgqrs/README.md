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

py-pgqrs can be installed using `pip` (requires Rust toolchain installed):

```bash
pip install .
```

Or for development:
```bash
maturin develop
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

### Producer

```python
import pgqrs
import asyncio

async def produce():
    dsn = "postgres://user:pass@localhost:5432/db"
    producer = pgqrs.Producer(dsn, "my_queue", "producer-host", 1234)

    msg_id = await producer.enqueue({"task": "process_image", "url": "..."})
    print(f"Enqueued job {msg_id}")

asyncio.run(produce())
```

### Consumer

```python
import pgqrs
import asyncio

async def consume():
    dsn = "postgres://user:pass@localhost:5432/db"
    consumer = pgqrs.Consumer(dsn, "my_queue", "consumer-host", 5678)

    messages = await consumer.dequeue()
    for msg in messages:
        print(f"Processing {msg.id}: {msg.payload}")
        # Process...
        await consumer.archive(msg.id)

asyncio.run(consume())
```

### Administration

```python
import pgqrs
import asyncio

async def admin_tasks():
    dsn = "postgres://user:pass@localhost:5432/db"
    admin = pgqrs.Admin(dsn)

    # Setup
    await admin.install()
    await admin.create_queue("my_queue")

    # Monitoring
    queues = await admin.get_queues()
    print(f"Queue Count: {await queues.count()}")

asyncio.run(admin_tasks())
```

## Testing

Tests use `pytest` and `testcontainers` to run against a real Postgres instance.

```bash
pip install pytest pytest-asyncio testcontainers psycopg[binary]
pytest
```
