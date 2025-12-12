# py-pgqrs

Python bindings for `pgqrs`, a high-performance Postgres-backed queue.

## Installation

py-pgqrs can be installed using `pip` (requires Rust toolchain installed):

```bash
pip install .
```

Or for development:
```bash
maturin develop
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
