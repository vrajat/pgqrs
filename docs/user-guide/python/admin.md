# Admin (Python)

The `Admin` class provides queue management, schema administration, and access to table operations.

## Creating an Admin

```python
from pgqrs import Admin

admin = Admin(dsn="postgresql://localhost/mydb")
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dsn` | `str` | PostgreSQL connection string |

## Methods

### install

Install the pgqrs schema (tables, indexes, constraints).

```python
await admin.install()
print("Schema installed successfully")
```

Safe to call multiple times—it's idempotent.

### verify

Verify that the pgqrs schema is correctly installed.

```python
await admin.verify()
print("Schema verification passed")
```

Raises an error if the schema is missing or corrupted.

### create_queue

Create a new queue.

```python
queue = await admin.create_queue("email-notifications")
print(f"Created queue: {queue.queue_name} (ID: {queue.id})")
```

**Returns:** `QueueInfo` with:

| Property | Type | Description |
|----------|------|-------------|
| `id` | `int` | Unique queue ID |
| `queue_name` | `str` | Queue name |
| `created_at` | `str` | Creation timestamp (ISO format) |

## Table Access

Admin provides access to table operations through async accessor methods:

### get_workers

Get access to worker table operations.

```python
workers = await admin.get_workers()
count = await workers.count()
print(f"Total workers: {count}")
```

### get_queues

Get access to queue table operations.

```python
queues = await admin.get_queues()
count = await queues.count()
print(f"Total queues: {count}")
```

### get_messages

Get access to message table operations.

```python
messages = await admin.get_messages()
count = await messages.count()
print(f"Total messages: {count}")
```

### get_archive

Get access to archive table operations.

```python
archive = await admin.get_archive()
count = await archive.count()
print(f"Total archived messages: {count}")
```

## QueueInfo

The `QueueInfo` object returned by `create_queue()`:

| Property | Type | Description |
|----------|------|-------------|
| `id` | `int` | Unique queue ID |
| `queue_name` | `str` | Queue name |
| `created_at` | `str` | Creation timestamp (ISO 8601 format) |

## Usage Examples

### Initial Setup

```python
import asyncio
from pgqrs import Admin

async def setup_pgqrs():
    admin = Admin("postgresql://localhost/mydb")

    # Install schema
    await admin.install()
    print("Schema installed")

    # Verify installation
    await admin.verify()
    print("Schema verified")

    # Create application queues
    queues = ["emails", "notifications", "tasks", "reports"]

    for name in queues:
        try:
            queue = await admin.create_queue(name)
            print(f"Created queue: {queue.queue_name}")
        except RuntimeError as e:
            if "already exists" in str(e):
                print(f"Queue {name} already exists")
            else:
                raise

asyncio.run(setup_pgqrs())
```

### Monitoring Dashboard

```python
async def print_stats(admin):
    """Print queue statistics."""
    queues = await admin.get_queues()
    messages = await admin.get_messages()
    workers = await admin.get_workers()
    archive = await admin.get_archive()

    print("=== pgqrs Statistics ===")
    print(f"Queues:   {await queues.count()}")
    print(f"Workers:  {await workers.count()}")
    print(f"Messages: {await messages.count()}")
    print(f"Archived: {await archive.count()}")

asyncio.run(print_stats(Admin("postgresql://localhost/mydb")))
```

### FastAPI Admin Endpoints

```python
from fastapi import FastAPI, HTTPException
from pgqrs import Admin

app = FastAPI()
admin: Admin | None = None

@app.on_event("startup")
async def startup():
    global admin
    admin = Admin("postgresql://localhost/mydb")
    await admin.install()

@app.post("/queues/{name}")
async def create_queue(name: str):
    try:
        queue = await admin.create_queue(name)
        return {
            "id": queue.id,
            "name": queue.queue_name,
            "created_at": queue.created_at
        }
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/stats")
async def get_stats():
    queues = await admin.get_queues()
    messages = await admin.get_messages()
    workers = await admin.get_workers()
    archive = await admin.get_archive()

    return {
        "queues": await queues.count(),
        "messages": await messages.count(),
        "workers": await workers.count(),
        "archived": await archive.count()
    }
```

### Health Check

```python
async def health_check(admin: Admin) -> dict:
    """Perform a health check on pgqrs."""
    try:
        await admin.verify()

        messages = await admin.get_messages()
        workers = await admin.get_workers()

        return {
            "healthy": True,
            "schema_valid": True,
            "message_count": await messages.count(),
            "worker_count": await workers.count()
        }
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e)
        }
```

## Full Example

```python
import asyncio
from pgqrs import Admin, Producer, Consumer

async def main():
    dsn = "postgresql://localhost/mydb"

    # Admin setup
    admin = Admin(dsn)

    print("Installing schema...")
    await admin.install()
    await admin.verify()
    print("Schema ready!")

    # Create queues
    print("\nCreating queues...")
    queue1 = await admin.create_queue("tasks")
    queue2 = await admin.create_queue("notifications")
    print(f"  - {queue1.queue_name} (id: {queue1.id})")
    print(f"  - {queue2.queue_name} (id: {queue2.id})")

    # Get table accessors
    queues = await admin.get_queues()
    workers = await admin.get_workers()
    messages = await admin.get_messages()
    archive = await admin.get_archive()

    # Show initial stats
    print("\nInitial stats:")
    print(f"  Queues:   {await queues.count()}")
    print(f"  Workers:  {await workers.count()}")
    print(f"  Messages: {await messages.count()}")
    print(f"  Archived: {await archive.count()}")

    # Create some activity
    print("\nCreating messages...")
    producer = Producer(dsn, "tasks", "producer", 3000)
    for i in range(5):
        await producer.enqueue({"task": i})
    print("  Sent 5 messages")

    # Consume
    consumer = Consumer(dsn, "tasks", "consumer", 3001)
    msgs = await consumer.dequeue()
    for msg in msgs:
        await consumer.archive(msg.id)
    print(f"  Processed and archived {len(msgs)} messages")

    # Final stats
    print("\nFinal stats:")
    print(f"  Messages: {await messages.count()}")
    print(f"  Archived: {await archive.count()}")
    print(f"  Workers:  {await workers.count()}")

if __name__ == "__main__":
    asyncio.run(main())
```

## See Also

- [Producer (Python)](producer.md) - Creating messages
- [Consumer (Python)](consumer.md) - Processing messages
- [Admin (Rust)](../rust/admin.md) - Rust API reference
- [CLI Reference](../cli-reference.md) - Command-line administration
