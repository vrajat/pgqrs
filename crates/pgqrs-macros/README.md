# pgqrs-macros

> [!CAUTION]
> **Internal Crate**: This crate (`pgqrs-macros`) is a support crate containing procedural macros for the main `pgqrs` library.
> You should likely use the `pgqrs` crate directly, which re-exports these macros.
>
> See the [pgqrs crate](https://crates.io/crates/pgqrs) for the main documentation.

---

# pgqrs

[![Rust](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml/badge.svg)](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/pgqrs.svg)](https://badge.fury.io/py/pgqrs)

**pgqrs** is a PostgreSQL-backed durable workflow engine and job queue. Written in Rust with Python bindings.

## Features

### Core
* **Library-only**: No servers to operate. Use directly in your Rust or Python applications.
* **Connection Pooler Compatible**: Works with pgBouncer and pgcat for connection scaling.

### Job Queue
* **Efficient**: Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching.
* **Exactly-once Delivery**: Guarantees within visibility timeout window.
* **Message Archiving**: Built-in audit trails and historical data retention.

### Durable Workflows
* **Crash Recovery**: Resume from the last completed step after failures.
* **Exactly-once Steps**: Completed steps are never re-executed.
* **Persistent State**: All workflow progress stored in PostgreSQL.

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
    pgqrs::admin(&store).create_queue("tasks").await?;

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
            // Your processing logic here
            Ok(())
        })
        .execute(&store)
        .await?;

    Ok(())
}
```

#### Python

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
    print(f"Enqueued: {msg_id}")

    # Consumer: process jobs
    async def handler(msg):
        print(f"Processing: {msg.payload}")
        return True

    await pgqrs.consume(store, "tasks", handler)

asyncio.run(main())
```

### Durable Workflows

Orchestrate multi-step processes that survive crashes:

#### Rust

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

#### Python

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

## Installation

### Python

```bash
pip install pgqrs
```

### Rust

```toml
[dependencies]
pgqrs = "0.5"
pgqrs-macros = "0.5"  # For workflow macros
```

## Documentation

- **[Full Documentation](https://vrajat.github.io/pgqrs/)** - Complete guides and API reference
- **[Rust API Docs](https://docs.rs/pgqrs)** - Rust crate documentation
- **[Python Examples](py-pgqrs/tests/test_pgqrs.py)** - Python test suite with examples

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
