# pgqrs

[![Rust](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml/badge.svg)](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/pgqrs.svg)](https://badge.fury.io/py/pgqrs)

**pgqrs** is a PostgreSQL-backed durable workflow engine and job queue.
Written in Rust with Python bindings.

## Documentation

- **[Rust Documentation](crates/pgqrs/README.md)**
- **[Python Documentation](py-pgqrs/README.md)**

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

### Rust Example

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

### Python Example

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

## Installation

### Python

```bash
pip install pgqrs
```

### Rust

```toml
[dependencies]
pgqrs = "0.4.0"
```

## Development

Prerequisites:
- **Rust**: 1.70+
- **Python**: 3.8+
- **PostgreSQL**: 12+

### Setup

We use `make` to manage the development lifecycle.

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
