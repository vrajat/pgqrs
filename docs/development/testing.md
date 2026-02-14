# Testing Guide

How to run and write tests for pgqrs.

## Prerequisites

- Rust 1.70+
- PostgreSQL 14+ (running instance)
- Docker (optional, for containerized testing)

## Running Tests

### Quick Start

```bash
# Set database connection
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# Run all tests
cargo test

# Run with logging
RUST_LOG=debug cargo test -- --nocapture
```

### Test Categories

#### Unit Tests

Run fast, in-memory tests:

```bash
cargo test --lib
```

#### Integration Tests

Run tests that require PostgreSQL:

```bash
cargo test --test '*'
```

#### Specific Test File

```bash
cargo test --test integration_tests
```

#### Pattern Matching

```bash
# Run tests containing "queue"
cargo test queue

# Run tests containing "producer"
cargo test producer
```

## Test Database Setup

### Option 1: Docker

```bash
# Start PostgreSQL
docker run -d \
  --name pgqrs-test \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16

# Set connection
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
```

### Option 2: Local PostgreSQL

```bash
# Create test database
createdb pgqrs_test

# Set connection
export PGQRS_DSN="postgresql://localhost/pgqrs_test"
```

### Option 3: Separate Test Schema

Use a dedicated schema to avoid conflicts:

```bash
export PGQRS_SCHEMA="pgqrs_test"
```

## Writing Tests

### Unit Tests

Place in the same file as the code:

```rust
// src/config.rs
pub fn parse_dsn(dsn: &str) -> Result<Config> {
    // ...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dsn_basic() {
        let config = parse_dsn("postgresql://localhost/db").unwrap();
        assert_eq!(config.database, "db");
    }

    #[test]
    fn test_parse_dsn_with_port() {
        let config = parse_dsn("postgresql://localhost:5433/db").unwrap();
        assert_eq!(config.port, 5433);
    }

    #[test]
    fn test_parse_dsn_invalid() {
        let result = parse_dsn("invalid");
        assert!(result.is_err());
    }
}
```

### Integration Tests

Place in `tests/` directory:

```rust
// tests/queue_tests.rs
use pgqrs::{Admin, Config};

#[tokio::test]
async fn test_create_queue() {
    let config = Config::from_env().expect("PGQRS_DSN required");
    let admin = pgqrs::connect(dsn).await.unwrap();

    // Setup
    admin.install().await.unwrap();

    // Test
    let queue = store.queue("test_queue").await.unwrap();
    assert_eq!(queue.queue_name, "test_queue");

    // Cleanup
    admin.delete_queue("test_queue").await.ok();
}
```

### Test Fixtures

Create reusable test setup:

```rust
// tests/common/mod.rs
use pgqrs::{Admin, Config};

pub struct TestFixture {
    pub admin: Admin,
    pub queue_name: String,
}

impl TestFixture {
    pub async fn new() -> Self {
        let config = Config::from_env().unwrap();
        let admin = pgqrs::connect(dsn).await.unwrap();

        admin.install().await.unwrap();

        let queue_name = format!("test_{}", uuid::Uuid::new_v4());
        store.queue(&queue_name).await.unwrap();

        Self { admin, queue_name }
    }

    pub async fn cleanup(&self) {
        self.admin.delete_queue(&self.queue_name).await.ok();
    }
}

// Usage
#[tokio::test]
async fn test_with_fixture() {
    let fixture = TestFixture::new().await;

    // Your test code here

    fixture.cleanup().await;
}
```

### Async Tests

Use `tokio::test` for async tests:

```rust
#[tokio::test]
async fn test_async_operation() {
    let result = some_async_function().await;
    assert!(result.is_ok());
}

// With custom runtime
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_operations() {
    // Tests that need multiple threads
}
```

## Python Tests

### Setup

```bash
cd py-pgqrs

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dev dependencies
pip install maturin pytest pytest-asyncio

# Build and install
maturin develop
```

### Running Python Tests

```bash
# Run all tests
pytest

# Run specific test
pytest tests/test_basic.py::test_enqueue

# With verbose output
pytest -v

# With coverage
pytest --cov=pgqrs
```

### Writing Python Tests

```python
# tests/test_producer.py
import pytest
import pytest_asyncio
from pgqrs import Admin, Producer

@pytest_asyncio.fixture
async def admin():
    admin = Admin("postgresql://localhost/test")
    await admin.install()
    yield admin

@pytest_asyncio.fixture
async def queue(admin):
    queue = await store.queue("test_queue")
    yield queue
    await admin.delete_queue("test_queue")

@pytest.mark.asyncio
async def test_enqueue(queue):
    producer = Producer(
        "postgresql://localhost/test",
        "test_queue",
        "test-host",
        3000,
    )

    msg_id = await producer.enqueue({"task": "test"})

    assert msg_id is not None
    assert isinstance(msg_id, int)
```

## Test Coverage

### Rust Coverage

Using `cargo-llvm-cov`:

```bash
# Install
cargo install cargo-llvm-cov

# Run with coverage
cargo llvm-cov

# Generate HTML report
cargo llvm-cov --html
open target/llvm-cov/html/index.html
```

### Python Coverage

```bash
pytest --cov=pgqrs --cov-report=html
open htmlcov/index.html
```

## CI/CD Tests

Tests run automatically on:

- Every push
- Every pull request

### CI Configuration

The project uses GitHub Actions:

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - 5432:5432

    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Run tests
        env:
          PGQRS_DSN: postgresql://postgres:postgres@localhost:5432/postgres
        run: cargo test
```

## Best Practices

1. **Isolate tests** - Each test should clean up after itself
2. **Use unique names** - Avoid conflicts with UUIDs or timestamps
3. **Test edge cases** - Empty inputs, nulls, large values
4. **Test errors** - Verify error handling works correctly
5. **Keep tests fast** - Mock expensive operations when possible
6. **Document tests** - Explain what each test verifies

## Troubleshooting

### Connection Issues

```bash
# Check PostgreSQL is running
pg_isready -h localhost

# Test connection
psql $PGQRS_DSN -c "SELECT 1"
```

### Cleanup Failures

```bash
# Manually clean up test data
psql $PGQRS_DSN -c "DROP SCHEMA pgqrs_test CASCADE"
```

### Flaky Tests

- Check for race conditions
- Ensure proper async handling
- Use explicit waits when needed
