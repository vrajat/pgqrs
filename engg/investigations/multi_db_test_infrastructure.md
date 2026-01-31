# Design Doc: Multi-Database Test Infrastructure

## Objective

Enable running the full pgqrs test suite against multiple database backends (Postgres, SQLite, Turso) via environment variables and CLI arguments. Provide a clean, extensible abstraction that:

1. Works uniformly across Rust and Python tests
2. Supports running on a single backend, all backends, or a subset
3. Is implementable as a single focused task
4. Scales to 3-4 database backends without significant refactoring

## Current State

### Rust Tests (`crates/pgqrs/tests/`)

Each test file has a `create_store()` function:

```rust
async fn create_store() -> pgqrs::store::AnyStore {
    let database_url = common::get_postgres_dsn(Some("pgqrs_test_schema")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_test_schema")
        .expect("Failed to create config");
    pgqrs::store::AnyStore::connect(&config).await.expect("Failed to create store")
}
```

The `common` module in `tests/common/mod.rs` provides:

```rust
pub async fn get_postgres_dsn(schema: Option<&str>) -> String {
    let external_dsn = std::env::var("PGQRS_TEST_DSN").ok();
    // Falls back to testcontainers if not set
    container::get_postgres_dsn(schema).await
}
```

### Python Tests (`py-pgqrs/tests/conftest.py`)

```python
@pytest.fixture(scope="session")
def postgres_dsn():
    dsn = os.environ.get("PGQRS_TEST_DSN")
    if dsn:
        yield dsn
    else:
        with PostgresContainer("postgres:15") as postgres:
            yield postgres.get_connection_url().replace("+psycopg2", "")
```

### Makefile

```makefile
test-rust:
	cargo test --workspace

test-py:
	$(UV) run pytest py-pgqrs

test: build
	cargo test --workspace
	$(UV) run pytest py-pgqrs
```

## Design

### Core Abstraction: `TestBackend` Enum

```rust
// crates/pgqrs/tests/common/backend.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestBackend {
    Postgres,
    Sqlite,
    Turso,
}

impl TestBackend {
    /// Parse from environment variable or string
    pub fn from_env() -> Self {
        match std::env::var("PGQRS_TEST_BACKEND").as_deref() {
            Ok("sqlite") => Self::Sqlite,
            Ok("turso") => Self::Turso,
            Ok("postgres") | Ok("pg") | _ => Self::Postgres, // Default
        }
    }

    /// Parse from string (for CLI args)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "pg" => Some(Self::Postgres),
            "sqlite" => Some(Self::Sqlite),
            "turso" => Some(Self::Turso),
            _ => None,
        }
    }

    /// Get the DSN environment variable name for this backend
    pub fn dsn_env_var(&self) -> &'static str {
        match self {
            Self::Postgres => "PGQRS_TEST_POSTGRES_DSN",
            Self::Sqlite => "PGQRS_TEST_SQLITE_PATH",
            Self::Turso => "PGQRS_TEST_TURSO_DSN",
        }
    }
}
```

### Backend-Specific DSN Providers

```rust
// crates/pgqrs/tests/common/providers.rs

use super::backend::TestBackend;

/// Trait for backend-specific test setup
#[async_trait::async_trait]
pub trait TestDsnProvider: Send + Sync {
    /// Get or create a DSN for testing
    async fn get_dsn(&self, schema: Option<&str>) -> String;

    /// Clean up after tests (optional)
    async fn cleanup(&self) {}
}

/// Postgres provider - uses testcontainers or external DSN
pub struct PostgresProvider;

#[async_trait::async_trait]
impl TestDsnProvider for PostgresProvider {
    async fn get_dsn(&self, schema: Option<&str>) -> String {
        let external = std::env::var("PGQRS_TEST_POSTGRES_DSN").ok();
        container::get_postgres_dsn(schema, external.as_deref()).await
    }
}

/// SQLite provider - uses temp file or specified path
pub struct SqliteProvider;

#[async_trait::async_trait]
impl TestDsnProvider for SqliteProvider {
    async fn get_dsn(&self, schema: Option<&str>) -> String {
        if let Ok(path) = std::env::var("PGQRS_TEST_SQLITE_PATH") {
            format!("sqlite://{}", path)
        } else {
            // Create temp file with schema name for isolation
            let name = schema.unwrap_or("test");
            let path = std::env::temp_dir().join(format!("pgqrs_test_{}.db", name));
            format!("sqlite://{}", path.display())
        }
    }
}

/// Turso provider - requires external DSN (no testcontainer available)
pub struct TursoProvider;

#[async_trait::async_trait]
impl TestDsnProvider for TursoProvider {
    async fn get_dsn(&self, _schema: Option<&str>) -> String {
        std::env::var("PGQRS_TEST_TURSO_DSN")
            .expect("PGQRS_TEST_TURSO_DSN must be set for Turso tests")
    }
}

/// Get provider for a backend
pub fn get_provider(backend: TestBackend) -> Box<dyn TestDsnProvider> {
    match backend {
        TestBackend::Postgres => Box::new(PostgresProvider),
        TestBackend::Sqlite => Box::new(SqliteProvider),
        TestBackend::Turso => Box::new(TursoProvider),
    }
}
```

### Unified `create_store()` Function

```rust
// crates/pgqrs/tests/common/mod.rs

mod backend;
mod providers;

pub use backend::TestBackend;
pub use providers::{get_provider, TestDsnProvider};

/// Create a store for the currently selected test backend
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    let backend = TestBackend::from_env();
    create_store_for_backend(backend, schema).await
}

/// Create a store for a specific backend
pub async fn create_store_for_backend(
    backend: TestBackend,
    schema: &str,
) -> pgqrs::store::AnyStore {
    let provider = get_provider(backend);
    let dsn = provider.get_dsn(Some(schema)).await;

    let config = pgqrs::config::Config::from_dsn_with_schema(dsn, schema)
        .expect("Failed to create config");

    pgqrs::store::AnyStore::connect(&config)
        .await
        .expect("Failed to create store")
}

/// Get the current test backend (for skip logic)
pub fn current_backend() -> TestBackend {
    TestBackend::from_env()
}

/// Skip test if not running on specified backend
#[macro_export]
macro_rules! skip_unless_backend {
    ($backend:expr) => {
        if common::current_backend() != $backend {
            eprintln!("Skipping test: requires {:?} backend", $backend);
            return;
        }
    };
}

/// Skip test if running on specified backend
#[macro_export]
macro_rules! skip_on_backend {
    ($backend:expr) => {
        if common::current_backend() == $backend {
            eprintln!("Skipping test: not supported on {:?} backend", $backend);
            return;
        }
    };
}
```

### Test File Migration

**Before:**
```rust
async fn create_store() -> pgqrs::store::AnyStore {
    let database_url = common::get_postgres_dsn(Some("pgqrs_worker_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_worker_test")
        .expect("Failed to create config");
    pgqrs::store::AnyStore::connect(&config).await.expect("Failed to create store")
}

#[tokio::test]
async fn test_worker_lifecycle() {
    let store = create_store().await;
    // ...
}
```

**After:**
```rust
mod common;

#[tokio::test]
async fn test_worker_lifecycle() {
    let store = common::create_store("pgqrs_worker_test").await;
    // ...
}

#[tokio::test]
async fn test_postgres_specific_feature() {
    skip_unless_backend!(common::TestBackend::Postgres);
    let store = common::create_store("pgqrs_pg_specific").await;
    // ...
}
```

### Python Test Infrastructure

```python
# py-pgqrs/tests/conftest.py

import os
import pytest
import tempfile
from enum import Enum
from typing import Generator


class TestBackend(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    TURSO = "turso"

    @classmethod
    def from_env(cls) -> "TestBackend":
        backend = os.environ.get("PGQRS_TEST_BACKEND", "postgres").lower()
        try:
            return cls(backend)
        except ValueError:
            return cls.POSTGRES


def get_backend() -> TestBackend:
    return TestBackend.from_env()


@pytest.fixture(scope="session")
def test_backend() -> TestBackend:
    """Returns the current test backend."""
    return get_backend()


@pytest.fixture(scope="session")
def database_dsn(test_backend: TestBackend) -> Generator[str, None, None]:
    """
    Provides a database DSN appropriate for the selected backend.
    """
    if test_backend == TestBackend.POSTGRES:
        dsn = os.environ.get("PGQRS_TEST_POSTGRES_DSN")
        if dsn:
            yield dsn
        else:
            from testcontainers.postgres import PostgresContainer
            with PostgresContainer("postgres:15") as postgres:
                yield postgres.get_connection_url().replace("+psycopg2", "")

    elif test_backend == TestBackend.SQLITE:
        path = os.environ.get("PGQRS_TEST_SQLITE_PATH")
        if path:
            yield f"sqlite://{path}"
        else:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                yield f"sqlite://{f.name}"

    elif test_backend == TestBackend.TURSO:
        dsn = os.environ.get("PGQRS_TEST_TURSO_DSN")
        if not dsn:
            pytest.skip("PGQRS_TEST_TURSO_DSN not set")
        yield dsn


# Convenience decorators for backend-specific tests
def requires_backend(backend: TestBackend):
    """Skip test unless running on specified backend."""
    return pytest.mark.skipif(
        get_backend() != backend,
        reason=f"Test requires {backend.value} backend"
    )


def skip_on_backend(backend: TestBackend):
    """Skip test when running on specified backend."""
    return pytest.mark.skipif(
        get_backend() == backend,
        reason=f"Test not supported on {backend.value} backend"
    )


# Aliases for postgres_dsn compatibility during migration
@pytest.fixture(scope="session")
def postgres_dsn(database_dsn: str) -> str:
    """Legacy alias for database_dsn."""
    return database_dsn
```

**Test file example:**
```python
# py-pgqrs/tests/test_pgqrs.py

from conftest import requires_backend, skip_on_backend, TestBackend

@pytest.mark.asyncio
async def test_basic_enqueue_dequeue(database_dsn, schema):
    """Works on all backends."""
    store, admin = await setup_test(database_dsn, schema)
    # ...

@requires_backend(TestBackend.POSTGRES)
@pytest.mark.asyncio
async def test_pgbouncer_compatibility(database_dsn, schema):
    """Only runs on Postgres."""
    # ...

@skip_on_backend(TestBackend.SQLITE)
@pytest.mark.asyncio
async def test_concurrent_workers(database_dsn, schema):
    """Skipped on SQLite (single-writer limitation)."""
    # ...
```

### Makefile Targets

```makefile
# Database backend for testing (postgres, sqlite, turso)
PGQRS_TEST_BACKEND ?= postgres

# Run tests on a specific backend
test-rust:
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo test --workspace

test-py:
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

test: build test-rust test-py

# Convenience targets for each backend
test-postgres:
	$(MAKE) test PGQRS_TEST_BACKEND=postgres

test-sqlite:
	$(MAKE) test PGQRS_TEST_BACKEND=sqlite

test-turso:
	@if [ -z "$$PGQRS_TEST_TURSO_DSN" ]; then \
		echo "Error: PGQRS_TEST_TURSO_DSN must be set"; \
		exit 1; \
	fi
	$(MAKE) test PGQRS_TEST_BACKEND=turso

# Run on all available backends
test-all-backends:
	@echo "=== Testing on Postgres ==="
	$(MAKE) test-postgres
	@echo ""
	@echo "=== Testing on SQLite ==="
	$(MAKE) test-sqlite
	@if [ -n "$$PGQRS_TEST_TURSO_DSN" ]; then \
		echo ""; \
		echo "=== Testing on Turso ==="; \
		$(MAKE) test-turso; \
	else \
		echo ""; \
		echo "=== Skipping Turso (PGQRS_TEST_TURSO_DSN not set) ==="; \
	fi

# Run on a subset (comma-separated)
# Usage: make test-backends BACKENDS=postgres,sqlite
test-backends:
	@for backend in $$(echo "$(BACKENDS)" | tr ',' ' '); do \
		echo "=== Testing on $$backend ==="; \
		$(MAKE) test PGQRS_TEST_BACKEND=$$backend; \
		echo ""; \
	done
```

### CLI Arguments for cargo test

For more granular control, tests can accept backend via test name filtering:

```bash
# Run all tests on sqlite
PGQRS_TEST_BACKEND=sqlite cargo test --workspace

# Run specific test on specific backend
PGQRS_TEST_BACKEND=postgres cargo test worker_lifecycle

# Run only Postgres-specific tests
cargo test postgres_specific
```

### CI/CD Integration

```yaml
# .github/workflows/test.yml

name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        backend: [postgres, sqlite]
        # Turso requires credentials, handle separately

    steps:
      - uses: actions/checkout@v4

      - name: Setup Rust
        uses: dtolnay/rust-action@stable

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Run tests
        run: make test
        env:
          PGQRS_TEST_BACKEND: ${{ matrix.backend }}

  test-turso:
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      # ... setup steps ...
      - name: Run Turso tests
        run: make test-turso
        env:
          PGQRS_TEST_TURSO_DSN: ${{ secrets.TURSO_TEST_DSN }}
```

## File Changes Summary

| File | Change |
|------|--------|
| `crates/pgqrs/tests/common/mod.rs` | Add `TestBackend` enum, unified `create_store()`, skip macros |
| `crates/pgqrs/tests/common/backend.rs` | New: `TestBackend` enum definition |
| `crates/pgqrs/tests/common/providers.rs` | New: `TestDsnProvider` trait and implementations |
| `crates/pgqrs/tests/*.rs` | Migrate to `common::create_store(schema)` |
| `py-pgqrs/tests/conftest.py` | Add `TestBackend`, `database_dsn` fixture, decorators |
| `py-pgqrs/tests/test_*.py` | Migrate to `database_dsn` fixture |
| `Makefile` | Add backend-specific and multi-backend targets |
| `.github/workflows/test.yml` | Add matrix strategy for backends |
| `docs/development/testing.md` | Document multi-backend testing |

## Implementation Order

1. **Create backend abstraction** (`backend.rs`, `providers.rs`)
2. **Update `common/mod.rs`** with unified `create_store()`
3. **Migrate Rust test files** (one-by-one, all follow same pattern)
4. **Update Python `conftest.py`** with new fixtures
5. **Migrate Python test files** to new fixtures
6. **Add Makefile targets**
7. **Update CI workflow**
8. **Document in testing.md**

## Extensibility

Adding a new backend (e.g., DuckDB) requires:

1. Add variant to `TestBackend` enum
2. Add DSN environment variable mapping
3. Create `DuckDbProvider` implementing `TestDsnProvider`
4. Add to `get_provider()` match
5. Add Python enum variant and fixture case
6. Add Makefile target

Total: ~50 lines of code across 4 files.

## Dependencies

- Issue #139: Workflow APIs must be DB-agnostic for workflow tests to run on all backends
- Issue #140: `execute_raw()` interface needed for backend-agnostic test setup/cleanup
- SQLite and Turso store implementations must exist (feature-gated is fine)

## Acceptance Criteria

- [ ] `PGQRS_TEST_BACKEND` env var selects backend in both Rust and Python tests
- [ ] `make test-postgres`, `make test-sqlite`, `make test-turso` work
- [ ] `make test-all-backends` runs full suite on all available backends
- [ ] Backend-specific tests can be skipped with macros/decorators
- [ ] CI runs tests on Postgres and SQLite in matrix
- [ ] Adding a 4th backend requires <100 lines of code
- [ ] All existing tests pass on Postgres (no regression)
- [ ] Documentation updated
