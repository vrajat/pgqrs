# Design Doc: Database Abstraction for pgqrs

## Objective
Enable `pgqrs` to support multiple database backends—**Postgres**, **SQLite**, and **Turso**—by introducing a robust database abstraction layer.

## Architecture: The Store Pattern
We will replace direct `sqlx::PgPool` usage with a cohesive **Store Trait** system. This segregates business logic from data access logic and wraps complex database operations.

### 1. Store Traits (`src/store/mod.rs`)
The `Store` trait acts as the entry point for accessing specific repositories and managing transactions.

```rust
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    // Repositories
    type QueueStore: QueueStore<Error = Self::Error>;
    type MessageStore: MessageStore<Error = Self::Error>;
    type WorkerStore: WorkerStore<Error = Self::Error>;

    // Accessors
    fn queues(&self) -> &Self::QueueStore;
    fn messages(&self) -> &Self::MessageStore;
    fn workers(&self) -> &Self::WorkerStore;

    // Transaction management
    async fn begin(&self) -> Result<Box<dyn Transaction<Error = Self::Error>>>;
}
```

#### Complex Operations Layer
The User noted a missing layer for "complex DB operations" (e.g., atomic state transitions in `lifecycle.rs`).
These operations often rely on DB-specific features (locking, CTEs) and cannot be efficiently assembled from simple CRUD.
**Decision**: These complex operations will be defined as methods on the specific Stores (e.g., `WorkerStore`).
*   `WorkerStore::suspend_worker(id)`: Handles the atomic state transition SQL.
*   `MessageStore::enqueue_batch(...)`: Handles batch inserts (using `COPY` for Postgres, multiple `INSERT`s for SQLite).

This keeps the *invocation* of logic in `worker/` but moves the *implementation* of the complex script to `store/postgres/workers.rs`, etc.

### 2. Module Organization
*   `src/store/` (New)
    *   `mod.rs`: Trait definitions (`Store`, `QueueStore`).
    *   `postgres/`
        *   `mod.rs`: `PostgresStore` implementation.
        *   `queues.rs`: `QueueStore` impl using `sqlx`.
        *   `workers.rs`: `WorkerStore` impl including atomic lifecycle SQL.
    *   `sqlite/`: `rusqlite`/`sqlx` based implementation.
    *   `turso/`: implementation using `libsql` (connecting via `turso://` scheme).
*   `src/worker/`:
    *   Removes all raw SQL.
    *   `Producer<S: Store>`, `Consumer<S: Store>`.

### 3. Turso Integration
*   Use `turso` crate.
*   Connection URI scheme: `turso://...`.
*   Turso support will be feature-gated (`feature = "turso"`).

### 4. Testing Strategy
We will support running tests against different backends using environment variables and `make` targets.

*   **Default**: `make test` runs SQLite (fast, no deps).
*   **Postgres**: `make test DB=postgres` runs Postgres tests.
    *   Tests requiring Postgres will check `std::env::var("PG_DSN")` or start a container via `testcontainers` if the feature is enabled.
*   **Running All**: `make test-all` runs for all available backends.

#### Implementation details
*   Tests will use a helper `get_test_store()` that returns a `Box<dyn Store>` (or enum wrapper) based on configuration.
*   We'll avoid complex macros for now. Structuring tests to use the generic `Store` trait allows running the same test logic against different instantiated stores.

### 5. Python Bindings
Python bindings need concrete types. We will use an enum wrapper to hide the generic `Store`.

```rust
// py-pgqrs/src/lib.rs

pub enum PyStore {
    Postgres(PostgresStore),
    Sqlite(SqliteStore),
    Turso(TursoStore),
}

// Producer wrapper delegates to inner store
#[pyclass]
pub struct Producer {
    inner: Arc<dyn Any>, // or specific enum wrapper
}
```

*   `Config.from_dsn` detects scheme (`postgres://`, `sqlite://`, `turso://`).
*   Instantiates the correct `PyStore` variant.
