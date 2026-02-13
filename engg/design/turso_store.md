# Design Doc: TursoStore Implementation

## Objective

Integrate Turso (Rust rewrite of SQLite) as a storage backend for pgqrs, providing a modern async-first database engine with performance optimizations beyond traditional SQLite. Turso targets edge deployments, serverless environments, and applications requiring high-performance local storage with async I/O capabilities.

## Background

pgqrs currently supports PostgreSQL and SQLite as storage backends. Turso is a complete Rust rewrite of SQLite (https://github.com/tursodatabase/turso) that provides:

- **Native async/await support** with Tokio integration
- **High-performance I/O** via `io_uring` on Linux
- **SQLite compatibility** for file formats and SQL dialect
- **Modern Rust API** eliminating FFI overhead
- **Experimental features** like MVCC (`BEGIN CONCURRENT`), encryption, and incremental computation

Unlike libSQL (which is a C fork of SQLite), Turso is a from-scratch Rust implementation that integrates naturally with Rust async ecosystems.

## Design Decisions

### Turso vs SQLite Backend

| Aspect | SQLite (via sqlx) | Turso (native Rust) |
|--------|-------------------|---------------------|
| Language | C (via FFI) | Pure Rust |
| Async support | Blocking with sqlx wrapper | Native async/await |
| I/O backend | Standard syscalls | Pluggable (syscall, io_uring, memory) |
| Connection model | sqlx connection pool | Direct Turso connections |
| API style | SQL-focused (sqlx) | Prepared statement focused |
| Performance overhead | FFI boundary crossing | Zero-cost Rust abstractions |
| File format | SQLite 3 | SQLite 3 compatible |

**Design Decision**: Implement TursoStore as a **separate backend** rather than replacing SqliteStore because:

1. **Different APIs**: Turso uses `turso` crate, not `sqlx`, requiring different integration patterns
2. **Feature availability**: Turso is BETA; SQLite is production-ready
3. **Use case differentiation**:
   - SQLite: Maximum compatibility, embedded uses, low-dependency
   - Turso: Performance-critical async workloads, modern Rust stacks, edge computing

### Connection Model

**Question**: How should connections be managed with Turso's API?

**Answer**: Turso uses a Database → Connection pattern similar to rusqlite:

```rust
// Turso API pattern
let db = Builder::new_local("path.db").build().await?;  // Creates Database
let conn = db.connect()?;                                // Creates Connection
```

**Design Decision**: Use a shared `Arc<Database>` with connection-per-worker:

```rust
pub struct TursoStore {
    db: Arc<turso_sdk_kit::rsapi::TursoDatabase>,  // Shared database instance
    config: Config,
    // Table and worker structs will create connections on-demand
}
```

- **Shared Database**: `Arc<TursoDatabase>` allows multiple connections from same database
- **Connection-per-operation**: Each worker/table operation gets its own connection
- **No connection pool**: Turso connections are lightweight Rust objects, not OS resources

### Write Concurrency

**Question**: Does Turso support concurrent writes better than SQLite?

**Answer**: Turso offers experimental **MVCC with `BEGIN CONCURRENT`**:

```rust
// Standard SQLite behavior (single writer)
conn.execute("BEGIN IMMEDIATE", ()).await?;  // Blocks other writers

// Turso experimental MVCC
conn.execute("BEGIN CONCURRENT", ()).await?;  // Multiple writers possible
```

**Design Decision**:

1. **Default mode**: Use standard SQLite semantics (single writer) for compatibility
2. **Optional MVCC**: Allow enabling `BEGIN CONCURRENT` via feature flag or config
3. **Documentation**: Clearly mark MVCC as experimental (Turso is BETA)

```rust
pub struct TursoStoreConfig {
    pub enable_mvcc: bool,  // Default: false
}
```

**Limitations with MVCC**:
- Still experimental in Turso (as of 2026-01)
- May have different transaction isolation semantics
- Requires thorough testing before production use

### Async I/O Strategy

**Question**: Should we use Turso's io_uring backend on Linux?

**Answer**: Turso supports multiple I/O backends via configuration:

| Backend | Platform | Use Case |
|---------|----------|----------|
| `syscall` | All platforms | Default, portable |
| `io_uring` | Linux only | High-performance, async I/O |
| `memory` | All platforms | Testing, in-memory databases |

**Design Decision**: Make I/O backend **configurable** with sensible defaults:

```rust
pub enum TursoIoBackend {
    Auto,      // Platform-specific best choice
    Syscall,   // Portable standard I/O
    IoUring,   // Linux io_uring (high performance)
    Memory,    // In-memory (testing)
}

impl TursoStore {
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let io_backend = config.turso_io_backend.unwrap_or(TursoIoBackend::Auto);

        let builder = match io_backend {
            TursoIoBackend::Auto => {
                #[cfg(target_os = "linux")]
                { Builder::new_local(dsn) /* io_uring if available */ }
                #[cfg(not(target_os = "linux"))]
                { Builder::new_local(dsn) /* syscall */ }
            }
            TursoIoBackend::Syscall => Builder::new_local(dsn),
            TursoIoBackend::IoUring => Builder::new_local(dsn).with_vfs("io_uring")?,
            TursoIoBackend::Memory => Builder::new_local(":memory:"),
        };

        let db = builder.build().await?;
        Ok(Self { db: Arc::new(db), config: config.clone() })
    }
}
```

### Encryption Support

Turso supports **encryption at rest** as an experimental feature:

```rust
let db = Builder::new_local("encrypted.db")
    .enable_encryption()
    .with_encryption_key(&key)
    .build()
    .await?;
```

**Design Decision**: Expose encryption configuration in TursoStore:

```rust
pub struct TursoStoreConfig {
    pub encryption_key: Option<String>,  // Hex-encoded encryption key
}
```

## Architecture

### Module Structure

```
crates/pgqrs/src/store/
├── mod.rs              # Exports Store trait, AnyStore
├── any.rs              # AnyStore enum with Postgres + Sqlite + Turso variants
├── postgres/           # PostgreSQL implementation
├── sqlite/             # SQLite (via sqlx) implementation
└── turso/              # NEW: Turso (native Rust) implementation
    ├── mod.rs          # TursoStore struct, Store trait impl
    ├── tables/
    │   ├── mod.rs
    │   ├── pgqrs_queues.rs
    │   ├── pgqrs_messages.rs
    │   ├── pgqrs_workers.rs
    │   ├── pgqrs_archive.rs
    │   └── pgqrs_workflows.rs
    ├── worker/
    │   ├── mod.rs
    │   ├── admin.rs
    │   ├── producer.rs
    │   └── consumer.rs
    └── workflow/
        ├── mod.rs
        ├── handle.rs
        └── guard.rs
```

### TursoStore Struct

```rust
use turso::{Builder, Database, Connection};
use std::sync::Arc;

#[derive(Clone)]
pub struct TursoStore {
    db: Arc<Database>,
    config: Config,
    turso_config: TursoStoreConfig,
}

pub struct TursoStoreConfig {
    pub enable_mvcc: bool,
    pub io_backend: Option<TursoIoBackend>,
    pub encryption_key: Option<String>,
}

impl TursoStore {
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let turso_config = config.turso_config.clone().unwrap_or_default();

        let mut builder = Builder::new_local(dsn);

        // Apply encryption if configured
        if let Some(key) = &turso_config.encryption_key {
            builder = builder.enable_encryption().with_encryption_key(key);
        }

        // Apply I/O backend
        if let Some(backend) = &turso_config.io_backend {
            builder = match backend {
                TursoIoBackend::IoUring => builder.with_vfs("io_uring")?,
                TursoIoBackend::Syscall => builder.with_vfs("syscall")?,
                TursoIoBackend::Memory => builder, // :memory: path handles this
                TursoIoBackend::Auto => builder,
            };
        }

        let db = builder.build().await?;

        Ok(Self {
            db: Arc::new(db),
            config: config.clone(),
            turso_config,
        })
    }

    /// Create a new connection for operations
    fn connect(&self) -> Result<Connection> {
        self.db.connect()
    }
}

#[async_trait]
impl Store for TursoStore {
    type Db = (); // Turso doesn't use sqlx::Database

    fn concurrency_model(&self) -> ConcurrencyModel {
        if self.turso_config.enable_mvcc {
            ConcurrencyModel::Experimental // MVCC is experimental
        } else {
            ConcurrencyModel::SingleProcess // Standard SQLite semantics
        }
    }

    fn backend_name(&self) -> &'static str {
        "turso"
    }

    // ... other trait methods delegate to table/worker implementations
}
```

### AnyStore Integration

```rust
// crates/pgqrs/src/store/any.rs

#[derive(Clone, Debug)]
pub enum AnyStore {
    Postgres(PostgresStore),
    Sqlite(SqliteStore),
    Turso(TursoStore),  // NEW
}

impl AnyStore {
    pub async fn connect(config: &Config) -> Result<Self> {
        if config.dsn.starts_with("postgres://") || config.dsn.starts_with("postgresql://") {
            let pool = /* existing postgres connection logic */;
            Ok(AnyStore::Postgres(PostgresStore::new(pool, config)))
        } else if config.dsn.starts_with("sqlite://") || config.dsn.starts_with("sqlite:") {
            let store = SqliteStore::new(&config.dsn, config).await?;
            Ok(AnyStore::Sqlite(store))
        } else if config.dsn.starts_with("turso://") || config.dsn.starts_with("file:") {
            // turso:// prefix for explicit Turso selection
            // file: prefix can be used for file-based Turso databases
            let store = TursoStore::new(&config.dsn, config).await?;
            Ok(AnyStore::Turso(store))
        } else {
            Err(Error::InvalidConfig {
                message: format!("Unsupported DSN scheme: {}", config.dsn),
            })
        }
    }
}
```

**DSN Schemes**:
- `turso://path/to/db.db` - Explicit Turso backend
- `turso://:memory:` - In-memory Turso database
- `file:path/to/db.db` - File-based Turso (alternative syntax)

## SQL Dialect and Migrations

### Reusing SQLite Migrations

Since Turso is SQLite-compatible, we can **reuse existing SQLite migrations**:

```rust
// crates/pgqrs/src/store/turso/mod.rs

impl TursoStore {
    pub async fn run_migrations(&self) -> Result<()> {
        let conn = self.connect()?;

        // Read migration files from SQLite migrations directory
        // Turso uses same DDL syntax as SQLite
        let migrations = include_dir!("../../../migrations/sqlite");

        for migration_file in migrations.files() {
            let sql = migration_file.contents_utf8().unwrap();
            conn.execute_batch(sql).await?;
        }

        Ok(())
    }
}
```

**No new migration files needed** - Turso is SQLite-compatible for:
- Data types (TEXT, INTEGER, REAL, BLOB)
- Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
- Indexes (CREATE INDEX)
- Triggers (if we use them)

### Timestamp Handling

Turso uses same timestamp approach as SQLite:

```rust
// Reuse existing SQLite timestamp helpers
pub use crate::store::sqlite::{
    parse_sqlite_timestamp,
    format_sqlite_timestamp,
};
```

## Critical Implementation Patterns

### Message Dequeue with Turso

Turso supports the same patterns as SQLite:

```rust
// crates/pgqrs/src/store/turso/worker/consumer.rs

impl TursoConsumer {
    pub async fn dequeue(&self, count: usize) -> Result<Vec<QueueMessage>> {
        let conn = self.store.connect()?;

        // Start transaction (use CONCURRENT if MVCC enabled)
        let tx_sql = if self.store.turso_config.enable_mvcc {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN IMMEDIATE"
        };
        conn.execute(tx_sql, ()).await?;

        // Select unclaimed messages
        let mut stmt = conn.prepare(
            r#"
            SELECT id, queue_id, payload, vt, enqueued_at, read_ct,
                   dequeued_at, producer_worker_id, consumer_worker_id
            FROM pgqrs_messages
            WHERE queue_id = ?1
              AND vt <= datetime('now')
              AND consumer_worker_id IS NULL
            ORDER BY enqueued_at ASC
            LIMIT ?2
            "#
        ).await?;

        let mut rows = stmt.query([self.queue_id, count as i64]).await?;
        let mut messages = Vec::new();

        while let Some(row) = rows.next().await? {
            messages.push(QueueMessage::from_turso_row(row)?);
        }

        if messages.is_empty() {
            conn.execute("ROLLBACK", ()).await?;
            return Ok(vec![]);
        }

        // Claim messages
        for msg in &messages {
            conn.execute(
                r#"
                UPDATE pgqrs_messages
                SET consumer_worker_id = ?1,
                    dequeued_at = datetime('now'),
                    read_ct = read_ct + 1,
                    vt = datetime('now', '+' || ?2 || ' seconds')
                WHERE id = ?3
                "#,
                [self.worker_id, self.config.default_lock_time_seconds, msg.id]
            ).await?;
        }

        conn.execute("COMMIT", ()).await?;
        Ok(messages)
    }
}
```

### Async Batch Operations

Turso's async API allows efficient batch operations:

```rust
// crates/pgqrs/src/store/turso/tables/pgqrs_messages.rs

impl MessageTable for TursoMessageTable {
    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[Value],
        params: BatchInsertParams,
    ) -> Result<Vec<i64>> {
        let conn = self.store.connect()?;

        // Prepare statement once
        let mut stmt = conn.prepare(
            r#"
            INSERT INTO pgqrs_messages (queue_id, payload, producer_worker_id)
            VALUES (?1, ?2, ?3)
            RETURNING id
            "#
        ).await?;

        let mut ids = Vec::with_capacity(payloads.len());

        // Execute in transaction for atomicity
        conn.execute("BEGIN", ()).await?;

        for payload in payloads {
            let mut rows = stmt.query([
                queue_id.to_string(),
                payload.to_string(),
                params.producer_worker_id.to_string(),
            ]).await?;

            if let Some(row) = rows.next().await? {
                let id: i64 = row.get(0)?;
                ids.push(id);
            }

            stmt.reset()?; // Reset for next iteration
        }

        conn.execute("COMMIT", ()).await?;
        Ok(ids)
    }
}
```

## Implementation Plan

### Phase 1: Foundation (Week 1)

| Task | Est. | Description |
|------|------|-------------|
| 1.1 | 1h | Add `turso` crate dependency to `Cargo.toml` |
| 1.2 | 1h | Create `store/turso/mod.rs` with basic TursoStore struct |
| 1.3 | 2h | Implement `Store` trait skeleton for TursoStore |
| 1.4 | 1h | Update `AnyStore::connect()` to recognize turso:// DSNs |
| 1.5 | 2h | Add migration runner using SQLite migration files |
| 1.6 | 1h | Basic integration test with in-memory Turso |

**Milestone**: `AnyStore::connect("turso://:memory:")` returns a valid TursoStore.

### Phase 2: Table Repositories (Week 2)

| Task | Est. | Description |
|------|------|-------------|
| 2.1 | 2h | Implement `TursoQueueTable` (QueueTable trait) |
| 2.2 | 3h | Implement `TursoMessageTable` (MessageTable trait) |
| 2.3 | 2h | Implement `TursoWorkerTable` (WorkerTable trait) |
| 2.4 | 2h | Implement `TursoArchiveTable` (ArchiveTable trait) |
| 2.5 | 2h | Implement `TursoWorkflowTable` (WorkflowTable trait) |
| 2.6 | 1h | Reuse SQLite timestamp helpers |
| 2.7 | 2h | Add row mapping helpers (Turso Row → domain types) |

**Milestone**: All table CRUD operations pass unit tests with Turso.

### Phase 3: Workers (Week 3)

| Task | Est. | Description |
|------|------|-------------|
| 3.1 | 2h | Implement `TursoAdmin` (Admin trait) |
| 3.2 | 2h | Implement `TursoProducer` (Producer trait) |
| 3.3 | 4h | Implement `TursoConsumer` with async dequeue logic |
| 3.4 | 2h | Implement heartbeat and worker lifecycle |
| 3.5 | 2h | Test MVCC mode (BEGIN CONCURRENT) if enabled |

**Milestone**: Basic enqueue/dequeue cycle works end-to-end with Turso.

### Phase 4: Workflows (Week 4)

| Task | Est. | Description |
|------|------|-------------|
| 4.1 | 3h | Implement `TursoWorkflow` handle |
| 4.2 | 3h | Implement `TursoStepGuard` for durable step execution |
| 4.3 | 2h | Test workflow resume after simulated crash |

**Milestone**: Durable workflow example runs on Turso.

### Phase 5: Performance & Polish (Week 5)

| Task | Est. | Description |
|------|------|-------------|
| 5.1 | 3h | Benchmark Turso vs SQLite performance |
| 5.2 | 2h | Test io_uring backend on Linux |
| 5.3 | 2h | Test encryption at rest functionality |
| 5.4 | 2h | Add `skip_on_backend!(Turso)` for incompatible tests |
| 5.5 | 2h | Documentation: when to use Turso vs SQLite |
| 5.6 | 1h | Update README with Turso setup instructions |

**Milestone**: `make test PGQRS_TEST_BACKEND=turso` passes.

## Testing Strategy

### Unit Tests

Each table repository should have unit tests using in-memory Turso:

```rust
#[tokio::test]
async fn test_turso_queue_insert() {
    let db = turso::Builder::new_local(":memory:").build().await.unwrap();
    let store = TursoStore::new_with_database(db, &Config::default());

    // Run migrations
    store.run_migrations().await.unwrap();

    let table = TursoQueueTable::new(store.clone());
    let queue = table.insert(NewQueue { queue_name: "test".into() }).await.unwrap();

    assert_eq!(queue.queue_name, "test");
    assert!(queue.id > 0);
}
```

### Integration Tests

Reuse multi-database test infrastructure:

```rust
// crates/pgqrs/tests/turso_tests.rs

mod common;

#[tokio::test]
async fn test_turso_enqueue_dequeue() {
    skip_unless_backend!(common::TestBackend::Turso);

    let store = common::create_store("test_enqueue").await;
    let admin = store.admin(&store.config()).await.unwrap();

    admin.install().await.unwrap();
    store.queue("tasks").await.unwrap();

    let producer = store.producer("tasks", "localhost", 0, &store.config()).await.unwrap();
    producer.enqueue(&json!({"task": "hello"})).await.unwrap();

    let consumer = store.consumer("tasks", "localhost", 0, &store.config()).await.unwrap();
    let msgs = consumer.dequeue(1).await.unwrap();

    assert_eq!(msgs.len(), 1);
}
```

### Performance Benchmarks

Compare Turso vs SQLite:

```rust
#[bench]
fn bench_turso_enqueue_1000(b: &mut Bencher) {
    let rt = Runtime::new().unwrap();
    let store = rt.block_on(TursoStore::new(":memory:", &Config::default())).unwrap();

    b.iter(|| {
        for i in 0..1000 {
            rt.block_on(producer.enqueue(&json!({"id": i}))).unwrap();
        }
    });
}
```

## Usage Examples

### Basic Turso Usage

```rust
use pgqrs::{connect, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File-based Turso database
    let config = Config::from_dsn("turso://./my_queues.db")?;
    let store = connect(&config).await?;

    // Or in-memory
    let config = Config::from_dsn("turso://:memory:")?;
    let store = connect(&config).await?;

    // Check backend
    println!("Backend: {}", store.backend_name());  // "turso"

    // Use normally
    let admin = store.admin(&config).await?;
    admin.install().await?;
    store.queue("tasks").await?;

    let producer = store.producer("tasks", "localhost", 0, &config).await?;
    producer.enqueue(&serde_json::json!({"job": "process_data"})).await?;

    Ok(())
}
```

### With MVCC Enabled

```rust
let mut config = Config::from_dsn("turso://./queues.db")?;
config.turso_config = Some(TursoStoreConfig {
    enable_mvcc: true,  // Experimental concurrent writes
    io_backend: None,
    encryption_key: None,
});

let store = connect(&config).await?;
// Multiple consumers can now dequeue concurrently
```

### With io_uring (Linux)

```rust
let mut config = Config::from_dsn("turso://./queues.db")?;
config.turso_config = Some(TursoStoreConfig {
    enable_mvcc: false,
    io_backend: Some(TursoIoBackend::IoUring),  // High-performance async I/O
    encryption_key: None,
});

let store = connect(&config).await?;
```

### With Encryption

```rust
let mut config = Config::from_dsn("turso://./encrypted.db")?;
config.turso_config = Some(TursoStoreConfig {
    enable_mvcc: false,
    io_backend: None,
    encryption_key: Some("0123456789abcdef...".to_string()),  // Hex key
});

let store = connect(&config).await?;
```

## Limitations & Considerations

| Limitation | Details | Workaround |
|------------|---------|------------|
| **BETA Status** | Turso is under active development | Use SQLite for production-critical systems |
| **MVCC Experimental** | `BEGIN CONCURRENT` may have bugs | Default to standard SQLite semantics |
| **No network protocol** | Turso is in-process only | Use PostgreSQL for distributed systems |
| **io_uring Linux-only** | High-performance I/O limited to Linux | Falls back to syscall on other platforms |
| **Encryption experimental** | May impact performance | Test thoroughly before production use |

## Feature Comparison Matrix

| Feature | PostgreSQL | SQLite (sqlx) | Turso (native) |
|---------|-----------|---------------|----------------|
| **Async I/O** | Native | Blocking + executor | Native (io_uring) |
| **Concurrent writes** | ✅ Row-level locks | ❌ Single writer | ⚠️ Experimental MVCC |
| **FFI overhead** | libpq (C) | SQLite (C) | ✅ Zero (pure Rust) |
| **Network access** | ✅ TCP/Unix | ❌ File-only | ❌ File-only |
| **Encryption** | ✅ TLS + pgcrypto | ❌ Not standard | ⚠️ Experimental |
| **Performance** | High (clustered) | Medium (single-node) | **High (async + io_uring)** |
| **Maturity** | ✅ Production | ✅ Production | ⚠️ BETA |

## When to Use Each Backend

### Use PostgreSQL when:
- Multi-process or distributed workers required
- High write concurrency (1000s of msg/sec)
- Network-accessible queue needed
- Full ACID across multiple servers

### Use SQLite when:
- Maximum compatibility required
- Embedding in applications (C FFI available)
- Proven production stability critical
- No async runtime (blocking I/O acceptable)

### Use Turso when:
- **Pure Rust stack** with async/await
- **Edge deployments** with io_uring (Linux)
- **Performance-critical local queues**
- **Modern Rust ecosystem integration**
- Willing to accept BETA status risks

## Relationship to GitHub Issues

### Issue #108: Turso Support
This design doc addresses the core Turso integration tracked in https://github.com/vrajat/pgqrs/issues/108.

### Issues #109-113: SQLite Foundation Work
Issues #109, #110, #111, and #113 likely covered SQLite-specific implementation tasks such as:
- Table trait implementations
- Migration creation
- Worker implementations
- Testing infrastructure

**Analysis**: These do NOT need separate Turso issues because:

1. **Shared Migrations**: Turso reuses SQLite migrations (SQLite-compatible DDL)
2. **Similar Patterns**: TursoStore follows same architectural patterns as SqliteStore
3. **Trait Implementations**: Same Store/Table/Worker traits, different backend
4. **Test Infrastructure**: Multi-backend test framework already exists

**Recommendation**: Close #109-113 as "completed for SQLite" and track Turso-specific work under #108 only. Any Turso-unique issues (MVCC, io_uring, encryption) should be tracked as sub-tasks of #108 or new issues.

## Open Questions

1. **Should we make MVCC the default once Turso stabilizes it?**
   - Pro: Better concurrent write performance
   - Con: Different transaction semantics than PostgreSQL/SQLite
   - Recommendation: Keep disabled by default, opt-in via config

2. **Should Turso replace SQLite backend long-term?**
   - Pro: Pure Rust, better performance, native async
   - Con: Turso still BETA, SQLite has decades of production use
   - Recommendation: Keep both backends for now, revisit after Turso 1.0

3. **How to handle Turso version compatibility?**
   - Turso is evolving rapidly (v0.4.2 as of 2026-01-07)
   - Recommendation: Pin to specific Turso crate version, test on upgrades

4. **Should we support Turso's experimental features by default?**
   - MVCC, encryption, incremental computation
   - Recommendation: All experimental features opt-in via explicit config

## Next Steps

1. **Review and approve this design** with pgqrs maintainers
2. **Confirm GitHub issue strategy**: #108 only, or separate issues for Turso components?
3. **Begin Phase 1 implementation**: Foundation work (TursoStore struct, migrations, AnyStore integration)
4. **Set up CI matrix** to test Turso backend alongside PostgreSQL and SQLite
5. **Document performance benchmarks** once implementation complete

## Appendix: File Structure Checklist

```
crates/pgqrs/
├── Cargo.toml                          # Add turso crate dependency
├── migrations/
│   ├── postgres/                       # Existing
│   └── sqlite/                         # REUSED for Turso (SQLite-compatible)
└── src/
    ├── config.rs                       # Add TursoStoreConfig
    └── store/
        ├── mod.rs                      # Add `pub mod turso;`
        ├── any.rs                      # Add Turso variant, turso:// DSN detection
        └── turso/                      # NEW
            ├── mod.rs                  # TursoStore, Store impl
            ├── tables/
            │   ├── mod.rs
            │   ├── pgqrs_queues.rs
            │   ├── pgqrs_messages.rs
            │   ├── pgqrs_workers.rs
            │   ├── pgqrs_archive.rs
            │   └── pgqrs_workflows.rs
            ├── worker/
            │   ├── mod.rs
            │   ├── admin.rs
            │   ├── producer.rs
            │   └── consumer.rs
            └── workflow/
                ├── mod.rs
                ├── handle.rs
                └── guard.rs
```
