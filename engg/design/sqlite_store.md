# Design Doc: SqliteStore Implementation

## Objective

Add SQLite as a storage backend for pgqrs, enabling local-first, serverless queue and workflow operations. SQLite support targets single-process deployments where simplicity and zero-ops are prioritized over horizontal scalability.

## Background

pgqrs currently supports PostgreSQL as its only storage backend. The architecture is designed for extensibility via the `Store` trait and `AnyStore` enum. Adding SQLite enables:

- **Local development** without running PostgreSQL
- **Embedded applications** that need durable queues
- **Edge deployments** where a full database server is impractical
- **Testing** with lightweight, disposable databases

## Design Decisions

### Connection Model

**Question**: Should every producer and consumer get its own connection? Does the SQLite driver support multiple open connections?

**Answer**: SQLite and sqlx support multiple connections via `SqlitePool`, but with critical limitations:

| Aspect | PostgreSQL | SQLite |
|--------|------------|--------|
| Concurrent writers | Yes (row-level locks) | No (single writer) |
| Concurrent readers | Yes | Yes (with WAL mode) |
| Connection pool | Recommended (16+ connections) | Limited benefit (1-4 connections) |
| Cross-process access | Fully supported | Supported but risky |

**Design Decision**: Use a shared `SqlitePool` with a small connection pool (max 4 connections):

```rust
pub struct SqliteStore {
    pool: SqlitePool,  // Shared across all workers in-process
    // ...
}
```

- **Writers** will serialize automatically due to SQLite's locking
- **Readers** can proceed concurrently (visibility timeout checks, status queries)
- **WAL mode** is enabled for better read concurrency during writes

### Write Serialization

**Question**: All workers are writers. So SQLite will serialize all access?

**Answer**: Yes. Every producer (enqueue), consumer (dequeue, ack, heartbeat), and admin operation involves writes. SQLite will serialize these.

**Implications**:

1. **No SKIP LOCKED**: PostgreSQL's `FOR UPDATE SKIP LOCKED` allows multiple consumers to grab different messages concurrently. SQLite must use `BEGIN IMMEDIATE` which blocks other writers.

2. **Throughput ceiling**: A single SQLite database will max out at ~50-100K writes/second on modern SSDs. For pgqrs workloads (enqueue + dequeue + heartbeat), expect ~10-30K messages/second throughput.

3. **No worker contention benefits**: Multiple consumers don't improve throughput—they just queue up for the write lock.

**Design Decision**: Accept serialization as a fundamental characteristic. Document that SQLite backend is for:
- Low-to-medium throughput workloads (< 10K msg/sec)
- Single-consumer patterns (or few consumers)
- Development/testing environments

### Single-Process Enforcement

**Question**: SQLite should be supported in a single process. Is there a way to enforce or check for that?

**Answer**: SQLite *can* work across processes (using file locks), but it's problematic:
- File lock contention across processes is slow
- Crashes can leave stale locks
- No visibility into other processes' intent

**Design Decision**: Enforce single-process usage through architecture, not runtime checks:

1. **Shared pool via `Arc<SqliteStore>`**: The `SqliteStore` is designed to be cloned (it wraps `Arc` internally). All producers/consumers in a process share the same pool.

2. **`ConcurrencyModel::SingleProcess` signal**: The `Store::concurrency_model()` method returns `SingleProcess` for SQLite. User code can check this:

```rust
match store.concurrency_model() {
    ConcurrencyModel::MultiProcess => {
        // Safe to spawn multiple processes
    }
    ConcurrencyModel::SingleProcess => {
        // Keep all workers in this process
    }
}
```

3. **Exclusive locking mode (optional)**: SQLite supports `PRAGMA locking_mode=EXCLUSIVE` which holds the lock for the connection lifetime. This prevents other processes from connecting but improves performance.

4. **Documentation**: Clearly document that SQLite backend assumes single-process deployment.

**Runtime detection (optional enhancement)**:
```rust
impl SqliteStore {
    /// Check if another process has the database open
    /// Returns error if multi-process access detected
    pub async fn verify_exclusive_access(&self) -> Result<()> {
        // Attempt to set exclusive locking mode
        // If another process holds a lock, this will fail
        sqlx::query("PRAGMA locking_mode=EXCLUSIVE")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
```

## Architecture

### Module Structure

```
crates/pgqrs/src/store/
├── mod.rs              # Exports Store trait, AnyStore
├── any.rs              # AnyStore enum with Postgres + Sqlite variants
├── postgres/           # Existing PostgreSQL implementation
│   ├── mod.rs
│   ├── tables/
│   ├── worker/
│   └── workflow/
└── sqlite/             # NEW: SQLite implementation
    ├── mod.rs          # SqliteStore struct, Store trait impl
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

### SqliteStore Struct

```rust
use sqlx::{SqlitePool, Sqlite};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    config: Config,
    queues: Arc<SqliteQueueTable>,
    messages: Arc<SqliteMessageTable>,
    workers: Arc<SqliteWorkerTable>,
    archive: Arc<SqliteArchiveTable>,
    workflows: Arc<SqliteWorkflowTable>,
}

impl SqliteStore {
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)  // Limited pool for SQLite
            .after_connect(|conn, _meta| Box::pin(async move {
                // Enable WAL mode for better concurrency
                sqlx::query("PRAGMA journal_mode=WAL").execute(conn).await?;
                // Reasonable busy timeout (5 seconds)
                sqlx::query("PRAGMA busy_timeout=5000").execute(conn).await?;
                Ok(())
            }))
            .connect(dsn)
            .await?;
        
        Ok(Self {
            pool: pool.clone(),
            config: config.clone(),
            queues: Arc::new(SqliteQueueTable::new(pool.clone())),
            messages: Arc::new(SqliteMessageTable::new(pool.clone())),
            workers: Arc::new(SqliteWorkerTable::new(pool.clone())),
            archive: Arc::new(SqliteArchiveTable::new(pool.clone())),
            workflows: Arc::new(SqliteWorkflowTable::new(pool)),
        })
    }
}

#[async_trait]
impl Store for SqliteStore {
    type Db = Sqlite;

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }

    fn backend_name(&self) -> &'static str {
        "sqlite"
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
}

impl AnyStore {
    pub async fn connect(config: &Config) -> Result<Self> {
        if config.dsn.starts_with("postgres://") || config.dsn.starts_with("postgresql://") {
            let pool = /* existing postgres connection logic */;
            Ok(AnyStore::Postgres(PostgresStore::new(pool, config)))
        } else if config.dsn.starts_with("sqlite://") || config.dsn.starts_with("sqlite:") {
            let store = SqliteStore::new(&config.dsn, config).await?;
            Ok(AnyStore::Sqlite(store))
        } else {
            Err(Error::InvalidConfig {
                message: format!("Unsupported DSN scheme: {}", config.dsn),
            })
        }
    }
}
```

## SQL Dialect Mapping

### PostgreSQL to SQLite Translation

| PostgreSQL | SQLite | Notes |
|------------|--------|-------|
| `BIGSERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` | SQLite rowid aliasing |
| `VARCHAR` | `TEXT` | SQLite has no varchar limit |
| `TIMESTAMPTZ` | `TEXT` | Store as ISO8601 UTC strings |
| `JSONB` | `TEXT` | Store as JSON strings |
| `NOW()` | `datetime('now')` | SQLite datetime function |
| `ENUM type` | `TEXT CHECK(col IN (...))` | No native enums |
| `make_interval(secs => $1)` | `datetime(col, '+' \|\| $1 \|\| ' seconds')` | String concatenation |
| `$1::type` | `?` / `CAST(? AS type)` | Positional params, explicit casts |
| `unnest($1::jsonb[])` | Loop or multi-row VALUES | No array unnesting |
| `FOR UPDATE SKIP LOCKED` | `BEGIN IMMEDIATE` + app logic | See dequeue pattern below |
| `RETURNING *` | `RETURNING *` | Supported in SQLite 3.35+ |

### Migrations

Create `crates/pgqrs/migrations/sqlite/` with dialect-specific SQL:

**`01_create_queues.sql`**:
```sql
CREATE TABLE IF NOT EXISTS pgqrs_queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT UNIQUE NOT NULL,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);
```

**`02_create_workers.sql`**:
```sql
CREATE TABLE IF NOT EXISTS pgqrs_workers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    hostname TEXT NOT NULL,
    port INTEGER NOT NULL,
    status TEXT DEFAULT 'ready' NOT NULL 
        CHECK(status IN ('ready', 'suspended', 'stopped')),
    last_heartbeat TEXT DEFAULT (datetime('now')) NOT NULL,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workers_queue_id ON pgqrs_workers(queue_id);
CREATE INDEX IF NOT EXISTS idx_workers_status ON pgqrs_workers(status);
```

**`03_create_messages.sql`**:
```sql
CREATE TABLE IF NOT EXISTS pgqrs_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,  -- JSON stored as TEXT
    vt TEXT DEFAULT (datetime('now')),  -- visibility timeout
    enqueued_at TEXT DEFAULT (datetime('now')),
    read_ct INTEGER DEFAULT 0,
    dequeued_at TEXT,
    producer_worker_id INTEGER REFERENCES pgqrs_workers(id),
    consumer_worker_id INTEGER REFERENCES pgqrs_workers(id)
);

CREATE INDEX IF NOT EXISTS idx_messages_queue_vt ON pgqrs_messages(queue_id, vt);
CREATE INDEX IF NOT EXISTS idx_messages_consumer ON pgqrs_messages(consumer_worker_id);
```

**`04_create_archive.sql`**:
```sql
CREATE TABLE IF NOT EXISTS pgqrs_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_message_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    archived_at TEXT DEFAULT (datetime('now')) NOT NULL,
    read_ct INTEGER NOT NULL,
    archive_reason TEXT NOT NULL 
        CHECK(archive_reason IN ('completed', 'failed', 'expired', 'manual')),
    producer_worker_id INTEGER,
    consumer_worker_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_archive_queue_id ON pgqrs_archive(queue_id);
CREATE INDEX IF NOT EXISTS idx_archive_reason ON pgqrs_archive(archive_reason);
```

**`05_create_workflows.sql`**:
```sql
CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    input TEXT NOT NULL,  -- JSON
    status TEXT DEFAULT 'pending' NOT NULL
        CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL
);

CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES pgqrs_workflows(id),
    step_id TEXT NOT NULL,
    status TEXT DEFAULT 'pending' NOT NULL
        CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE(workflow_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON pgqrs_workflow_steps(workflow_id);
```

## Critical Implementation Patterns

### Message Dequeue (No SKIP LOCKED)

PostgreSQL uses `FOR UPDATE SKIP LOCKED` for concurrent consumers. SQLite requires a different approach:

```rust
// crates/pgqrs/src/store/sqlite/worker/consumer.rs

impl SqliteConsumer {
    pub async fn dequeue(&self, count: usize) -> Result<Vec<QueueMessage>> {
        // BEGIN IMMEDIATE acquires RESERVED lock, blocking other writers
        let mut tx = self.pool.begin().await?;
        
        // SQLite doesn't support SKIP LOCKED, so we:
        // 1. Select unclaimed messages (consumer_worker_id IS NULL)
        // 2. Update them atomically in the same transaction
        let messages: Vec<SqliteMessageRow> = sqlx::query_as(
            r#"
            SELECT id, queue_id, payload, vt, enqueued_at, read_ct, 
                   dequeued_at, producer_worker_id, consumer_worker_id
            FROM pgqrs_messages
            WHERE queue_id = ?
              AND vt <= datetime('now')
              AND consumer_worker_id IS NULL
            ORDER BY id
            LIMIT ?
            "#
        )
        .bind(self.queue_id)
        .bind(count as i64)
        .fetch_all(&mut *tx)
        .await?;

        if messages.is_empty() {
            tx.rollback().await?;
            return Ok(vec![]);
        }

        // Claim the messages
        let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        
        let claim_sql = format!(
            r#"
            UPDATE pgqrs_messages 
            SET consumer_worker_id = ?,
                dequeued_at = datetime('now'),
                read_ct = read_ct + 1,
                vt = datetime('now', '+{} seconds')
            WHERE id IN ({})
            "#,
            self.config.default_lock_time_seconds,
            placeholders
        );
        
        let mut query = sqlx::query(&claim_sql).bind(self.worker_id);
        for id in &ids {
            query = query.bind(*id);
        }
        query.execute(&mut *tx).await?;

        tx.commit().await?;
        
        Ok(messages.into_iter().map(Into::into).collect())
    }
}
```

### Timestamp Handling

SQLite stores timestamps as TEXT. Create helper functions:

```rust
// crates/pgqrs/src/store/sqlite/mod.rs

use chrono::{DateTime, Utc};

/// Parse SQLite TEXT timestamp to DateTime<Utc>
pub fn parse_sqlite_timestamp(s: &str) -> Result<DateTime<Utc>> {
    // SQLite datetime() returns "YYYY-MM-DD HH:MM:SS" format
    DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::Internal { 
            message: format!("Invalid timestamp: {}", e) 
        })
}

/// Format DateTime<Utc> for SQLite TEXT storage
pub fn format_sqlite_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}
```

### Batch Insert (No unnest)

```rust
// crates/pgqrs/src/store/sqlite/tables/pgqrs_messages.rs

impl MessageTable for SqliteMessageTable {
    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[Value],
        params: BatchInsertParams,
    ) -> Result<Vec<i64>> {
        if payloads.is_empty() {
            return Ok(vec![]);
        }

        let mut tx = self.pool.begin().await?;
        let mut ids = Vec::with_capacity(payloads.len());

        for payload in payloads {
            let id: i64 = sqlx::query_scalar(
                r#"
                INSERT INTO pgqrs_messages (queue_id, payload, producer_worker_id)
                VALUES (?, ?, ?)
                RETURNING id
                "#
            )
            .bind(queue_id)
            .bind(payload.to_string())
            .bind(params.producer_worker_id)
            .fetch_one(&mut *tx)
            .await?;
            
            ids.push(id);
        }

        tx.commit().await?;
        Ok(ids)
    }
}
```

**Performance note**: For large batches, consider multi-row VALUES:

```rust
// For batches > 100, build multi-row insert
let values_clause = payloads.iter()
    .map(|_| "(?, ?, ?)")
    .collect::<Vec<_>>()
    .join(", ");

let sql = format!(
    "INSERT INTO pgqrs_messages (queue_id, payload, producer_worker_id) VALUES {}",
    values_clause
);
// ... bind all values
```

## Implementation Plan

### Phase 1: Foundation (Week 1)

| Task | Est. | Description |
|------|------|-------------|
| 1.1 | 30m | Add `"sqlite"` to sqlx features in `Cargo.toml` |
| 1.2 | 2h | Create SQLite migrations in `migrations/sqlite/` |
| 1.3 | 2h | Implement `SqliteStore` struct and `Store` trait skeleton |
| 1.4 | 1h | Update `AnyStore::connect()` for sqlite:// DSNs |
| 1.5 | 1h | Add basic integration test with in-memory SQLite |

**Milestone**: `AnyStore::connect("sqlite::memory:")` returns a valid store.

### Phase 2: Table Repositories (Week 2)

| Task | Est. | Description |
|------|------|-------------|
| 2.1 | 2h | Implement `SqliteQueueTable` (QueueTable trait) |
| 2.2 | 3h | Implement `SqliteMessageTable` (MessageTable trait) |
| 2.3 | 2h | Implement `SqliteWorkerTable` (WorkerTable trait) |
| 2.4 | 2h | Implement `SqliteArchiveTable` (ArchiveTable trait) |
| 2.5 | 2h | Implement `SqliteWorkflowTable` (WorkflowTable trait) |
| 2.6 | 2h | Add timestamp and JSON helper functions |

**Milestone**: All table CRUD operations pass unit tests.

### Phase 3: Workers (Week 3)

| Task | Est. | Description |
|------|------|-------------|
| 3.1 | 2h | Implement `SqliteAdmin` (Admin trait) |
| 3.2 | 2h | Implement `SqliteProducer` (Producer trait) |
| 3.3 | 4h | Implement `SqliteConsumer` with dequeue logic (Consumer trait) |
| 3.4 | 2h | Implement heartbeat and worker lifecycle |

**Milestone**: Basic enqueue/dequeue cycle works end-to-end.

### Phase 4: Workflows (Week 4)

| Task | Est. | Description |
|------|------|-------------|
| 4.1 | 3h | Implement `SqliteWorkflow` handle |
| 4.2 | 3h | Implement `SqliteStepGuard` for durable step execution |
| 4.3 | 2h | Test workflow resume after simulated crash |

**Milestone**: Durable workflow example runs on SQLite.

### Phase 5: Testing & Polish (Week 5)

| Task | Est. | Description |
|------|------|-------------|
| 5.1 | 3h | Implement multi-database test infrastructure per design doc |
| 5.2 | 2h | Run full test suite against SQLite, fix failures |
| 5.3 | 2h | Add `skip_on_backend!(Sqlite)` for concurrency tests |
| 5.4 | 2h | Performance benchmarking and documentation |
| 5.5 | 1h | Update README and user guide with SQLite instructions |

**Milestone**: `make test PGQRS_TEST_BACKEND=sqlite` passes.

## Testing Strategy

### Unit Tests

Each table repository should have unit tests using in-memory SQLite:

```rust
#[tokio::test]
async fn test_sqlite_queue_insert() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::migrate!("../migrations/sqlite").run(&pool).await.unwrap();
    
    let table = SqliteQueueTable::new(pool);
    let queue = table.insert(NewQueue { queue_name: "test".into() }).await.unwrap();
    
    assert_eq!(queue.queue_name, "test");
    assert!(queue.id > 0);
}
```

### Integration Tests

Use the multi-database test infrastructure:

```rust
// crates/pgqrs/tests/sqlite_tests.rs

mod common;

#[tokio::test]
async fn test_sqlite_enqueue_dequeue() {
    skip_unless_backend!(common::TestBackend::Sqlite);
    
    let store = common::create_store("test_enqueue").await;
    let admin = store.admin(&store.config()).await.unwrap();
    
    admin.install().await.unwrap();
    admin.create_queue("tasks").await.unwrap();
    
    let producer = store.producer("tasks", "localhost", 0, &store.config()).await.unwrap();
    producer.enqueue(&json!({"task": "hello"})).await.unwrap();
    
    let consumer = store.consumer("tasks", "localhost", 0, &store.config()).await.unwrap();
    let msgs = consumer.dequeue(1).await.unwrap();
    
    assert_eq!(msgs.len(), 1);
}
```

### Tests to Skip on SQLite

```rust
#[tokio::test]
async fn test_concurrent_consumers() {
    // SQLite serializes all writes, so concurrent consumer test is meaningless
    skip_on_backend!(common::TestBackend::Sqlite);
    // ... test multiple consumers grabbing different messages
}

#[tokio::test] 
async fn test_multi_process_workers() {
    // SQLite is single-process only
    skip_on_backend!(common::TestBackend::Sqlite);
    // ... test workers in separate processes
}
```

## Usage Examples

### Basic SQLite Usage

```rust
use pgqrs::{connect, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File-based SQLite database
    let config = Config::from_dsn("sqlite://./my_queues.db")?;
    let store = connect(&config).await?;
    
    // Or in-memory (for testing)
    let config = Config::from_dsn("sqlite::memory:")?;
    let store = connect(&config).await?;
    
    // Check backend
    println!("Backend: {}", store.backend_name());  // "sqlite"
    println!("Concurrency: {:?}", store.concurrency_model());  // SingleProcess
    
    // Use normally - all producers/consumers share the same connection pool
    let admin = store.admin(&config).await?;
    admin.install().await?;
    admin.create_queue("tasks").await?;
    
    let producer = store.producer("tasks", "localhost", 0, &config).await?;
    producer.enqueue(&serde_json::json!({"job": "process_data"})).await?;
    
    Ok(())
}
```

### Python Usage

```python
import pgqrs

# SQLite backend
store = await pgqrs.connect("sqlite://./queues.db")

# Check we're in single-process mode
assert store.concurrency_model == "single_process"

async with store.admin() as admin:
    await admin.install()
    await admin.create_queue("tasks")

async with store.producer("tasks") as producer:
    await producer.enqueue({"job": "hello"})

async with store.consumer("tasks") as consumer:
    messages = await consumer.dequeue(10)
    for msg in messages:
        print(f"Processing: {msg.payload}")
        await consumer.ack(msg.id)
```

## Limitations & Documentation

Document these limitations clearly:

| Limitation | Reason | Workaround |
|------------|--------|------------|
| Single-process only | SQLite file locking is unreliable across processes | Use PostgreSQL for multi-process |
| ~10K msg/sec ceiling | Single-writer serialization | Use PostgreSQL for high throughput |
| No SKIP LOCKED | SQLite limitation | Accept serialized dequeue |
| No schema isolation | SQLite has no schemas | Use separate database files |
| 64-bit integers only | SQLite INTEGER is 64-bit signed | Same as PostgreSQL BIGINT |

## Open Questions

1. **Should we support `sqlite::memory:` with shared cache for testing multi-connection scenarios?**
   - `sqlite:file::memory:?cache=shared` allows multiple connections to same in-memory DB
   - Useful for testing but not production

2. **Should migrations auto-run on connect?**
   - PostgreSQL: User calls `admin.install()`
   - SQLite: Could auto-migrate since it's single-user
   - Recommendation: Keep consistent behavior, require explicit `install()`

3. **WAL mode always or configurable?**
   - WAL is better for read concurrency but uses more disk space
   - Recommendation: Default to WAL, allow `PGQRS_SQLITE_JOURNAL_MODE` env var override

## Appendix: File Structure Checklist

```
crates/pgqrs/
├── Cargo.toml                          # Add "sqlite" to sqlx features
├── migrations/
│   ├── postgres/                       # Existing
│   └── sqlite/                         # NEW
│       ├── 01_create_queues.sql
│       ├── 02_create_workers.sql
│       ├── 03_create_messages.sql
│       ├── 04_create_archive.sql
│       └── 05_create_workflows.sql
└── src/
    └── store/
        ├── mod.rs                      # Add `pub mod sqlite;`
        ├── any.rs                      # Add Sqlite variant
        └── sqlite/                     # NEW
            ├── mod.rs                  # SqliteStore, Store impl
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
