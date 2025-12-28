# Database Abstraction Layer - Design Document

**Status:** Draft
**Author:** pgqrs team
**Date:** 2024-12-28
**Epic:** #112 (Support Multiple Database Backends)

---

## 1. Overview

This document specifies the design for supporting multiple database backends (PostgreSQL, SQLite, Turso) in pgqrs. The design prioritizes:

1. **Clean public API** - Users interact with concrete types (`Admin`, `Producer`, `Consumer`), not generics
2. **DSN-based backend selection** - Database chosen at runtime via connection string
3. **Internal extensibility** - New backends can be added without changing public API
4. **Encapsulated complexity** - Transactions, locking strategies, and DB-specific SQL are hidden

---

## 2. Terminology

| Term | Definition |
|------|------------|
| **Store** | Top-level trait providing access to tables and cross-table operations |
| **Table** | Trait for single-table operations (CRUD + business logic) |
| **AnyStore** | Enum wrapper that hides concrete store implementations from users |
| **Backend** | A specific database implementation (Postgres, SQLite, Turso) |

---

## 3. Architecture

### 3.1 Layer Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PUBLIC API LAYER                                │
│                                                                         │
│   Admin              Producer            Consumer           Workflow    │
│   - install()        - enqueue()         - dequeue()        - run()     │
│   - verify()         - batch_enqueue()   - archive()        - step()    │
│   - create_queue()   - replay_dlq()      - release()                    │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  These types are NOT generic. Users create them via:            │   │
│   │    Admin::new(&config).await                                    │   │
│   │    Producer::new(&config, "queue_name").await                   │   │
│   └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          ANYSTORE LAYER                                 │
│                                                                         │
│   pub enum AnyStore {                                                   │
│       Postgres(PostgresStore),                                          │
│       #[cfg(feature = "sqlite")]                                        │
│       Sqlite(SqliteStore),                                              │
│       #[cfg(feature = "turso")]                                         │
│       Turso(TursoStore),                                                │
│   }                                                                     │
│                                                                         │
│   - Implements Store trait via delegation                               │
│   - Created from DSN: AnyStore::connect("postgres://...").await         │
│   - Hides concrete backend type from callers                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           STORE LAYER                                   │
│                                                                         │
│   trait Store {                                                         │
│       // Table accessors                                                │
│       fn queues(&self) -> &dyn QueueTable;                              │
│       fn messages(&self) -> &dyn MessageTable;                          │
│       fn workers(&self) -> &dyn WorkerTable;                            │
│       fn archive(&self) -> &dyn ArchiveTable;                           │
│       fn workflows(&self) -> &dyn WorkflowTable;                        │
│                                                                         │
│       // Cross-table operations (transactions managed internally)       │
│       async fn install(&self) -> Result<()>;                            │
│       async fn verify(&self) -> Result<()>;                             │
│       async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>>; │
│       // ... more cross-table operations                                │
│   }                                                                     │
│                                                                         │
│   Implementations: PostgresStore, SqliteStore, TursoStore               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           TABLE LAYER                                   │
│                                                                         │
│   trait QueueTable: Table { ... }                                       │
│   trait MessageTable: Table { ... }                                     │
│   trait WorkerTable: Table { ... }                                      │
│   trait ArchiveTable: Table { ... }                                     │
│   trait WorkflowTable: Table { ... }                                    │
│                                                                         │
│   Each contains:                                                        │
│   - CRUD operations (get, insert, delete, list)                         │
│   - Single-table business operations (dequeue_atomic, etc.)             │
│                                                                         │
│   Implementations: PostgresQueueTable, SqliteQueueTable, etc.           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Module Structure

```
src/
├── store/
│   ├── mod.rs              # Store trait, Table traits, AnyStore enum
│   ├── error.rs            # Store-specific error types
│   ├── postgres/
│   │   ├── mod.rs          # PostgresStore implementation
│   │   ├── queues.rs       # PostgresQueueTable
│   │   ├── messages.rs     # PostgresMessageTable
│   │   ├── workers.rs      # PostgresWorkerTable
│   │   ├── archive.rs      # PostgresArchiveTable
│   │   └── workflows.rs    # PostgresWorkflowTable
│   ├── sqlite/             # (feature = "sqlite")
│   │   ├── mod.rs          # SqliteStore implementation
│   │   ├── queues.rs       # SqliteQueueTable
│   │   ├── messages.rs     # SqliteMessageTable
│   │   ├── workers.rs      # SqliteWorkerTable
│   │   ├── archive.rs      # SqliteArchiveTable
│   │   └── workflows.rs    # SqliteWorkflowTable
│   └── turso/              # (feature = "turso")
│       └── ...
├── worker/
│   ├── mod.rs
│   ├── admin.rs            # Admin (non-generic, uses AnyStore)
│   ├── producer.rs         # Producer (non-generic, uses AnyStore)
│   ├── consumer.rs         # Consumer (non-generic, uses AnyStore)
│   └── lifecycle.rs        # WorkerLifecycle (non-generic)
└── workflow/
    ├── mod.rs              # Workflow (non-generic, uses AnyStore)
    └── ...
```

---

## 5. Trait Definitions

### 5.1 Base Table Trait

```rust
// src/store/mod.rs

use async_trait::async_trait;

/// Base trait for all table implementations.
/// Provides common functionality and marks a type as a table.
pub trait Table: Send + Sync + 'static {
    /// The name of the underlying database table
    fn table_name(&self) -> &'static str;
}
```

### 5.2 QueueTable Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait QueueTable: Table {
    // === CRUD Operations ===

    /// Get queue by name
    async fn get_by_name(&self, name: &str) -> Result<QueueInfo>;

    /// Get queue by ID
    async fn get_by_id(&self, id: i64) -> Result<QueueInfo>;

    /// Create a new queue
    async fn create(&self, name: &str) -> Result<QueueInfo>;

    /// Check if queue exists
    async fn exists(&self, name: &str) -> Result<bool>;

    /// Delete queue by name (returns number of deleted rows)
    async fn delete(&self, name: &str) -> Result<u64>;

    /// List all queues
    async fn list(&self) -> Result<Vec<QueueInfo>>;

    // === Business Operations ===

    /// Get queue metrics (message counts, etc.)
    async fn get_metrics(&self, queue_id: i64) -> Result<QueueMetrics>;

    /// Purge all messages from a queue (returns count purged)
    async fn purge(&self, queue_id: i64) -> Result<u64>;
}
```

### 5.3 MessageTable Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait MessageTable: Table {
    // === CRUD Operations ===

    /// Get message by ID
    async fn get(&self, id: i64) -> Result<QueueMessage>;

    /// Insert a single message
    async fn insert(
        &self,
        queue_id: i64,
        worker_id: i64,
        payload: &Value,
        delay_seconds: Option<u32>,
    ) -> Result<i64>;

    /// Insert multiple messages (batch)
    async fn insert_batch(
        &self,
        queue_id: i64,
        worker_id: i64,
        payloads: &[Value],
    ) -> Result<Vec<i64>>;

    /// Delete message by ID
    async fn delete(&self, id: i64) -> Result<bool>;

    /// Delete multiple messages
    async fn delete_batch(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>>;

    // === Business Operations ===

    /// Atomically dequeue messages for processing.
    ///
    /// Implementation notes:
    /// - PostgreSQL: Uses `FOR UPDATE SKIP LOCKED`
    /// - SQLite: Uses claiming pattern with explicit transaction
    /// - Turso: Uses claiming pattern
    ///
    /// This method handles all locking internally.
    async fn dequeue_atomic(
        &self,
        queue_id: i64,
        worker_id: i64,
        limit: usize,
        vt_seconds: u32,
        max_read_ct: u32,
    ) -> Result<Vec<QueueMessage>>;

    /// Extend visibility timeout for a message
    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<bool>;

    /// Release message back to queue (make immediately available)
    async fn release(&self, id: i64, worker_id: i64) -> Result<bool>;

    /// Release multiple messages
    async fn release_batch(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>>;

    /// Count active messages by worker
    async fn count_by_worker(&self, worker_id: i64) -> Result<i64>;
}
```

### 5.4 WorkerTable Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait WorkerTable: Table {
    // === CRUD Operations ===

    /// Register a new worker
    async fn register(
        &self,
        queue_id: Option<i64>,
        hostname: &str,
        port: i32,
    ) -> Result<WorkerInfo>;

    /// Get worker by ID
    async fn get(&self, worker_id: i64) -> Result<WorkerInfo>;

    /// Get worker status
    async fn get_status(&self, worker_id: i64) -> Result<WorkerStatus>;

    /// List workers (optionally filtered by queue)
    async fn list(&self, queue_id: Option<i64>) -> Result<Vec<WorkerInfo>>;

    // === Business Operations ===

    /// Send heartbeat for worker
    async fn heartbeat(&self, worker_id: i64) -> Result<()>;

    /// Check if worker is healthy (heartbeat within timeout)
    async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool>;

    /// Suspend worker (atomic state transition: Ready -> Suspended)
    async fn suspend(&self, worker_id: i64) -> Result<()>;

    /// Resume worker (atomic state transition: Suspended -> Ready)
    async fn resume(&self, worker_id: i64) -> Result<()>;

    /// Shutdown worker (atomic state transition: * -> Stopped)
    async fn shutdown(&self, worker_id: i64) -> Result<()>;

    /// Get health statistics for workers
    async fn health_stats(&self, timeout: Duration) -> Result<WorkerHealthStats>;
}
```

### 5.5 ArchiveTable Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait ArchiveTable: Table {
    // === CRUD Operations ===

    /// Get archived message by ID
    async fn get(&self, id: i64) -> Result<ArchivedMessage>;

    /// List archived messages (paginated)
    async fn list(&self, limit: i64, offset: i64) -> Result<Vec<ArchivedMessage>>;

    /// Count total archived messages
    async fn count(&self) -> Result<i64>;

    // === Business Operations ===

    /// Archive a single message atomically (delete from messages + insert here)
    /// Returns None if message not found or not owned by worker
    async fn archive_message(
        &self,
        msg_id: i64,
        worker_id: i64,
    ) -> Result<Option<ArchivedMessage>>;

    /// Archive multiple messages atomically
    async fn archive_batch(
        &self,
        msg_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<i64>>;

    /// List DLQ messages (messages exceeding max attempts)
    async fn list_dlq(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>>;

    /// Count DLQ messages
    async fn count_dlq(&self, max_attempts: i32) -> Result<i64>;

    /// List archived messages by worker
    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>>;
}
```

### 5.6 WorkflowTable Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait WorkflowTable: Table {
    // === CRUD Operations ===

    /// Create a new workflow
    async fn create(&self, data: NewWorkflow) -> Result<WorkflowRecord>;

    /// Get workflow by ID
    async fn get(&self, id: i64) -> Result<WorkflowRecord>;

    /// Get workflow by workflow_id (UUID)
    async fn get_by_workflow_id(&self, workflow_id: Uuid) -> Result<WorkflowRecord>;

    /// List workflows (paginated)
    async fn list(&self, limit: i64, offset: i64) -> Result<Vec<WorkflowRecord>>;

    /// Count workflows
    async fn count(&self) -> Result<i64>;

    /// Delete workflow
    async fn delete(&self, id: i64) -> Result<u64>;

    // === Business Operations ===

    /// Check if step is completed; if not, claim it for execution.
    ///
    /// Returns:
    /// - StepCheckResult::Completed(output) if step already done
    /// - StepCheckResult::Claimed if step is now claimed by this executor
    /// - StepCheckResult::Failed(error) if step previously failed
    async fn check_or_claim_step(
        &self,
        workflow_id: Uuid,
        step_id: &str,
    ) -> Result<StepCheckResult>;

    /// Mark step as completed with output
    async fn complete_step(
        &self,
        workflow_id: Uuid,
        step_id: &str,
        output: Value,
    ) -> Result<()>;

    /// Mark step as failed with error
    async fn fail_step(
        &self,
        workflow_id: Uuid,
        step_id: &str,
        error: Value,
    ) -> Result<()>;

    /// Update workflow status
    async fn update_status(
        &self,
        workflow_id: Uuid,
        status: WorkflowStatus,
    ) -> Result<()>;
}
```

### 5.7 Store Trait

```rust
// src/store/mod.rs

#[async_trait]
pub trait Store: Send + Sync + 'static {
    // === Table Accessors ===

    fn queues(&self) -> &dyn QueueTable;
    fn messages(&self) -> &dyn MessageTable;
    fn workers(&self) -> &dyn WorkerTable;
    fn archive(&self) -> &dyn ArchiveTable;
    fn workflows(&self) -> &dyn WorkflowTable;

    // === Lifecycle Operations ===

    /// Install/migrate database schema
    async fn install(&self) -> Result<()>;

    /// Verify database integrity
    ///
    /// Checks:
    /// - All required tables exist
    /// - Referential integrity (no orphaned records)
    ///
    /// Runs in a read-only transaction.
    async fn verify(&self) -> Result<()>;

    // === Cross-Table Business Operations ===

    /// Move messages exceeding max_attempts to archive (DLQ).
    ///
    /// Implementation:
    /// - PostgreSQL: Single CTE (DELETE...INSERT...RETURNING)
    /// - SQLite: Explicit transaction (SELECT, INSERT, DELETE)
    ///
    /// Returns IDs of archived messages.
    async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>>;

    /// Replay a message from DLQ back to active queue.
    ///
    /// Atomically moves message from archive to messages table.
    /// Returns the new message if successful.
    async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>>;

    /// Release messages held by dead/stale workers.
    ///
    /// Finds workers with no heartbeat within timeout,
    /// releases their messages back to queue.
    /// Returns count of released messages.
    async fn release_zombie_messages(&self, worker_timeout: Duration) -> Result<u64>;

    /// Purge stale workers and release their messages.
    ///
    /// Returns statistics about purged workers and released messages.
    async fn purge_stale_workers(&self, timeout: Duration) -> Result<WorkerPurgeResult>;

    /// Get system-wide statistics.
    ///
    /// Aggregates data from all tables:
    /// - Queue count
    /// - Active worker count
    /// - Pending message count
    /// - DLQ count
    /// - Archive count
    async fn get_system_stats(&self) -> Result<SystemStats>;

    // === Capabilities ===

    /// Returns the concurrency model supported by this backend.
    fn concurrency_model(&self) -> ConcurrencyModel;

    /// Returns the backend name (e.g., "postgres", "sqlite", "turso")
    fn backend_name(&self) -> &'static str;
}

/// Concurrency model supported by a backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyModel {
    /// Single process only. Multiple processes will cause data corruption.
    /// Used by: SQLite, Turso
    SingleProcess,

    /// Multiple processes supported with proper locking.
    /// Used by: PostgreSQL
    MultiProcess,
}
```

---

## 6. AnyStore Enum

```rust
// src/store/mod.rs

/// Runtime-selected database store.
///
/// This enum wraps concrete store implementations and routes method calls
/// to the appropriate backend. Users never interact with this directly;
/// it's used internally by Admin, Producer, Consumer, etc.
#[derive(Clone)]
pub enum AnyStore {
    /// PostgreSQL backend (always available)
    Postgres(PostgresStore),

    /// SQLite backend (requires feature = "sqlite")
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteStore),

    /// Turso backend (requires feature = "turso")
    #[cfg(feature = "turso")]
    Turso(TursoStore),
}

impl AnyStore {
    /// Connect to a database using a DSN.
    ///
    /// The backend is selected based on the DSN scheme:
    /// - `postgres://` or `postgresql://` → PostgreSQL
    /// - `sqlite://` or `sqlite:` → SQLite
    /// - `turso://` or `libsql://` → Turso
    ///
    /// # Example
    /// ```rust
    /// let store = AnyStore::connect("postgres://user:pass@localhost/mydb").await?;
    /// let store = AnyStore::connect("sqlite:///path/to/db.sqlite").await?;
    /// let store = AnyStore::connect("turso://my-db.turso.io").await?;
    /// ```
    pub async fn connect(dsn: &str) -> Result<Self> {
        if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            let pool = PgPool::connect(dsn).await?;
            Ok(AnyStore::Postgres(PostgresStore::new(pool)))
        }
        #[cfg(feature = "sqlite")]
        else if dsn.starts_with("sqlite://") || dsn.starts_with("sqlite:") {
            let store = SqliteStore::connect(dsn).await?;
            Ok(AnyStore::Sqlite(store))
        }
        #[cfg(feature = "turso")]
        else if dsn.starts_with("turso://") || dsn.starts_with("libsql://") {
            let store = TursoStore::connect(dsn).await?;
            Ok(AnyStore::Turso(store))
        }
        else {
            Err(Error::UnsupportedDsn(dsn.to_string()))
        }
    }

    /// Connect with configuration
    pub async fn connect_with_config(config: &Config) -> Result<Self> {
        let mut store = Self::connect(&config.dsn).await?;
        // Apply config (max_read_ct, etc.)
        store.configure(config);
        Ok(store)
    }
}

// Implement Store trait for AnyStore via delegation
#[async_trait]
impl Store for AnyStore {
    fn queues(&self) -> &dyn QueueTable {
        match self {
            AnyStore::Postgres(s) => s.queues(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.queues(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.queues(),
        }
    }

    fn messages(&self) -> &dyn MessageTable {
        match self {
            AnyStore::Postgres(s) => s.messages(),
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.messages(),
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.turso(),
        }
    }

    // ... delegate all other methods similarly ...

    async fn verify(&self) -> Result<()> {
        match self {
            AnyStore::Postgres(s) => s.verify().await,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(s) => s.verify().await,
            #[cfg(feature = "turso")]
            AnyStore::Turso(s) => s.verify().await,
        }
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        match self {
            AnyStore::Postgres(_) => ConcurrencyModel::MultiProcess,
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(_) => ConcurrencyModel::SingleProcess,
            #[cfg(feature = "turso")]
            AnyStore::Turso(_) => ConcurrencyModel::SingleProcess,
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            AnyStore::Postgres(_) => "postgres",
            #[cfg(feature = "sqlite")]
            AnyStore::Sqlite(_) => "sqlite",
            #[cfg(feature = "turso")]
            AnyStore::Turso(_) => "turso",
        }
    }
}
```

---

## 7. Public API Types

### 7.1 Admin

```rust
// src/worker/admin.rs

/// Administrative interface for managing pgqrs infrastructure.
///
/// # Example
/// ```rust
/// let config = Config::from_dsn("postgres://localhost/mydb");
/// let admin = Admin::new(&config).await?;
///
/// admin.install().await?;
/// admin.verify().await?;
///
/// let queue = admin.create_queue("jobs").await?;
/// ```
pub struct Admin {
    store: AnyStore,
    config: Config,
    worker_info: Option<WorkerInfo>,
}

impl Admin {
    /// Create a new Admin instance.
    ///
    /// Connects to the database specified in config.dsn.
    pub async fn new(config: &Config) -> Result<Self> {
        let store = AnyStore::connect_with_config(config).await?;
        Ok(Self {
            store,
            config: config.clone(),
            worker_info: None,
        })
    }

    /// Register this Admin as a worker (enables lifecycle management)
    pub async fn register(&mut self, hostname: &str, port: i32) -> Result<WorkerInfo> {
        let worker = self.store.workers().register(None, hostname, port).await?;
        self.worker_info = Some(worker.clone());
        Ok(worker)
    }

    // === Lifecycle ===

    pub async fn install(&self) -> Result<()> {
        self.store.install().await
    }

    pub async fn verify(&self) -> Result<()> {
        self.store.verify().await
    }

    // === Queue Management ===

    pub async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
        self.store.queues().create(name).await
    }

    pub async fn delete_queue(&self, name: &str) -> Result<u64> {
        self.store.queues().delete(name).await
    }

    pub async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        self.store.queues().list().await
    }

    pub async fn get_queue(&self, name: &str) -> Result<QueueInfo> {
        self.store.queues().get_by_name(name).await
    }

    // === Statistics ===

    pub async fn get_system_stats(&self) -> Result<SystemStats> {
        self.store.get_system_stats().await
    }

    pub async fn get_queue_metrics(&self, queue_name: &str) -> Result<QueueMetrics> {
        let queue = self.store.queues().get_by_name(queue_name).await?;
        self.store.queues().get_metrics(queue.id).await
    }

    // === Maintenance ===

    pub async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>> {
        self.store.dlq_batch(max_attempts).await
    }

    pub async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>> {
        self.store.replay_from_dlq(archived_id).await
    }

    pub async fn purge_stale_workers(&self, timeout: Duration) -> Result<WorkerPurgeResult> {
        self.store.purge_stale_workers(timeout).await
    }

    // === Capabilities ===

    pub fn concurrency_model(&self) -> ConcurrencyModel {
        self.store.concurrency_model()
    }

    pub fn backend_name(&self) -> &'static str {
        self.store.backend_name()
    }
}
```

### 7.2 Producer

```rust
// src/worker/producer.rs

/// Producer interface for enqueueing messages to a queue.
///
/// # Example
/// ```rust
/// let config = Config::from_dsn("postgres://localhost/mydb");
/// let producer = Producer::new(&config, "jobs", "localhost", 8080).await?;
///
/// let msg = producer.enqueue(&json!({"task": "send_email"})).await?;
/// ```
pub struct Producer {
    store: AnyStore,
    queue_info: QueueInfo,
    worker_info: WorkerInfo,
    config: Config,
    validator: PayloadValidator,
}

impl Producer {
    /// Create a new Producer for the specified queue.
    pub async fn new(
        config: &Config,
        queue_name: &str,
        hostname: &str,
        port: i32,
    ) -> Result<Self> {
        let store = AnyStore::connect_with_config(config).await?;
        let queue_info = store.queues().get_by_name(queue_name).await?;
        let worker_info = store.workers().register(Some(queue_info.id), hostname, port).await?;

        Ok(Self {
            store,
            queue_info,
            worker_info,
            config: config.clone(),
            validator: PayloadValidator::new(config.validation_config.clone()),
        })
    }

    /// Enqueue a single message
    pub async fn enqueue(&self, payload: &Value) -> Result<QueueMessage> {
        self.validator.validate(payload)?;
        let id = self.store.messages().insert(
            self.queue_info.id,
            self.worker_info.id,
            payload,
            None,
        ).await?;
        self.store.messages().get(id).await
    }

    /// Enqueue a message with delay
    pub async fn enqueue_delayed(&self, payload: &Value, delay_seconds: u32) -> Result<QueueMessage> {
        self.validator.validate(payload)?;
        let id = self.store.messages().insert(
            self.queue_info.id,
            self.worker_info.id,
            payload,
            Some(delay_seconds),
        ).await?;
        self.store.messages().get(id).await
    }

    /// Enqueue multiple messages
    pub async fn batch_enqueue(&self, payloads: &[Value]) -> Result<Vec<QueueMessage>> {
        self.validator.validate_batch(payloads)?;
        let ids = self.store.messages().insert_batch(
            self.queue_info.id,
            self.worker_info.id,
            payloads,
        ).await?;

        let mut messages = Vec::with_capacity(ids.len());
        for id in ids {
            messages.push(self.store.messages().get(id).await?);
        }
        Ok(messages)
    }

    /// Replay a message from DLQ
    pub async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>> {
        self.store.replay_from_dlq(archived_id).await
    }

    // Worker lifecycle methods
    pub fn worker_id(&self) -> i64 { self.worker_info.id }
    pub fn queue_name(&self) -> &str { &self.queue_info.queue_name }
}
```

### 7.3 Consumer

```rust
// src/worker/consumer.rs

/// Consumer interface for dequeuing and processing messages.
///
/// # Example
/// ```rust
/// let config = Config::from_dsn("postgres://localhost/mydb");
/// let consumer = Consumer::new(&config, "jobs", "localhost", 8081).await?;
///
/// loop {
///     let messages = consumer.dequeue().await?;
///     for msg in messages {
///         process(&msg)?;
///         consumer.archive(msg.id).await?;
///     }
/// }
/// ```
pub struct Consumer {
    store: AnyStore,
    queue_info: QueueInfo,
    worker_info: WorkerInfo,
    config: Config,
}

impl Consumer {
    /// Create a new Consumer for the specified queue.
    pub async fn new(
        config: &Config,
        queue_name: &str,
        hostname: &str,
        port: i32,
    ) -> Result<Self> {
        let store = AnyStore::connect_with_config(config).await?;
        let queue_info = store.queues().get_by_name(queue_name).await?;
        let worker_info = store.workers().register(Some(queue_info.id), hostname, port).await?;

        Ok(Self {
            store,
            queue_info,
            worker_info,
            config: config.clone(),
        })
    }

    /// Dequeue messages for processing.
    ///
    /// Returns up to `config.default_max_batch_size` messages.
    /// Messages are locked for `config.default_lock_time_seconds`.
    pub async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.store.messages().dequeue_atomic(
            self.queue_info.id,
            self.worker_info.id,
            self.config.default_max_batch_size,
            self.config.default_lock_time_seconds,
            self.config.max_read_ct as u32,
        ).await
    }

    /// Dequeue with custom batch size and lock time
    pub async fn dequeue_with_options(
        &self,
        batch_size: usize,
        lock_seconds: u32,
    ) -> Result<Vec<QueueMessage>> {
        self.store.messages().dequeue_atomic(
            self.queue_info.id,
            self.worker_info.id,
            batch_size,
            lock_seconds,
            self.config.max_read_ct as u32,
        ).await
    }

    /// Archive a processed message (move to archive table)
    pub async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
        self.store.archive().archive_message(msg_id, self.worker_info.id).await
    }

    /// Archive multiple messages
    pub async fn archive_batch(&self, msg_ids: &[i64]) -> Result<Vec<i64>> {
        self.store.archive().archive_batch(msg_ids, self.worker_info.id).await
    }

    /// Delete a message without archiving
    pub async fn delete(&self, msg_id: i64) -> Result<bool> {
        self.store.messages().delete(msg_id).await
    }

    /// Release a message back to queue (make available immediately)
    pub async fn release(&self, msg_id: i64) -> Result<bool> {
        self.store.messages().release(msg_id, self.worker_info.id).await
    }

    /// Extend visibility timeout for a message
    pub async fn extend_visibility(&self, msg_id: i64, additional_seconds: u32) -> Result<bool> {
        self.store.messages().extend_visibility(
            msg_id,
            self.worker_info.id,
            additional_seconds,
        ).await
    }

    // Worker lifecycle methods
    pub fn worker_id(&self) -> i64 { self.worker_info.id }
    pub fn queue_name(&self) -> &str { &self.queue_info.queue_name }
}
```

---

## 8. PostgresStore Implementation Example

```rust
// src/store/postgres/mod.rs

pub mod queues;
pub mod messages;
pub mod workers;
pub mod archive;
pub mod workflows;

use sqlx::PgPool;

/// PostgreSQL implementation of the Store trait.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    queues: PostgresQueueTable,
    messages: PostgresMessageTable,
    workers: PostgresWorkerTable,
    archive: PostgresArchiveTable,
    workflows: PostgresWorkflowTable,
    max_read_ct: u32,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            queues: PostgresQueueTable::new(pool.clone()),
            messages: PostgresMessageTable::new(pool.clone()),
            workers: PostgresWorkerTable::new(pool.clone()),
            archive: PostgresArchiveTable::new(pool.clone()),
            workflows: PostgresWorkflowTable::new(pool.clone()),
            pool,
            max_read_ct: 5,
        }
    }

    pub fn with_max_read_ct(mut self, max_read_ct: u32) -> Self {
        self.max_read_ct = max_read_ct;
        self.messages = self.messages.with_max_read_ct(max_read_ct);
        self
    }
}

// SQL constants for cross-table operations
const CHECK_TABLE_EXISTS: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = $1
    )
"#;

const CHECK_ORPHANED_MESSAGES: &str = r#"
    SELECT COUNT(*) FROM pgqrs_messages m
    LEFT OUTER JOIN pgqrs_queues q ON m.queue_id = q.id
    WHERE q.id IS NULL
"#;

const DLQ_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages WHERE read_ct >= $1
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id,
                  payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id,
         payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT id, queue_id, producer_worker_id, consumer_worker_id,
           payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

const REPLAY_FROM_DLQ: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_archive
        WHERE id = $1 AND consumer_worker_id IS NULL AND dequeued_at IS NULL
        RETURNING original_msg_id, queue_id, payload, enqueued_at, vt, read_ct
    )
    INSERT INTO pgqrs_messages (id, queue_id, payload, enqueued_at, vt, read_ct)
    SELECT original_msg_id, queue_id, payload, enqueued_at, NOW(), read_ct
    FROM archived_msg
    RETURNING id, queue_id, payload, enqueued_at, vt, read_ct,
              producer_worker_id, consumer_worker_id, dequeued_at;
"#;

#[async_trait]
impl Store for PostgresStore {
    fn queues(&self) -> &dyn QueueTable { &self.queues }
    fn messages(&self) -> &dyn MessageTable { &self.messages }
    fn workers(&self) -> &dyn WorkerTable { &self.workers }
    fn archive(&self) -> &dyn ArchiveTable { &self.archive }
    fn workflows(&self) -> &dyn WorkflowTable { &self.workflows }

    async fn install(&self) -> Result<()> {
        MIGRATOR.run(&self.pool).await.map_err(|e| Error::Migration(e.to_string()))
    }

    async fn verify(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Check table existence
        for table in &["pgqrs_queues", "pgqrs_workers", "pgqrs_messages", "pgqrs_archive"] {
            let exists: bool = sqlx::query_scalar(CHECK_TABLE_EXISTS)
                .bind(table)
                .fetch_one(&mut *tx)
                .await?;
            if !exists {
                return Err(Error::TableNotFound(table.to_string()));
            }
        }

        // Check referential integrity
        let orphaned: i64 = sqlx::query_scalar(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&mut *tx)
            .await?;
        if orphaned > 0 {
            return Err(Error::IntegrityViolation {
                message: format!("{} orphaned messages found", orphaned),
            });
        }

        // ... more integrity checks ...

        tx.commit().await?;
        Ok(())
    }

    async fn dlq_batch(&self, max_attempts: i32) -> Result<Vec<i64>> {
        sqlx::query_scalar(DLQ_BATCH)
            .bind(max_attempts)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn replay_from_dlq(&self, archived_id: i64) -> Result<Option<QueueMessage>> {
        sqlx::query_as(REPLAY_FROM_DLQ)
            .bind(archived_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::MultiProcess
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    // ... other methods ...
}
```

---

## 9. Usage Examples

### 9.1 Basic Usage

```rust
use pgqrs::{Admin, Producer, Consumer, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // User only knows about DSN - no generics, no store types
    let config = Config::from_dsn("postgres://user:pass@localhost/mydb");

    // Setup
    let admin = Admin::new(&config).await?;
    admin.install().await?;
    admin.create_queue("jobs").await?;

    // Produce
    let producer = Producer::new(&config, "jobs", "localhost", 8080).await?;
    producer.enqueue(&serde_json::json!({"task": "send_email"})).await?;

    // Consume
    let consumer = Consumer::new(&config, "jobs", "localhost", 8081).await?;
    let messages = consumer.dequeue().await?;
    for msg in messages {
        println!("Processing: {}", msg.payload);
        consumer.archive(msg.id).await?;
    }

    Ok(())
}
```

### 9.2 Switching Databases

```rust
// PostgreSQL
let config = Config::from_dsn("postgres://localhost/mydb");
let admin = Admin::new(&config).await?;

// SQLite (same API!)
let config = Config::from_dsn("sqlite:///path/to/db.sqlite");
let admin = Admin::new(&config).await?;

// Turso (same API!)
let config = Config::from_dsn("turso://my-db.turso.io?auth_token=xxx");
let admin = Admin::new(&config).await?;
```

### 9.3 Checking Backend Capabilities

```rust
let admin = Admin::new(&config).await?;

match admin.concurrency_model() {
    ConcurrencyModel::MultiProcess => {
        println!("Running in distributed mode");
    }
    ConcurrencyModel::SingleProcess => {
        println!("Warning: Single process mode only");
    }
}

println!("Using backend: {}", admin.backend_name());
```

---

## 10. Migration Strategy

### Phase 1: Create Store Infrastructure
1. Define all traits in `src/store/mod.rs`
2. Implement `PostgresStore` and all `Postgres*Table` types
3. Implement `AnyStore` enum (Postgres-only initially)

### Phase 2: Refactor Public Types
1. Convert `Admin` to use `AnyStore` (remove generic `S`)
2. Convert `Producer` to use `AnyStore`
3. Convert `Consumer` to use `AnyStore`
4. Convert `Workflow` to use `AnyStore`

### Phase 3: Add SQLite Backend
1. Implement `SqliteStore` and all `Sqlite*Table` types
2. Add `Sqlite` variant to `AnyStore` enum
3. Add `sqlite` feature flag

### Phase 4: Add Turso Backend
1. Implement `TursoStore` and all `Turso*Table` types
2. Add `Turso` variant to `AnyStore` enum
3. Add `turso` feature flag

### Phase 5: Update Python Bindings
1. Update `py-pgqrs` to use new non-generic types
2. Expose `concurrency_model()` and `backend_name()` to Python

---

## 11. Testing Strategy

### 11.1 Conformance Tests

All backends must pass the same test suite:

```rust
// tests/store_conformance.rs

async fn test_queue_crud(store: &impl Store) {
    let queue = store.queues().create("test_queue").await.unwrap();
    assert_eq!(queue.queue_name, "test_queue");

    let fetched = store.queues().get_by_name("test_queue").await.unwrap();
    assert_eq!(fetched.id, queue.id);

    store.queues().delete("test_queue").await.unwrap();
    assert!(store.queues().get_by_name("test_queue").await.is_err());
}

// Run against all backends
#[tokio::test]
async fn postgres_conformance() {
    let store = PostgresStore::new(test_pg_pool()).await;
    test_queue_crud(&store).await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn sqlite_conformance() {
    let store = SqliteStore::connect(":memory:").await.unwrap();
    test_queue_crud(&store).await;
}
```

### 11.2 Backend-Specific Tests

Test backend-specific behavior (e.g., SKIP LOCKED for Postgres):

```rust
// tests/postgres_specific.rs

#[tokio::test]
async fn test_skip_locked_behavior() {
    // Test that concurrent dequeue doesn't return same messages
}
```

---

## 12. Summary

| Aspect | Decision |
|--------|----------|
| **Generic exposure** | None - public types are concrete |
| **Backend selection** | Runtime, via DSN scheme |
| **Abstraction pattern** | Enum wrapper (`AnyStore`) |
| **Business logic location** | `Store` trait (cross-table) or `*Table` traits (single-table) |
| **Transaction management** | Encapsulated in Store implementations |
| **SQL location** | In backend-specific modules (`postgres/*.rs`, `sqlite/*.rs`) |
| **Feature flags** | `sqlite`, `turso` (postgres always available) |
