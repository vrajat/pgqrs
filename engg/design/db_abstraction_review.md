# Database Abstraction Layer - Architecture Review

**Date:** 2024-12-26  
**Related PR:** #118  
**Epic:** #112 (Support Multiple Database Backends)

## Executive Summary

The Store trait pattern is well-designed and appropriate for supporting Postgres, SQLite, and Turso backends. This document captures architectural recommendations for implementation phases 2b through 5.

---

## 1. Abstraction Quality

### Current Strengths

- **Associated types** provide compile-time type safety without boxing overhead
- **Repository segregation** (`QueueStore`, `MessageStore`, `WorkerStore`, etc.) follows single-responsibility principle
- **Composition over inheritance** - `PostgresStore` composes repositories via `Arc`

### Recommendation: Add Capabilities Trait

Since different DBs have different capabilities, make this explicit:

```rust
pub trait StoreCapabilities {
    /// Whether this store supports SKIP LOCKED semantics natively
    fn supports_skip_locked(&self) -> bool;
    
    /// Whether this store supports multi-process concurrent access
    fn supports_multi_process(&self) -> bool;
    
    /// Whether this store supports database-level transactions
    fn supports_transactions(&self) -> bool;
}
```

This allows callers to query capabilities and adjust behavior (e.g., Python bindings could warn users about single-process limitations).

---

## 2. Handling SKIP LOCKED for SQLite/Turso

### Problem

PostgreSQL's `FOR UPDATE SKIP LOCKED` is critical for efficient queue behavior in multi-consumer scenarios. SQLite/Turso don't support this.

### Recommended Strategy: Claiming Pattern

For SQLite/Turso, implement a **claiming pattern**:

```rust
// SQLite MessageStore::dequeue implementation concept
async fn dequeue(&self, queue_id: i64, worker_id: i64, limit: usize, vt_seconds: u32) -> Result<Vec<QueueMessage>> {
    // 1. SELECT messages WHERE vt <= NOW() AND consumer_worker_id IS NULL LIMIT N
    // 2. UPDATE SET consumer_worker_id = $worker_id, vt = NOW() + vt_seconds 
    //    WHERE id IN (...) AND consumer_worker_id IS NULL  -- Re-check for concurrent claim
    // 3. Return only successfully claimed messages
}
```

This provides **eventually consistent** behavior without `SKIP LOCKED`, acceptable for single-process SQLite.

### Documentation Requirement

Clearly document that SQLite/Turso provide weaker concurrency guarantees than PostgreSQL.

---

## 3. Single-Process vs Multi-Process

### Recommendation: Explicit Concurrency Model

```rust
pub enum ConcurrencyModel {
    /// Single process only (SQLite, Turso)
    SingleProcess,
    /// Multi-process safe with advisory locks (PostgreSQL)
    MultiProcess,
}

impl Store for SqliteStore {
    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }
}
```

### Python Bindings

Surface this in Python so users understand the limitations:

```python
store = pgqrs.connect("sqlite:///mydb.db")
if store.concurrency_model == "single-process":
    print("Warning: SQLite backend supports single-process only")
```

---

## 4. API Ergonomics

### Current Issue

- Configuration scattered across constructor arguments
- `max_read_ct` passed to store constructor is a policy decision

### Recommendation: Builder Pattern

```rust
// Proposed ergonomic API
let store = PostgresStore::builder()
    .dsn("postgres://...")
    .max_connections(10)
    .build()
    .await?;

let producer = Producer::new(&store, &queue).await?;
```

For Python:

```python
store = pgqrs.connect("postgres://...")  # Auto-detects backend
producer = store.producer("my-queue")
```

---

## 5. Testing Strategy

### Recommendation: Trait-Based Conformance Tests

```rust
// tests/store_conformance.rs
pub async fn run_store_tests<S: Store>(store: S) {
    test_queue_crud(&store).await;
    test_message_lifecycle(&store).await;
    test_worker_registration(&store).await;
    // ...
}

// Then in postgres_tests.rs:
#[tokio::test]
async fn postgres_conformance() {
    let store = PostgresStore::new(...);
    run_store_tests(store).await;
}
```

This ensures **all stores pass the same tests**, preventing implementation drift.

---

## 6. Implementation Priority

### Risk Assessment

Phase 2b (refactoring `Producer`/`Consumer`/`Admin` to use `Store`) is **critical path**. Until complete, new backends cannot be integrated end-to-end.

### Recommended Order

| Phase | Priority | Rationale |
|-------|----------|-----------|
| 2b: Worker Refactoring | **High** | Validates abstraction design |
| 3a: SQLite Implementation | Medium | Simpler than Turso, good test case |
| 3b: Turso Implementation | Medium | After SQLite patterns established |
| 4: Python Bindings | Medium | Depends on 2b completion |
| 5: Documentation | Ongoing | Update incrementally |

---

## 7. Specific Code Notes

### `store/postgres/mod.rs`

```rust
impl PostgresStore {
    pub fn new(pool: PgPool, max_read_ct: u32) -> Self {
```

`max_read_ct` is a **policy** decision. Consider:
- Moving to a configuration struct
- Or making it a method parameter on `dequeue`

This improves testability and allows different limits per queue.

---

## Summary of Actionable Items

| Priority | Item | Issue/Phase |
|----------|------|-------------|
| **High** | Complete Phase 2b before new backends | #105 |
| **High** | Add `StoreCapabilities` trait | #115 |
| **High** | Document SQLite/Turso concurrency limitations | #111 |
| **Medium** | Implement conformance test suite | #110 |
| **Medium** | Consider builder pattern for ergonomics | Future |
| **Low** | Plugin architecture for custom stores | Future |

---

## Verdict

The design is solid and appropriate for the stated goals. Main areas to watch:
1. SKIP LOCKED equivalence - document weaker guarantees clearly
2. Worker refactoring - prioritize as integration test for abstraction
3. Python bindings - ensure capabilities are exposed to users
