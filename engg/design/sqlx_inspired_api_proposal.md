# API Proposal: sqlx-Inspired Functional Interface for pgqrs

**Date:** 2024-12-29
**Author:** pgqrs Team
**Status:** Draft Proposal

---

## 1. Motivation

The current pgqrs API requires obtaining worker instances (`Admin`, `Producer`, `Consumer`) from a `Store`, then calling methods on those instances. While this is clean for long-running workers, it creates friction for simple one-off operations:

```rust
// Current API - verbose for simple operations
let store = AnyStore::connect(&dsn).await?;
let admin = store.admin().await;
let producer = store.producer("my-queue").await?;
producer.enqueue(&my_message).await?;
producer.shutdown().await?; // Must clean up
```

**sqlx provides two patterns:**

```rust
// Pattern 1: Builder (functional entry point)
sqlx::query("SELECT * FROM users")
    .fetch_all(&pool)
    .await?;

// Pattern 2: Executor trait (methods directly on pool)
pool.fetch_one("SELECT * FROM users").await?;
pool.execute("DELETE FROM users").await?;
```

This proposal adapts **both** patterns for pgqrs queue and workflow operations.

---

## 2. Design Goals

1. **Minimal boilerplate** for one-off operations
2. **No hidden state** - operations take pool/store explicitly
3. **Composable** - chain operations naturally
4. **Type-safe** - leverage Rust's type system
5. **Backward compatible** - new API coexists with existing worker-based API

---

## 3. Proposed API

### 3.1 API Surface Area

pgqrs operations fall into 5 categories:

| Category | Operations | Typical User |
|----------|------------|--------------|
| **Queue ops** | enqueue, dequeue, batch, DLQ replay/purge, visibility | App developer |
| **Workflow ops** | create, step, complete, fail | App developer (workflow users) |
| **Worker ops** | heartbeat, suspend, resume, shutdown | Framework/operator |
| **Admin ops** | install, verify, create_queue, delete_queue | DevOps/setup |
| **Table ops** | Direct access to 5 tables | Debugging/advanced |

The 5 underlying tables:
1. `pgqrs_queues` - queue definitions
2. `pgqrs_messages` - active messages
3. `pgqrs_workers` - worker registrations
4. `pgqrs_archive` - processed messages
5. `pgqrs_workflows` / `pgqrs_workflow_steps` - workflow state

### 3.2 API Tiering (What Gets Top-Level Exposure)

Not all operations should be exposed at the crate root. A flat `pgqrs::*` namespace with everything would be overwhelming. We use a **tiered hierarchy**:

| Tier | Namespace | Categories | Frequency | Example |
|------|-----------|------------|-----------|---------|
| **Tier 1** | `pgqrs::` | Queue ops | 90% of usage | `pgqrs::produce()`, `pgqrs::consume()` |
| **Tier 2** | `pgqrs::workflow::` | Workflow ops | Common for workflow users | `pgqrs::workflow::step()` (deferred) |
| **Tier 3** | `pgqrs::admin()`, `store.*` | Admin, Worker, Table ops | Rare/specialized | `pgqrs::admin().install()` |

**Design Principle:** Only the most common operations get top-level exposure (`pgqrs::fn`). Everything else is namespaced or uses builder/object patterns.

```rust
// Tier 1: Top-level (core queue ops - 90% of usage)
pgqrs::produce(&store, "orders", &order).await?;
pgqrs::consume(&store, "orders", |msg| async { ... }).await?;

// Also Tier 1: Low-level queue ops (take worker reference)
pgqrs::enqueue(&producer, &order).await?;
pgqrs::dequeue(&consumer, 10).await?;
pgqrs::archive(&consumer, &msg).await?;

// Tier 2: Namespaced (workflows - deferred)
pgqrs::workflow::create(&store, &input).await?;
pgqrs::workflow::step(&store, wf_id, "validate", || async { ... }).await?;

// Tier 3: Builder pattern (admin - rare)
pgqrs::admin().install().execute(&store).await?;
pgqrs::admin().create_queue("orders").execute(&store).await?;

// Tier 3: Object-based (worker lifecycle, table access - specialized)
let consumer = store.consumer("orders").await?;
consumer.heartbeat().await?;
consumer.shutdown().await?;
store.tables().messages().list(100).await?;  // Direct table access
```

**Rationale for each tier:**
- **Tier 1 (top-level):** Queue operations are the primary use case. Developers reach for these constantly. Zero friction.
- **Tier 2 (namespaced):** Workflows are a distinct feature. Users who need them will import `pgqrs::workflow::*`. Keeps root namespace clean.
- **Tier 3 (builder/object):** Admin ops are one-time setup. Worker lifecycle is framework-level. Table access is debugging. These don't need shorthand.

### 3.3 Store Initialization

```rust
// Core entry point - connect to database
pub async fn connect(dsn: &str) -> Result<Store>;
pub async fn connect_with(config: Config) -> Result<Store>;
```

**Usage:**

```rust
// Simple connection (DSN-based backend selection)
let store = pgqrs::connect("postgres://localhost/mydb").await?;
let store = pgqrs::connect("sqlite://./local.db").await?;

// With configuration options
let store = pgqrs::connect_with(
    Config::new("postgres://localhost/mydb")
        .max_connections(10)
        .schema("myapp")
).await?;
```

The `Store` returned is used to create workers or passed to high-level functions.

### 3.4 Two-Level API Structure

pgqrs provides two API levels:

| Level | Functions | Worker Lifecycle | Use Case |
|-------|-----------|------------------|----------|
| **High-level** | `produce`, `consume` | Ephemeral (auto-managed) | Scripts, one-off ops, simple apps |
| **Low-level** | `enqueue`, `dequeue`, `archive` | Long-running (you manage) | High-volume, production workers |

```rust
// High-level API entry points
pub mod pgqrs {
    // Ephemeral producer: register → enqueue → cleanup
    pub async fn produce<T: Serialize>(store: &Store, queue: &str, msg: &T) -> Result<MessageId>;
    pub async fn produce_batch<T: Serialize>(store: &Store, queue: &str, msgs: &[T]) -> Result<Vec<MessageId>>;

    // Ephemeral consumer: register → dequeue → process → archive → cleanup
    pub async fn consume<F, Fut>(store: &Store, queue: &str, handler: F) -> Result<()>
    where
        F: FnOnce(Message) -> Fut,
        Fut: Future<Output = Result<()>>;

    pub async fn consume_batch<F, Fut>(store: &Store, queue: &str, batch_size: usize, handler: F) -> Result<()>
    where
        F: FnOnce(Vec<Message>) -> Fut,
        Fut: Future<Output = Result<()>>;
}

// Low-level API entry points (take worker reference)
pub mod pgqrs {
    // Requires a Producer (you manage lifecycle)
    pub async fn enqueue<T: Serialize>(producer: &dyn Producer, msg: &T) -> Result<MessageId>;
    pub async fn enqueue_batch<T: Serialize>(producer: &dyn Producer, msgs: &[T]) -> Result<Vec<MessageId>>;

    // Requires a Consumer (you manage lifecycle)
    pub async fn dequeue(consumer: &dyn Consumer, batch_size: usize) -> Result<Vec<Message>>;
    pub async fn archive(consumer: &dyn Consumer, msg: &Message) -> Result<()>;
    pub async fn archive_batch(consumer: &dyn Consumer, msgs: &[Message]) -> Result<()>;
}
```

### 3.5 High-Level API: produce / consume

**produce** - Creates ephemeral producer, enqueues message(s), cleans up:

```rust
// Single message
pgqrs::produce(&store, "orders", &order).await?;

// Batch
pgqrs::produce_batch(&store, "orders", &[order1, order2, order3]).await?;
```

**consume** - Creates ephemeral consumer, dequeues, runs handler, archives on success:

```rust
// Single message
pgqrs::consume(&store, "orders", |msg| async {
    let order: Order = msg.payload()?;
    process_order(&order).await?;
    Ok(())  // Success → auto-archive
}).await?;

// Batch
pgqrs::consume_batch(&store, "orders", 10, |msgs| async {
    for msg in msgs {
        let order: Order = msg.payload()?;
        process_order(&order).await?;
    }
    Ok(())  // Success → auto-archive all
}).await?;
```

**Behavior on error:**
- If handler returns `Err`, message is NOT archived (stays in queue for retry)
- Ephemeral consumer is cleaned up regardless

### 3.6 Low-Level API: enqueue / dequeue / archive

For high-volume production workers with explicit lifecycle management:

```rust
// Create long-running producer
let producer = store.producer("orders").await?;

// Enqueue messages (producer handles heartbeat)
pgqrs::enqueue(&producer, &order1).await?;
pgqrs::enqueue(&producer, &order2).await?;
pgqrs::enqueue_batch(&producer, &[order3, order4]).await?;

// Cleanup when done
producer.shutdown().await?;
```

```rust
// Create long-running consumer
let consumer = store.consumer("orders").await?;

loop {
    // You manage heartbeat
    consumer.heartbeat().await?;

    // Dequeue
    let msgs = pgqrs::dequeue(&consumer, 10).await?;

    for msg in &msgs {
        match process_message(msg).await {
            Ok(_) => pgqrs::archive(&consumer, msg).await?,
            Err(e) => {
                // Message stays in queue, will be retried or DLQ'd
                log::error!("Failed to process: {}", e);
            }
        }
    }
}

// Cleanup
consumer.shutdown().await?;
```

### 3.7 Comparison: High-Level vs Low-Level

| Aspect | High-Level (`produce`/`consume`) | Low-Level (`enqueue`/`dequeue`/`archive`) |
|--------|----------------------------------|-------------------------------------------|
| Worker creation | Auto (ephemeral) | Manual (`store.producer()`) |
| Heartbeat | Auto (background) | Manual (`worker.heartbeat()`) |
| Cleanup | Auto (on completion) | Manual (`worker.shutdown()`) |
| Archive | Auto (on handler success) | Manual (`pgqrs::archive()`) |
| Error handling | Return `Err` → message stays | You decide |
| Use case | Scripts, simple apps | Production workers |
| Overhead | Higher (per-operation) | Lower (amortized) |

### 3.8 Admin API

Admin operations are idempotent DDL-like operations with no message ownership concerns. No two-level API needed - just takes `&store`:

```rust
// Install schema
pgqrs::admin().install().execute(&store).await?;

// Verify installation
pgqrs::admin().verify().execute(&store).await?;

// Create queue
pgqrs::admin()
    .create_queue("email-queue")
    .with_dlq("email-dlq")
    .execute(&store)
    .await?;

// Delete queue
pgqrs::admin()
    .delete_queue("old-queue")
    .execute(&store)
    .await?;
```

### 3.9 Workflow API (Deferred)

```rust
// Create and start workflow
let wf = pgqrs::workflow(&input_data)
    .name("order-processing")
    .create(&store)
    .await?;

// Execute step with exactly-once semantics
let result = pgqrs::step("validate-inventory")
    .for_workflow(workflow_id)
    .execute(&store, || async {
        // This closure runs exactly once
        check_inventory().await
    })
    .await?;

// Complete workflow
pgqrs::workflow_complete(workflow_id)
    .with_output(&final_result)
    .execute(&store)
    .await?;
```

---

## 4. Worker Registration (Why Transient Workers Don't Work)

### 4.1 The Zombie Worker Problem

Consider this scenario without worker tracking:

```
t0: Consumer A dequeues message M (visibility_timeout = 30s)
t1: Consumer A becomes stuck or network partitioned
t2: Health check detects A unhealthy → M moves to DLQ
t3: Admin replays M → M back in queue
t4: Consumer B dequeues M
t5: Consumer A wakes up, thinks it owns M
t6: Consumer A calls ack() - succeeds! (no ownership check)
t7: Consumer B is now in inconsistent state
```

**This is a fundamental distributed systems problem.** Without worker registration:
- No ownership tracking (`locked_by_worker_id`)
- No health check (no heartbeat)
- No invalidation mechanism
- Zombie workers can corrupt state

### 4.2 How Worker Registration Solves This

With proper worker tracking:
- Every consumer has a registered `worker_id`
- Messages record `locked_by_worker_id` when dequeued
- Workers send heartbeats to prove liveness
- Health check invalidates unhealthy workers
- Operations (ack, nack, extend) verify worker is still valid
- When zombie A tries to ack() at t6, it fails because A's worker_id was invalidated

### 4.3 Implications for the API

**The high-level API (`produce`/`consume`) must still register workers.** The difference is lifecycle management, not registration:

```rust
// Low-level API - explicit lifecycle
let consumer = store.consumer("orders").await?;
loop {
    consumer.heartbeat().await?;  // Manual heartbeat
    let msgs = pgqrs::dequeue(&consumer, 10).await?;
    for msg in &msgs {
        process(msg).await?;
        pgqrs::archive(&consumer, msg).await?;
    }
}
consumer.shutdown().await?;  // Manual cleanup

// High-level API - managed lifecycle
pgqrs::consume(&store, "orders", |msg| async {
    process(&msg).await
}).await?;
// Ephemeral worker auto-registered, heartbeat auto-managed, cleanup automatic
```

### 4.4 Ephemeral Workers (Internal Detail)

The high-level functions (`produce`, `consume`) internally create **ephemeral workers**:

| Aspect | Long-running Worker | Ephemeral Worker |
|--------|---------------------|------------------|
| Registration | Yes | Yes |
| Heartbeat | Manual (`worker.heartbeat()`) | Auto (background task) |
| Cleanup | Manual (`worker.shutdown()`) | Auto (on completion) |
| Ownership tracking | Yes | Yes |
| Health check | Yes | Yes |
| Use case | High-volume production | One-off operations, scripts |

**Implementation (internal):**
- Ephemeral workers register with a short TTL (e.g., 60s)
- Background task sends heartbeat while operation is in progress
- Worker is deregistered when operation completes
- If process crashes, health check cleans up after TTL expires

This is an implementation detail - users just call `pgqrs::produce()` or `pgqrs::consume()`.

---

## 5. Comparison with sqlx

| sqlx Pattern | pgqrs High-Level | pgqrs Low-Level |
|-------------|------------------|-----------------|
| `sqlx::query("SQL")` | `pgqrs::produce(&store, q, &msg)` | `pgqrs::enqueue(&producer, &msg)` |
| `pool.execute(sql)` | `pgqrs::produce(...)` | `pgqrs::enqueue(...)` |
| `pool.fetch_one(sql)` | `pgqrs::consume(...)` | `pgqrs::dequeue(...)` |
| `.bind(value)` | `.to("queue").priority(High)` | - |
| `.fetch_one(&pool)` | `.execute(&store)` | `store.enqueue("q", &msg)` |
| `pool.execute(sql)` | - | `store.enqueue("q", &msg)` |
| `pool.fetch_one(sql)` | - | `store.dequeue("q")` |

**Naming Rationale:**

| Domain | Term | Rationale |
|--------|------|------------|
| Queue | `enqueue`/`dequeue` | ✅ Matches pgqrs domain (queue library) |
| Messaging | `send`/`receive` | ❌ Generic, less precise |
| Pub/Sub | `publish`/`subscribe` | ❌ Wrong model (pgqrs is not pub/sub) |

**Key Differences from sqlx:**
- sqlx operates on raw SQL; pgqrs operates on typed messages
- sqlx returns rows; pgqrs returns message IDs or messages
- pgqrs includes queue semantics (visibility, DLQ, etc.)

---

## 6. Full Example

```rust
use pgqrs;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderCreated {
    order_id: String,
    customer_id: String,
    total: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to database
    let store = pgqrs::connect("postgres://localhost/myapp").await?;

    // Setup (one-time)
    pgqrs::admin().install().execute(&store).await?;
    pgqrs::admin().create_queue("orders").execute(&store).await?;

    let order = OrderCreated {
        order_id: "ORD-123".into(),
        customer_id: "CUST-456".into(),
        total: 99.99,
    };

    // === High-level API (ephemeral workers, simple) ===

    // Produce: creates ephemeral producer, enqueues, cleans up
    pgqrs::produce(&store, "orders", &order).await?;

    // Consume: creates ephemeral consumer, dequeues, processes, archives
    pgqrs::consume(&store, "orders", |msg| async {
        let order: OrderCreated = msg.payload()?;
        println!("Processing order: {}", order.order_id);
        process_order(&order).await?;
        Ok(())  // Success → message archived
    }).await?;

    // Batch versions
    pgqrs::produce_batch(&store, "orders", &[order1, order2]).await?;

    pgqrs::consume_batch(&store, "orders", 10, |msgs| async {
        for msg in msgs {
            process_order(&msg.payload()?).await?;
        }
        Ok(())
    }).await?;

    // === Low-level API (long-running workers, production) ===

    // Create workers with explicit lifecycle
    let producer = store.producer("orders").await?;
    let consumer = store.consumer("orders").await?;

    // Enqueue with worker reference
    pgqrs::enqueue(&producer, &order).await?;
    pgqrs::enqueue_batch(&producer, &[order1, order2]).await?;

    // Consume with explicit control
    loop {
        consumer.heartbeat().await?;

        let msgs = pgqrs::dequeue(&consumer, 10).await?;
        for msg in &msgs {
            match process_order(&msg.payload()?).await {
                Ok(_) => pgqrs::archive(&consumer, msg).await?,
                Err(e) => log::error!("Failed: {}", e),  // Will retry
            }
        }
    }

    // Cleanup
    producer.shutdown().await?;
    consumer.shutdown().await?;

    Ok(())
}
```

---

## 7. Alternative Considered: Macro-Based API

An alternative approach uses procedural macros for compile-time queue binding:

```rust
// Using derive macro for type-safe queues
#[derive(Message)]
#[pgqrs(queue = "orders")]
struct OrderCreated { ... }

// Send is type-aware
pgqrs::send(&order).execute(&store).await?;  // Queue inferred from type

// Receive is type-safe
let msg: OrderCreated = pgqrs::receive::<OrderCreated>()
    .fetch_one(&store)
    .await?;
```

**Trade-offs:**
- More compile-time safety
- More complex implementation
- Less flexible (queue hardcoded in type)

**Recommendation:** Start with builder pattern; add macros later if needed.

---

## 8. Migration Path

The new APIs are additive - existing code continues to work:

```rust
// Old API (still works)
let producer = store.producer("orders").await?;
producer.enqueue(&msg).await?;

// New high-level API
pgqrs::produce(&store, "orders", &msg).await?;

// New low-level API
pgqrs::enqueue(&producer, &msg).await?;
```

Teams can choose the appropriate style:

| Use Case | Recommended API |
|----------|------------------|
| Scripts, one-off ops | `pgqrs::produce()` / `pgqrs::consume()` |
| Production workers | `store.producer()` + `pgqrs::enqueue()` |
| Admin operations | `pgqrs::admin().install()...` |

---

## 9. Implementation Plan

### Phase 1: High-Level Queue API
1. Implement `pgqrs::produce()` / `pgqrs::produce_batch()`
2. Implement `pgqrs::consume()` / `pgqrs::consume_batch()`
3. Implement ephemeral worker infrastructure

### Phase 2: Low-Level Queue API
1. Implement `pgqrs::enqueue()` / `pgqrs::enqueue_batch()`
2. Implement `pgqrs::dequeue()`
3. Implement `pgqrs::archive()` / `pgqrs::archive_batch()`

### Phase 3: Admin API
1. Implement `pgqrs::admin()` builder
2. Add shortcuts for common operations

### Phase 4: Workflow API
1. Implement `pgqrs::workflow()` builder
2. Implement `pgqrs::step()` builder

---

## 10. Open Questions

1. ~~**Naming:** `pgqrs::send` vs `pgqrs::enqueue` vs `pgqrs::publish`?~~ **Resolved:** Use `enqueue`/`dequeue` (queue terminology)
2. ~~**One API or two?**~~ **Resolved:** Both builder and direct methods (like sqlx)
3. **Error types:** Should builder errors be distinct from execution errors?
4. **Ephemeral worker limits:** Should there be a cap on concurrent ephemeral workers?
5. **Telemetry:** Should builders support injecting tracing spans?

---

## 11. Summary

This proposal introduces sqlx-inspired APIs for pgqrs:

**Two new patterns:**
1. **Builder pattern:** `pgqrs::enqueue(&msg).to("q").priority(High).execute(&store)`
2. **Direct pattern:** `store.enqueue("q", &msg)`

**Key decisions:**
- Use queue terminology: `enqueue`/`dequeue` (not send/receive)
- Support both patterns like sqlx (builder + executor trait)
- Coexist with existing worker-based API for long-running consumers

The implementation leverages existing trait infrastructure and can be rolled out incrementally.
