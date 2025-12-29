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

**Initial focus:** Queue ops and Workflow ops get the new ergonomic API. The right ergonomics for other categories will be informed by actual usage.

### 3.2 Store Initialization

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

The `Store` returned supports both API patterns:
- Passed to builders: `pgqrs::enqueue(&msg).to("q").execute(&store)`
- Called directly: `store.enqueue("q", &msg)`

### 3.3 Core Queue Operations

pgqrs will support **both** patterns like sqlx:

1. **Builder pattern** - `pgqrs::enqueue()` functional entry points
2. **Direct methods** - Methods directly on `Store`

```rust
// Core entry points in `pgqrs` crate root
pub mod pgqrs {
    /// Message production (queue terminology)
    pub fn enqueue<T: Serialize>(message: &T) -> EnqueueBuilder<T>;

    /// Message consumption (queue terminology)
    pub fn dequeue() -> DequeueBuilder;

    /// Batch enqueue
    pub fn enqueue_batch<T: Serialize>(messages: &[T]) -> EnqueueBatchBuilder<T>;
}
```

### 3.4 Direct Methods on Store

```rust
// Methods directly on Store (like sqlx's Executor trait)
impl AnyStore {
    /// Direct enqueue without builder
    pub async fn enqueue<T: Serialize>(
        &self,
        queue: &str,
        message: &T,
    ) -> Result<MessageId>;

    /// Direct dequeue without builder
    pub async fn dequeue<T: DeserializeOwned>(
        &self,
        queue: &str,
    ) -> Result<Option<Message<T>>>;

    /// Direct batch enqueue
    pub async fn enqueue_batch<T: Serialize>(
        &self,
        queue: &str,
        messages: &[T],
    ) -> Result<Vec<MessageId>>;
}
```

**Usage comparison:**

```rust
// Builder pattern (more options)
pgqrs::enqueue(&order)
    .to("orders")
    .priority(Priority::High)
    .delay(Duration::from_secs(30))
    .execute(&store)
    .await?;

// Direct method (simpler, fewer options)
store.enqueue("orders", &order).await?;
```

### 3.5 EnqueueBuilder - Message Production

```rust
// Simple enqueue
pgqrs::enqueue(&my_payload)
    .to("email-queue")
    .execute(&store)
    .await?;

// With options
pgqrs::enqueue(&my_payload)
    .to("email-queue")
    .priority(Priority::High)
    .delay(Duration::from_secs(30))
    .metadata(json!({"source": "api"}))
    .execute(&store)
    .await?;

// Batch enqueue
pgqrs::enqueue_batch(&[msg1, msg2, msg3])
    .to("email-queue")
    .execute(&store)
    .await?;
```

**Builder Implementation:**

```rust
pub struct EnqueueBuilder<'a, T> {
    message: &'a T,
    queue: Option<String>,
    priority: Option<Priority>,
    delay: Option<Duration>,
    metadata: Option<Value>,
}

impl<'a, T: Serialize> EnqueueBuilder<'a, T> {
    pub fn to(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Execute against any Store implementation
    pub async fn execute<S: Store>(self, store: &S) -> Result<MessageId> {
        let queue = self.queue.ok_or(Error::MissingQueue)?;

        // Get or create a transient producer
        let producer = store.transient_producer(&queue).await?;

        // Build envelope
        let envelope = MessageEnvelope {
            payload: serde_json::to_value(self.message)?,
            priority: self.priority.unwrap_or_default(),
            scheduled_at: self.delay.map(|d| Utc::now() + d),
            metadata: self.metadata,
        };

        producer.enqueue(&envelope).await
    }
}
```

### 3.6 DequeueBuilder - Message Consumption

```rust
// Dequeue single message
let msg = pgqrs::dequeue()
    .from("email-queue")
    .fetch_one(&store)
    .await?;

// Dequeue batch
let messages = pgqrs::dequeue()
    .from("email-queue")
    .batch(10)
    .visibility_timeout(Duration::from_secs(60))
    .fetch_all(&store)
    .await?;

// Streaming dequeue with auto-ack
pgqrs::dequeue()
    .from("email-queue")
    .stream(&store)
    .try_for_each(|msg| async {
        process(msg.payload()).await?;
        msg.ack().await
    })
    .await?;
```

**Builder Implementation:**

```rust
pub struct DequeueBuilder {
    queue: Option<String>,
    batch_size: usize,
    visibility_timeout: Option<Duration>,
}

impl DequeueBuilder {
    pub fn from(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_string());
        self
    }

    pub fn batch(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn visibility_timeout(mut self, timeout: Duration) -> Self {
        self.visibility_timeout = Some(timeout);
        self
    }

    /// Fetch one message
    pub async fn fetch_one<S: Store>(self, store: &S) -> Result<Option<Message>> {
        let queue = self.queue.ok_or(Error::MissingQueue)?;
        let consumer = store.transient_consumer(&queue).await?;
        consumer.dequeue(1, self.visibility_timeout).await.map(|v| v.into_iter().next())
    }

    /// Fetch batch of messages
    pub async fn fetch_all<S: Store>(self, store: &S) -> Result<Vec<Message>> {
        let queue = self.queue.ok_or(Error::MissingQueue)?;
        let consumer = store.transient_consumer(&queue).await?;
        consumer.dequeue(self.batch_size, self.visibility_timeout).await
    }

    /// Stream messages continuously
    pub fn stream<'s, S: Store + 's>(
        self,
        store: &'s S,
    ) -> impl Stream<Item = Result<Message>> + 's {
        // Implementation using async_stream or similar
    }
}
```

### 3.7 AdminBuilder - Queue Administration

```rust
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

// Install schema
pgqrs::admin()
    .install()
    .execute(&store)
    .await?;

// Verify installation
pgqrs::admin()
    .verify()
    .execute(&store)
    .await?;
```

### 3.8 WorkflowBuilder - Workflow Operations

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

## 4. Store Trait Extensions

To support transient operations, the `Store` trait gains two new methods:

```rust
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    // Existing methods...

    /// Create a transient producer (no worker registration)
    /// Used for one-off sends where worker lifecycle is not needed
    async fn transient_producer(&self, queue: &str) -> Result<Box<dyn Producer>>;

    /// Create a transient consumer (no worker registration)
    /// Used for one-off receives where worker lifecycle is not needed
    async fn transient_consumer(&self, queue: &str) -> Result<Box<dyn Consumer>>;
}
```

**Implementation Notes:**
- Transient workers use worker_id = 0 (or a special sentinel)
- They don't participate in heartbeat/health checking
- Suitable for CLI tools, scripts, and simple applications
- Not suitable for high-volume production consumers (use full workers instead)

---

## 5. Comparison with sqlx

| sqlx Pattern | pgqrs Builder | pgqrs Direct |
|-------------|---------------|---------------|
| `sqlx::query("SQL")` | `pgqrs::enqueue(&msg)` | - |
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
use pgqrs::{AnyStore, enqueue, dequeue, admin, step};
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
    let store = AnyStore::connect("postgres://localhost/myapp").await?;

    // Setup (one-time)
    pgqrs::admin().install().execute(&store).await?;
    pgqrs::admin().create_queue("orders").execute(&store).await?;

    // === Pattern 1: Builder API (more options) ===
    let order = OrderCreated {
        order_id: "ORD-123".into(),
        customer_id: "CUST-456".into(),
        total: 99.99,
    };

    let msg_id = pgqrs::enqueue(&order)
        .to("orders")
        .priority(Priority::High)
        .execute(&store)
        .await?;

    println!("Enqueued message: {}", msg_id);

    // === Pattern 2: Direct Store methods (simpler) ===
    let msg_id = store.enqueue("orders", &order).await?;

    // Dequeue using builder (with options)
    if let Some(msg) = pgqrs::dequeue()
        .from("orders")
        .visibility_timeout(Duration::from_secs(30))
        .fetch_one(&store)
        .await?
    {
        let order: OrderCreated = msg.payload()?;
        println!("Processing order: {}", order.order_id);

        process_order(&order).await?;
        msg.ack().await?;
    }

    // Dequeue using direct method (simple case)
    if let Some(msg) = store.dequeue::<OrderCreated>("orders").await? {
        process_order(&msg.payload).await?;
        msg.ack().await?;
    }

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
// Old API (still works for long-running workers)
let producer = store.producer("orders").await?;
producer.enqueue(&msg).await?;

// New Builder API
pgqrs::enqueue(&msg).to("orders").execute(&store).await?;

// New Direct API
store.enqueue("orders", &msg).await?;
```

Teams can choose the appropriate style:

| Use Case | Recommended API |
|----------|------------------|
| Long-running workers | `store.producer()` / `store.consumer()` |
| One-off operations | `store.enqueue()` / `store.dequeue()` |
| Operations with options | `pgqrs::enqueue().priority().delay()` |

---

## 9. Implementation Plan

### Phase 1: Core APIs
1. Implement direct methods: `store.enqueue()`, `store.dequeue()`
2. Implement `EnqueueBuilder` and `pgqrs::enqueue()`
3. Implement `DequeueBuilder` and `pgqrs::dequeue()`

### Phase 2: Admin Builders
1. Implement `AdminBuilder` and `pgqrs::admin()`
2. Add shortcuts for common operations

### Phase 3: Workflow Builders
1. Implement `WorkflowBuilder` and `pgqrs::workflow()`
2. Implement `StepBuilder` and `pgqrs::step()`

### Phase 4: Streaming
1. Add `DequeueBuilder::stream()` with proper backpressure
2. Consider integration with `tokio-stream`

---

## 10. Open Questions

1. ~~**Naming:** `pgqrs::send` vs `pgqrs::enqueue` vs `pgqrs::publish`?~~ **Resolved:** Use `enqueue`/`dequeue` (queue terminology)
2. ~~**One API or two?**~~ **Resolved:** Both builder and direct methods (like sqlx)
3. **Error types:** Should builder errors be distinct from execution errors?
4. **Connection pooling:** How do transient operations interact with pool limits?
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
