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

**sqlx's approach** offers a more ergonomic pattern for database operations:

```rust
// sqlx - query returns an executor-accepting builder
sqlx::query("SELECT * FROM users")
    .fetch_all(&pool)
    .await?;
```

This proposal adapts sqlx's pattern for queue and workflow operations.

---

## 2. Design Goals

1. **Minimal boilerplate** for one-off operations
2. **No hidden state** - operations take pool/store explicitly
3. **Composable** - chain operations naturally
4. **Type-safe** - leverage Rust's type system
5. **Backward compatible** - new API coexists with existing worker-based API

---

## 3. Proposed API

### 3.1 Entry Points

```rust
// Core entry points in `pgqrs` crate root
pub mod pgqrs {
    /// Queue administration
    pub fn admin() -> AdminBuilder;
    
    /// Message production
    pub fn send<T: Serialize>(message: &T) -> SendBuilder<T>;
    
    /// Message consumption
    pub fn receive() -> ReceiveBuilder;
    
    /// Workflow operations
    pub fn workflow<I: Serialize>(input: &I) -> WorkflowBuilder<I>;
    
    /// Step execution
    pub fn step(step_id: &str) -> StepBuilder;
}
```

### 3.2 SendBuilder - Message Production

```rust
// Simple send
pgqrs::send(&my_payload)
    .to("email-queue")
    .execute(&store)
    .await?;

// With options
pgqrs::send(&my_payload)
    .to("email-queue")
    .priority(Priority::High)
    .delay(Duration::from_secs(30))
    .metadata(json!({"source": "api"}))
    .execute(&store)
    .await?;

// Batch send
pgqrs::send_batch(&[msg1, msg2, msg3])
    .to("email-queue")
    .execute(&store)
    .await?;
```

**Builder Implementation:**

```rust
pub struct SendBuilder<'a, T> {
    message: &'a T,
    queue: Option<String>,
    priority: Option<Priority>,
    delay: Option<Duration>,
    metadata: Option<Value>,
}

impl<'a, T: Serialize> SendBuilder<'a, T> {
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

### 3.3 ReceiveBuilder - Message Consumption

```rust
// Receive single message
let msg = pgqrs::receive()
    .from("email-queue")
    .fetch_one(&store)
    .await?;

// Receive batch
let messages = pgqrs::receive()
    .from("email-queue")
    .batch(10)
    .visibility_timeout(Duration::from_secs(60))
    .fetch_all(&store)
    .await?;

// Streaming receive with auto-ack
pgqrs::receive()
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
pub struct ReceiveBuilder {
    queue: Option<String>,
    batch_size: usize,
    visibility_timeout: Option<Duration>,
}

impl ReceiveBuilder {
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

### 3.4 AdminBuilder - Queue Administration

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

### 3.5 WorkflowBuilder - Workflow Operations

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

| sqlx Pattern | pgqrs Equivalent |
|-------------|------------------|
| `sqlx::query("SQL")` | `pgqrs::send(&msg)` |
| `.bind(value)` | `.to("queue").priority(High)` |
| `.fetch_one(&pool)` | `.execute(&store)` |
| `.fetch_all(&pool)` | `.execute(&store)` (batch) |

**Key Differences:**
- sqlx operates on raw SQL; pgqrs operates on typed messages
- sqlx returns rows; pgqrs returns message IDs or messages
- pgqrs includes queue semantics (visibility, DLQ, etc.)

---

## 6. Full Example

```rust
use pgqrs::{AnyStore, send, receive, admin, step};
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
    
    // Send message (producer side)
    let order = OrderCreated {
        order_id: "ORD-123".into(),
        customer_id: "CUST-456".into(),
        total: 99.99,
    };
    
    let msg_id = pgqrs::send(&order)
        .to("orders")
        .priority(Priority::Normal)
        .execute(&store)
        .await?;
    
    println!("Enqueued message: {}", msg_id);
    
    // Receive message (consumer side)
    if let Some(msg) = pgqrs::receive()
        .from("orders")
        .visibility_timeout(Duration::from_secs(30))
        .fetch_one(&store)
        .await?
    {
        let order: OrderCreated = msg.payload()?;
        println!("Processing order: {}", order.order_id);
        
        // Process and acknowledge
        process_order(&order).await?;
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

The functional API is additive - existing code continues to work:

```rust
// Old API (still works)
let producer = store.producer("orders").await?;
producer.enqueue(&msg).await?;

// New API (coexists)
pgqrs::send(&msg).to("orders").execute(&store).await?;
```

Teams can migrate gradually or use both styles based on use case:
- **Workers (long-running):** Use existing `store.consumer()` API
- **Scripts/CLI/One-offs:** Use new `pgqrs::send()` / `pgqrs::receive()` API

---

## 9. Implementation Plan

### Phase 1: Core Builders
1. Implement `SendBuilder` and `pgqrs::send()`
2. Implement `ReceiveBuilder` and `pgqrs::receive()`
3. Add `transient_producer` / `transient_consumer` to Store trait

### Phase 2: Admin Builders
1. Implement `AdminBuilder` and `pgqrs::admin()`
2. Add shortcuts for common operations

### Phase 3: Workflow Builders
1. Implement `WorkflowBuilder` and `pgqrs::workflow()`
2. Implement `StepBuilder` and `pgqrs::step()`

### Phase 4: Streaming
1. Add `ReceiveBuilder::stream()` with proper backpressure
2. Consider integration with `tokio-stream`

---

## 10. Open Questions

1. **Naming:** `pgqrs::send` vs `pgqrs::enqueue` vs `pgqrs::publish`?
2. **Error types:** Should builder errors be distinct from execution errors?
3. **Connection pooling:** How do transient workers interact with pool limits?
4. **Telemetry:** Should builders support injecting tracing spans?

---

## 11. Summary

This proposal introduces a sqlx-inspired functional API for pgqrs that:

- Reduces boilerplate for simple operations
- Maintains type safety through builders
- Coexists with the existing worker-based API
- Follows Rust idioms for fluent interfaces

The implementation is straightforward given the existing trait infrastructure, and can be rolled out incrementally.
