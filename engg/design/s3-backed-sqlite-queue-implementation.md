# S3-Backed SQLite Queue (s3-sqlite Broker) - Design Document

**Status**: Draft Design
**Created**: 2026-02-20
**Epic**: Distributed Object Storage Queue

---

## 1. Problem Statement & Overview

**The Goal**: Provide a highly scalable, distributed durable execution queue backed purely by S3 (or any object storage) while leveraging SQLite's querying and transactional power as the underlying state engine.

**The Benefit**: This architecture marries the "infinitely scalable, pay-per-request" cost model of S3 with the rich, atomic SQL capabilities of `pgqrs`. It enables zero-maintenance distributed queues that require no running database servers.

**Design Priorities (Testability & Debuggability)**: To maximize operational visibility, testability, and deterministic behavior, the `S3Store` acts as the primary owner of the local database. It provides explicit APIs to the application allowing developers to choose between **Local Operations** (fast, optimistic) and **Durable Operations** (wait for S3). Synchronizing state to S3 is handled by an isolated, predictable background task owned by the `S3Store`.

---

## 2. Terminology

| Term | Definition |
|------|------------|
| **S3Store** | The primary object implementing `Store`. It manages the SQLite connection, exposes the new durability APIs, and spawns the Sync Task. |
| **Sync Task** | A Tokio background thread (or "scheduled executor") that periodically wakes up (or is triggered by write volume) to push the local SQLite file to S3. |
| **Local Write** | An operation committed to the local SQLite DB only. Returns immediately. Vulnerable to data loss if the node crashes before the next Sync Task. |
| **Durable Write** | An operation that commits to the local SQLite DB, queues a Sync Task, and blocks the caller until the Sync Task successfully persists the DB to S3. |
| **Local Read (Uncommitted Read)** | Reads data from the local SQLite DB, which may include data that has not yet been safely persisted to S3. |
| **Durable Read (Committed Read)** | Reads data only after ensuring the DB is synchronized with the latest confirmed S3 state. |
| **CAS (Compare-and-Swap)** | Using S3 `If-Match` ETags to ensure the Sync Task doesn't overwrite a newer queue state uploaded by a different node. |

---

## 3. The Durability API (Store Modes)

To provide maximum testability and avoid API bloat, durability is configured at the **Store Level**. When an `S3Store` is instantiated, it is given a specific mode that dictates its transaction and durability semantics. This mode cannot be changed. If a different mode is needed, a new `S3Store` instance must be created.

### 3.1 Store Configuration Modes

**`DurabilityMode::Durable` (Strict Consistency)**
- **Writes:** Operations are executed locally, but the client **blocks** (loops in a wait) until the background Sync Task successfully uploads the local DB to S3.
- **Reads:** Reads strictly reflect what has been globally committed to S3. Local, un-flushed writes happen in isolation and are hidden from reads until the S3 upload succeeds.
- *Equivalency:* `READ COMMITTED` / Synchronous replication. Maximum safety, higher latency.

**`DurabilityMode::Local` (Optimistic)**
- **Writes:** Operations are committed to the local DB only, and the client returns immediately. The Sync Task uploads the DB in the background. Highly performant, but risks data loss on node crash.
- **Reads:** Reads instantly see all local writes, regardless of whether they have reached S3 yet.
- *Equivalency:* `READ UNCOMMITTED` / Asynchronous commit. Extreme performance for localized, single-node processing pipelines.

---

## 4. Architecture & Component Design

### 4.1 "Hiding Local Changes": The Dual-File Architecture

To enforce `Durable` mode where reads must *not* see un-synced local writes, the `S3Store` manages **two physical connection stores** mapped to two identical, but purposefully segregated, SQLite files.

1. **`read_db.sqlite`**: Represents the globally confirmed state. It is a direct copy of the most recent successful S3 download. All read operations (`dequeue`, `list_workflows`) execute strictly against this file.
2. **`write_db.sqlite`**: The staging database. Mutating operations (`enqueue`, `update_status`) write here.

**How it works during a Durable Write:**
1. Client calls `enqueue()`.
2. `S3Store` writes the payload to `write_db.sqlite`.
3. `S3Store` registers a completion channel with the Sync Task and loops in an `.await` waiting for the channel to resolve.
4. *Meanwhile, a fast `dequeue` occurs from another worker.* It hits `read_db.sqlite`. It does **not** see the new payload, because it hasn't hit S3 yet.
5. The Sync Task activates. It uploads `write_db.sqlite` to S3.
6. The Upload Succeeds: The Sync Task issues an OS-level file copy from `write_db.sqlite` overwriting `read_db.sqlite`.
7. The Sync Task resolves the client's completion channel. The `enqueue()` call unblocks.
8. The next `dequeue` hits the newly updated `read_db.sqlite` and sees the committed payload.

*Note for `Local` Mode:* If configured in Local mode, both the read and write connection stores simply point to the identical `write_db.sqlite` file, meaning reads instantly see all local writes.

### 4.2 The `S3Store` Struct

The `S3Store` owns the connection stores and controls the isolation logic.

```rust
pub struct S3Store {
    // Write store (always mapped to write_db.sqlite)
    write_store: SqliteStore,

    // Read store (mapped to read_db.sqlite in Durable mode, write_db.sqlite in Local mode)
    read_store: SqliteStore,

    // Store configuration defining the isolation behavior
    mode: DurabilityMode,

    // Coordination channels to block clients waiting for a Sync Task flush
    flush_notifier: Arc<Notify>,
    last_flushed_seq: Arc<AtomicU64>,
    current_seq: Arc<AtomicU64>,

    // Channel to wake up the Sync Task early (e.g., threshold reached)
    wake_sync_task: Sender<()>,
}
```

### 4.2 The Background "Sync Task" (Scheduled Executor)

Rust does not have a built-in standard library "Scheduled Executor" (like Java's `ScheduledExecutorService`). In the async/Tokio ecosystem, this is universally implemented as a spawned `tokio::task` running a `select!` loop with a `tokio::time::interval`.

```rust
async fn run_sync_task(
    store: Weak<S3Store>, // Weak ref to stop if store is dropped
    s3_client: S3Client,
    bucket: String,
    key: String,
    mut wake_rx: Receiver<()>,
    interval_ms: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    let mut current_etag = Option::String;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Time-based flush
                flush_to_s3(&store, &s3_client, &bucket, &key, &mut current_etag).await;
            }
            Some(_) = wake_rx.recv() => {
                // Threshold or Durable Write forced a flush
                flush_to_s3(&store, &s3_client, &bucket, &key, &mut current_etag).await;
            }
            else => break, // Channel closed (Store dropped)
        }
    }
}
```

*Note on Testability:* Because `wake_rx` and `interval` are explicitly managed, during unit testing, we can set the interval to infinity and manually send `wake_rx` signals to deterministically step through the Sync Task logic without race conditions.

---

## 5. Recovery Semantics & Failure Modes

When dealing with distributed environments, network failures and concurrent writes across multiple pods are inevitable.

### Scenario A: S3 CAS Failure (Lost the race)
**Condition:** Two nodes run `S3Store` in Durable Mode. Both accept local writes. Node A flushes to S3 first and updates the global ETag. Node B reaches its Sync Task and attempts to flush.
**Result:** Node B gets a `PreconditionFailed` error from AWS because its `If-Match` ETag is stale.
**Recovery Strategy: Fast-Fail (Abort)**
Automatic conflict resolution on relational data is prone to severe bugs (e.g. sequence overlaps, invisible deadlocks). Therefore, we adopt a rigid fast-fail approach:
1. Node B's Sync Task aborts the S3 upload.
2. The Sync Task resolves all currently blocking local client `enqueue()` channels with a `ConflictError`. (These clients are thrown an exception, aborting their operations).
3. The Sync Task initiates a total state reset:
   - Discards `write_db.sqlite`.
   - Re-downloads the fresh DB from S3 (which now contains Node A's global writes).
   - Reinitializes `read_db.sqlite` and `write_db.sqlite`.
4. Result: The node is healthy again. The original clients that were looping/blocking on their local writes receive the failure and can gracefully retry their business logic against the fresh state.

### Scenario B: Node Crash with Un-flushed Local Writes
**Condition:** Node accepts 20 writes. Before the Sync Task executes, the node's power is cut.
**Result:** Data is lost from S3's perspective.
**Consistency Impact:**
- If the Store was in `Local` mode, this is accepted data loss.
- If the Store was in `Durable` mode, this is entirely safe: the 20 clients were still looping in `.await` waiting for the flush. From the client application's perspective, the connection dropped before the API call returned. No false acknowledgments were made.

### Scenario C: Sync Task Network Timeout
**Condition:** The Sync Task attempts to `PutObject` but the AWS network times out.
**Result:** Durable writes waiting on `flush_notifier.notified()` remain blocked. Local Writes continue to accumulate in `write_db.sqlite`.
**Recovery:** The Sync Task retries with exponential backoff. Once the network recovers, the flush succeeds, `read_db.sqlite` is updated, and all blocked Durable clients are unblocked simultaneously.

---

## 6. Python Bindings

The Python bindings map perfectly to this structure. Note that durability is configured on the store itself.

```python
# Create a deeply durable store (the default behavior)
store = await S3Store.create(bucket="my-pgqrs-bucket", mode="durable")

# Producer
producer = await store.producer("jobs")

# Write executes locally, loops in wait until S3 Sync Task Completes
await producer.enqueue({"data": 1})

# Consumer reads are strictly evaluating the S3-confirmed state
consumer = await store.consumer("jobs")
job = await consumer.dequeue()
```

If pure performance is preferred over durability, a node configures a local store:

```python
# Create a local optimistic store
store = await S3Store.create(bucket="my-pgqrs-bucket", mode="local")

producer = await store.producer("jobs")
consumer = await store.consumer("jobs")

# Returns instantly. Sync Task will upload in background
await producer.enqueue({"data": 2})

# Returns instantly, capable of reading the un-synced "data: 2" message
job = await consumer.dequeue()
```

The underlying Rust background task is dropped automatically when the `S3Store` object is garbage collected in Python, ensuring clean resource management without requiring explicit `.shutdown()` calls.
