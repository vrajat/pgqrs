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
- **Reads:** Reads strictly reflect what has been globally committed to S3. Before serving a durable read, the store checks the remote ETag/revision and, if it differs from local committed state, performs a **synchronous refresh** (`GET` + `read_db` rewrite) before returning data.
- **Read freshness guarantee:** Durable reads must not return stale committed data. They may block briefly to refresh if a newer committed S3 revision is detected.
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

**Durable Read Refresh Rule**
- Durable reads do not mutate local write state.
- Durable reads compare local committed revision (ETag) to remote committed revision.
- If revisions differ, the store synchronously refreshes local committed state (`read_db.sqlite`) from S3 before executing the read query.
- If revisions match, reads execute directly from `read_db.sqlite` without remote download.

**How it works during a Durable Write:**
1. Client calls `enqueue()`.
2. `S3Store` writes the payload to `write_db.sqlite`.
3. `S3Store` registers a completion channel with the Sync Task and loops in an `.await` waiting for the channel to resolve.
4. *Meanwhile, a fast `dequeue` occurs from another worker.* It hits `read_db.sqlite`. It does **not** see the new payload, because it hasn't hit S3 yet.
5. The Sync Task activates. It uploads `write_db.sqlite` to S3.
6. The Upload Succeeds: The Sync Task issues an OS-level file copy from `write_db.sqlite` overwriting `read_db.sqlite`.
7. The Sync Task resolves the client's completion channel. The `enqueue()` call unblocks.
8. The next `dequeue` hits the newly updated `read_db.sqlite` and sees the committed payload.

**Cross-node note:** If another node commits first, a durable reader on this node will detect the ETag mismatch and refresh `read_db.sqlite` synchronously before returning results.

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

---

## 7. Low-Level Implementation Plan

This section turns the architecture into executable implementation work in `pgqrs`.

### 7.1 Implementation Principles

- Keep `S3Store` as a thin orchestrator over existing `SqliteStore` table/worker logic.
- Avoid backend-wide trait churn unless required for shared APIs.
- Build deterministic test harnesses first for sync/CAS behavior, then wire full integration tests.
- Make conflict handling explicit and fail-fast (`ConflictError`/retry path), not hidden merge logic.

### 7.2 Phase Breakdown

#### Phase 0: Scaffolding and Feature Gating

**Target files**
- `crates/pgqrs/Cargo.toml`
- `crates/pgqrs/src/store/mod.rs`
- `crates/pgqrs/src/store/any.rs`
- `crates/pgqrs/src/config.rs`
- `crates/pgqrs/src/error.rs`

**Tasks**
- Add an `s3` feature and S3 client dependencies.
- Add `store::s3` module and `AnyStore::S3` variant.
- Add durability and sync config types:
  - `DurabilityMode` (`Durable`, `Local`)
  - `S3SyncConfig` (`flush_interval_ms`, `max_pending_ops`, `max_backoff_ms`, optional `vacuum_threshold`)
- Extend error taxonomy with a conflict/revision mismatch error used for CAS failures.

**Acceptance criteria**
- `cargo check --features "sqlite,s3"` passes.
- Non-s3 backends compile and behave unchanged.

#### Phase 1: Object Storage Client Abstraction

**Target files**
- `crates/pgqrs/src/store/s3/client.rs` (new)
- `crates/pgqrs/src/store/s3/mod.rs` (new)

**Tasks**
- Introduce a minimal object-storage trait used by sync logic:
  - `get_object(key) -> bytes + etag`
  - `put_object_if_match(key, bytes, etag) -> new_etag`
- Implement:
  - Real AWS S3 client adapter.
  - In-memory test fake with configurable conflict/timeouts.
- Normalize provider errors into `pgqrs::Error`.

**Acceptance criteria**
- Unit tests cover success path, timeout path, and CAS mismatch path.

#### Phase 2: Local State Manager (Dual-File SQLite)

**Target files**
- `crates/pgqrs/src/store/s3/state.rs` (new)
- `crates/pgqrs/src/store/s3/mod.rs` (new)

**Tasks**
- Create workspace layout for one logical queue DB state:
  - `write_db.sqlite`
  - `read_db.sqlite` (durable mode) or alias write DB (local mode)
- Startup bootstrap:
  - Download existing DB from S3 if present; otherwise initialize fresh DB and bootstrap schema.
  - Initialize both read/write files and `SqliteStore` handles.
- Add atomic file copy/promotion utility from write DB to read DB after successful flush.

**Acceptance criteria**
- Boot works for empty bucket and existing DB object.
- Reads in durable mode are isolated from unsynced local writes.

#### Phase 3: Sync Task and Flush Coordinator

**Target files**
- `crates/pgqrs/src/store/s3/sync.rs` (new)
- `crates/pgqrs/src/store/s3/mod.rs` (new)

**Tasks**
- Implement background sync loop using `tokio::select!` over:
  - periodic interval tick
  - explicit wake channel
  - shutdown channel/store drop
- Track mutation sequence (`current_seq`) and last committed sequence (`last_flushed_seq`).
- Implement durable wait path:
  - write op records target seq
  - blocks until `last_flushed_seq >= target_seq` or error
- Add exponential backoff retry on transient upload failures.

**Acceptance criteria**
- Durable writes block and unblock correctly after successful flush.
- Local mode writes return immediately while sync continues in background.

#### Phase 4: CAS Conflict Recovery

**Target files**
- `crates/pgqrs/src/store/s3/sync.rs` (new)
- `crates/pgqrs/src/store/s3/state.rs` (new)
- `crates/pgqrs/src/error.rs`

**Tasks**
- On CAS mismatch:
  - fail all pending durable waiters with conflict error
  - discard local write DB
  - re-download current object state
  - rebuild read/write stores and resume healthy operation
- Ensure no false acknowledgment is returned to durable callers during conflicts.

**Acceptance criteria**
- Concurrent writer simulation test verifies conflict + recovery flow.
- Post-recovery writes succeed against fresh ETag baseline.

#### Phase 5: Store API Wiring and Behavior Parity

**Target files**
- `crates/pgqrs/src/store/s3/mod.rs` (new)
- `crates/pgqrs/src/store/any.rs`
- `crates/pgqrs/src/lib.rs`
- `py-pgqrs/src/lib.rs`
- `py-pgqrs/python/pgqrs/__init__.pyi`

**Tasks**
- Implement `Store` for `S3Store` by delegating:
  - mutating operations to write store (+ durable/local sync semantics)
  - read operations to read store according to mode
- Expose Rust constructor APIs and Python bindings for S3-backed store creation.
- Preserve existing producer/consumer/admin ergonomics.

**Acceptance criteria**
- Existing APIs stay source-compatible for non-s3 users.
- New S3 store can run enqueue/dequeue lifecycle end-to-end.

#### Phase 6: Vacuum/Compaction and Observability

**Target files**
- `crates/pgqrs/src/store/s3/sync.rs` (new)
- `crates/pgqrs/src/store/s3/metrics.rs` (new, optional)
- `docs/user-guide/concepts/backends.md`

**Tasks**
- Add optional pre-flush compaction policy (`incremental_vacuum` or `VACUUM` thresholds).
- Emit structured metrics/logging:
  - flush latency
  - pending durable waiters
  - conflict count
  - last successful sync timestamp
- Document operational knobs and recommended defaults.

**Acceptance criteria**
- Compaction and telemetry can be validated in integration tests.

#### Phase 7: Test Matrix and Hardening

**Target files**
- `crates/pgqrs/tests/s3_store_tests.rs` (new)
- `crates/pgqrs/tests/s3_conflict_tests.rs` (new)
- `crates/pgqrs/tests/s3_recovery_tests.rs` (new)
- `docs/user-guide/guides/` (S3 usage guide)

**Tasks**
- Add deterministic tests for:
  - durable mode read isolation
  - local mode immediate visibility
  - CAS conflicts and fail-fast behavior
  - restart recovery semantics
- Add integration tests with fake object storage and optional real S3 smoke tests (guarded).

**Acceptance criteria**
- `cargo test --features "sqlite,s3"` passes with deterministic tests.
- S3 docs/guides include failure semantics and tuning guidance.

### 7.3 Open Design Items To Resolve Early

- `ADR-0003` currently says pgqrs should not own background daemons; this design introduces a store-owned sync task. Decide and document final direction before implementation starts.
- Confirm whether durable mode should apply to all mutating operations or only message enqueue/dequeue paths.
- Decide default behavior for missing object on startup: create-new vs fail-if-missing.

---

## 8. GitHub Issue Breakdown (Parent + Children)

Parent issue should track the epic and link the child implementation tracks below.

### Parent
- `S3-backed SQLite queue implementation (epic)`

### Children
- `S3 store scaffolding, feature flags, and config surface`
- `Object storage client abstraction and S3 adapter`
- `Dual-file SQLite state manager for durable/local read isolation`
- `Sync task coordinator and durable wait semantics`
- `CAS conflict handling and fast-fail recovery`
- `Store/API wiring and Python bindings for S3Store`
- `Compaction, observability, and S3 test matrix`
