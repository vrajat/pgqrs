# Store Access and Serialized DB Design

**Status:** Draft Design  
**Created:** 2026-03-20  
**Epic:** SQLite/Turso single-process usability

---

## 1. Overview

This document proposes a shared internal database-control abstraction that can be used across backends to route:

- read operations
- write operations
- backend-specific consistency policy layered on top of writes

The immediate driver is SQLite and Turso usability in a multi-threaded single-process application.
Today, SQLite/Turso are intended to be useful as local queue and workflow engines, but concurrent producer and consumer activity can fail quickly with `database is locked` errors.

The S3 backend already contains a useful pattern:

- a wrapper object owns a concrete local store
- read calls are routed through `with_read(...)`
- write calls are routed through `with_write(...)`
- extra policy can be added around write execution

That pattern currently lives inside `store/s3` as `SyncDb` plus `Tables<DB>`.
This document proposes extracting the generic part to the top-level store layer, keeping S3-specific sync/snapshot semantics in an S3-only derived trait, and introducing a reusable `SerializedLock` wrapper for SQLite/Turso.

Naming note:

- `Store` should remain reserved for the business-logic surface
- lower-level database-control abstractions should use `Db` and `Lock` oriented naming
- the generic trait in this design is `DbLock`
- the write-serialization wrapper in this design is `SerializedLock`
- the shared router shape is `Tables<Lock>`
- the actual naming cleanup should be phased and happen last
- S3 names should not be changed in the first implementation pass

---

## 2. Problem Statement

### 2.1 Product problem

For PostgreSQL, multi-process and multi-writer concurrency is a first-class operating mode.
For SQLite and Turso, it is not.

However, SQLite/Turso should still be usable for:

- a local queue in one process
- a local workflow engine in one process
- a multi-threaded application with at least one producer and one consumer

That requires the library to absorb backend write contention gracefully.
It is not sufficient to say that retry/backoff is purely an application problem.

### 2.2 Current architectural problem

The current S3 implementation has already introduced a reusable pattern, but it is trapped inside the S3 module:

- `SyncDb` combines store access routing with S3 durability semantics
- `Tables<DB>` is nested under `store/s3`
- `SnapshotDb` and `ConsistentDb` implement useful write-policy hooks, but only in the S3 context

This causes two issues:

1. The generic part of the abstraction cannot be reused cleanly by SQLite/Turso.
2. The S3 trait boundary is wider than necessary for non-S3 backends.
3. The current architecture does not yet expose a shared top-level `DbLock` abstraction even though that is the correct naming direction for the extracted generic layer.

`SyncDb` itself documents this problem: it explicitly says engineers should treat it as an S3 durability boundary, not a generic cross-backend abstraction.

---

## 3. Goals

- Extract a reusable internal trait for read/write routing across stores.
- Preserve the current S3 design intent without forcing S3 semantics onto other backends.
- Make it straightforward to add a serialized-write wrapper for SQLite and Turso.
- Move `Tables<Lock>` into a shared parent module so multiple backends can reuse it.
- Keep the public API unchanged.
- Keep backend-specific concurrency policy internal.
- Defer naming cleanup until the end so logic and algorithm changes land first.

---

## 4. Non-Goals

- Replacing the public `Store` trait.
- Changing user-facing `Admin`, `Producer`, `Consumer`, or workflow APIs.
- Making SQLite/Turso multi-process backends.
- Solving every lock-contention case in this document.
- Implementing batching as part of the first serialized-write design.

---

## 5. Current State

### 5.1 Useful parts of the current S3 design

The current S3 stack has three important ideas:

1. A backend wrapper can own a concrete local store and expose policy hooks.
2. Table operations can be routed centrally through `with_read(...)` and `with_write(...)`.
3. Different write policies can be implemented by wrapping `with_write(...)`.

This is visible in:

- `SyncDb` in `crates/pgqrs/src/store/s3/mod.rs`
- `Tables<DB>` in `crates/pgqrs/src/store/s3/tables.rs`
- `SnapshotDb` in `crates/pgqrs/src/store/s3/snapshot.rs`
- `ConsistentDb` in `crates/pgqrs/src/store/s3/consistent.rs`

### 5.2 Limits of the current `SyncDb`

`SyncDb` currently mixes:

- generic store access:
  - `config()`
  - `concurrency_model()`
  - `with_read_ref()`
  - `with_write_ref()`
  - `with_read()`
  - `with_write()`
- S3-specific lifecycle:
  - `snapshot()`
  - `sync()`

That is too broad for SQLite/Turso.
Those backends need read/write routing and write policy, but they do not need object-store synchronization semantics.

### 5.3 Limits of current backend structure

`Tables<DB>` currently lives in `store/s3`, even though it is not conceptually S3-specific.
It is a generic read/write router over table traits.

As a result:

- S3 benefits from centralized routing
- SQLite/Turso/Postgres cannot reuse the same routing mechanism without awkward module dependencies

---

## 6. Proposed Architecture

### 6.1 Split the abstraction into two layers

Introduce two internal traits:

1. `DbLock`
2. `SyncDb`

`DbLock` becomes the top-level reusable trait.
`SyncDb` becomes an S3-specific extension of `DbLock`.

### 6.2 Generic `DbLock` trait

`DbLock` captures the generic routing and policy boundary:

```rust
#[async_trait]
pub trait DbLock: Clone + Send + Sync + 'static {
    fn config(&self) -> &Config;
    fn concurrency_model(&self) -> ConcurrencyModel;

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;
}
```

This trait is the reusable seam for:

- direct pass-through stores
- serialized-write stores
- durable-write stores
- retrying-write stores

### 6.3 Naming direction

The naming in this design is:

- generic lower-level trait name is `DbLock`
- lock/policy wrappers use `Lock`-oriented naming where appropriate
- `SyncDb` remains unchanged in the first implementation pass even though it is S3-specific

Two constraints matter:

1. do not overload `Store`, because that name is already used for business logic
2. do not rename current S3 types in the first pass, because that mixes naming churn with logic refactoring

The deliberate phase split is:

- use `DbLock`, `SerializedLock`, and `Tables<Lock>` for the extracted shared layer
- keep current S3 type names unchanged initially
- only review S3-local naming later after the logic is stable

### 6.4 Proposed S3-specific derived trait

S3 should extend `DbLock` with remote-state operations:

```rust
#[async_trait]
pub trait SyncDb: DbLock {
    async fn snapshot(&mut self) -> Result<()>;
    async fn sync(&mut self) -> Result<()>;
}
```

This preserves the current S3 semantics without forcing them into the generic layer.

### 6.5 Shared `Tables<Lock>` moves to parent modules

Move the current `Tables<DB>` out of `store/s3/tables.rs` into the shared top-level file:

- `crates/pgqrs/src/store/dblock.rs`

This shared router should depend on `DbLock`, not `SyncDb`.

That gives all backends access to the same routing structure.

The intended generic form is:

```rust
pub struct Tables<Lock>
where
    Lock: DbLock,
{
    lock: Lock,
}
```

### 6.6 Introduce `SerializedLock`

Add a new generic internal wrapper:

```rust
pub struct SerializedLock<DB> {
    inner: DB,
    write_gate: Arc<tokio::sync::Mutex<()>>,
}
```

Where `DB` is a concrete local database target, initially:

- `SqliteStore`
- `TursoStore`

Its policy is simple:

- reads go straight through
- writes are serialized through a single async mutex
- optional bounded retry/backoff may be applied inside `with_write(...)`

This is not a queue actor.
It is a serialized write-policy wrapper over an existing store.

### 6.7 Backend mapping after refactor

| Backend | Access type | Notes |
|---|---|---|
| PostgreSQL | direct store or thin pass-through `DbLock` | no write serialization needed |
| SQLite | `SerializedLock<SqliteStore>` | first-class single-process write policy |
| Turso | `SerializedLock<TursoStore>` | same policy shape as SQLite |
| S3 local | `SnapshotDb: SyncDb` | extends `DbLock` |
| S3 durable | `ConsistentDb: SyncDb` | extends `DbLock`, adds sync-after-write |

---

## 7. Why `SerializedLock` Instead of Reusing `SyncDb` Directly

`SyncDb` is close, but not the right top-level trait as written.

Reasons:

- `snapshot()` and `sync()` are irrelevant to SQLite/Turso.
- the name `SyncDb` communicates object-store replication, not generic store access
- the current trait contract is intentionally framed as S3-specific
- keeping S3 semantics in the generic trait would blur the design boundary

The correct move is:

- preserve the pattern
- extract the generic part
- narrow `SyncDb` to the S3-specific extension

---

## 8. Detailed Design

### 8.1 New module structure

Proposed direction:

```text
crates/pgqrs/src/store/
  mod.rs
  any.rs
  dblock.rs            # StoreOpFuture, DbLock, SerializedLock, Tables<Lock>, maybe DirectDb
  postgres/
  sqlite/
  turso/
  s3/
    mod.rs             # SyncDb extends DbLock
    snapshot.rs
    consistent.rs
```

The intended filename is `dblock.rs`.

`DbLock`, `SerializedLock`, and `Tables<Lock>` can all live together there.

The ownership boundaries should be:

- top-level store access abstractions live under `store/`
- S3-specific sync abstractions live under `store/s3`
- backend implementations depend inward on shared store access modules

### 8.2 `Tables<Lock>` becomes a generic router

`Tables<Lock>` should become the shared implementation for:

- `MessageTable`
- `QueueTable`
- `WorkerTable`
- `WorkflowTable`
- `RunRecordTable`
- `StepRecordTable`

Its responsibility remains:

- route read methods through `with_read(...)`
- route mutating methods through `with_write(...)`

This is already the correct abstraction.
The problem is only placement and trait dependency.

### 8.3 Concrete stores remain concrete

The existing concrete stores should remain responsible for backend-native SQL and low-level setup:

- `SqliteStore`
- `TursoStore`
- `PostgresStore`

They should not be forced to implement `DbLock` directly if that complicates ownership.
A thin wrapper is acceptable.

Two reasonable options:

1. `SerializedLock<SqliteStore>` and `SerializedLock<TursoStore>` wrap concrete stores directly.
2. Introduce a minimal pass-through `DirectDb<DB>` wrapper so all routed backends have the same shape.

Option 2 is more uniform and is likely cleaner if `Tables<Lock>` becomes widely used.

### 8.4 Write serialization policy

`SerializedLock` should serialize all mutating operations exposed via `with_write(...)`.

That includes at least:

- enqueue and batch enqueue
- dequeue claim/update
- archive/delete/release/visibility extension
- worker registration and state changes
- workflow run/step state mutation
- bootstrap and raw write SQL

It should not serialize read-only calls.

### 8.5 Retry/backoff policy

Write serialization should be the first line of defense.
Bounded retry/backoff should remain a second line of defense for:

- SQLite `SQLITE_BUSY`
- SQLite `SQLITE_LOCKED`
- Turso/libSQL lock-contention equivalents

This retry policy should live close to `SerializedLock::with_write(...)` or in a shared helper used by SQLite/Turso wrappers.

This is preferable to scattering retries inside many table implementations.

### 8.6 Relationship to current Turso retry logic

The current Turso code already contains retry logic in lower-level query execution helpers.
This design does not require deleting that immediately.

However, the long-term direction should be:

- centralize write serialization and retry policy at the access layer
- minimize duplicated retry logic in table/query helpers
- keep idempotency-sensitive DML paths explicit

That cleanup can happen incrementally after the access abstraction lands.

---

## 9. Implementation Direction

### Phase 1: Extract shared access trait

Create a new top-level store access module.

Tasks:

- move `StoreOpFuture` out of `store/s3/mod.rs`
- add `DbLock`
- narrow `store/s3::SyncDb` so it extends `DbLock`
- keep existing S3 type names unchanged

Acceptance:

- S3 compiles with no behavior change
- top-level store module owns the generic routing abstractions

### Phase 2: Move `Tables<Lock>` to shared store module

Tasks:

- move generic routed table implementations out of `store/s3/tables.rs`
- make them generic over `DbLock`
- keep S3 using the moved shared router

Acceptance:

- S3 behavior unchanged
- shared router is now usable by other backends

### Phase 3: Introduce serialized wrapper for SQLite/Turso

Tasks:

- add `SerializedLock<DB>`
- add write gate
- optionally add common retry helper for lock errors
- wire SQLite and Turso through routed tables instead of direct table fields where appropriate

Acceptance:

- same-process producer + consumer flows no longer fail immediately with lock errors
- write contention becomes bounded waiting/retry, not immediate backend failure

### Phase 4: Rationalize backend wiring

Tasks:

- decide whether PostgreSQL uses direct table fields or a pass-through access wrapper
- decide whether SQLite/Turso stores expose concrete table fields internally or only routed tables
- reduce duplicated retry logic in Turso where centralized policy makes it redundant

Acceptance:

- backend wiring is internally consistent
- read/write routing policy is obvious from module structure

### Phase 5: Naming cleanup

Tasks:

- review whether the surrounding helper names need any final cleanup beyond `DbLock`, `SerializedLock`, and `Tables<Lock>`
- explicitly review whether any S3-local names should change at all
- avoid mixing behavior changes into this phase

Acceptance:

- naming is consistent with the rule that `Store` means business logic and `Db` means database control
- behavior is unchanged from the previous phase
- S3 naming changes, if any, are isolated and mechanical

---

## 10. Public API Impact

None intended.

Users should continue using:

- `pgqrs::connect(...)`
- `pgqrs::connect_with_config(...)`
- `Producer`
- `Consumer`
- `Admin`
- workflow APIs

All changes are internal implementation and architecture changes.

---

## 11. Testing Strategy

### 11.1 Extraction safety

- existing S3 tests should continue to pass with no semantic changes
- table routing tests should verify read methods use `with_read(...)` and mutating methods use `with_write(...)`

### 11.2 SQLite/Turso serialization behavior

Add tests for:

- one producer + one consumer in one process
- multiple producer tasks in one process
- dequeue + archive under concurrent activity
- workflow step/run mutation under concurrent tasks
- bounded retry under induced lock contention

### 11.3 Regression tests

Specifically verify that:

- lock contention does not surface as immediate user-visible failure in ordinary single-process workloads
- write ordering remains correct under serialized execution
- read-only operations remain concurrent and are not needlessly gated

---

## 12. Alternatives Considered

### 12.1 Leave `SyncDb` as-is and reuse it everywhere

Rejected because:

- the name and API are S3-specific
- `snapshot()` / `sync()` are not generic store concerns
- it would permanently couple SQLite/Turso to object-store language

### 12.2 Add retries only

Rejected as the main design because:

- retries mitigate contention but do not shape concurrency
- under sustained contention, retries mostly convert failure into latency and noise
- the library still lacks a clear single-process write policy

### 12.3 Build a full actor-based write queue first

Deferred because:

- it is a bigger operational and architectural step
- a serialized wrapper gives most of the immediate value with much less machinery
- batching/fairness/prioritization can be added later if needed

### 12.4 Keep `Tables<DB>` inside S3 and duplicate similar wrappers elsewhere

Rejected because:

- it duplicates the same routing logic across backends
- it hides the correct abstraction boundary in the wrong module

---

## 13. Decisions

Resolved decisions carried into this design:

- PostgreSQL is out of scope for this refactor for now. Do not force it onto `DbLock` unless that becomes necessary later.
- Keep the refactor focused on SQLite/Turso for now.
- Prefer a generic `SerializedLock` over backend-specific `SerializedSqliteLock` and `SerializedTursoLock`, provided the implementation stays simple.
- Leave current Turso retry cleanup alone for now. Rationalizing lower-level Turso retries is a separate follow-up after the access layer lands.
- SQLite/Turso pool sizing defaults are a separate task and should not be folded into this change set.

---

## 14. Recommendation

Proceed with:

1. extract `DbLock` from the generic part of current `SyncDb`
2. narrow `SyncDb` into an S3-specific extension trait without renaming it yet
3. move `Tables<Lock>` into shared store infrastructure
4. add `SerializedLock` as the write-policy wrapper for SQLite/Turso
5. review S3-local naming only after the logic is stable

This keeps the current S3 design intact, gives SQLite/Turso a clear single-process concurrency policy, and establishes a reusable internal abstraction for future backend-specific consistency policies.
