# Database Abstraction Layer Implementation Plan

This document outlines the plan for implementing the database abstraction layer to support multiple backends (Postgres, SQLite, Turso).

## Goals
- Decouple `Producer`, `Consumer`, and `WorkerLifecycle` from direct SQLx dependency.
- Introduce `Store` trait and specific `Repository` traits for Queues, Messages, Workers, Archive, and Workflows.
- Implement `PostgresStore` as the reference implementation (preserving existing behavior).
- Add `SqliteStore` and `TursoStore` implementations.
- Update Python bindings to support the new abstraction.

## User Review Required
> [!IMPORTANT]
> The `Store` trait definition now includes `WorkflowStore`. The `Result` type alias in `src/store/mod.rs` was replaced with `std::result::Result` to avoid conflicts.

## Proposed Changes

### Core Abstraction (Phase 1 - Completed)
#### [MODIFY] [src/store/mod.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/mod.rs)
- Defined `Store`, `QueueStore`, `MessageStore`, `WorkerStore`, `ArchiveStore`, `WorkflowStore` traits.
- Updated signatures to use generic `Self::Error`.

### Phase 2a: Extend Store Traits (Issue #115)
#### [MODIFY] [src/store/mod.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/mod.rs)
- Add administrative/metric methods:
    - `Store::install()`, `Store::verify()`
    - `QueueStore::metrics(name)`, `QueueStore::list_metrics()`
    - `QueueStore::purge(name)`
    - `WorkerStore::health_stats(timeout)`
    - `WorkerStore::purge_stale(timeout)`
    - `MessageStore::dlq_batch(max_attempts)` (move to archive)
    - `MessageStore::count_active_by_worker(worker_id)`
    - `MessageStore::replay_dlq(archived_id)`

#### [MODIFY] [src/store/postgres/mod.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/mod.rs)
- Implement `install` (migrate) and `verify` (check tables).

#### [MODIFY] [src/store/postgres/queues.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/queues.rs)
- Implement metrics queries.

#### [MODIFY] [src/store/postgres/workers.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/workers.rs)
- Implement health stats and purge.

#### [MODIFY] [src/store/postgres/messages.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/messages.rs)
- Implement `count_active_by_worker` and `replay_dlq`.


### Postgres Implementation (Phase 2 - In Progress)
#### [NEW] [src/store/postgres/mod.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/mod.rs)
- Implements `Store` for `PostgresStore`.
- Composes `queues`, `messages`, `workers`, `archive`, `workflows` repositories.

#### [NEW] [src/store/postgres/queues.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/queues.rs)
- Implements `QueueStore` for Postgres.

#### [NEW] [src/store/postgres/messages.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/messages.rs)
- Implements `MessageStore` for Postgres.
- Includes `enqueue_batch`, `dequeue`, `extend_visibility` logic.

#### [NEW] [src/store/postgres/workers.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/workers.rs)
- Implements `WorkerStore` for Postgres.
- Handles atomic state transitions (Ready <-> Suspended <-> Stopped).

#### [NEW] [src/store/postgres/archive.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/archive.rs)
- Implements `ArchiveStore` for Postgres.
- Added DLQ listing and counting.

#### [NEW] [src/store/postgres/workflows.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/workflows.rs)
- Implements `WorkflowStore` for Postgres.

### Phase 2b: Refactor Workers (Issue #105)
#### [MODIFY] [src/store/mod.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/mod.rs)
- Add missing methods required by workers:
    - `MessageStore::count_active_by_worker(worker_id)`
    - `MessageStore::replay_dlq(archived_id)`

#### [MODIFY] [src/store/postgres/messages.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/store/postgres/messages.rs)
- Implement `count_active_by_worker` and `replay_dlq`.

#### [MODIFY] [src/worker/lifecycle.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/worker/lifecycle.rs)
- Change struct to `pub struct WorkerLifecycle<S: Store>`.
- Remove `pool: PgPool`, add `store: S`.
- Update `new` to accept `S`.
- Replace `sqlx` queries with `self.store.workers()...` calls.

#### [MODIFY] [src/worker/producer.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/worker/producer.rs)
- Change struct to `pub struct Producer<S: Store>`.
- Remove `pool: PgPool`, add `store: S`.
- Change `lifecycle` field to `WorkerLifecycle<S>`.
- Update `new` to accept `S`.
- Replace `sqlx` queries and `Messages` usage with `self.store.messages()...`.

#### [MODIFY] [src/worker/consumer.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/worker/consumer.rs)
- Change struct to `pub struct Consumer<S: Store>`.
- Remove `pool`, add `store: S`.
- Change `lifecycle` field to `WorkerLifecycle<S>`.
- Update `new` to accept `S`.
- Replace `sqlx` calls with `store` methods.

#### [MODIFY] [src/worker/admin.rs](file:///Users/rajatvenkatesh/code/pgqrs/crates/pgqrs/src/worker/admin.rs)
- Change struct to `pub struct Admin<S: Store>`.
- Remove `pool`, `queues`, `messages` etc. (access via `store`).
- Delegate `install`, `verify`, `metrics` logic to `Store` implementation.


## Verification Plan

### Automated Tests
- Run `make test` to verify all existing tests pass with the new `PostgresStore` implementation.
- Add specific unit tests for `sqlite` and `turso` feature flags when implemented.

### Manual Verification
- Verify CLI commands still work against a local Postgres instance (using `src/main.rs` update).
