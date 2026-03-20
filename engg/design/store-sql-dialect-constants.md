# Store SQL Dialect Layer - Design Document

**Status:** Draft
**Author:** pgqrs team
**Date:** 2026-03-20
**Epic:** Store Module Cleanup

---

## 1. Overview

This document proposes a refactor of backend-specific SQL in the `store` module from runtime table accessors to an internal dialect layer.

The immediate trigger is the duplication of workflow step SQL in the S3 backend. `S3Store` delegates operational work into an inner SQLite-backed store, but the `StepRecordTable` trait also exposes synchronous SQL accessor methods such as `sql_acquire_step()`. Reusing those methods through `SyncDb` created an abstraction mismatch: the runtime delegation layer is primarily async, while SQL metadata access is synchronous and static.

The proposed design separates:

1. **Runtime table operations**: async CRUD and business methods on `QueueTable`, `MessageTable`, `StepRecordTable`, and related traits
2. **Backend dialect metadata**: static SQL constants and backend capability differences selected through backend-specific dialect types

This lets S3 reuse SQLite SQL directly without asking an inner store instance for constant strings, and creates a reusable pattern for broader store cleanup.

---

## 2. Problem Statement

### 2.1 Current State

Today, `StepRecordTable` mixes two different responsibilities:

1. executing step operations
2. providing backend-specific SQL text for callers that build raw queries

Example:

```rust
#[async_trait]
pub trait StepRecordTable: Send + Sync {
    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord>;
    fn sql_acquire_step(&self) -> &'static str;
    fn sql_clear_retry(&self) -> &'static str;
    fn sql_complete_step(&self) -> &'static str;
    fn sql_fail_step(&self) -> &'static str;
}
```

This works for direct backends like SQLite and Postgres, but it is awkward for S3:

- S3 is operationally a wrapper over a local SQLite store.
- `SyncDb` exposes the inner store via async callback methods (`with_read`, `with_write`).
- The SQL accessors are sync methods returning static metadata.
- To delegate those sync accessors through `SyncDb`, we needed separate sync callback helpers (`with_read_ref`, `with_write_ref`) or duplication of the SQL strings.

The code currently chooses duplication in `store/s3/tables.rs`, which sidesteps trait churn but leaves backend SQL spread across multiple files.

### 2.2 Why This Is a Design Smell

The SQL strings are:

- backend-specific
- static
- not instance-dependent
- not operational table behavior

That makes them a poor fit for runtime trait-object access.

The current design forces the store runtime abstraction to also carry static backend metadata. This is the wrong dependency direction. The caller should choose the backend dialect at compile time in the backend implementation layer, not ask a live store object for constant SQL text.

---

## 3. Goals

1. Remove SQL string duplication between SQLite and S3 where S3 semantics intentionally match SQLite.
2. Keep SQL metadata access synchronous and static.
3. Introduce a shared internal dialect model that can be reused across stores.
4. Reduce `Store`, `StepRecordTable`, and `SyncDb` surface area where possible.
5. Make backend-specific SQL easier to find, review, and reuse.
6. Create a pattern that can extend beyond workflow step queries to other backend-specific SQL and capability differences.

---

## 4. Non-Goals

1. Redesign all table APIs in one pass.
2. Eliminate all raw SQL construction from the codebase.
3. Introduce runtime dialect trait objects or dynamic backend SQL dispatch.
4. Change backend semantics or query behavior.
5. Generalize query builders across all backends beyond what is needed for cleanup.

---

## 5. Design Principles

### 5.1 Separate Runtime Behavior from Static Metadata

Traits like `Store`, `QueueTable`, and `StepRecordTable` should represent operations performed against a live backend instance.

Static SQL text is not such an operation. It is backend metadata and should live in backend dialect definitions.

### 5.2 Prefer Compile-Time Dialect Selection

Backend implementations already know which backend they are compiling for:

- SQLite tables know they are SQLite
- Postgres tables know they are Postgres
- Turso tables know they are Turso
- S3 tables know they should use SQLite-compatible SQL

This means SQL selection does not require runtime polymorphism. The dialect layer should be compile-time and backend-specific.

### 5.3 Let S3 Reuse SQLite Dialect, Not SQLite Store Instances

S3 should reuse SQLite query definitions because S3's local execution engine is SQLite. That reuse should happen by using `SqliteDialect`, not by reaching through the runtime `SyncDb` layer into an inner `SqliteStore`.

### 5.4 Prefer Composition Over Inheritance for Backend Families

Rust does not need inheritance to model shared backend behavior here.

Backends with significant overlap, such as SQLite and Turso, should share reusable SQL bundles and capability constants by composition:

- shared structs in `store/dialect.rs`
- shared constant bundles for SQLite-family behavior
- backend dialect types that point to those bundles

This preserves backend-specific identities while allowing substantial reuse without runtime indirection.

---

## 6. Proposed Architecture

### 6.1 Shared Dialect Contract in `store/`

Introduce a shared internal dialect contract in `store/dialect.rs`.

Example:

```rust
pub(crate) struct StepSql {
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
}

pub(crate) struct DialectCaps {
    pub supports_returning: bool,
    pub supports_skip_locked: bool,
    pub is_sqlite_family: bool,
}

pub(crate) trait SqlDialect {
    const STEP: StepSql;
    // later:
    // const RUN: RunSql;
    // const MESSAGE: MessageSql;
    // const WORKER: WorkerSql;
    // const CAPS: DialectCaps;
}
```

This trait should remain `pub(crate)` and should not become part of the public `pgqrs` API.

### 6.2 One `dialect.rs` Per Backend

Each backend should define its dialect in a single file:

```text
store/
  dialect.rs
  sqlite/
    dialect.rs
  postgres/
    dialect.rs
  turso/
    dialect.rs
  s3/
    ... no dialect.rs initially if S3 reuses SqliteDialect directly
```

Recommended backend types:

```rust
pub(crate) struct SqliteDialect;
pub(crate) struct PostgresDialect;
pub(crate) struct TursoDialect;
```

S3 should initially use `SqliteDialect` directly. A separate `S3Dialect` should only be added if S3 query behavior diverges from SQLite.

### 6.3 Group SQL by Query Family

Define backend SQL bundles as static structs.

Example:

```rust
pub(crate) struct StepSql {
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
}
```

Then provide reusable constant bundles and backend mappings:

```rust
pub(crate) const SQLITE_FAMILY_STEP_SQL: StepSql = StepSql { ... };

pub(crate) struct SqliteDialect;
impl SqlDialect for SqliteDialect {
    const STEP: StepSql = SQLITE_FAMILY_STEP_SQL;
}

pub(crate) struct TursoDialect;
impl SqlDialect for TursoDialect {
    const STEP: StepSql = SQLITE_FAMILY_STEP_SQL;
}

pub(crate) struct PostgresDialect;
impl SqlDialect for PostgresDialect {
    const STEP: StepSql = StepSql { ... };
}
```

This keeps SQL near the backend implementation but separate from runtime table types while still allowing backend-family reuse.

### 6.4 Remove Sync SQL Accessors from Runtime Table Traits

After migration, `StepRecordTable` should contain only operational methods:

```rust
#[async_trait]
pub trait StepRecordTable: Send + Sync {
    async fn insert(&self, data: NewStepRecord) -> Result<StepRecord>;
    async fn get(&self, id: i64) -> Result<StepRecord>;
    async fn list(&self) -> Result<Vec<StepRecord>>;
    async fn count(&self) -> Result<i64>;
    async fn delete(&self, id: i64) -> Result<u64>;
    async fn execute(&self, query: QueryBuilder) -> Result<StepRecord>;
}
```

The caller would use the backend dialect directly instead of calling `table.sql_acquire_step()`.

### 6.5 Simplify S3 Delegation

Once SQL metadata no longer has to flow through `SyncDb`, S3 can remain focused on:

- `with_read`
- `with_write`
- `snapshot`
- `sync`

If nothing else needs synchronous metadata access through `SyncDb`, `with_read_ref` and `with_write_ref` become removable.

---

## 7. Detailed Design

### 7.1 Minimal First Step

The smallest useful refactor is:

1. introduce `store/dialect.rs` with the shared SQL bundle structs and `SqlDialect` trait
2. add `sqlite/dialect.rs`, `postgres/dialect.rs`, and `turso/dialect.rs`
3. move workflow step SQL into those files
4. have SQLite, Turso, and S3 use shared SQLite-family constant bundles where appropriate
5. keep `StepRecordTable::sql_*` temporarily for compatibility

This reduces duplication immediately while limiting churn.

### 7.2 Final Target State

The preferred end state is:

1. SQL constants live behind backend dialect types
2. callers construct queries from backend dialect constants directly
3. `StepRecordTable::sql_*` methods are removed
4. unused sync callback helpers are removed from `SyncDb`

### 7.3 Dialect Layer Shape

Recommended shared definitions in `store/dialect.rs`:

```rust
pub(crate) struct StepSql {
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
}

pub(crate) struct DialectCaps {
    pub supports_returning: bool,
    pub supports_skip_locked: bool,
    pub is_sqlite_family: bool,
}

pub(crate) trait SqlDialect {
    const STEP: StepSql;
    // later:
    // const RUN: RunSql;
    // const MESSAGE: MessageSql;
    // const WORKER: WorkerSql;
    // const CAPS: DialectCaps;
}
```

This combines:

- grouped structs for coherent query families
- a trait in `store/` for uniform backend modeling
- backend-specific zero-sized types for compile-time selection

### 7.4 Reuse Across Backend Families

The dialect layer should support substantial reuse between SQLite-family backends.

Example:

```rust
pub(crate) const SQLITE_FAMILY_STEP_SQL: StepSql = StepSql { ... };

pub(crate) struct SqliteDialect;
impl SqlDialect for SqliteDialect {
    const STEP: StepSql = SQLITE_FAMILY_STEP_SQL;
}

pub(crate) struct TursoDialect;
impl SqlDialect for TursoDialect {
    const STEP: StepSql = SQLITE_FAMILY_STEP_SQL;
}
```

This is not inheritance. It is composition through shared constant bundles.

If Turso diverges later for one query family, it can override only that family while continuing to share the rest.

### 7.5 Recommendation

Adopt **Option C** with a shared `SqlDialect` trait in `store/dialect.rs` and a single `dialect.rs` file per backend.

Reasoning:

- It gives a uniform backend modeling pattern.
- It keeps SQL and backend capabilities out of runtime table traits.
- It allows significant reuse across similar backends such as SQLite and Turso.
- It scales better than ad hoc constants once more query families move into the dialect layer.
- It keeps the public API unchanged because the trait remains internal.

---

## 8. Example Usage After Refactor

Current pattern in workflow code:

```rust
self.store
    .workflow_steps()
    .execute(QueryBuilder::new(self.store.workflow_steps().sql_acquire_step()))
    .await?;
```

Proposed pattern:

```rust
use crate::store::dialect::SqlDialect;
use crate::store::sqlite::dialect::SqliteDialect;

self.store
    .workflow_steps()
    .execute(QueryBuilder::new(SqliteDialect::STEP.acquire))
    .await?;
```

For S3-backed execution, the S3 workflow code would use `SqliteDialect` directly rather than ask the inner store instance for SQL metadata.

For Postgres:

```rust
use crate::store::dialect::SqlDialect;
use crate::store::postgres::dialect::PostgresDialect;
```

This keeps runtime execution and SQL selection clearly separated.

---

## 9. Impact on Existing Traits

### 9.1 `StepRecordTable`

**Current issue**: The trait mixes execution behavior with static SQL metadata.

**Proposed change**: Remove `sql_*` methods after callers have migrated to the dialect layer.

**Result**: Smaller trait, cleaner contract, less backend leakage through trait objects.

### 9.2 `Store`

No direct API change is required beyond callers no longer depending on `workflow_steps().sql_*()`.

### 9.3 `SqlDialect`

**Proposed change**:

- add an internal shared trait in `store/dialect.rs`
- define backend zero-sized dialect types in each backend's `dialect.rs`
- allow SQLite-family backends to share constant bundles by composition

**Result**: SQL and backend capability differences become a first-class internal design concept without polluting the public API.

### 9.4 `SyncDb`

**Current issue**: sync callback helpers exist to support sync metadata access through the inner store abstraction.

**Proposed change**:

- keep `with_read` / `with_write`
- remove `with_read_ref` / `with_write_ref` if no other call sites require them

**Result**: `SyncDb` remains focused on operational store access and synchronization policy, not metadata lookup.

---

## 10. Migration Plan

### Phase 1: Introduce Dialect Layer

1. Create `store/dialect.rs` with shared SQL bundle structs and the `SqlDialect` trait.
2. Add `sqlite/dialect.rs`, `postgres/dialect.rs`, and `turso/dialect.rs`.
3. Move existing workflow step SQL constants into those files.
4. Reuse shared SQLite-family constant bundles for Turso and S3 where appropriate.

### Phase 2: Migrate Callers

1. Update workflow code and any other raw-query callers to use backend dialect types directly.
2. Stop calling `table.sql_*()` in new code.

### Phase 3: Remove Transitional API

1. Remove `sql_*` methods from `StepRecordTable`.
2. Remove implementations of those methods in backend table types.
3. Remove `with_read_ref` / `with_write_ref` from `SyncDb` if they are unused.

### Phase 4: Expand As Needed

1. Apply the same pattern to run, message, and worker SQL where useful.
2. Add capability structs such as `DialectCaps` once callers need them.

---

## 11. Testing Strategy

### 11.1 Unit Tests

1. Query behavior remains covered by existing backend table tests.
2. Add small tests for any helper module that maps backend dialects or reuses shared SQLite-family bundles.

### 11.2 Regression Checks

1. SQLite and S3 workflow step paths should continue to execute identical SQL for shared operations.
2. Postgres workflow step behavior should remain unchanged.
3. Turso behavior should remain unchanged when it reuses SQLite-family constants.

### 11.3 Refactor Safety

The main risk is wiring the wrong dialect into a caller. This is mitigated by:

- keeping constants grouped by query family
- using explicit backend dialect types
- limiting the first migration to workflow step SQL
- relying on existing behavior tests for step execution paths

---

## 12. Alternatives Considered

### 12.1 Keep SQL Duplication in S3

Pros:

- lowest immediate churn

Cons:

- duplicates backend behavior definitions
- creates cleanup debt
- scales poorly as more backend-specific queries need reuse

Rejected because the duplication is already a symptom of the abstraction mismatch.

### 12.2 Keep SQL Accessors on Traits Permanently

Pros:

- familiar current pattern
- direct lookup from runtime table objects

Cons:

- couples static SQL metadata to runtime trait objects
- keeps `SyncDb` pressure for sync metadata access
- makes S3 delegation more awkward than needed

Rejected as the long-term shape.

### 12.3 Make SQL Accessors Async

Pros:

- would fit the `with_read` / `with_write` style mechanically

Cons:

- conceptually wrong for static string lookup
- adds async overhead and noise for constant metadata
- infects callers with unnecessary `.await`

Rejected.

### 12.4 Use a Dynamic Runtime Dialect Trait Object

Pros:

- can centralize SQL selection behind a common interface

Cons:

- unnecessary indirection
- not needed because backend choice is already encoded in implementation modules

Rejected.

### 12.5 Use Backend Inheritance

Pros:

- attractive if thinking in terms of "Turso extends SQLite"

Cons:

- not a natural Rust fit for this problem
- would not materially simplify query-family reuse
- risks turning dialect selection into an unnecessary type hierarchy

Rejected in favor of composition through shared constant bundles and backend dialect types.

---

## 13. Open Questions

1. Should workflow code own backend-specific query selection directly, or should each dialect expose helper constructors around `QueryBuilder` in addition to raw SQL bundles?
2. Should Turso explicitly define its own `TursoDialect` even when all current query families point to SQLite-family constants?
3. Should the first refactor stop at workflow step SQL, or include workflow run SQL in the same pass?
4. Once `with_read_ref` / `with_write_ref` are removed, do any other parts of S3 need synchronous access to the inner store for diagnostics or metadata?

---

## 14. Recommendation

Proceed with a staged refactor:

1. Introduce `store/dialect.rs` with the internal `SqlDialect` trait and shared query-family structs.
2. Add a single `dialect.rs` file per backend.
3. Have S3 reuse `SqliteDialect` directly and let Turso reuse SQLite-family bundles by composition.
4. Migrate callers away from `StepRecordTable::sql_*`.
5. Remove the sync SQL accessors from `StepRecordTable`.
6. Remove `with_read_ref` / `with_write_ref` from `SyncDb` if they remain unused.

This gives the store module a cleaner separation between runtime behavior and backend SQL metadata, reduces S3-specific duplication, and creates a reusable pattern for further store cleanup.
