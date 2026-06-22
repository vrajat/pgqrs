# ADR-0004: Postgres-Only Durable Coordination with Shared Worker Protocol

## Status

Accepted

## Context

pgqrs currently supports Postgres, SQLite, Turso, and S3 behind a shared `Store`
abstraction and runtime backend selection. Postgres is the primary production
backend, but the non-Postgres backends now shape public APIs, feature flags,
tests, documentation, and implementation complexity.

The project direction is to make pgqrs live up to its name: Postgres should be
the only required durable state and synchronization point. The background-worker
model used by `pg_durable` is a useful reference: a PostgreSQL extension can own
schema, coordinate work, and execute built-in workloads without a separate
application service.

pgqrs also needs to preserve support for arbitrary Rust and Python application
functions. A Postgres background worker cannot discover or execute those
functions unless the code is compiled into a Postgres extension, embedded into
the server process, or invoked through an out-of-process worker. That means the
target model cannot be SQL-only execution; it must be a shared worker shape with
different executor implementations.

At the same time, `s3q` depends directly on the current S3-backed SQLite queue
implementation through `pgqrs::store::s3::S3Store`, queue builders, workers,
message types, and inspection tables. Removing S3 support from pgqrs without a
transition path would break s3q.

## Decision

pgqrs will become Postgres-only in a major breaking release.

The S3-backed SQLite queue substrate that s3q needs will be vendored into s3q
before non-Postgres backends are removed from pgqrs. s3q will own that
implementation privately rather than depending on pgqrs for S3, SQLite, or
generic store abstractions.

pgqrs will introduce a shared worker protocol over Postgres durable state. All
worker implementations will follow the same lifecycle: register identity and
capabilities, heartbeat, lease ready work, execute one invocation, checkpoint
result or error, and participate in retry, cancellation, completion, and
dead-letter behavior.

The PostgreSQL background worker will be an admin worker and built-in executor.
It will be built as a `pgrx` extension loaded through `shared_preload_libraries`.
It owns extension schema, performs maintenance, coordinates scheduling, reclaims
expired leases, handles retries and delayed work, moves exhausted work to DLQ,
enforces cancellation and timeouts, and executes built-in capabilities.

Built-in background-worker capabilities may include SQL execution, sleeps and
timers, internal orchestration control, lease reclamation, retries, DLQ movement,
metrics, and other extension-owned administrative operations. These workloads
can run with only Postgres installed.

Rust and Python workers will use the same protocol to advertise and execute
user-defined functions outside Postgres. They are peer worker implementations,
not a separate coordination system. Postgres remains the only durable state and
synchronization point for both built-in and external-function workloads.

## Consequences

### Positive

- pgqrs can simplify around one database, one concurrency model, and one durable
  coordination story.
- The product positioning becomes clearer: Postgres-only durable coordination,
  built-in Postgres workloads, and optional Rust/Python function workers.
- Built-in workloads can run with only Postgres installed and configured.
- Rust and Python functions remain first-class through external workers using
  the same leasing, checkpointing, retry, cancellation, and result protocol.
- The background worker can own admin responsibilities that should not require a
  user application process to be alive.
- s3q can continue to evolve independently as the S3-backed queue product.

### Negative

- This is a major breaking change for users of SQLite, Turso, S3, `AnyStore`,
  backend feature flags, and backend-specific Python handles.
- Full Rust/Python function execution is not operationally Postgres-only; users
  must run language workers for application code that lives outside Postgres.
- The shared protocol needs explicit capability names, function versioning,
  worker compatibility rules, and failure semantics.
- s3q will temporarily duplicate code that originated in pgqrs, increasing its
  local maintenance burden.
- A PostgreSQL extension runtime adds operational constraints: users must be
  able to install extensions, configure `shared_preload_libraries`, restart
  Postgres, and manage worker privileges.

### Neutral

- SQL/in-extension jobs and Rust/Python function jobs are different executor
  classes over the same durable substrate.
- Existing app-process worker APIs can inform the external worker runtime
  design, but the common boundary should be the worker protocol rather than
  direct sharing of executor internals.
- Non-Postgres history remains available through old releases and through the
  vendored s3q implementation, not through pgqrs mainline.

## Alternatives Considered

### Alternative 1: Shared Queue Core Crate

- **Description**: Extract generic queue primitives plus SQLite/S3 store support
  into a new crate consumed by both pgqrs and s3q.
- **Pros**: Avoids code duplication and preserves a shared implementation.
- **Cons**: Keeps pgqrs conceptually tied to multi-backend abstractions and
  creates another public maintenance surface.
- **Reason for rejection**: The goal is to simplify pgqrs decisively around
  Postgres. s3q can own the S3-specific implementation.

### Alternative 2: Compatibility Branch or Old pgqrs Version

- **Description**: Keep s3q pinned to an old pgqrs branch/version while pgqrs
  main removes non-Postgres backends.
- **Pros**: Fastest way to unblock pgqrs cleanup.
- **Cons**: Leaves s3q on a stale dependency and makes fixes harder to apply.
- **Reason for rejection**: s3q should continue to work as an actively
  maintained product, not as a consumer of a frozen pgqrs line.

### Alternative 3: SQL-Only Extension Runtime

- **Description**: Make the Postgres background worker the only executor and
  constrain all jobs/workflows to SQL-shaped work.
- **Pros**: Strongest "Postgres is all you need" story and closest fit to the
  pg_durable model.
- **Cons**: Drops arbitrary Rust/Python application function support.
- **Reason for rejection**: pgqrs should support Rust and Python functions
  through the same durable coordination protocol.

### Alternative 4: In-Postgres Rust/Python Code Hosting

- **Description**: Execute user Rust/Python code inside Postgres through
  extension plugins, PL/Python, or an embedded interpreter.
- **Pros**: Allows more workloads to run with only Postgres installed.
- **Cons**: Raises safety, dependency isolation, packaging, memory management,
  privilege, crash-containment, and upgrade complexity.
- **Reason for rejection**: This may be explored for specific built-ins or
  advanced deployments, but it should not be the default user-function model.

### Alternative 5: External-Worker-Only Postgres Library

- **Description**: Remove non-Postgres backends but keep pgqrs as an
  application-embedded Rust/Python library with external workers only.
- **Pros**: Smaller implementation step and fewer Postgres extension deployment
  constraints.
- **Cons**: Misses the background admin worker model and cannot run built-in
  workloads with only Postgres.
- **Reason for rejection**: The desired product direction includes an in-Postgres
  admin worker and built-in executor.

### Alternative 6: Extension-First Rewrite

- **Description**: Build the pgrx extension runtime before decoupling s3q.
- **Pros**: Moves fastest toward the new pgqrs identity.
- **Cons**: Breaks or blocks s3q because it currently depends directly on pgqrs
  S3 internals.
- **Reason for rejection**: s3q compatibility is a required constraint, so it
  must be decoupled first.

## References

- `s3q` dependency on `pgqrs::store::s3::S3Store`
- `pg_durable` PostgreSQL extension/background-worker architecture
- `engg/adr/0003-s3-backed-sqlite-queue.md`

---

**Date**: 2026-06-19  
**Author(s)**: Rajat Venkatesh  
**Reviewers**: TBD
