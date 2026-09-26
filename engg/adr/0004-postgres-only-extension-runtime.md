# ADR-0004: Postgres-Only Durable Runtime with Shared Coordination Semantics

## Status

Accepted

## Context

pgqrs currently carries abstractions for multiple durable backends. That broadens
the public API and makes correctness-sensitive behavior—leasing, retries,
cancellation, recovery, and administrative maintenance—harder to define and test
consistently.

Postgres is already the natural coordination point for pgqrs workloads. It
provides transactions, row-level concurrency control, durable state, and a
well-understood extension mechanism. Concentrating pgqrs on Postgres lets the
project define one durable protocol instead of reproducing similar behavior
across storage engines.

The runtime must still support two materially different deployment environments:

| Environment | Administration | SQL execution | Application execution |
| --- | --- | --- | --- |
| Hosted Postgres, where extensions or background workers may be unavailable | External admin process | External SQL worker | External Rust or Python workers |
| Self-hosted Postgres, where the pgqrs extension can run background workers | Extension background worker | Extension built-ins | External Rust or Python workers |

Extension-managed administration is a core part of the target architecture. An
external administrator is necessary for hosted Postgres and is the simpler
reference implementation to deliver first, but it is not the final architecture
for every deployment.

The two administration paths must not evolve into separate implementations of
queue semantics. Concurrency-sensitive transitions need one durable contract so
that a hosted deployment and a self-hosted deployment behave the same way.

## Decision

pgqrs will use Postgres as its only durable backend and synchronization point.
Future pgqrs releases will remove the S3, SQLite, and Turso backends and the
public abstractions required solely to support them. Projects that still need
those backends may remain on the last compatible pgqrs release while they absorb
the relevant functionality; that migration is not a prerequisite for the pgqrs
change.

The durable coordination contract will live in a versioned Postgres schema.
Atomic database operations will own correctness-sensitive state transitions,
including claiming and renewing leases, acknowledging outcomes, scheduling
retries, cancellation, and administrative recovery. Runtime processes will own
lifecycle, wake-up and polling behavior, observability, and invoking work, but
will not reimplement those state transitions independently.

pgqrs will support two implementations of the administration role:

1. An external admin process for hosted Postgres.
2. A background worker in the pgqrs Postgres extension for self-hosted Postgres.

Both implementations will call the same versioned database operations. The
external implementation will be built first as the reference implementation;
this is delivery sequencing, not a reduction in the extension's responsibility.

SQL work will follow the same deployment model:

1. Hosted Postgres uses an external SQL worker.
2. Self-hosted Postgres may execute supported built-in SQL workloads through the
   extension.

Arbitrary Rust and Python application code remains outside Postgres. External
workers will use the same coordination protocol and advertise their runtime
capabilities. The extension will not load or execute arbitrary application code.

The shared protocol will define, at minimum:

- protocol and capability compatibility;
- lease ownership, renewal, expiry, and fencing;
- retry scheduling and terminal outcomes;
- cancellation and deadline semantics;
- dead-letter and recovery behavior; and
- bounded, observable execution.

Execution is at least once. Correctness will come from fenced leases, durable
state transitions, idempotent handlers where applicable, and explicit
checkpointing. pgqrs will not claim blanket exactly-once execution of arbitrary
side effects.

SQL execution must use a restricted database role and enforce configurable
statement, time, and result limits. Extension background workers must not turn a
job payload into unrestricted superuser SQL.

Schema ownership and migration mechanics will be designed so hosted and
self-hosted installations have one canonical schema history. This ADR does not
require the schema to be owned by `CREATE EXTENSION`; it requires both runtime
modes to operate against compatible versions of the same contract.

Implementation will proceed in independently reviewable stages:

1. Specify the coordination protocol, invariants, state machines, and versioning.
2. Reduce the public API to the Postgres-only model.
3. Implement the external administrator against the shared database operations.
4. Implement the external SQL worker.
5. Implement extension-managed administration against those same operations.
6. Implement the supported extension SQL built-ins.
7. Demonstrate parity with failure, recovery, upgrade, and compatibility tests
   for both deployment modes.

The target release is complete only when both the hosted and self-hosted modes
meet their acceptance criteria. The stages are not to be combined into one
aggregate implementation change.

## Consequences

### Positive

- One transactional backend defines queue and workflow correctness.
- Hosted and self-hosted installations share observable behavior and recovery
  semantics.
- External implementations provide a testable reference for extension workers.
- Extension background workers can provide low-operational-overhead
  administration and built-in SQL execution for self-hosted Postgres.
- Removing backend-neutral abstractions makes the public API and test matrix
  smaller.

### Negative

- Users of S3, SQLite, and Turso must remain on an older release or move that
  functionality into another project.
- The extension introduces packaging, Postgres-version compatibility, upgrade,
  and operational-support costs.
- Shared database operations become a compatibility boundary that must be
  versioned and migrated carefully.
- At-least-once execution requires handlers and workflow authors to account for
  retries and duplicate attempts.

### Neutral

- Hosted Postgres remains fully supported without requiring extension access.
- The extension is optional for installation, but its administration and
  built-in execution paths are required parts of the pgqrs architecture.
- Exact schema layout, API names, packaging, and rollout details belong in
  focused design documents and implementation changes.

## Alternatives Considered

### Continue Supporting Multiple Durable Backends

- **Description**: Retain the backend-neutral API and improve each backend.
- **Pros**: Preserves existing deployment choices.
- **Cons**: Multiplies protocol, migration, recovery, and testing work.
- **Reason for rejection**: The additional surface area prevents pgqrs from
  concentrating on a single well-defined correctness model.

### Extension-Only Runtime

- **Description**: Require the Postgres extension for administration and SQL
  execution.
- **Pros**: Keeps runtime behavior close to the data and simplifies deployment in
  self-hosted environments.
- **Cons**: Excludes hosted Postgres services that do not permit the extension or
  background workers.
- **Reason for rejection**: Hosted Postgres is a required deployment mode.

### External-Only Runtime

- **Description**: Run all administration and execution in external processes.
- **Pros**: Works uniformly with hosted Postgres and is simpler to package.
- **Cons**: Gives up the operational and lifecycle benefits of a Postgres
  background worker for self-hosted installations.
- **Reason for rejection**: Extension-managed administration is a core product
  capability, not an optional follow-up.

### Separate Semantics for External and Extension Workers

- **Description**: Implement queue and recovery rules independently in each
  runtime.
- **Pros**: Lets each runtime optimize without a database-level contract.
- **Cons**: Creates divergent behavior, duplicated correctness logic, and a much
  larger failure matrix.
- **Reason for rejection**: Deployment choice must not change durable semantics.

### Execute Arbitrary Application Code Inside Postgres

- **Description**: Load Rust or Python handlers into the database process.
- **Pros**: Avoids an external worker for more workloads.
- **Cons**: Expands the database trust boundary and creates unacceptable safety,
  resource-isolation, and operational risks.
- **Reason for rejection**: Only constrained, built-in SQL workloads belong in
  the extension; application runtimes remain external.

## References

- [ADR-0002: Workflow Trigger and Worker Redesign](0002-workflow-trigger-worker-redesign.md)
- [ADR-0003: S3-Backed SQLite Queue](0003-s3-backed-sqlite-queue.md)
- [pgqrs README](../../README.md)

---

**Date**: 2026-09-26  
**Author(s)**: pgqrs maintainers  
**Reviewers**: pgqrs maintainers
