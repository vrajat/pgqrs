# About pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust with Python bindings. Built for Postgres. Also supports SQLite and Turso.

---

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

---

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres (production), SQLite/Turso (testing, CLI, embedded)
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

---

## Design Principles

pgqrs is designed for **reliability, simplicity, and scalability**.

### Reliability First

pgqrs prioritizes reliability over features:

- **Well-tested:** Comprehensive test coverage across Postgres, SQLite, and Turso
- **Predictable:** No surprising edge cases or hidden failure modes
- **Observable:** State is always inspectable - from database queries to logs/metrics/traces
- **Debuggable:** Clear error messages, visible state, transactional semantics
- **Grows with observability needs:** Start with SQL queries, add logging/metrics/tracing as needed

### Simplicity Where It Matters

The user-facing API is simple. Complexity is hidden when not needed, available when required:

- **Progressive APIs:** Simple cases use defaults (ephemeral resources), complex cases allow control (managed resources)
- **Builder patterns:** Fluent APIs guide correct usage and reveal options gradually
- **Minimal concepts:** Learn core patterns once, apply them everywhere
- **No magic:** Explicit execution, visible state, predictable behavior

### Scales With Your Project

Start simple, scale when needed, using the same API:

- **No rewrite when scaling:** Same API from prototype to production
- **No scaling cliff:** Add workers, add databases, tune policies - no architectural change
- **Grows with requirements:** Simple by default, complexity available when needed
- **Backend flexibility:** SQLite for development/testing, Postgres for production scale

### Enables Reliable Applications

pgqrs helps users build reliable systems:

- **Testable workflows:** Deterministic execution, controllable async behavior
- **Test isolation:** Simulate steps, control execution flow, verify state
- **Clear contracts:** Exactly-once semantics, idempotency guarantees
- **Transaction safety:** Database guarantees extend to workflow execution
- **User reliability:** pgqrs reliability enables application reliability

---

## What Postgres-Native Means

When using Postgres, pgqrs leverages database-native features for performance and reliability:

- **SKIP LOCKED:** Lock-free concurrent message dequeue (PostgreSQL 9.5+)
- **ACID transactions:** Transactional consistency for exactly-once semantics
- **Row-level locks:** No session-level state (connection pooler compatible)
- **Referential integrity:** Foreign key constraints maintain data consistency

For SQLite and Turso, pgqrs adapts gracefully:

- **Polling-based dequeue:** Database-level locks instead of row-level
- **Simpler schema:** No advisory locks or advanced concurrency features
- **Write serialization:** Single writer at a time (SQLite limitation)
- **Same API:** No code changes needed when switching backends

---

## What's Out of Scope

pgqrs **intentionally avoids** features that require background daemons or orchestration servers.

### Sleep and Scheduling

**Not supported:** `workflow.sleep(duration)` or automatic time-based resumption

**Why:** Would require a background process to poll timestamps and wake workflows

**Alternative:** Use external schedulers (cron, systemd timers, Kubernetes CronJobs)

### Automatic Event Delivery

**Not supported:** `workflow.wait_for_event(name)` with automatic matching

**Why:** Would require a background process to match incoming events to waiting workflows

**Alternative:** Manual event checking via explicit polling in application code

### Automatic Retry Scheduling

**Not supported:** Automatic retry with delays (e.g., retry after 10 seconds)

**Why:** Would require a background process to schedule future retries

**Alternative:** Manual retry via re-enqueuing failed steps in application logic

---

## When to Use pgqrs

### ✅ Good Fit

- Background job processing (email, image processing, data transformation)
- Multi-step workflows (order processing, approval chains, ETL pipelines)
- Distributed task coordination (worker pools, job queues)
- CLI tools (stateful operations, resumable tasks)
- Testing (fast, isolated, deterministic execution)
- Embedded applications (SQLite/Turso backends)

### ❌ Not a Good Fit

- Sub-second latency requirements (database round-trip overhead)
- Millions of messages per second (high-throughput streaming)
- Complex event stream processing (stateful stream transformations)
- Built-in time-based scheduling (needs external scheduler)
- Real-time message delivery (sub-100ms delivery guarantees)

pgqrs is designed for **reliable, durable work** - not real-time, high-frequency messaging.

---

*This philosophy document guides design and implementation decisions for pgqrs. It defines what we are, what we're not, and why.*
