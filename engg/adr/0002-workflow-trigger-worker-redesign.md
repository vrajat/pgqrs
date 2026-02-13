# ADR-0002: Workflow Trigger/Worker Architecture Redesign

## Status

Proposed

## Context

The current workflow implementation in pgqrs has a fundamental architectural flaw: **it doesn't properly model the relationship between triggers (who submit workflow runs) and workers (who execute them)**.

### Current Design Problems

1. **No Worker Discovery**: No mechanism for workers to advertise "I can execute workflow X"
2. **No Trigger Validation**: Triggers can submit workflows that no worker can handle, leading to runs that sit in PENDING state indefinitely
3. **Awkward API**: Global `pgqrs::step(workflow_id, "step_id")` function instead of `workflow.step("step_id")` method
4. **Confused Responsibilities**: Workflow object doesn't manage its own steps
5. **No Run Concept**: Workflows are treated as one-time executions, not reusable definitions with multiple runs
6. **No Result Retrieval**: No clear way to check workflow results after triggering

### Developer Experience Issues

**Trigger Side (Current):**
```rust
// No validation - how do I know if anyone can execute this?
let workflow = pgqrs::workflow()
    .name("process_data")
    .arg(&input)?
    .create(&store)
    .await?;
// How do I get the result later?
```

**Worker Side (Current):**
```rust
// No registration API at all!
// Workers just... exist? How does pgqrs know what they can do?
```

### Real-World Usage Pattern

In production applications:
- **Triggers** are scattered: HTTP endpoints, cron jobs, event handlers
- **Workers** are centralized: dedicated processes polling for work
- **Validation must happen at trigger time**: Fast-fail if no worker available
- **Results must be retrievable**: Trigger might not wait, but needs to check later

### Technical Constraints

- **Postgres, SQLite, Turso backends**: Must work across all three
- **Database-driven architecture**: State lives in database, not memory
- **Crash recovery**: Workflows must resume after worker crashes
- **Multi-worker support**: Multiple workers can handle same workflow type

## Decision

We will redesign the workflow system around a **clear separation between triggers and workers**, with the following core architectural decisions:

### 1. Workflow Definition vs. Workflow Run

- **Workflow Definition**: A named template (e.g., "zip_files")
  - Stored in `pgqrs_workflows` table
  - Created when worker registers
  - Reusable across multiple runs
  - No versioning - use naming convention for versions (e.g., "zip_files_v1", "zip_files_v2")

- **Workflow Run**: A specific execution instance (run_id + parameters)
  - Stored in `pgqrs_workflow_runs` table
  - Created when trigger executes
  - Has lifecycle: PENDING → RUNNING → SUCCESS/ERROR/PAUSED

### 2. Queue-per-Workflow Mapping (1:1)

- Each workflow has exactly one queue (enforced via FK in schema)
- Example: workflow "zip_files" → queue "zip_files"
- Strong FK link: `pgqrs_workflows.queue_id` → `pgqrs_queues.queue_id`
- Simplifies routing and validation
- Workers register as consumers on workflow queues

### 3. Worker Registration & Advertisement

Workers must explicitly register with handlers:

```rust
// Create workflow definition (one-time setup, idempotent)
store.create_workflow("zip_files").await?;

// Register worker with handler
let worker = consumer("worker-1", 8080, "zip_files")
    .handler(zip_files_handler)
    .create(&store)
    .await?;
```

This creates:
- Queue "zip_files" (via `create_workflow()`)
- Workflow definition with FK to queue (via `create_workflow()`)
- Worker record as consumer on queue (via `consumer().create()`)
- Handler stored in memory (not persisted to DB)

**Versioning:** Include version in workflow name (e.g., "zip_files_v1", "zip_files_v2"). See design doc section 11 for best practices.

### 4. API Design: Noun-Verb Pattern

**Trigger API** follows noun-verb pattern:

```rust
// Trigger workflow (returns run_id)
let run_id = workflow("zip_files")
    .trigger(&params)?
    .execute(&store)
    .await?;

// Check status later
let run = workflow("zip_files")
    .get_run(run_id, &store)
    .await?;
```

**Worker Registration** uses fluent builder:

```rust
let worker = consumer("worker-1", 8080, "zip_files")
    .handler(zip_files_handler)  // In-memory, not persisted
    .create(&store)
    .await?;
```

**Design Principle**: Each API call updates ONE table (with justified exceptions for tightly-coupled operations)

### 5. Single-Table Update Principle

Most API calls update exactly one table:

**One-table operations:**
- `consumer().create()` → Updates `pgqrs_workers` only
- `worker.set_handler()` → In-memory only (no DB update)

**Justified multi-table operations (semantically ONE operation):**
- `store.create_workflow()` → Creates workflow + queue (tightly coupled via FK)
- `workflow().trigger()` → Creates run + enqueues message (tightly coupled - need run_id for tracking)

Rationale: Workflows inherently need queues (FK enforced), and triggering inherently needs both run creation and enqueueing.

### 6. Message Payload: run_id Only (DRY)

Message payload contains only run_id, not full params:

```json
{"run_id": 12345}
```

**Worker processing:**
1. Dequeue message with run_id
2. Fetch run from `pgqrs_workflow_runs` (includes params in `run.input`)
3. Parse params from `run.input`
4. Execute handler

**Benefits:**
- DRY: Params stored once in `pgqrs_workflow_runs.input` (single source of truth)
- Smaller messages
- Clean separation: queue transports ID, DB stores data

### 7. Improved Step API

**Current (awkward):**
```rust
let step_res = pgqrs::step(workflow.id(), "step1")
    .acquire(&store)
    .await?;
```

**New (natural):**
```rust
async fn handler(ctx: &mut dyn Workflow, input: Input) -> Result<Output> {
    let result = ctx.step("step1", || async {
        // work
    }).await?;
    
    Ok(output)
}
```

### 8. DRY Retry Mechanism

- **No retry_at in steps table** - Message visibility is single source of truth
- Transient errors update `message.visible_after`, not duplicate timestamp in steps
- Worker updates visibility and moves on (doesn't re-poll or block)
- Pause support via periodic visibility checks (global config: pause_check_interval)
- See [design doc section 10](../design/workflow-redesign-v2.md#10-error-handling--retry-strategies) for complete error handling details

### 9. Result Retrieval Patterns

**Non-blocking (return run_id immediately):**
```rust
let run_id = workflow("zip_files")
    .trigger(&params)?
    .execute(&store)
    .await?;
```

**Poll later:**
```rust
let run = workflow("zip_files")
    .get_run(run_id, &store)
    .await?;
    
match run.status {
    RunStatus::Success => { /* use run.output() */ }
    RunStatus::Error => { /* handle run.error */ }
    _ => { /* still running */ }
}
```

**Future enhancement (blocking wait):**
```rust
// Potential future API
let result: ZipResult = workflow("zip_files")
    .trigger(&params)?
    .wait()
    .timeout(Duration::from_secs(300))
    .execute(&store)
    .await?;
```

### 10. Versioning Strategy

- **No built-in versioning** - Workflows identified by name only
- **User responsibility** - Include version in workflow name ("process_file_v1", "process_file_v2")
- **pgqrs perspective** - Different names = different workflows
- **Benefits**: Simplicity, flexibility, no magic defaults, user control
- See [design doc section 11](../design/workflow-redesign-v2.md#11-versioning-best-practice) for naming conventions and deployment strategies

## Consequences

### Positive

1. **Clear Mental Model**: Triggers vs. workers is intuitive
2. **Noun-Verb API**: `workflow().trigger()`, `workflow().get_run()` reads naturally
3. **Single-Table Updates**: Most operations touch one table (cleaner, easier to understand)
4. **DRY Principle**: Params in DB once, run_id in messages only
5. **Strong FK Enforcement**: Workflow → queue relationship enforced in schema
6. **Handler in Memory**: Not persisted, simpler worker startup
7. **Better Developer Experience**: 
   - Workers register with clear `consumer().handler(fn).create()`
   - Triggers use intuitive `workflow().trigger()`
   - Natural API: `ctx.step()` instead of global `step()`
8. **Proper Reusability**: Workflow definition can be triggered multiple times
9. **Result Retrieval**: Clear patterns for getting results (poll, async notify future)
10. **Monitoring**: Can track metrics per workflow (success rate, latency, queue depth)
11. **Crash Recovery**: Steps stored per run_id, recovery works naturally
12. **DRY Retry**: Message visibility is single source of truth (no duplicate retry_at column)
13. **Pause Support**: Workflows can wait for external events (human approval, scheduled time)
14. **Simple Versioning**: No built-in version field - users control versioning via naming

### Negative

1. **Breaking API Changes**: 
   - `pgqrs::workflow().create()` → `workflow().trigger().execute()`
   - No multi-workflow worker registration (was never supported)
   - Global `step()` → method on Workflow trait
   - workflow_id now references definition, not execution
2. **Schema Migration Required**: Three new tables (workflows with queue FK, runs, steps)
3. **Migration Complexity**: Existing workflows must be migrated to new schema
4. **Queue Proliferation**: One queue per workflow name
   - 100 workflows = 100 queues
   - With versioning: Could be more (e.g., "process_file_v1", "process_file_v2")
   - Mitigated: Postgres handles thousands of tables easily
   - Workers poll multiple queues efficiently
5. **Learning Curve**: Developers must understand:
   - Workflow definition vs. run
   - Setup (`create_workflow`) vs. trigger (`workflow().trigger()`)
   - Consumer registration with handlers

### Neutral

1. **Explicit Multi-Table Operations**: `create_workflow()` and `trigger()` update 2 tables each
   - Semantically justified (tightly coupled operations)
   - Clear what's happening
   - Trade-off: simplicity vs. atomic operations
2. **No Version Field**: Versioning via naming convention instead of schema field
3. **One-to-One Queue Mapping**: Simpler than multi-queue, but less flexible
4. **Service Orchestration**: Service layer handles multi-worker polling (not pgqrs)
   - pgqrs stays minimal
   - Services have full control (tokio::select!, threads, etc.)

## Alternatives Considered

### Alternative 1: Multi-Queue Workers (One worker, many workflows)

**Description**: Allow single worker to register for multiple workflows
```rust
workflow_worker()
    .register("zip_files", handler1)
    .register("send_emails", handler2)
    .start(&store).await?;
```

**Pros**:
- Less boilerplate (one worker declaration)
- Single polling loop

**Cons**:
- Blurs "one worker per workflow" concept
- Complicates worker lifecycle (one failure affects all workflows)
- Harder to scale individual workflows independently
- Confusion: Is worker a database concept or runtime concept?

**Reason for rejection**: Violates separation of concerns. Service layer should orchestrate multiple workers, not pgqrs. Decided to keep pgqrs minimal with clear 1:1 worker-to-workflow mapping.

### Alternative 2: Runner Abstraction in pgqrs

**Description**: Add `WorkflowRunner` to pgqrs that manages multiple workers
```rust
let runner = WorkflowRunner::new(&store);
runner.register("zip_files", handler1);
runner.register("send_emails", handler2);
runner.start().await?;
```

**Pros**:
- More batteries-included
- Simpler for common case

**Cons**:
- Adds complexity to pgqrs
- Dictates orchestration strategy (tokio::select!, threads, etc.)
- Different services have different needs (async runtime, logging, metrics)
- Bloats library scope

**Reason for rejection**: Services should control orchestration. Provide `poll_all()` helper for common case, but don't require it. Keep pgqrs focused on database-backed workflows, not process management.

### Alternative 3: Lazy Validation (Skip worker checks)

**Description**: Don't validate workers exist, just enqueue and hope
```rust
// Skip: Check queue has active consumers
let run_id = store.create_workflow_run(...)?;
store.enqueue_message(...)?;
```

**Pros**:
- Simpler trigger implementation
- Faster trigger execution

**Cons**:
- Silent failures: runs sit in PENDING forever
- Poor developer experience: "Why isn't my workflow running?"
- Harder to debug production issues
- No feedback at trigger time

**Reason for rejection**: Fast-fail principle. Better to error immediately than fail silently. Slight validation overhead is worth it for better DX.

### Alternative 4: Queue-per-Version (Multiple queues per workflow)

**Description**: Create separate queue for each version
- "zip_files_v1" → queue "zip_files_v1"
- "zip_files_v2" → queue "zip_files_v2"

**Pros**:
- Clean version isolation
- No version mismatch possible

**Cons**:
- Queue explosion (N workflows × M versions)
- Harder to monitor aggregate metrics
- Complicates queue naming

**Reason for rejection**: Version matching can be handled at validation layer. Queue-per-workflow is cleaner. Can revisit if versioning becomes critical.

### Alternative 5: Payload-Based Routing (One queue for all workflows)

**Description**: Single queue, route by message payload
```json
{"workflow": "zip_files", "run_id": 123}
```

**Pros**:
- One queue regardless of workflow count
- Simpler queue management

**Cons**:
- Workers must filter messages they can't handle
- Wasted polling cycles
- Harder to monitor per-workflow queue depth
- Validation more complex (check payload structure)

**Reason for rejection**: Queue-per-workflow is cleaner and more efficient. Postgres handles many queues well.

## References

- [Detailed Design Document](../design/workflow-redesign-v2.md) - Complete architecture, examples, implementation roadmap
- [Issue #152: Workflows cannot restart after error or crash](https://github.com/vrajat/pgqrs/issues/152) - Absorbed by this redesign (see design doc section 10)
- Current implementation files:
  - `crates/pgqrs/src/builders/workflow.rs` - Current workflow builder
  - `crates/pgqrs/src/store/mod.rs` - Store trait (Workflow trait lines 253-276)
  - `crates/pgqrs/tests/workflow_tests.rs` - Current test suite
  - `docs/user-guide/concepts/durable-workflows.md` - Current documentation
- Schema files (for migration planning):
  - `crates/pgqrs/src/store/postgres/migrations/` - Postgres migrations
  - `crates/pgqrs/src/store/sqlite/migrations/` - SQLite migrations
  - `crates/pgqrs/src/store/turso/migrations/` - Turso migrations
- Related ADRs:
  - [ADR-0001: Scheduled Retry with retry_at Timestamps](./0001-scheduled-retry-timestamps.md) - Non-blocking retry pattern (philosophy aligned, implementation superseded by message visibility)

## Implementation Notes

### Schema Overview

Three new tables:

```sql
-- Queues (existing table, unchanged)
CREATE TABLE pgqrs_queues (
    queue_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow definitions (templates) - NEW TABLE
CREATE TABLE pgqrs_workflows (
    workflow_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    queue_id BIGINT NOT NULL REFERENCES pgqrs_queues(queue_id),  -- Strong FK: 1:1 relationship
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow runs (executions) - NEW TABLE
CREATE TABLE pgqrs_workflow_runs (
    run_id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL REFERENCES pgqrs_workflows(workflow_id),
    status VARCHAR(50) NOT NULL,  -- PENDING, RUNNING, PAUSED, SUCCESS, ERROR
    input JSONB,      -- Workflow params (single source of truth)
    output JSONB,     -- Workflow result
    error JSONB,      -- Error details
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    worker_id BIGINT REFERENCES pgqrs_workers(worker_id)
);

-- Step completions (for crash recovery) - NEW TABLE
CREATE TABLE pgqrs_workflow_steps (
    run_id BIGINT NOT NULL REFERENCES pgqrs_workflow_runs(run_id),
    step_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,  -- PENDING, RUNNING, PAUSED, SUCCESS, ERROR
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    -- NOTE: No retry_at - message.visible_after is single source of truth (DRY)
    PRIMARY KEY (run_id, step_id)
);
```

**Schema Relationships:**
```
pgqrs_workflows ──1:1──> pgqrs_queues (FK: queue_id)
       │
       │ 1:N
       ▼
pgqrs_workflow_runs
       │
       │ 1:N
       ▼
pgqrs_workflow_steps
```

### Key API Changes

**Setup:**
```rust
// Create workflow definition + queue (one operation)
store.create_workflow("zip_files").await?;
```

**Worker Registration:**
```rust
// Fluent builder with handler in memory
let worker = consumer("worker-1", 8080, "zip_files")
    .handler(zip_files_handler)
    .create(&store)
    .await?;

worker.poll_forever().await?;
```

**Triggering:**
```rust
// Noun-verb pattern
let run_id = workflow("zip_files")
    .trigger(&params)?
    .execute(&store)
    .await?;
```

**Check Status:**
```rust
let run = workflow("zip_files")
    .get_run(run_id, &store)
    .await?;
```

**Steps (inside handler):**
```rust
async fn handler(ctx: &mut dyn Workflow, input: Input) -> Result<Output> {
    let res = ctx.step("step1", || async {
        // work
    }).await?;
    
    Ok(output)
}
```

### Migration Strategy

1. **Schema migration**: Run new table creation migrations
2. **Data migration**: Convert existing workflows to new format
3. **API transition**: Mark old API `#[deprecated]`, add new API
4. **Gradual rollout**: Support both APIs during transition period
5. **Remove old API**: In next major version

See [design doc section 9](../design/workflow-redesign-v2.md#9-migration-path) for detailed migration plan.

### Implementation Phases

**Phase 1: Core Infrastructure** (MVP)
- New schema tables
- Worker registration API
- Trigger API (non-blocking)
- Basic validation

**Phase 2: Execution Engine**
- Worker polling and run execution
- Step execution with crash recovery
- Result storage

**Phase 3: Result Retrieval**
- Polling pattern
- Blocking wait with timeout
- Async notification (Postgres LISTEN/NOTIFY)

**Phase 4: Advanced Features**
- Metrics and monitoring
- Multi-backend testing (Postgres, SQLite, Turso)

See [design doc section 14](../design/workflow-redesign-v2.md#14-implementation-roadmap) for complete roadmap.

---

**Date**: 2026-02-07  
**Author(s)**: Rajat Venkatesh, Architecture Team  
**Reviewers**: (Pending)
