# Workflow Redesign v2: Trigger/Worker Architecture

**Status**: Draft Design  
**Created**: 2026-02-06  
**Author**: Architecture Team

---

## 1. Problem Statement

### Current Design Issues

The current workflow implementation has a fundamental architectural flaw: **it doesn't properly model the relationship between triggers (who submit workflow runs) and workers (who execute them)**.

**Pain Points:**

1. **No Worker Discovery**: No way for workers to advertise "I can execute workflow X"
2. **No Trigger Validation**: Triggers can submit workflows that no worker can handle
3. **Awkward API**: `pgqrs::step(workflow_id, "step_id")` instead of `workflow.step("step_id")`
4. **Confused Responsibilities**: Workflow object doesn't manage its own steps
5. **No Run Concept**: Workflows are treated as one-time executions, not reusable definitions with multiple runs

### Developer Experience Problems

**Trigger Side:**
```rust
// Current: No validation, returns workflow object (not run)
let workflow = pgqrs::workflow()
    .name("process_data")
    .arg(&input)?
    .create(&store)
    .await?;
// How do I know if anyone can execute this?
// How do I get the result later?
```

**Worker Side:**
```rust
// Current: No registration API at all!
// Workers just... exist? How does pgqrs know what they can do?
```

### Why Trigger/Worker Separation Matters

In real applications:
- **Triggers** are scattered: HTTP endpoints, cron jobs, event handlers
- **Workers** are centralized: dedicated processes polling for work
- **Validation must happen at trigger time**: Fast-fail if no worker available
- **Results must be retrievable**: Trigger might not wait, but needs to check later

---

## 2. Proposed Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         APPLICATION SERVICE                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────┐                           ┌──────────────────┐  │
│  │    TRIGGER     │                           │     WORKER       │  │
│  │                │                           │                  │  │
│  │ • HTTP handler │                           │ Discovers:       │  │
│  │ • Cron job     │                           │  - zip_files     │  │
│  │ • Event handler│                           │  - send_emails   │  │
│  │                │                           │                  │  │
│  │ Submits run:   │                           │ Advertises:      │  │
│  │  workflow=zip  │                           │  "I handle A,B"  │  │
│  │  params={...}  │                           │                  │  │
│  └───────┬────────┘                           └─────────▲────────┘  │
│          │                                              │            │
│          │ 1. trigger()                    3. poll() & execute()    │
│          │    validate                        report progress       │
│          │    return run_id                                         │
│          ▼                                              │            │
│  ┌───────────────────────────────────────────────────────┴────────┐ │
│  │                                                                 │ │
│  │          PGQRS WORKFLOW ENGINE                                 │ │
│  │                                                                 │ │
│  │  Routing: workflow_name → queue_name (1:1)                     │ │
│  │                                                                 │ │
│  │  2. Enqueue run:                                               │ │
│  │     - Check queue "zip_files" exists                           │ │
│  │     - Check queue has active consumers                         │ │
│  │     - Create workflow_run record (PENDING)                     │ │
│  │     - Enqueue to queue "zip_files"                             │ │
│  │                                                                 │ │
│  │  State:                                                        │ │
│  │     - Workflow definitions (name, version)                     │ │
│  │     - Workflow runs (run_id, status, input, output)            │ │
│  │     - Step completions (for crash recovery)                    │ │
│  │                                                                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                              ▲                                        │
│                              │                                        │
│                              ▼                                        │
│                     ┌─────────────────┐                              │
│                     │   POSTGRESQL    │                              │
│                     │   SQLITE/TURSO  │                              │
│                     │                 │                              │
│                     │ pgqrs_workflows │                              │
│                     │ pgqrs_workflow_ │                              │
│                     │       runs      │                              │
│                     │ pgqrs_workflow_ │                              │
│                     │       steps     │                              │
│                     │ pgqrs_queues    │                              │
│                     │ pgqrs_workers   │                              │
│                     └─────────────────┘                              │
└───────────────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Workflow Definition**: A named, versioned template (e.g., "zip_files" v1.0)  
**Workflow Run**: A specific execution instance (run_id + parameters)  
**Queue**: One queue per workflow name (1:1 mapping)  
**Worker**: Can subscribe to multiple queues (multiple workflows)

### Run Lifecycle

```
PENDING → RUNNING → SUCCESS (terminal)
                 ↘ ERROR (terminal)
                 ↘ PAUSED (waiting for external event)
```

**PENDING**: Run created, waiting for worker to pick up  
**RUNNING**: Worker executing steps  
**SUCCESS**: Completed with output value (terminal - message archived)  
**ERROR**: Failed with permanent error (terminal - message archived)  
**PAUSED**: Waiting for external event (e.g., human approval)

---

## 3. Database Schema

### Schema Design (From Scratch)

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
    queue_id BIGINT NOT NULL REFERENCES pgqrs_queues(queue_id),  -- Strong FK: workflow → queue (1:1)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow runs (executions) - NEW TABLE
CREATE TABLE pgqrs_workflow_runs (
    run_id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL REFERENCES pgqrs_workflows(workflow_id),
    
    status VARCHAR(50) NOT NULL, -- PENDING, RUNNING, PAUSED, SUCCESS, ERROR
    input JSONB,      -- Workflow params (single source of truth)
    output JSONB,     -- Workflow result
    error JSONB,      -- Error details
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    worker_id BIGINT REFERENCES pgqrs_workers(worker_id),
    
    INDEX idx_run_status (status),
    INDEX idx_run_workflow (workflow_id)
);

-- Step state (for crash recovery) - NEW TABLE
CREATE TABLE pgqrs_workflow_steps (
    run_id BIGINT NOT NULL REFERENCES pgqrs_workflow_runs(run_id),
    step_id VARCHAR(255) NOT NULL,
    
    status VARCHAR(50) NOT NULL, -- PENDING, RUNNING, PAUSED, SUCCESS, ERROR
    input JSONB,
    output JSONB,
    error JSONB,
    
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    -- NOTE: No retry_at column - message.visible_after is single source of truth
    
    PRIMARY KEY (run_id, step_id)
);

-- Workers (existing table, unchanged)
CREATE TABLE pgqrs_workers (
    worker_id BIGSERIAL PRIMARY KEY,
    queue_id BIGINT NOT NULL REFERENCES pgqrs_queues(queue_id),
    hostname VARCHAR(255),
    port INT,
    status VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Messages (existing table, unchanged)
-- Workflow message payload format: {"run_id": 12345}
```

### Schema Relationships

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

**Key Design Decisions:**

1. **Strong FK Link (workflow → queue)**: Enforces 1:1 relationship in schema
2. **Params in run.input only**: Message payload contains only `{"run_id": 12345}`, not full params (DRY)
3. **No retry_at in steps**: Message `visible_after` is single source of truth for retry timing

### Relationship to Existing Tables

**pgqrs_queues**: One queue per workflow (1:1 via FK)
- Queue "zip_files" is owned by workflow "zip_files"
- Queue "send_emails" is owned by workflow "send_emails"

**pgqrs_workers**: Workers register as consumers on queues
- Worker A: subscribes to ["zip_files", "send_emails"] queues
- Worker B: subscribes to ["send_emails"] queue only

**pgqrs_messages**: Workflow runs are enqueued as messages
- Message payload: `{"run_id": 12345}` (run_id only, params in DB)
- Worker dequeues, fetches run details from `pgqrs_workflow_runs`
- Message `visible_after` is **single source of truth** for retry timing (no retry_at in steps)

---

## 4. API Design

### 4.1 Setup: Create Workflow (One-time)

```rust
use pgqrs;

#[tokio::main]
async fn setup() -> Result<(), Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Install schema (first time only)
    pgqrs::admin(&store).install().await?;
    
    // Create workflow definitions (idempotent)
    store.create_workflow("zip_files").await?;
    store.create_workflow("send_emails").await?;
    
    // Behind the scenes (each call):
    // 1. INSERT INTO pgqrs_queues (name) VALUES ('zip_files') RETURNING queue_id
    // 2. INSERT INTO pgqrs_workflows (name, queue_id) VALUES ('zip_files', queue_id)
    
    Ok(())
}
```

**Note:** `create_workflow()` updates TWO tables (`pgqrs_queues` + `pgqrs_workflows`), but semantically it's ONE operation (workflow inherently needs a queue). The FK relationship enforces 1:1 coupling.

### 4.2 Worker Registration (Rust)

```rust
use pgqrs::{consumer, Workflow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ZipParams {
    bucket: String,
    prefix: String,
}

#[derive(Serialize, Deserialize)]
struct ZipResult {
    archive_path: String,
    file_count: usize,
}

// Workflow handler function
async fn zip_files_handler(
    ctx: &mut dyn Workflow,
    params: ZipParams,
) -> Result<ZipResult, anyhow::Error> {
    // Step 1: List files
    let files = ctx.step("list_files", || async {
        s3::list_objects(&params.bucket, &params.prefix).await
    }).await?;
    
    // Step 2: Download files
    let local_files = ctx.step("download_files", || async {
        download_all(&files).await
    }).await?;
    
    // Step 3: Create archive
    let archive_path = ctx.step("create_archive", || async {
        zip::create(&local_files).await
    }).await?;
    
    Ok(ZipResult {
        archive_path,
        file_count: files.len(),
    })
}

async fn send_emails_handler(
    ctx: &mut dyn Workflow,
    params: EmailParams,
) -> Result<EmailResult, anyhow::Error> {
    // Implementation...
    Ok(EmailResult { sent: true })
}

#[tokio::main]
async fn worker_main() -> Result<(), Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Register workers with handlers (fluent builder)
    let zip_worker = consumer("worker-1", 8080, "zip_files")
        .handler(zip_files_handler)
        .create(&store)
        .await?;
    
    let email_worker = consumer("worker-1", 8080, "send_emails")
        .handler(send_emails_handler)
        .create(&store)
        .await?;
    
    // Behind the scenes (each call):
    // 1. Lookup queue_id for queue name
    // 2. INSERT INTO pgqrs_workers (queue_id, hostname, port, status) VALUES (...)
    // 3. Store handler in memory (not persisted to DB)
    
    // Service orchestrates polling (explicit control)
    tokio::select! {
        result = zip_worker.poll_forever() => {
            eprintln!("zip_worker stopped: {:?}", result);
        }
        result = email_worker.poll_forever() => {
            eprintln!("email_worker stopped: {:?}", result);
        }
    }
    
    Ok(())
}
```

**Key Points:**
- **Fluent builder**: `.handler(fn)` attaches handler before `.create()`
- **Handler in memory**: Not persisted to DB, attached at worker startup
- **One table update**: Only `pgqrs_workers` table is modified
- **Service orchestrates**: Application decides how to poll multiple workers (tokio::select!, threads, etc.)

**Implementation Detail:**

The fluent builder also provides a `set_handler()` method internally:

```rust
impl Consumer {
    /// Set or change the workflow handler (in-memory only)
    pub fn set_handler(&mut self, handler: WorkflowHandler) {
        self.handler = Some(handler);
    }
}
```

### 4.3 Trigger Workflow (Rust)

```rust
use pgqrs::workflow;

// Non-blocking: submit and continue
async fn handle_upload(bucket: String, prefix: String) -> Result<i64, Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Trigger workflow (returns run_id immediately)
    let run_id = workflow("zip_files")
        .trigger(&ZipParams { bucket, prefix })?
        .execute(&store)
        .await?;
    
    println!("Started workflow run: {}", run_id);
    Ok(run_id)
}

// Behind the scenes:
// 1. Lookup workflow by name → get workflow_id and queue_id
// 2. INSERT INTO pgqrs_workflow_runs (workflow_id, input, status='PENDING') RETURNING run_id
// 3. INSERT INTO pgqrs_messages (queue_id, payload) VALUES (queue_id, '{"run_id": ...}')
// Returns run_id
```

**Note:** `workflow().trigger()` follows the noun-verb pattern and updates TWO tables (`pgqrs_workflow_runs` + `pgqrs_messages`), but semantically it's ONE operation (triggering a workflow run requires enqueuing it).

### 4.4 Check Status Later

```rust
use pgqrs::workflow;

async fn check_run_status(run_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Get run status
    let run = workflow("zip_files")
        .get_run(run_id, &store)
        .await?;
    
    match run.status {
        RunStatus::Success => {
            let result: ZipResult = run.output()?;
            println!("Completed: {}", result.archive_path);
        }
        RunStatus::Error => {
            println!("Failed: {:?}", run.error);
        }
        RunStatus::Running | RunStatus::Pending => {
            println!("Still running...");
        }
        RunStatus::Paused => {
            println!("Paused (waiting for external event)");
        }
    }
    Ok(())
}
```

### 4.5 Message Payload Structure

**Design Decision:** Message contains only `run_id`, not full params (DRY principle)

```json
{"run_id": 12345}
```

**Worker Processing Flow:**

```rust
// Worker polls queue
let msg = worker.dequeue().await?;

// Extract run_id from message payload
let run_id: i64 = msg.payload["run_id"].as_i64()?;

// Fetch run from DB (includes params in run.input)
let run = store.get_workflow_run(run_id).await?;

// Parse params from run.input (single source of truth)
let params: ZipParams = serde_json::from_value(run.input)?;

// Execute handler
let result = handler(&mut WorkflowContext::new(run_id, &store), params).await;

// Update run status
match result {
    Ok(output) => {
        store.update_run(run_id, RunStatus::Success, Some(output), None).await?;
        msg.archive().await?;  // Terminal state
    }
    Err(e) => {
        handle_error(run_id, e, msg, &store).await?;
    }
}
```

**Benefits:**
- ✅ DRY: Params stored only in `pgqrs_workflow_runs.input`
- ✅ Clean separation: Queue transports run_id, DB stores params
- ✅ Smaller messages
- ✅ Single source of truth for workflow params

### 4.6 Python API

```python
from pgqrs import workflow, consumer, connect
from dataclasses import dataclass

@dataclass
class ZipParams:
    bucket: str
    prefix: str

@dataclass
class ZipResult:
    archive_path: str
    file_count: int

# Workflow handler
async def zip_files_handler(ctx, params: ZipParams) -> ZipResult:
    # Step 1: List files
    files = await ctx.step("list_files", lambda: s3.list_objects(params.bucket, params.prefix))
    
    # Step 2: Download
    local_files = await ctx.step("download_files", lambda: download_all(files))
    
    # Step 3: Create archive
    archive_path = await ctx.step("create_archive", lambda: zip.create(local_files))
    
    return ZipResult(archive_path=archive_path, file_count=len(files))

# Worker registration
async def worker_main():
    store = await connect("postgresql://localhost/mydb")
    
    # Setup (idempotent)
    await store.create_workflow("zip_files")
    
    # Register worker with handler
    worker = await consumer("worker-1", 8080, "zip_files") \
        .handler(zip_files_handler) \
        .create(store)
    
    # Poll forever
    await worker.poll_forever()

# Trigger (non-blocking)
async def handle_upload(bucket: str, prefix: str) -> int:
    store = await connect("postgresql://localhost/mydb")
    
    run_id = await workflow("zip_files") \
        .trigger(ZipParams(bucket=bucket, prefix=prefix)) \
        .execute(store)
    
    print(f"Started run: {run_id}")
    return run_id

# Check status
async def check_status(run_id: int):
    store = await connect("postgresql://localhost/mydb")
    
    run = await workflow("zip_files") \
        .get_run(run_id, store)
    
    if run.status == "SUCCESS":
        result = run.output()  # Automatically deserialized
        print(f"Done: {result.archive_path}")
    elif run.status == "ERROR":
        print(f"Failed: {run.error}")
    else:
        print("Still running...")
```


---

## 5. Worker Discovery & Macros

### 5.1 Macro/Decorator Support

**Rust**: Procedural macro for workflow discovery

```rust
use pgqrs::{workflow_macro, discover_workflows};

// Annotate workflow handlers
#[workflow(name = "zip_files")]
async fn zip_files_handler(
    ctx: &mut dyn Workflow,
    params: ZipParams,
) -> Result<ZipResult, anyhow::Error> {
    // Implementation...
    Ok(ZipResult { /* ... */ })
}

#[workflow(name = "send_emails")]
async fn send_emails_handler(
    ctx: &mut dyn Workflow,
    params: EmailParams,
) -> Result<EmailResult, anyhow::Error> {
    // Implementation...
    Ok(EmailResult { /* ... */ })
}

// Auto-discovery in specific module
#[tokio::main]
async fn main() -> Result<()> {
    let store = pgqrs::connect("postgres://localhost/mydb").await?;
    
    // Discover workflows in this module
    let workflows = discover_workflows_in!(crate::workflows);
    
    // Register all discovered workflows
    for (name, handler) in workflows {
        store.create_workflow(name).await?;
        
        consumer("worker-1", 8080, name)
            .handler(handler)
            .create(&store)
            .await?;
    }
    
    Ok(())
}
```

**Python**: Decorator-based discovery

```python
from pgqrs import workflow_decorator, discover_workflows

# Annotate workflow handlers
@workflow(name="zip_files")
async def zip_files_handler(ctx, params: ZipParams) -> ZipResult:
    # Implementation...
    return ZipResult(...)

@workflow(name="send_emails")
async def send_emails_handler(ctx, params: EmailParams) -> EmailResult:
    # Implementation...
    return EmailResult(...)

# Auto-discovery
async def main():
    store = await connect("postgres://localhost/mydb")
    
    # Discover all decorated workflows
    workflows = discover_workflows()
    
    # Register all
    for name, handler in workflows.items():
        await store.create_workflow(name)
        
        await consumer("worker-1", 8080, name) \
            .handler(handler) \
            .create(store)
```

### 5.2 Discovery Scope

**Option B (Explicit Module)** - Recommended for clarity

```rust
// Discover workflows in specific module only
let workflows = discover_workflows_in!(crate::workflows);
```

```python
# Discover workflows in specific module only
from myapp import workflows
workflows = discover_workflows(workflows)
```

**Why explicit scope?**
- Avoids scanning entire codebase (performance)
- Clear intent (which module contains workflows)
- Prevents accidental registration

### 5.3 Manual Registration (Always Available)

Macros/decorators are **optional conveniences**. Manual registration always works:

```rust
// Explicit registration (no macros needed)
store.create_workflow("zip_files").await?;

let worker = consumer("worker-1", 8080, "zip_files")
    .handler(zip_files_handler)
    .create(&store)
    .await?;
```

### 5.4 Implementation Gaps

**Current State:**
- ✅ Manual registration API works
- ⚠️ Macro/decorator infrastructure exists but incomplete
- ❌ Discovery mechanism not implemented
- ❌ Auto-registration not implemented

**To Complete:**
1. Implement `discover_workflows_in!()` macro (Rust)
2. Implement `discover_workflows()` function (Python)
3. Ensure macros collect workflow metadata at compile time
4. Support runtime iteration over discovered workflows

---

## 6. Validation & Error Handling

### 6.1 Trigger Validation

**Pre-flight checks** before accepting workflow run:

```rust
// Inside workflow().trigger().execute()
async fn execute(&self, store: &Store) -> Result<i64> {
    // 1. Check workflow is registered
    let workflow = store.get_workflow_by_name(&self.workflow_name)
        .await?
        .ok_or(Error::WorkflowNotFound)?;
    
    // 2. Queue automatically exists (created with workflow via FK)
    let queue_id = workflow.queue_id;
    
    // 3. Check queue has active consumers (optional warning)
    let active_workers = store.count_workers_for_queue(queue_id, WorkerStatus::Ready)
        .await?;
    
    if active_workers == 0 {
        // Warning: No workers available, but still accept run
        log::warn!("No active workers for workflow '{}', run will wait in queue", self.workflow_name);
    }
    
    // 4. Create run
    let run_id = store.create_workflow_run(
        workflow.workflow_id,
        &self.params,
    ).await?;
    
    // 5. Enqueue message
    store.enqueue_message(queue_id, json!({"run_id": run_id}))
        .await?;
    
    Ok(run_id)
}
```

### 6.2 Error Types

```rust
pub enum WorkflowError {
    /// Workflow name not registered (no worker advertised it)
    WorkflowNotFound { name: String },
    
    /// Run failed during execution
    ExecutionFailed {
        run_id: i64,
        error: serde_json::Value,
    },
    
    /// Serialization/deserialization error
    SerializationError { source: serde_json::Error },
}
```

---

## 7. Result Retrieval

### 7.1 Immediate Return (Non-blocking)

```rust
let run_id = workflow("zip_files")
    .trigger(&params)?
    .execute(&store)
    .await?;

// Returns run_id immediately, check later
```

### 7.2 Polling Pattern

```rust
async fn wait_for_result<T>(run_id: i64, workflow_name: &str, store: &Store) -> Result<T> {
    loop {
        let run = workflow(workflow_name)
            .get_run(run_id, store)
            .await?;
        
        match run.status {
            RunStatus::Success => {
                return Ok(serde_json::from_value(run.output)?);
            }
            RunStatus::Error => {
                return Err(Error::ExecutionFailed { run_id, error: run.error });
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
```

### 7.3 Async Notification (Postgres)

```rust
// Worker side: NOTIFY on completion
async fn complete_run(run_id: i64, output: Value, store: &Store) -> Result<()> {
    store.update_run(run_id, RunStatus::Success, Some(output), None).await?;
    
    // Notify waiting triggers
    store.execute_raw(&format!("NOTIFY workflow_run_{}, 'completed'", run_id)).await?;
    
    Ok(())
}

// Trigger side: LISTEN for notification
async fn wait_for_result_async<T>(run_id: i64, workflow_name: &str, store: &Store) -> Result<T> {
    // Subscribe to notifications
    let mut listener = store.listen(&format!("workflow_run_{}", run_id)).await?;
    
    loop {
        tokio::select! {
            _ = listener.recv() => {
                // Got notification, fetch result
                let run = workflow(workflow_name)
                    .get_run(run_id, store)
                    .await?;
                    
                if run.status == RunStatus::Success {
                    return Ok(serde_json::from_value(run.output)?);
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(300)) => {
                return Err(Error::Timeout);
            }
        }
    }
}
```

### 7.4 Timeout Handling (Future Enhancement)

```rust
// Potential future API
workflow("zip_files")
    .trigger(&params)?
    .wait()  // blocks until completion
    .timeout(Duration::from_secs(300))  // 5 minute timeout
    .execute(&store)
    .await?
```

---

## 8. Comparison with Current Design

| Aspect | Current Design | Proposed Design |
|--------|----------------|-----------------|
| **Workflow Concept** | One-time execution object | Reusable definition + multiple runs |
| **Run Tracking** | workflow_id only | run_id separate from workflow_id |
| **Queue Mapping** | No concept | 1:1 workflow → queue (enforced via FK) |
| **Trigger API** | `pgqrs::workflow().create()` | `workflow("name").trigger()` (noun-verb) |
| **Worker Registration** | None | `consumer().handler(fn).create()` |
| **Handler Storage** | N/A | In-memory (not persisted) |
| **Message Payload** | Workflow params | `{"run_id": 12345}` only |
| **Step API** | `pgqrs::step(wf_id, "step")` | `ctx.step("step", closure)` |
| **Result Retrieval** | Unclear | `workflow().get_run()` |

### API Comparison

**Current:**
```rust
// Awkward: workflow object but step() is global function
let mut workflow = pgqrs::workflow()
    .name("test_wf")
    .arg(&input)?
    .create(&store)
    .await?;

let step_res = pgqrs::step(workflow.id(), "step1")
    .acquire(&store)
    .await?;
```

**Proposed:**
```rust
// Clean: workflow is a definition, run is an execution
// Setup (one-time)
store.create_workflow("test_wf").await?;

// Trigger (creates run)
let run_id = workflow("test_wf")
    .trigger(&input)?
    .execute(&store)
    .await?;

// Inside handler:
async fn test_wf_handler(ctx: &mut dyn Workflow, input: Input) -> Result<Output> {
    let result = ctx.step("step1", || async {
        // work
    }).await?;
    
    Ok(output)
}
```


---

## 9. Migration Path

*(For reference only - not required for initial implementation)*

### Breaking Changes

1. **API**: `pgqrs::workflow()` → `trigger().workflow()`
2. **Schema**: workflow_id now references definition, not execution
3. **Step API**: Global `step()` → method on Workflow trait

### Deprecation Strategy

**Phase 1**: Add new API alongside old
```rust
// Old (deprecated)
let workflow = pgqrs::workflow().create(&store).await?;

// New
let run_id = trigger().workflow("name").execute(&store).await?;
```

**Phase 2**: Mark old API as `#[deprecated]`

**Phase 3**: Remove old API in next major version

### Data Migration

```sql
-- Migrate existing workflows to new schema
INSERT INTO pgqrs_workflows (name)
SELECT DISTINCT name
FROM old_pgqrs_workflows;

-- Migrate existing executions to runs
INSERT INTO pgqrs_workflow_runs (workflow_id, status, input, output, error, created_at)
SELECT w.workflow_id, old.status, old.input, old.output, old.error, old.created_at
FROM old_pgqrs_workflows old
JOIN pgqrs_workflows w ON w.name = old.name;
```

---

## 10. Error Handling & Retry Strategies

### 10.1 Core Principles

1. **Message visibility is single source of truth** for retry timing (no `retry_at` in steps - DRY)
2. **Terminal states archive messages** (SUCCESS, ERROR with permanent failure)
3. **Non-terminal states keep messages in queue** (transient errors, paused, crashes)
4. **Same run continues** (don't create new run for retry - preserves step state and audit trail)
5. **Worker never re-polls** (updates message visibility and moves on)

### 10.2 State Matrix

| Scenario | Message State | Run Status | Step Status | Worker Action |
|----------|--------------|------------|-------------|---------------|
| **Success** | ARCHIVED | SUCCESS | All SUCCESS | Archive message |
| **Permanent Error** | ARCHIVED | ERROR | Some ERROR | Archive message |
| **Worker Crash** | INVISIBLE → timeout → VISIBLE | RUNNING | Some SUCCESS, some PENDING | Another worker dequeues |
| **Transient Error** | visible_after = NOW() + retry_delay | RUNNING | PENDING | Update visibility, move on |
| **Paused** | visible_after = NOW() + pause_interval | PAUSED | PAUSED | Check condition, bump visibility, move on |

### 10.3 Scenario Details

#### Scenario 1: Worker Successfully Executes Workflow ✅ TERMINAL

```rust
// Worker dequeues message {"run_id": 123}
// Handler executes all steps successfully
// Worker updates run: status = SUCCESS, output = {...}
// Worker archives message
```

**Final State:**
- Message: ARCHIVED (work complete)
- Run: status = SUCCESS, completed_at = NOW()
- Steps: All status = SUCCESS

**Why terminal?** Work completed successfully, no reason to re-execute.

---

#### Scenario 2: Step Returns Permanent Error ❌ TERMINAL

```rust
async fn handler(ctx: &mut dyn Workflow, params: Params) -> Result<Output> {
    let file = ctx.step("download", || async {
        s3::download(&params.key).await
            .map_err(|e| match e {
                S3Error::NotFound => PermanentError::new("File not found"),
                S3Error::NetworkTimeout => TransientError::new("Network timeout"),
                _ => PermanentError::new(format!("S3 error: {}", e)),
            })
    }).await?;
    
    Ok(output)
}
```

**Handler returns permanent error:**
```rust
// Worker updates run: status = ERROR, error = "File not found"
// Worker archives message
```

**Final State:**
- Message: ARCHIVED (work complete, failed permanently)
- Run: status = ERROR, error = "File not found"
- Steps: step "download": status = ERROR, error = "File not found"

**Why terminal?** Retrying won't help. File doesn't exist. Service can create **new run** if needed (with different params).

**Key distinction:** Same run = retry with same params. New run = retry with different params.

---

#### Scenario 3: Worker Crashes 💥 NON-TERMINAL

```rust
// Worker dequeues message {"run_id": 123} (message invisible for visibility_timeout)
// Worker updates run: status = RUNNING
// Step "download" executes, saves to DB: status = SUCCESS
// Worker crashes (hardware failure, OOM, SIGKILL)
// Message visibility timeout expires (e.g., 5 minutes)
// Different worker dequeues same message {"run_id": 123}
// Worker calls handler again
// Step "download" checks DB, sees SUCCESS, returns cached result
// Step "process" executes for first time
// Worker completes, updates run: status = SUCCESS
// Worker archives message
```

**State while crashed (before timeout):**
- Message: INVISIBLE (dequeued but not acked, waiting for timeout)
- Run: status = RUNNING, started_at = T0
- Steps: step "download": status = SUCCESS (completed before crash)

**State after timeout expires:**
- Message: VISIBLE AGAIN (timed out, available for dequeue)
- Run: status = RUNNING (unchanged)
- Steps: Same (download still SUCCESS)

**Why non-terminal?** Work not complete. Another worker can pick up and continue from last completed step.

**Key insight:** Steps are idempotent. Handler re-executes, steps check DB before running.

---

#### Scenario 4: Step Returns Transient Error 🔄 NON-TERMINAL

```rust
async fn handler(ctx: &mut dyn Workflow, params: Params) -> Result<Output> {
    let data = ctx.step("api_call", || async {
        external_api::call(&params)
            .await
            .map_err(|e| match e {
                ApiError::RateLimit => TransientError::with_backoff(30),  // 30s
                ApiError::Timeout => TransientError::with_backoff(10),    // 10s
                ApiError::Unauthorized => PermanentError::new("Auth failed"),
                _ => TransientError::with_backoff(60),
            })
    }).await?;
    
    Ok(output)
}
```

**Handler returns transient error:**
```rust
// Worker catches TransientError
// Worker updates step: status = PENDING (no retry_at - DRY!)
// Worker updates message: visible_after = NOW() + 30s
// Worker moves on to next message (doesn't block)

// ... 30 seconds later ...

// Message becomes visible
// Worker (same or different) dequeues message {"run_id": 123}
// Worker calls handler
// Step "api_call" executes again (status was PENDING)
// If succeeds: continues to next step
// If fails again: repeat with exponential backoff
```

**State during wait period:**
- Message: visible_after = NOW() + 30s (in queue but invisible)
- Run: status = RUNNING
- Steps: step "api_call": status = PENDING

**Why non-terminal?** Transient error, retry likely to succeed.

**Key insight:** Worker updates message visibility and moves on. Doesn't hold message or block on sleep.

**Single source of truth:** `message.visible_after` controls retry timing (no duplicate `retry_at` in steps table).

---

#### Scenario 5: Step Wants to Pause ⏸️ NON-TERMINAL

```rust
async fn handler(ctx: &mut dyn Workflow, params: Params) -> Result<Output> {
    // Send approval request
    ctx.step("send_approval_request", || async {
        email::send_approval_request(&params.approver).await
    }).await?;
    
    // Pause until approval received
    let approval_data = ctx.step("wait_for_approval", || async {
        Err(PauseError::new("Waiting for approval"))
    }).await?;
    
    // Process approval
    let result = ctx.step("process_approval", || async {
        process(approval_data).await
    }).await?;
    
    Ok(result)
}
```

**Handler returns pause error:**
```rust
// Worker catches PauseError
// Worker updates step: status = PAUSED
// Worker updates run: status = PAUSED, paused_at = NOW()
// Worker updates message: visible_after = NOW() + pause_check_interval (e.g., 1 hour)
// Worker moves on

// ... 1 hour later ...

// Message becomes visible
// Worker dequeues message {"run_id": 123}
// Worker checks run: status = PAUSED
// Worker checks if resume condition met (e.g., query approval DB)
// If not met: update visibility to NOW() + pause_check_interval, move on
// If met: update run status = RUNNING, execute handler
```

**State while paused:**
- Message: visible_after = NOW() + pause_check_interval (bumped periodically)
- Run: status = PAUSED, paused_at = T0
- Steps: step "wait_for_approval": status = PAUSED

**Resume via external event:**
```rust
// Approval webhook arrives
async fn approval_webhook(run_id: i64, approval_data: Value) -> Result<()> {
    let store = get_store();
    
    // Option 1: Update step with approval data, let worker poll naturally
    store.update_step(run_id, "wait_for_approval", 
        StepStatus::Pending, Some(approval_data)).await?;
    store.update_run(run_id, RunStatus::Running).await?;
    
    // Option 2: Make message immediately visible (skip wait)
    let message = store.find_message_by_run_id(run_id).await?;
    store.update_message_visibility(message.message_id, Utc::now()).await?;
    
    Ok(())
}
```

**Why non-terminal?** Work not complete, but cannot progress without external event.

**Configuration:**
```rust
// Global configuration
let config = WorkflowConfig {
    pause_check_interval: Duration::hours(1),  // Default: check every hour
    message_visibility_timeout: Duration::minutes(5),  // For crash recovery
};
let store = pgqrs::connect_with(config).await?;
```

**Pause check interval:**
- **Default:** 1 hour (most paused workflows wait hours/days)
- **Configurable globally** (not per-workflow - keep it simple)
- **Trade-off:** Longer = less worker churn, but slower to react when resumed

---

### 10.4 Retry Semantics: Same Run vs. New Run

**Same Run (Retry):**
- Use when: Transient error, worker crash, pause/resume
- Preserves: Step completion state (skip already-successful steps)
- Audit trail: One run_id from start to finish
- Idempotency: Steps check (run_id, step_id) in DB before executing

**New Run (Re-trigger):**
- Use when: Permanent error, want to retry with different params
- Fresh start: All steps execute from beginning
- Audit trail: Multiple run_ids, can track which attempt succeeded
- Different params: Fix the issue (e.g., correct file path)

**Example:**
```rust
// Permanent error: file not found
let run_id_1 = trigger()
    .workflow("process_file")
    .params(&Params { key: "wrong/path.txt" })?
    .execute(&store).await?;
// Completes with ERROR: "File not found"

// Check status
let run = pgqrs::get_run(run_id_1, &store).await?;
assert_eq!(run.status, RunStatus::Error);

// Retry with CORRECTED params (new run)
let run_id_2 = trigger()
    .workflow("process_file")
    .params(&Params { key: "correct/path.txt" })?  // Fixed!
    .execute(&store).await?;
// Fresh run, may succeed
```

---

### 10.5 Implementation: DRY Retry Mechanism

**No `retry_at` in steps table. Message visibility is single source of truth.**

**When step fails transiently:**
```rust
async fn handle_step_transient_error(
    message_id: i64,
    retry_delay: Duration,
    store: &Store,
) -> Result<()> {
    // Update message visibility (single source of truth)
    let visible_after = Utc::now() + retry_delay;
    store.update_message_visibility(message_id, visible_after).await?;
    
    // Step state remains PENDING (or doesn't exist yet)
    // No duplicate retry_at timestamp
    
    Ok(())
}
```

**When worker dequeues message:**
```rust
// Worker dequeues (visibility timeout passed)
let message = store.dequeue(queue_id).await?;
let run_id = message.payload["run_id"];

// Worker calls handler
// Handler calls step
// Step checks DB: status = PENDING (no retry_at to check)
// Step executes
```

**Benefits:**
- ✅ No duplicate timestamps (DRY principle)
- ✅ Queue infrastructure handles retry naturally
- ✅ Simpler implementation (one less column, one less check)
- ✅ Message visibility semantics are intuitive ("when can this work continue?")

---

### 10.6 Error Types

```rust
pub enum WorkflowError {
    /// Permanent error - run should go to ERROR state, archive message
    Permanent { message: String },
    
    /// Transient error - run stays RUNNING, update message visibility
    Transient { 
        message: String, 
        retry_after: Duration,
    },
    
    /// Pause - run goes to PAUSED, bump message visibility periodically
    Paused { 
        message: String,
        resume_condition: Value,
    },
}
```

**Error classification guidance:**
- **Permanent:** Invalid input, auth failure, resource not found, logic error
- **Transient:** Network timeout, rate limit, temporary unavailability, deadlock
- **Paused:** Human approval needed, external event required, scheduled time not reached

---

## 11. Versioning Best Practice

pgqrs does not have built-in versioning. Workflows are identified by name only. To support multiple versions of a workflow, **include the version in the workflow name**.

### Recommended Naming Convention

```rust
// Pattern: {workflow}_{version}
workflow_worker()
    .workflow("process_file_v1", handler_v1)
    .register(&store).await?;

workflow_worker()
    .workflow("process_file_v2", handler_v2)
    .register(&store).await?;

// Trigger specific "version"
trigger()
    .workflow("process_file_v2")  // Just a name to pgqrs
    .params(&params)?
    .execute(&store).await?;
```

**From pgqrs perspective:** These are two completely separate workflows with different names.

**From user perspective:** Two versions of the same logical workflow.

### Deployment Strategies

**Blue-Green Deployment:**
```rust
// 1. Deploy workers with new version
workflow_worker()
    .workflow("process_file_v2", handler_v2)
    .register(&store).await?;

// 2. Update triggers to use new version
trigger()
    .workflow("process_file_v2")  // Switch from v1 to v2
    .params(&params)?
    .execute(&store).await?;

// 3. Wait for v1 queue to drain
// 4. Decommission v1 workers
```

**Gradual Rollout:**
```rust
// Route percentage to each version
let workflow_name = if rand::random::<f64>() < 0.9 {
    "process_file_v1"  // 90% of traffic
} else {
    "process_file_v2"  // 10% of traffic (canary)
};

trigger()
    .workflow(workflow_name)
    .params(&params)?
    .execute(&store).await?;
```

### Naming Conventions

**Semver-style (recommended):**
```rust
"process_file_1_0_0"  // Major.minor.patch
"process_file_2_0_0"  // Breaking change
"process_file_2_1_0"  // New feature
```

**Why underscores?** Avoid special characters in queue names (`.` may have restrictions).

**Date-based:**
```rust
"process_file_2024_02_07"
"process_file_2024_03_15"
```

**Git hash-based:**
```rust
"process_file_a3f2c1b"
"process_file_d9e4f2a"
```

**Environment-based:**
```rust
"process_file_prod"
"process_file_staging"
```

### Why No Built-in Versioning?

1. **Simplicity:** One less concept in pgqrs
2. **Flexibility:** Users can version however they want (semver, date, hash, environment)
3. **Clarity:** "Different workflow name" is clearer than "same workflow, different version"
4. **No Magic:** No implicit "default version" or "latest" resolution
5. **User Control:** Versioning strategy is application-level concern, not infrastructure

---

## 12. Open Questions

### 12.1 Multi-tenancy

**Question**: How do we isolate workflows across tenants?

**Options:**
- Schema-per-tenant (existing pattern)
- Tenant-ID in workflow/run tables
- Queue-naming convention: `{tenant}_zip_files`

### 12.2 Performance

**Concern**: Is queue-per-workflow scalable?

**Analysis**:
- 100 workflows = 100 queues
- Each worker polls multiple queues (not a problem)
- PostgreSQL handles thousands of tables easily

**Alternative**: Route by message payload instead of queue
```json
{"workflow": "zip_files", "run_id": 123}
```
Pro: One queue for all workflows  
Con: Workers must filter messages they can't handle

### 12.3 Monitoring

**What to track:**
- Run success/failure rates per workflow
- Average execution time per workflow
- Queue depth per workflow
- Worker health per workflow

**Metrics API:**
```rust
let metrics = pgqrs::workflow_metrics("zip_files", &store).await?;
println!("Success rate: {:.2}%", metrics.success_rate);
println!("Avg duration: {:?}", metrics.avg_duration);
```

---

## 13. Examples

### 13.1 Complete Example: File Processing Service

```rust
use pgqrs::{workflow, consumer, Workflow};
use serde::{Deserialize, Serialize};
use tokio;

// === Data Types ===

#[derive(Serialize, Deserialize)]
struct ProcessFileParams {
    bucket: String,
    key: String,
}

#[derive(Serialize, Deserialize)]
struct ProcessFileResult {
    processed_key: String,
    size_bytes: usize,
}

// === Workflow Handler ===

async fn process_file_handler(
    ctx: &mut dyn Workflow,
    params: ProcessFileParams,
) -> Result<ProcessFileResult, anyhow::Error> {
    // Step 1: Download file
    let file_data = ctx.step("download", || async {
        s3::download(&params.bucket, &params.key).await
    }).await?;
    
    // Step 2: Process (e.g., resize image)
    let processed_data = ctx.step("process", || async {
        image::resize(&file_data).await
    }).await?;
    
    // Step 3: Upload result
    let processed_key = ctx.step("upload", || async {
        s3::upload(&params.bucket, &processed_data).await
    }).await?;
    
    Ok(ProcessFileResult {
        processed_key,
        size_bytes: processed_data.len(),
    })
}

// === Worker Process ===

#[tokio::main]
async fn worker_main() -> Result<(), Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Install schema (first time only)
    pgqrs::admin(&store).install().await?;
    
    // Create workflow definition (idempotent)
    store.create_workflow("process_file").await?;
    
    // Register worker with handler
    let worker = consumer("worker-1", 8080, "process_file")
        .handler(process_file_handler)
        .create(&store)
        .await?;
    
    // Poll forever
    worker.poll_forever().await?;
    
    Ok(())
}

// === Trigger (HTTP Handler) ===

async fn handle_upload(
    bucket: String,
    key: String,
) -> Result<String, Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Trigger workflow (non-blocking)
    let run_id = workflow("process_file")
        .trigger(&ProcessFileParams { bucket, key })?
        .execute(&store)
        .await?;
    
    Ok(format!("Processing started: run_id={}", run_id))
}

// === Check Result Later ===

async fn get_result(run_id: i64) -> Result<ProcessFileResult, Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    let run = workflow("process_file")
        .get_run(run_id, &store)
        .await?;
    
    match run.status {
        RunStatus::Success => {
            Ok(run.output()?)
        }
        RunStatus::Error => {
            Err(format!("Workflow failed: {:?}", run.error).into())
        }
        _ => {
            Err("Still running".into())
        }
    }
}
```

### 13.2 Crash Recovery Example

```rust
// Worker crashes after step 1 completes
async fn process_file_handler(ctx: &mut dyn Workflow, params: ProcessFileParams) 
    -> Result<ProcessFileResult, anyhow::Error> 
{
    // Step 1: Download (completes, saved to DB)
    let file_data = ctx.step("download", || async {
        s3::download(&params.bucket, &params.key).await
    }).await?;
    
    // CRASH HERE! Worker dies.
    
    // Step 2: Process (never executes)
    let processed_data = ctx.step("process", || async {
        image::resize(&file_data).await
    }).await?;
    
    Ok(...)
}

// When worker restarts:
// 1. Message becomes visible again (visibility timeout expired)
// 2. Worker dequeues same message {"run_id": 123}
// 3. Worker fetches run from DB
// 4. Worker calls handler again
// 5. Step "download" checks DB, sees SUCCESS, returns cached result
// 6. Step "process" executes for first time
// 7. Run completes successfully

// User sees no difference - workflow resumed seamlessly
```

### 13.3 Multi-Worker Example

```rust
#[tokio::main]
async fn multi_worker_main() -> Result<()> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    
    // Setup workflows (one-time, idempotent)
    store.create_workflow("zip_files").await?;
    store.create_workflow("send_emails").await?;
    
    // Register multiple workers
    let zip_worker = consumer("worker-1", 8080, "zip_files")
        .handler(zip_files_handler)
        .create(&store)
        .await?;
    
    let email_worker = consumer("worker-1", 8080, "send_emails")
        .handler(send_emails_handler)
        .create(&store)
        .await?;
    
    // Service orchestrates polling
    tokio::select! {
        result = zip_worker.poll_forever() => {
            eprintln!("zip_worker stopped: {:?}", result);
        }
        result = email_worker.poll_forever() => {
            eprintln!("email_worker stopped: {:?}", result);
        }
    }
    
    Ok(())
}
```

---

## 14. Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] New schema: pgqrs_workflows, pgqrs_workflow_runs, pgqrs_workflow_steps
- [ ] Worker registration API
- [ ] Trigger API (non-blocking)
- [ ] Basic validation (queue exists, workers available)

### Phase 2: Execution Engine
- [ ] Worker polling and run execution
- [ ] Step execution with crash recovery
- [ ] Result storage
- [ ] Error handling

### Phase 3: Result Retrieval
- [ ] Polling pattern implementation
- [ ] Async notification (Postgres LISTEN/NOTIFY)
- [ ] Timeout handling

### Phase 4: Advanced Features
- [ ] Metrics and monitoring
- [ ] Multi-worker coordination
- [ ] Performance optimization

### Phase 5: Language Support
- [ ] Python bindings
- [ ] Decorator-based discovery
- [ ] Examples and documentation

---

## 15. Conclusion

This redesign fundamentally improves the developer experience by:

1. **Clear separation**: Triggers submit runs, workers execute them
2. **Strong FK relationship**: Workflow → Queue (1:1) enforced in schema
3. **Clean API**: `workflow().trigger()` (noun-verb), `consumer().handler(fn).create()`
4. **DRY principle**: Message contains only run_id, params stored in DB once
5. **Natural retry**: Message queue handles crash recovery and transient errors
6. **DRY retry mechanism**: Message visibility is single source of truth (no duplicate retry_at)
7. **Same run continues**: Preserves step state and audit trail across retries
8. **Pause support**: Workflows can wait for external events (human approval, scheduled time)
9. **Reusable workflows**: One definition, many runs
10. **Handler in memory**: Not persisted, attached at worker startup
11. **Single-table updates**: Each API call updates one table (except semantically-coupled operations)

**API Design Principles:**
- **Noun-verb pattern**: `workflow().trigger()`, `workflow().get_run()`
- **Fluent builders**: `consumer().handler(fn).create()`
- **Single responsibility**: Each method updates one table (with justified exceptions)
- **Message payload**: Contains only run_id (DRY - params in DB)

### Issues Resolved

**Issue #152: Workflows cannot restart after error or crash**
- **Root cause:** Current design has no message queue for workflows, no retry mechanism
- **Resolution:** Message queue provides natural retry. Same run continues via message redelivery
- **Details:** See section 10.3 (Worker Crashes) and 10.4 (Transient Errors)

### Next Steps

1. Review design document and gather feedback
2. Validate schema works across all backends (Postgres, SQLite, Turso)
3. Begin Phase 1 implementation (see section 13)

