# Engineering Design: pgqrs Durable Workflows

**Status:** Draft
**Initial PRD:** [pgqrs-durable-workflows-prd.md](./pgqrs-durable-workflows-prd.md)

## 1. Overview

This document details the engineering design for adding **Durable Workflows** to `pgqrs`. The goal is to allow multi-step workflows that survive crashes and restarts, guaranteeing exact-once logical execution of steps.

## 2. Terminology

*   **Workflow**: A durable, interruptible execution of code, composed of multiple steps. Internally represented as a DAG (Directed Acyclic Graph).
*   **Step**: A single, atomic unit of execution within a workflow.
*   **Executor**: The process or thread responsible for running the workflow.

## 3. Database Schema

We will introduce two new tables to track workflow and step execution.

### 3.1 `pgqrs_workflows`

Stores the state of the overall workflow execution.

```sql
CREATE TYPE pgqrs_workflow_status AS ENUM ('PENDING', 'RUNNING', 'SUCCESS', 'ERROR');

CREATE TABLE pgqrs_workflows (
    workflow_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executor_id VARCHAR(255)
);
```

### 3.2 `pgqrs_workflow_steps`

Stores the state of individual steps within a workflow.

```sql
CREATE TABLE pgqrs_workflow_steps (
    workflow_id UUID NOT NULL REFERENCES pgqrs_workflows(workflow_id) ON DELETE CASCADE,
    step_id VARCHAR(255) NOT NULL, -- Logical ID of the step (e.g., function name)
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, step_id)
);
```

## 4. Rust API

The Rust API provides both low-level RAII guards and high-level procedural macros.

### 4.1 Low-Level: `StepGuard`

The `StepGuard` manages the state transitions of a step.

*   `new`: Checks if the step is already `SUCCESS`.
    *   If `SUCCESS`, it returns `Ok(None)` but **Crucially** includes the deserialized output of the previously completed step.
    *   If `PENDING`/`RUNNING` (crash recovery) or new, it returns `Ok(Some(Guard))`.
*   `success`: Persists output and marks step as `SUCCESS`.
*   `fail`: Persists error and marks step as `ERROR`.

```rust
pub struct StepGuard { /* ... */ }

pub enum StepResult<T> {
    /// Step needs to be executed.
    Execute(StepGuard),
    /// Step was already completed, here is the result.
    Skipped(T),
}

impl StepGuard {
    pub async fn new<T: DeserializeOwned>(pool: &PgPool, workflow_id: Uuid, step_id: &str) -> Result<StepResult<T>> {
        // ... implementation checking DB ...
    }
}
```

### 4.2 High-Level: Procedural Macros

We will support `#[pgqrs_workflow]` and `#[pgqrs_step]` macros to reduce boilerplate.

```rust
use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

#[pgqrs_step]
async fn fetch_data(url: String) -> Result<String> {
    reqwest::get(&url).await?.text().await?
}

#[pgqrs_step]
async fn process_data(data: String) -> Result<ParsedData> {
    parse(data)
}

#[pgqrs_workflow]
async fn data_pipeline(url: String) -> Result<ParsedData> {
    // Macros expand to StepGuard logic usage
    let data = fetch_data(url).await?;
    let result = process_data(data).await?;
    Ok(result)
}
```

**Macro Expansion Logic:**
The `#[pgqrs_step]` macro wraps the function body. It attempts to create a `StepGuard`.
- If `Skipped(val)`: Returns `val` immediately.
- If `Execute(guard)`: Runs the body. If success, calls `guard.success(output)` and returns output. If error, calls `guard.fail(error)` and returns error.

## 5. Python API

The Python API mirrors the Rust experience, leveraging decorators and `py-pgqrs` bindings.

### 5.1 Annotations & Classes

*   `@pgqrs.workflow`: Designates a function as a workflow entry point.
*   `@pgqrs.step`: Designates a function as a distinct step.

**Bindings:**
We will expose a `WorkflowContext` class to Python, which wraps the Rust connection pool and `workflow_id`.

```python
# py-pgqrs/src/lib.rs (Conceptual)
#[pyclass]
struct ClientContext {
    inner: Arc<pgqrs::WorkflowClient>,
}

#[pymethods]
impl ClientContext {
    fn check_step(&self, step_id: &str) -> PyResult<StepResult> {
        // Calls the Rust core primitive `StepGuard::new`
        // Returns status/output without Python knowing about SQL tables
        self.inner.check_step(step_id).map_err(...)
    }
}
```

### 5.2 Python Usage

The Python decorators will use this `ClientContext` to interact with the system. **Crucially, the Python layer does not execute SQL directly.** It delegates all state management to the underlying Rust `StepGuard`.

```python
import pgqrs

@pgqrs.step(id="fetch")
def fetch_data(ctx, url: str) -> str:
    # Context is passed automatically to handle persistence
    return requests.get(url).text

@pgqrs.step(id="process")
def process_data(ctx, data: str) -> dict:
    return json.loads(data)

@pgqrs.workflow
def my_pipeline(ctx, url: str):
    # Workflow logic looks like normal python code
    data = fetch_data(ctx, url)
    return process_data(ctx, data)
```

**Implementation Detail:**
The `@pgqrs.step` decorator intercepts the call. It calls into Rust (via `PyO3` exposed method on the context).
- The Rust method (`StepGuard::new`) performs the DB lookup and locking.
- If status is `SUCCESS`, Rust returns the deserialized output. Python returns this value immediately.
- If status is `PENDING`, Python executes the function.
- Upon return, Python calls `guard.success(output)` (binding to Rust).

## 6. Execution Model: Concurrency & Dynamics

### 6.1 Dynamic Workflows
Since `pgqrs` workflows are **code-first**, dynamic workflows (conditionals, loops) are supported out of the box. The structure of the DAG is determined at runtime.

```python
@pgqrs.workflow
def dynamic_flow(ctx, condition: bool):
    step_a(ctx)
    if condition:
        step_b(ctx)  # This step only exists in DB for this run if condition is True
    else:
        step_c(ctx)
```

### 6.2 Concurrent Steps
To support true parallel execution of steps (e.g., "Fan-out"), we need to leverage the `pgqrs` queue system itself.

1.  **Async/Await Concurrency:** Within a single executor (process), multiple `async` steps can run concurrently (e.g., Rust `tokio::join!`). `StepGuard` handles DB locking to ensure atomic updates.
2.  **Distributed Parallelism:** A step can "spawn" other steps by enqueueing them as standard `pgqrs` messages. The parent workflow then waits (suspends) until child steps complete. *Note: This is an advanced feature for Phase 2.*

**Phase 1 Strategy:**
Support **Concurrent Async Execution** within the workflow executor.
```rust
// Rust
let (res1, res2) = tokio::join!(step_a(), step_b());
```
This works because `StepGuard` will track each step independently by ID.

## 7. Case Study: Airflow Compatibility

Let's examine how an idiomatic Airflow DAG maps to `pgqrs`.

**Airflow Example:**
```python
with DAG("my_dag", start_date=datetime(2023, 1, 1)) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(task_id="sleep", bash_command="sleep 5")
    t3 = TemplatedOperator(task_id="templated", ...)

    t1 >> [t2, t3]
```

**pgqrs Equivalent (Python):**

```python
import pgqrs
import subprocess

@pgqrs.step(id="print_date")
def print_date(ctx):
    subprocess.run(["date"])

@pgqrs.step(id="sleep")
def sleep_step(ctx):
    subprocess.run(["sleep", "5"])

@pgqrs.step(id="templated")
def templated_step(ctx):
    # ... logic ...
    pass

@pgqrs.workflow
def my_dag(ctx):
    # t1 runs first
    print_date(ctx)

    # t2 and t3 run concurrenty (conceptually)
    # In synchronous Python, this runs sequentially:
    sleep_step(ctx)
    templated_step(ctx)

    # To achieve parallel execution in Python, we would use asyncio
    # or a future framing where `step` returns a Promise/Future.
```

**Analysis:**
- **Dependencies:** Airflow uses `>>` to build a graph object. `pgqrs` uses imperative code execution order. `t1` completes before `t2` is called.
- **Branching:** Airflow `[t2, t3]` implies parallel execution. In `pgqrs` code-first approach, this maps to `asyncio.gather(t2(), t3())` or threading.
- **Retries:** Airflow handles retries via executor. `pgqrs` relies on the Workflow Executor crashing and restarting. When it restarts, `t1` is skipped (already done), and it proceeds to `t2`.

**Conclusion:** `pgqrs` can support Airflow-like patterns but shifts the definition from a "Static Graph Object" to "Imperative Code", offering more flexibility for dynamic flows but requiring explicit handling of concurrency.
