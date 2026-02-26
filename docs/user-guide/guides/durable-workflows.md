# Durable Workflows Guide

This guide walks you through building a **durable workflow** with pgqrs v0.14. Durable workflows are multi-step processes that survive worker crashes, handle transient errors with retries, and can even pause for external events.

## Prerequisites

- pgqrs v0.14+ installed
- PostgreSQL running
- Database connection string (DSN)

## What We'll Build

We'll create data processing workflows that demonstrate:

1. **Successful execution** - basic workflow completion
2. **Crash recovery** - workflow resumes from cached step results
3. **Transient errors** - automatic retry with backoff
4. **Pausing** - wait for external events (human approval, webhooks)

## Setup

The snippets in this page focus on the durable workflow patterns.

They assume you already have:

- `store` (connected + bootstrapped)

If you want fully runnable examples end-to-end, use the guide tests directly:

- Rust: `crates/pgqrs/tests/concurrent_tests.rs`

## Workflow Patterns

### Crash Recovery

When a worker crashes mid-execution, the workflow can resume from the last completed step. Step results are cached in the database to avoid repeating work.

#### 1. Define the Workflow with Durable Steps

Use `pgqrs::workflow_step()` wrappers alongside `#[pgqrs_workflow]` to automatically track the completion status and cached results for each step.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_crash_define"
    ```

#### 2. Execute and Simulate Crash

When the workflow begins executing on the first consumer, it processes `step1` and saves the output (e.g., `{"data": "from step 1"}`).

=== "Rust"

    ```rust
    // Trigger the workflow run
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_crash_trigger"

    // Start processing
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_crash_first_run"
    ```

If the consumer process crashes before finishing `step2`, the in-memory execution halts. The `run` status remains active in the database.

#### 3. Release and Recover

Usually, the orchestrator detects crashed workers via timeouts and releases the message back to the queue. For demonstration, we manually release that worker's messages.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_crash_simulate"
    ```

A new consumer picks up the workflow run. When the execution hits `step1` again, pgqrs sees it is already cached, skips executing the closure, and returns the cached result immediately. The execution smoothly resumes at `step2`.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_crash_recovery"
    ```

### Transient Errors

When a step fails with a transient error (e.g., network timeout), the workflow will be retried automatically with backoff.

#### 1. Define the Transient Error Step

If a closure inside `pgqrs::workflow_step()` returns a `pgqrs::Error::Transient`, it signals to the orchestrator to automatically retry the step later.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_transient_define"
    ```

#### 2. Execute the Workflow

When the worker encounters the transient error, it will halt execution but automatically schedule the run to retry in the background.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_transient_run"
    ```

#### 3. Inspect the Paused Status

The workflow run remains in the `Running` state, while the specific step is marked with `Error` and is assigned a `retry_at` timestamp in the database.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_transient_inspect"
    ```

### Pausing for External Events

Workflows can pause execution and wait for external events (like human approval or webhook callbacks).

#### 1. Define a Paused Step

Returning `pgqrs::Error::Paused` tells the workflow to stop its execution until it is externally resumed or until the timeout `resume_after` is reached.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_pause_define"
    ```

#### 2. Process Output

Just like transient errors, when the worker runs into the pause signal, it releases the workflow run smoothly.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_pause_run"
    ```

#### 3. Inspect Status

The overall workflow run transitions into a `Paused` state. It won't execute further until the `resume_after` duration expires or an admin resumes it manually.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:durable_workflow_pause_inspect"
    ```

## Step-by-Step: Basic Workflow

For a complete step-by-step guide, see [Basic Workflow](basic-workflow.md).

## Advanced: Manual Step Control

For advanced scenarios where you need more control over step execution, you can use the manual step API:

```rust
// Acquire a step
let step = pgqrs::step()
    .run(&run)
    .name("fetch_data")
    .execute()
    .await?;

// Check if step needs execution
if step.status() == pgqrs::WorkflowStatus::Running {
    // Do the work
    let result = do_work().await?;

    // Mark step complete
    run.complete_step("fetch_data", result).await?;
}
```

## Best Practices

1. **Idempotency**: Ensure your step closures are idempotent, especially if they have side effects outside of pgqrs.
2. **Step Granularity**: Balance between durability and performance. Too many small steps create database overhead; too few large steps mean more work is repeated on crash.
3. **Use workflow_step**: Prefer `pgqrs::workflow_step()` for automatic step caching. Only use manual step API when you need fine-grained control.

## Next Steps

- [Workflow API Reference](../api/workflows.md) - Complete API details.
- [Concepts: Durable Execution](../concepts/durable-workflows.md) - Deep dive into the architecture.
