# Basic Workflow Guide

This guide walks you through setting up a complete producer-consumer workflow using the new Trigger/Worker architecture introduced in pgqrs v0.14.

## What You'll Build

A simple task queue system where:

1. A **Trigger** (Producer) submits tasks to a workflow.
2. A **Worker** (Consumer) discovers and executes those tasks.
3. You monitor the workflow status and results.

## Prerequisites

- pgqrs v0.14+ installed
- PostgreSQL running
- Database connection string (DSN)

## Setup

The snippets in this page focus on the workflow patterns.

They assume you already have:

- `store` (connected + bootstrapped)

If you want fully runnable examples end-to-end, use the guide tests directly:

- Rust: `crates/pgqrs/tests/guide_tests.rs`


## Step 1: Define and Create the Workflow

In pgqrs v0.14, workflows must be defined before they can be triggered. This is an idempotent operation that sets up the necessary queues and metadata.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_workflow_define"
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def setup():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        
        # Install schema
        await pgqrs.admin(store).install()
        print("✓ Schema installed")

        # Create workflow definition
        await store.create_workflow("process_task")
        print("✓ Workflow 'process_task' defined")

    asyncio.run(setup())
    ```

## Step 2: Trigger the Workflow

The trigger submits a "run" of the workflow. It doesn't execute the work itself; it enqueues the input for a worker to pick up.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_workflow_trigger_ephemeral"
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        params = {"id": 1, "payload": "Hello pgqrs"}

        # Trigger the workflow
        run_id = await pgqrs.workflow("process_task") \
            .trigger(params) \
            .execute(store)

        print(f"✓ Triggered workflow run: {run_id}")

    asyncio.run(main())
    ```

## Step 3: Create a Worker to Process the Workflow

The worker registers for a workflow queue and polls for messages. The workflow builder handles the run lifecycle automatically.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_workflow_consumer_start"
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Register worker for the workflow queue
        consumer = await pgqrs.consumer("worker-1", 8080, "process_task").create(store)

        print("Worker started. Waiting for runs...")
        while True:
            messages = await consumer.dequeue(batch_size=1)
            if not messages:
                continue

            msg = messages[0]
            run = await pgqrs.run().message(msg).store(store).execute()

            step = await run.acquire_step("process_task", current_time=run.current_time)
            if step.status == "EXECUTE":
                await process_task(msg.payload)
                await step.guard.success({"status": "success"})

            await run.complete({"status": "success"})
            await consumer.archive(msg.id)

    asyncio.run(main())
    ```

## Step 4: Check Results

You can retrieve the status and output of a workflow run at any time using its message ID.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_workflow_get_result"
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        message_id = 1

        run = await pgqrs.workflow("process_task").get_run(message_id, store)

        print(f"Run Status: {run.status}")
        print(f"Output: {run.output}")

    asyncio.run(main())
    ```

## Key Concepts in v0.14

- **Workflow Definition**: A named template (e.g., "process_task") that maps to a queue.
- **Workflow Run**: A specific execution instance with its own `run_id`, input, and output.
- **Trigger**: The client that submits a run (noun-verb API: `workflow().trigger()`).
- **Worker**: The process that executes the handler (fluent API: `consumer().handler().create()`).
- **workflow_step**: Wraps step execution for durability (crash recovery).

## Next Steps

- [Durable Workflows](durable-workflows.md) - Learn how to build multi-step, crash-resistant workflows using `pgqrs::workflow_step()`.
- [Workflow API Reference](../api/workflows.md) - Detailed API documentation.
