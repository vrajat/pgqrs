# Durable Workflows Guide

This guide walks you through building a **durable workflow** with pgqrs v0.14. Durable workflows are multi-step processes that survive worker crashes, handle transient errors with retries, and can even pause for external events.

## Prerequisites

- pgqrs v0.14+ installed
- PostgreSQL running
- Database connection string (DSN)

## What We'll Build

We'll create a data processing pipeline that:

1. **Fetches data** from an external source.
2. **Transforms the data** (simulating a crash-prone step).
3. **Saves results** to a destination.

The workflow will use the `ctx.step()` API to ensure each step is recorded in the database, allowing it to resume from where it left off if interrupted.

## Step 1: Define the Workflow Handler

In pgqrs v0.14, the workflow logic is encapsulated in a handler function. Each atomic unit of work is wrapped in a `ctx.step()` call.

=== "Rust"

    ```rust
    use pgqrs::{Workflow, TransientError, Step};
    use serde::{Serialize, Deserialize};
    use std::time::Duration;

    #[derive(Serialize, Deserialize, Debug)]
    struct PipelineParams {
        url: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct PipelineResult {
        processed_count: usize,
    }

    // The workflow handler
    async fn data_pipeline_handler(
        ctx: &mut dyn Workflow,
        params: PipelineParams,
    ) -> Result<PipelineResult, anyhow::Error> {
        println!("[workflow] Starting pipeline for {}", params.url);

        // Step 1: Fetch data (with transient error handling)
        let data = ctx.step("fetch_data", || async {
            match fetch_from_api(&params.url).await {
                Ok(d) => Ok(d),
                Err(e) if e.is_transient() => {
                    // This will update message visibility and retry later
                    Err(TransientError::with_backoff(30).into())
                }
                Err(e) => Err(e.into()),
            }
        }).await?;

        // Step 2: Transform data (cached result if worker crashes after this)
        let transformed = ctx.step("transform_data", || async {
            println!("[transform_data] Processing {} records", data.len());
            Ok(transform_logic(data))
        }).await?;

        // Step 3: Save results
        ctx.step("save_results", || async {
            println!("[save_results] Saving {} records", transformed.len());
            save_to_db(transformed).await?;
            Ok(())
        }).await?;

        Ok(PipelineResult { processed_count: transformed.len() })
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs
    from pgqrs import TransientError, Step

    # The workflow handler
    async def data_pipeline_handler(ctx, params):
        print(f"[workflow] Starting pipeline for {params['url']}")

        # Step 1: Fetch data
        async def fetch():
            try:
                return await fetch_from_api(params['url'])
            except Exception as e:
                if is_transient(e):
                    # Retry in 30 seconds
                    raise TransientError(retry_after=30)
                raise e

        data = await ctx.step("fetch_data", fetch)

        # Step 2: Transform data
        async def transform():
            print(f"[transform_data] Processing {len(data)} records")
            return transform_logic(data)

        transformed = await ctx.step("transform_data", transform)

        # Step 3: Save results
        async def save():
            print(f"[save_results] Saving {len(transformed)} records")
            await save_to_db(transformed)

        await ctx.step("save_results", save)

        return {"processed_count": len(transformed)}
    ```

## Step 2: Register the Worker

The worker process registers the handler and starts polling for runs.

=== "Rust"

    ```rust
    use pgqrs::consumer;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Setup workflow definition (idempotent)
        store.create_workflow("data_pipeline").await?;

        // Register worker
        let worker = consumer("worker-1", 8080, "data_pipeline")
            .handler(data_pipeline_handler)
            .create(&store)
            .await?;

        println!("Worker started. Waiting for runs...");
        worker.poll_forever().await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Setup workflow definition
        await store.create_workflow("data_pipeline")

        # Register worker
        worker = await pgqrs.consumer("worker-1", 8080, "data_pipeline") \
            .handler(data_pipeline_handler) \
            .create(store)

        print("Worker started. Waiting for runs...")
        await worker.poll_forever()

    asyncio.run(main())
    ```

## Step 3: Trigger the Workflow

Submit a run from any client (e.g., an HTTP handler).

=== "Rust"

    ```rust
    use pgqrs::workflow;

    async fn handle_request(url: String) -> Result<i64, Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        let run_id = workflow("data_pipeline")
            .trigger(&PipelineParams { url })?
            .execute(&store)
            .await?;

        Ok(run_id)
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def handle_request(url):
        store = await pgqrs.connect("postgresql://localhost/mydb")

        run_id = await pgqrs.workflow("data_pipeline") \
            .trigger({"url": url}) \
            .execute(store)

        return run_id
    ```

## Crash Recovery Demo

One of the most powerful features of durable workflows is **crash recovery**.

### How it works:
1. A worker dequeues a run and starts executing `data_pipeline_handler`.
2. `ctx.step("fetch_data", ...)` completes and the result is saved to the `pgqrs_workflow_steps` table.
3. **The worker process crashes** (e.g., OOM, hardware failure).
4. The message visibility timeout in the queue expires.
5. **Another worker** (or the same one after restart) dequeues the same run.
6. The handler starts from the beginning, but when it calls `ctx.step("fetch_data", ...)`, pgqrs sees the completed result in the database and **returns it immediately without re-executing the closure**.
7. The workflow continues from the next uncompleted step.

## Advanced Pausing for External Events

Workflows can pause execution and wait for an external event (like human approval).

=== "Rust"

    ```rust
    async fn approval_workflow(ctx: &mut dyn Workflow, params: Params) -> Result<(), anyhow::Error> {
        // Step 1: Send approval request
        ctx.step("send_request", || async { send_email(&params.approver).await }).await?;

        // Step 2: Pause until approval received
        let approval_data = ctx.step("wait_for_approval", || async {
            // This will pause the run and release the worker
            Err(Step::pause(Duration::from_secs(3600)).into())
        }).await?;

        // Step 3: Process approval
        ctx.step("process_approval", || async { process(approval_data).await }).await?;

        Ok(())
    }
    ```

### Resuming a Paused Workflow
When an external event occurs (e.g., a webhook), you can update the step state to resume the workflow:

```rust
// In your webhook handler
store.update_step(run_id, "wait_for_approval", StepStatus::Success, Some(approval_data)).await?;
// Make the message visible immediately to resume
store.resume_run(run_id).await?;
```

## Best Practices

1. **Idempotency**: Ensure your step closures are idempotent, especially if they have side effects outside of pgqrs.
2. **Step Granularity**: Balance between durability and performance. Too many small steps create database overhead; too few large steps mean more work is repeated on crash.
3. **Error Classification**: Use `TransientError` for things that might succeed on retry (network issues) and permanent errors for logic or data issues.

## Next Steps

- [Workflow API Reference](../api/workflows.md) - Complete API details.
- [Concepts: Durable Execution](../concepts/durable-workflows.md) - Deep dive into the architecture.
