# Workflows API

`pgqrs` provides durable workflow capabilities, allowing you to define multi-step processes that persist state and recover automatically from failures.

## Defining Workflows

Both Rust and Python APIs use declarative attributes/decorators to define workflows and steps.

=== "Rust"

    ```rust
    use pgqrs::workflow::Workflow;
    use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

    // Define a step
    #[pgqrs_step]
    async fn fetch_data(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
        // ... logic ...
        Ok("data".to_string())
    }

    // Define the workflow
    #[pgqrs_workflow]
    async fn data_pipeline(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
        let data = fetch_data(ctx, url).await?;
        Ok(data)
    }
    ```

=== "Python"

    ```python
    from pgqrs import PyWorkflow
    from pgqrs.decorators import workflow, step

    # Define a step
    @step
    async def fetch_data(ctx: PyWorkflow, url: str) -> str:
        # ... logic ...
        return "data"

    # Define the workflow
    @workflow
    async def data_pipeline(ctx: PyWorkflow, url: str) -> str:
        data = await fetch_data(ctx, url)
        return data
    ```

## Core Concepts

### Steps

Steps are the fundamental unit of durability.

*   **Identity:** Steps are identified by their function name (Rust) or function name (Python).
*   **Idempotency:** If a step completes successfully, its result is saved. Subsequent calls (e.g., after a crash) return the saved result immediately without re-executing limits.
*   **Serialization:** Inputs and outputs must be JSON-serializable (Python) or implement `Serialize`/`Deserialize` (Rust).

### Workflow Context

The context object (`&Workflow` in Rust, `PyWorkflow` in Python) provides access to the workflow's state and is required as the first argument to all step and workflow functions.

## Execution

### Starting a Workflow

=== "Rust"

    ```rust
    // Create workflow instance
    let workflow = pgqrs::admin(&store)
        .create_workflow("data_pipeline", &"http://example.com")
        .await?;

    // Run the workflow function
    let result = data_pipeline(&workflow, "http://example.com").await?;
    ```

=== "Python"

    ```python
    # Create workflow instance
    ctx = await admin.create_workflow("data_pipeline", "http://example.com")

    # Run the workflow function
    result = await data_pipeline(ctx, "http://example.com")
    ```

## Error Handling

Exceptions/Errors within steps are automatically captured and persisted.

=== "Rust"

    ```rust
    #[pgqrs_step]
    async fn risky_step(ctx: &Workflow) -> Result<(), anyhow::Error> {
        // Any error returned here is persisted
        // The step becomes "FAILED"
        Err(anyhow::anyhow!("Something went wrong"))
    }
    ```

=== "Python"

    ```python
    @step
    async def risky_step(ctx: PyWorkflow):
        # Exception raised here is persisted
        # The step becomes "FAILED"
        raise ValueError("Something went wrong")
    ```

## Low-Level API

For advanced users requiring dynamic step definitions or manual control.

=== "Rust"

    Use `StepGuard` directly.

    ```rust
    use pgqrs::workflow::{StepGuard, StepResult};

    match StepGuard::acquire(pool, wf_id, "step_name").await? {
        StepResult::Skipped(val) => Ok(val),
        StepResult::Execute(guard) => {
            let res = do_work();
            guard.success(&res).await?;
            Ok(res)
        }
    }
    ```

=== "Python"

    Use `ctx.acquire_step()`.

    ```python
    step = await ctx.acquire_step("step_name")
    if step.status == "SKIPPED":
        return step.value
    elif step.status == "EXECUTE":
        try:
            res = do_work()
            await step.guard.success(res)
            return res
        except Exception as e:
            await step.guard.fail(str(e))
            raise
    ```
