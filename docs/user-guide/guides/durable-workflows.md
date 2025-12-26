# Durable Workflows Guide

This guide walks you through building a durable workflow with pgqrs, from basic setup to handling crash recovery.

## Prerequisites

Before starting, ensure you have:

1. pgqrs installed ([Installation Guide](../getting-started/installation.md))
2. A running PostgreSQL database
3. The pgqrs schema installed (`pgqrs install`)

## What We'll Build

We'll create a data processing workflow that:

1. Fetches data from an external source
2. Transforms the data
3. Saves results to a destination

The workflow will survive crashes and resume from where it left off.

## Building the Workflow

### Step 1: Define Steps

Steps are the atomic units of your workflow:

=== "Rust"

    Use the `#[pgqrs_step]` macro for clean, declarative steps:

    ```rust
    use pgqrs::workflow::Workflow;
    use pgqrs_macros::{pgqrs_workflow, pgqrs_step};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FetchedData {
        url: String,
        records: Vec<Record>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Record {
        id: i32,
        name: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TransformedData {
        records: Vec<TransformedRecord>,
        count: usize,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TransformedRecord {
        id: i32,
        name: String,
        processed: bool,
    }

    #[pgqrs_step]
    async fn fetch_data(ctx: &Workflow, url: &str) -> Result<FetchedData, anyhow::Error> {
        println!("[fetch_data] Fetching from {}", url);

        // Simulate API call
        let data = FetchedData {
            url: url.to_string(),
            records: vec![
                Record { id: 1, name: "Alice".to_string() },
                Record { id: 2, name: "Bob".to_string() },
            ],
        };

        Ok(data)
    }

    #[pgqrs_step]
    async fn transform_data(ctx: &Workflow, data: FetchedData) -> Result<TransformedData, anyhow::Error> {
        println!("[transform_data] Processing {} records", data.records.len());

        let records: Vec<TransformedRecord> = data.records
            .into_iter()
            .map(|r| TransformedRecord {
                id: r.id,
                name: r.name.to_uppercase(),
                processed: true,
            })
            .collect();

        Ok(TransformedData {
            count: records.len(),
            records,
        })
    }

    #[pgqrs_step]
    async fn save_results(ctx: &Workflow, results: TransformedData) -> Result<String, anyhow::Error> {
        println!("[save_results] Saving {} records", results.count);

        Ok(format!("Saved {} records successfully", results.count))
    }
    ```

=== "Python"

    Each step is decorated with `@step`:

    ```python
    import asyncio
    from pgqrs import Admin, PyWorkflow
    from pgqrs.decorators import workflow, step

    @step
    async def fetch_data(ctx: PyWorkflow, url: str) -> dict:
        """Fetch data from an external source."""
        print(f"[fetch_data] Fetching from {url}")

        # Simulate API call
        await asyncio.sleep(1)
        return {
            "url": url,
            "records": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }

    @step
    async def transform_data(ctx: PyWorkflow, data: dict) -> dict:
        """Transform the fetched data."""
        print(f"[transform_data] Processing {len(data['records'])} records")

        transformed = []
        for record in data["records"]:
            transformed.append({
                "id": record["id"],
                "name": record["name"].upper(),
                "processed": True
            })

        return {"records": transformed, "count": len(transformed)}

    @step
    async def save_results(ctx: PyWorkflow, results: dict) -> str:
        """Save the processed results."""
        print(f"[save_results] Saving {results['count']} records")

        # Simulate database write
        await asyncio.sleep(0.5)
        return f"Saved {results['count']} records successfully"
    ```

### Step 2: Define the Workflow

The workflow orchestrates the steps:

=== "Rust"

    Use `#[pgqrs_workflow]` to mark the entry point:

    ```rust
    #[pgqrs_workflow]
    async fn data_pipeline(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
        println!("[workflow] Starting pipeline for {}", url);

        let data = fetch_data(ctx, url).await?;
        println!("[workflow] Fetched {} records", data.records.len());

        let results = transform_data(ctx, data).await?;
        println!("[workflow] Transformed {} records", results.count);

        let message = save_results(ctx, results).await?;
        println!("[workflow] Complete: {}", message);

        Ok(message)
    }
    ```

=== "Python"

    Decorate with `@workflow`:

    ```python
    @workflow
    async def data_pipeline(ctx: PyWorkflow, url: str):
        """A durable data processing pipeline."""
        print(f"[workflow] Starting pipeline for {url}")

        # Each step is tracked independently
        data = await fetch_data(ctx, url)
        print(f"[workflow] Fetched data: {data}")

        results = await transform_data(ctx, data)
        print(f"[workflow] Transformed: {results}")

        message = await save_results(ctx, results)
        print(f"[workflow] Complete: {message}")

        return message
    ```

### Step 3: Run the Workflow

Create and execute the workflow:

=== "Rust"

    ```rust
    use pgqrs::{Admin, Config};
    use pgqrs::workflow::Workflow;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let dsn = "postgresql://user:password@localhost:5432/mydb";

        // Create admin and install schema
        let config = Config::from_dsn(dsn.to_string());
        let admin = Admin::new(&config).await?;
        admin.install().await?;

        // Create workflow
        let workflow = Workflow::create(
            admin.pool().clone(),
            "data_pipeline",
            &"https://api.example.com/data",
        ).await?;

        println!("Created workflow ID: {}", workflow.id());

        // Execute workflow (macros handle start/success/fail automatically)
        let result = data_pipeline(&workflow, "https://api.example.com/data").await?;

        println!("Final result: {}", result);
        Ok(())
    }
    ```

=== "Python"

    ```python
    async def main():
        # Connect to PostgreSQL
        dsn = "postgresql://user:password@localhost:5432/mydb"
        admin = Admin(dsn, None)

        # Install schema (creates workflow tables)
        await admin.install()

        # Create a new workflow instance
        workflow_ctx = await admin.create_workflow(
            "data_pipeline",  # workflow name
            "https://api.example.com/data"  # input argument
        )
        print(f"Created workflow ID: {workflow_ctx.id()}")

        # Execute the workflow
        result = await data_pipeline(workflow_ctx, "https://api.example.com/data")
        print(f"Final result: {result}")

    if __name__ == "__main__":
        asyncio.run(main())
    ```

### Expected Output

```
Created workflow ID: 1
[workflow] Starting pipeline for https://api.example.com/data
[fetch_data] Fetching from https://api.example.com/data
[workflow] Fetched data: {'url': 'https://api.example.com/data', 'records': [...]}
[transform_data] Processing 2 records
[workflow] Transformed: {'records': [...], 'count': 2}
[save_results] Saving 2 records
[workflow] Complete: Saved 2 records successfully
Final result: Saved 2 records successfully
```

## Crash Recovery Demo

Let's simulate a crash and demonstrate recovery:

=== "Rust"

    ```rust
    use pgqrs::workflow::Workflow;
    use pgqrs_macros::{pgqrs_workflow, pgqrs_step};
    use std::sync::atomic::{AtomicBool, Ordering};

    // Simulate a crash on first run
    static SIMULATE_CRASH: AtomicBool = AtomicBool::new(true);

    #[pgqrs_step]
    async fn step_one(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
        println!("[step_one] Executing");
        Ok(format!("processed_{}", data))
    }

    #[pgqrs_step]
    async fn step_two(ctx: &Workflow, data: String) -> Result<String, anyhow::Error> {
        println!("[step_two] Executing");

        if SIMULATE_CRASH.swap(false, Ordering::SeqCst) {
            println!("[step_two] SIMULATING CRASH!");
            return Err(anyhow::anyhow!("Simulated crash!"));
        }

        Ok(format!("step2_{}", data))
    }

    #[pgqrs_workflow]
    async fn crash_demo(ctx: &Workflow, input_data: &str) -> Result<String, anyhow::Error> {
        let result1 = step_one(ctx, input_data).await?;
        println!("After step_one: {}", result1);

        let result2 = step_two(ctx, result1).await?;
        println!("After step_two: {}", result2);

        Ok(result2)
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let dsn = "postgresql://localhost:5432/mydb";
        let config = Config::from_dsn(dsn.to_string());
        let admin = Admin::new(&config).await?;
        admin.install().await?;

        // Create workflow
        let workflow = Workflow::create(admin.pool().clone(), "crash_demo", &"test").await?;
        let wf_id = workflow.id();
        println!("Created workflow: {}", wf_id);

        // RUN 1: Will crash in step_two
        println!("\n=== RUN 1 (will crash) ===");
        match crash_demo(&workflow, "test").await {
            Err(e) => println!("Caught crash: {}", e),
            Ok(_) => {}
        }

        // RUN 2: Resume with same workflow
        println!("\n=== RUN 2 (resuming) ===");

        // The workflow object contains the workflow ID.
        // Since we're in the same process, we can reuse it directly.
        // In production across process restarts, you'd reload using:
        //   let workflow = Workflow::new(pool.clone(), wf_id);

        let result = crash_demo(&workflow, "test").await?;
        println!("Final result: {}", result);
        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin, PyWorkflow
    from pgqrs.decorators import workflow, step

    # Simulate a crash on first run
    SIMULATE_CRASH = True

    @step
    async def step_one(ctx: PyWorkflow, data: str) -> str:
        print("[step_one] Executing")
        return f"processed_{data}"

    @step
    async def step_two(ctx: PyWorkflow, data: str) -> str:
        global SIMULATE_CRASH

        print("[step_two] Executing")

        if SIMULATE_CRASH:
            SIMULATE_CRASH = False
            print("[step_two] SIMULATING CRASH!")
            raise RuntimeError("Simulated crash!")

        return f"step2_{data}"

    @workflow
    async def crash_demo(ctx: PyWorkflow, input_data: str):
        result1 = await step_one(ctx, input_data)
        print(f"After step_one: {result1}")

        result2 = await step_two(ctx, result1)
        print(f"After step_two: {result2}")

        return result2

    async def demo():
        dsn = "postgresql://localhost:5432/mydb"
        admin = Admin(dsn, None)
        await admin.install()

        # Create workflow
        wf_ctx = await admin.create_workflow("crash_demo", "test")
        wf_id = wf_ctx.id()
        print(f"Created workflow: {wf_id}")

        # RUN 1: Will crash in step_two
        print("\n=== RUN 1 (will crash) ===")
        try:
            await crash_demo(wf_ctx, "test")
        except RuntimeError as e:
            print(f"Caught crash: {e}")

        # RUN 2: Resume with same workflow ID
        print("\n=== RUN 2 (resuming) ===")

        # The workflow context (wf_ctx) contains the workflow ID.
        # Since we're in the same process, we can reuse it directly.
        # In production across process restarts, you'd reload using:
        #   wf_ctx = await admin.get_workflow(wf_id)

        result = await crash_demo(wf_ctx, "test")
        print(f"Final result: {result}")

    if __name__ == "__main__":
        asyncio.run(demo())
    ```

### Expected Output

```
Created workflow: 1

=== RUN 1 (will crash) ===
[step_one] Executing
After step_one: processed_test
[step_two] Executing
[step_two] SIMULATING CRASH!
Caught crash: Simulated crash!

=== RUN 2 (resuming) ===
After step_one: processed_test     # step_one SKIPPED, cached result returned
[step_two] Executing               # step_two runs again
After step_two: step2_processed_test
Final result: step2_processed_test
```

Notice that in Run 2:
- `step_one` was **skipped** - it returned the cached result without executing
- `step_two` **executed** - it wasn't marked as complete before the crash

## Best Practices

### 1. Use Descriptive Step IDs

Step IDs should clearly describe the operation:

=== "Rust"

    With macros, the function name becomes the step ID:

    ```rust
    // Good - descriptive function name
    #[pgqrs_step]
    async fn fetch_user_profile(ctx: &Workflow, user_id: &str) -> Result<User, anyhow::Error> {
        // ...
    }

    // Bad - ambiguous name
    #[pgqrs_step]
    async fn step1(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
        // ...
    }
    ```

=== "Python"

    The step ID defaults to the function name:

    ```python
    # Good - descriptive function name
    @step
    async def fetch_user_profile(ctx: PyWorkflow, user_id: str) -> dict:
        ...

    # Bad - ambiguous name
    @step
    async def step1(ctx: PyWorkflow, data: str) -> str:
        ...
    ```

### 2. Make Steps Idempotent

For external side effects, use idempotency keys:

=== "Rust"

    ```rust
    #[pgqrs_step]
    async fn send_email(ctx: &Workflow, user_id: &str, template: &str) -> Result<String, anyhow::Error> {
        // Use workflow ID + step for idempotency
        let idempotency_key = format!("email-{}-{}", ctx.id(), user_id);

        email_service.send(
            user_id,
            template,
            &idempotency_key
        ).await?;

        Ok("sent".to_string())
    }
    ```

=== "Python"

    ```python
    @step
    async def send_email(ctx: PyWorkflow, user_id: str, template: str) -> str:
        # Use workflow ID + step for idempotency
        idempotency_key = f"email-{ctx.id()}-{user_id}"

        await email_service.send(
            user_id=user_id,
            template=template,
            idempotency_key=idempotency_key
        )
        return "sent"
    ```

### 3. Handle Errors Appropriately

Decide whether errors should be terminal or recoverable:

=== "Rust"

    ```rust
    use std::io;
    use tokio::time::Duration;

    #[pgqrs_step]
    async fn risky_call(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
        for attempt in 0..3 {
            match external_api.call(data).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Check for transient errors (e.g., timeout)
                    let is_transient = e.downcast_ref::<io::Error>()
                        .map(|io_err| io_err.kind() == io::ErrorKind::TimedOut)
                        .unwrap_or(false);

                    if is_transient {
                        if attempt == 2 {
                            return Err(e);  // Terminal after 3 attempts
                        }
                        tokio::time::sleep(Duration::from_secs(2_u64.pow(attempt))).await;
                    } else {
                        return Err(e);  // Non-transient, fail immediately
                    }
                }
            }
        }
        unreachable!()
    }
    ```

=== "Python"

    ```python
    @step
    async def risky_call(ctx: PyWorkflow, data: dict) -> dict:
        for attempt in range(3):
            try:
                return await external_api.call(data)
            except TransientError:
                if attempt == 2:
                    raise  # Terminal after 3 attempts
                await asyncio.sleep(2 ** attempt)

        raise RuntimeError("Should not reach here")
    ```

### 4. Keep Steps Reasonably Sized

Balance between durability and performance:

=== "Rust"

    ```rust
    // Good: Logical unit of work
    #[pgqrs_step]
    async fn process_batch(ctx: &Workflow, batch: Vec<Item>) -> Result<BatchResult, anyhow::Error> {
        let results: Vec<_> = batch.into_iter()
            .map(|item| transform(item))
            .collect();
        Ok(BatchResult { processed: results.len(), results })
    }

    // Too granular: One step per item creates overhead
    #[pgqrs_step]
    async fn process_one(ctx: &Workflow, item: Item) -> Result<Item, anyhow::Error> {
        Ok(transform(item))  // Called 1000 times = 1000 DB writes
    }
    ```

=== "Python"

    ```python
    # Good: Logical unit of work
    @step
    async def process_batch(ctx: PyWorkflow, batch: list) -> dict:
        results = []
        for item in batch:
            results.append(transform(item))
        return {"processed": len(results), "results": results}

    # Too granular: One step per item creates overhead
    @step
    async def process_one(ctx: PyWorkflow, item: dict) -> dict:
        return transform(item)  # Called 1000 times = 1000 DB writes
    ```

## Monitoring Workflows

Query workflow status directly from PostgreSQL:

```sql
-- Check workflow status
SELECT workflow_id, name, status, created_at, updated_at
FROM pgqrs_workflows
WHERE name = 'data_pipeline'
ORDER BY created_at DESC;

-- Check step progress
SELECT w.workflow_id, w.name, w.status as wf_status,
       s.step_id, s.status as step_status, s.completed_at
FROM pgqrs_workflows w
LEFT JOIN pgqrs_workflow_steps s ON w.workflow_id = s.workflow_id
WHERE w.workflow_id = 1;

-- Find stuck workflows (running for too long)
SELECT * FROM pgqrs_workflows
WHERE status = 'RUNNING'
AND updated_at < NOW() - INTERVAL '1 hour';
```

## Next Steps

- [Durable Workflows Concepts](../concepts/durable-workflows.md): Deeper understanding of the architecture
- [Rust Workflow API](../rust/workflows.md): Complete Rust API reference
- [Python Workflow API](../python/workflows.md): Complete Python API reference
