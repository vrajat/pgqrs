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

## Step 1: Set Up the Workflow

In pgqrs v0.14, workflows must be defined before they can be triggered. This is an idempotent operation that sets up the necessary queues and metadata.

=== "Rust"

    ```rust
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Install schema (idempotent)
        pgqrs::admin(&store).install().await?;
        println!("✓ Schema installed");

        // Create workflow definition (idempotent)
        // This automatically creates a 1:1 mapped queue named "process_task"
        store.create_workflow("process_task").await?;
        println!("✓ Workflow 'process_task' defined");

        Ok(())
    }
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

## Step 2: Create the Trigger (Producer)

The trigger submits a "run" of the workflow. It doesn't execute the work itself; it enqueues the input for a worker to pick up.

=== "Rust"

    ```rust
    use pgqrs::workflow;
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize)]
    struct TaskParams {
        id: i32,
        payload: String,
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        let params = TaskParams { id: 1, payload: "Hello pgqrs".to_string() };

        // Trigger the workflow
        let run_id = workflow("process_task")
            .trigger(&params)?
            .execute(&store)
            .await?;

        println!("✓ Triggered workflow run: {}", run_id);
        Ok(())
    }
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

## Step 3: Create the Worker (Consumer)

The worker registers itself as a handler for a specific workflow. It polls the queue, creates the run state, and executes your handler function.

=== "Rust"

    ```rust
    use pgqrs::{consumer, Workflow};
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct TaskParams {
        id: i32,
        payload: String,
    }

    // The handler function that performs the work
    async fn task_handler(ctx: &mut dyn Workflow, params: TaskParams) -> Result<(), anyhow::Error> {
        println!("Processing task {} with payload: {}", params.id, params.payload);
        
        // Simulate work
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        
        println!("✓ Task {} complete", params.id);
        Ok(())
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Register worker with handler
        let worker = consumer("worker-1", 8080, "process_task")
            .handler(task_handler)
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

    # The handler function
    async def task_handler(ctx, params):
        print(f"Processing task {params['id']} with payload: {params['payload']}")
        
        # Simulate work
        await asyncio.sleep(1)
        
        print(f"✓ Task {params['id']} complete")
        return {"status": "success"}

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Register worker with handler
        worker = await pgqrs.consumer("worker-1", 8080, "process_task") \
            .handler(task_handler) \
            .create(store)

        print("Worker started. Waiting for runs...")
        await worker.poll_forever()

    asyncio.run(main())
    ```

## Step 4: Check Results

You can retrieve the status and output of a workflow run at any time using its `run_id`.

=== "Rust"

    ```rust
    use pgqrs::workflow;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        let run_id = 1; // Use the ID from Step 2

        let run = workflow("process_task")
            .get_run(run_id, &store)
            .await?;

        println!("Run Status: {:?}", run.status);
        if let Some(output) = run.output {
            println!("Output: {:?}", output);
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        run_id = 1

        run = await pgqrs.workflow("process_task").get_run(run_id, store)

        print(f"Run Status: {run.status}")
        print(f"Output: {run.output}")

    asyncio.run(main())
    ```

## Key Concepts in v0.14

- **Workflow Definition**: A named template (e.g., "process_task") that maps to a queue.
- **Workflow Run**: A specific execution instance with its own `run_id`, input, and output.
- **Trigger**: The client that submits a run (noun-verb API: `workflow().trigger()`).
- **Worker**: The process that executes the handler (fluent API: `consumer().handler().create()`).

## Next Steps

- [Durable Workflows](durable-workflows.md) - Learn how to build multi-step, crash-resistant workflows using `ctx.step()`.
- [Workflow API Reference](../api/workflows.md) - Detailed API documentation.
