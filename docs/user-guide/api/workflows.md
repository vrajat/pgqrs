# Workflows API

`pgqrs` provides durable workflow capabilities based on a **Trigger/Worker architecture**. This allows you to define multi-step processes that persist state, recover automatically from failures, and scale independently.

## Core Concepts

- **Workflow Definition**: A named template (e.g., "process_file") that defines the logic to be executed.
- **Workflow Run**: A specific execution instance of a workflow, identified by a `run_id`.
- **Trigger**: An application component (e.g., HTTP handler, cron job) that submits a workflow run.
- **Worker**: A centralized process that polls for work and executes workflow handlers.
- **Step**: The fundamental unit of durability. Results are persisted to ensure idempotency and crash recovery.

---

## Defining Workflows

Workflows are defined as handler functions that take a context object and input parameters.

=== "Rust"

    ```rust
    use pgqrs::Workflow;
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
    ```

=== "Python"

    ```python
    from pgqrs import workflow, step
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
    @workflow(name="zip_files")
    async def zip_files_handler(ctx, params: ZipParams) -> ZipResult:
        # Step 1: List files
        files = await ctx.step("list_files", lambda: s3.list_objects(params.bucket, params.prefix))
        
        # Step 2: Download
        local_files = await ctx.step("download_files", lambda: download_all(files))
        
        # Step 3: Create archive
        archive_path = await ctx.step("create_archive", lambda: zip.create(local_files))
        
        return ZipResult(archive_path=archive_path, file_count=len(files))
    ```

---

## Worker Registration

Workers must register with the `pgqrs` store and provide a handler for a specific workflow.

=== "Rust"

    ```rust
    use pgqrs::consumer;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        
        // Create workflow definition (idempotent)
        store.create_workflow("zip_files").await?;
        
        // Register worker with handler
        let worker = consumer("worker-1", 8080, "zip_files")
            .handler(zip_files_handler)
            .create(&store)
            .await?;
        
        // Start polling for work
        worker.poll_forever().await?;
        
        Ok(())
    }
    ```

=== "Python"

    ```python
    from pgqrs import connect, consumer

    async def main():
        store = await connect("postgresql://localhost/mydb")
        
        # Setup (idempotent)
        await store.create_workflow("zip_files")
        
        # Register worker with handler
        worker = await consumer("worker-1", 8080, "zip_files") \
            .handler(zip_files_handler) \
            .create(store)
        
        # Poll forever
        await worker.poll_forever()
    ```

---

## Triggering Workflows

Triggers submit workflow runs by enqueuing a message. This is a non-blocking operation that returns a reference to the queued message.

=== "Rust"

    ```rust
    use pgqrs::workflow;

    async fn handle_upload(bucket: String, prefix: String) -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        
        // Trigger workflow (returns queue message)
        let message = workflow("zip_files")
            .trigger(&ZipParams { bucket, prefix })?
            .execute(&store)
            .await?;
        
        println!("Queued workflow message: {}", message.id());
        Ok(())
    }
    ```

=== "Python"

    ```python
    from pgqrs import workflow

    async def handle_upload(bucket: str, prefix: str):
        store = await connect("postgresql://localhost/mydb")
        
        message = await workflow("zip_files") \
            .trigger(ZipParams(bucket=bucket, prefix=prefix)) \
            .execute(store)
        
        print(f"Queued message: {message.id}")
    ```

---

## Checking Status & Results

You can check the status of a run or wait for its result using the message returned during triggering.

=== "Rust"

    ```rust
    use pgqrs::run;

    async fn check_status(message: QueueMessage, store: &Store) -> Result<(), Box<dyn std::error::Error>> {
        // Non-blocking status snapshot
        let run = run()
            .status(message)
            .store(store)
            .get()
            .await?;
        
        match run.status {
            RunStatus::Success => {
                let result: ZipResult = run.output()?;
                println!("Completed: {}", result.archive_path);
            }
            RunStatus::Error => println!("Failed: {:?}", run.error),
            RunStatus::Running | RunStatus::Queued => println!("In progress..."),
            RunStatus::Paused => println!("Paused (waiting for external event)"),
        }
        Ok(())
    }

    async fn wait_for_result(message: QueueMessage, store: &Store) -> Result<ZipResult, Box<dyn std::error::Error>> {
        // Blocking wait for result
        let result: ZipResult = run()
            .status(message)
            .store(store)
            .result()
            .await?;
        
        Ok(result)
    }
    ```

=== "Python"

    ```python
    from pgqrs import run

    async def check_status(message, store):
        run_info = await run() \
            .status(message) \
            .store(store) \
            .get()
        
        if run_info.status == "SUCCESS":
            print(f"Done: {run_info.output()}")
        elif run_info.status == "ERROR":
            print(f"Failed: {run_info.error}")
        else:
            print(f"Status: {run_info.status}")

    async def wait_for_result(message, store):
        # Blocking wait
        result = await run() \
            .status(message) \
            .store(store) \
            .result()
        return result
    ```

---

## Error Handling & Retries

`pgqrs` distinguishes between transient and permanent errors to manage retries effectively.

### Transient vs. Permanent Errors

- **Transient Errors**: Network timeouts, rate limits, etc. The worker updates the message visibility for a later retry. The same `run_id` continues from the last successful step.
- **Permanent Errors**: Invalid input, logic errors, etc. The run is marked as `ERROR`, and the message is archived.

### Pausing Workflows

Workflows can be paused to wait for external events (e.g., human approval).

=== "Rust"

    ```rust
    use pgqrs::workflow::Step;
    use std::time::Duration;

    // Inside handler
    let approval_data = ctx.step("wait_for_approval", || async {
        // Return a pause error with a duration
        Err(Step::pause(Duration::from_secs(3600)))
    }).await?;
    ```

---

## Steps & Idempotency

Steps are the fundamental unit of durability in `pgqrs`.

- **Identity**: Steps are identified by a unique string ID within a workflow.
- **Persistence**: If a step completes successfully, its result is saved. Subsequent executions (e.g., after a crash) return the saved result immediately.
- **Serialization**: Inputs and outputs must be JSON-serializable.
- **Side Effects**: Steps should be idempotent or wrap side-effecting operations to ensure they only run once.
