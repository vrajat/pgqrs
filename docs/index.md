# pgqrs

**pgqrs is a postgres-native, durable execution engine.**

Write multi-step, crash-resilient workflows with exactly-once transactional semantics using **SQL**, **Rust**, or **Python**, backed entirely by PostgreSQL.

---

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. Each step executes exactly once, progress state persists in the database, and processes resume from the last completed step:

* **Crash Recovery:** If a worker or database connection fails mid-run, execution resumes from the last completed step.
* **Exactly-Once Semantics:** Completed steps are cached and never re-executed.
* **Database Transactions:** Step execution and status tracking are executed within ACID transactions.
* **Operational Simplicity:** Queue state, workflow state, and step results live in one transactional store.

---

## Core APIs

`pgqrs` provides three core APIs to build, run, and monitor durable execution flows:

### 1. Workflow API
Define the multi-step execution logic and register it.

=== "SQL API"

    Define a workflow declaratively inside PostgreSQL. Workflows are picked up and executed by the background `pgqrs sql-worker` daemon:

    ```sql
    SELECT pgqrs_workflow(
        'archive_files',
        ARRAY[
            pgqrs_step('list_files', 'SELECT path FROM files WHERE folder = (:input->>''folder'')'),
            pgqrs_step('create_zip', 'INSERT INTO zip_log (files) VALUES (:list_files) RETURNING zip_path')
        ]
    );
    ```

=== "Rust API"

    Define workflows using async Rust and the `#[pgqrs_workflow]` macro:

    ```rust
    #[pgqrs::pgqrs_workflow(name = "archive_files")]
    async fn archive_files(
        run: &pgqrs::Run,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, pgqrs::Error> {
        let files = pgqrs::workflow_step(run, "list_files", || async {
            Ok::<_, pgqrs::Error>(vec![format!("{}/report.csv", input["folder"].as_str().unwrap())])
        }).await?;

        let zip_path = pgqrs::workflow_step(run, "create_zip", || async {
            Ok::<_, pgqrs::Error>(format!("{}.zip", files[0]))
        }).await?;

        Ok(serde_json::json!({ "zip_path": zip_path }))
    }
    ```

=== "Python API"

    Define workflows using decorated Python functions:

    ```python
    from pgqrs.decorators import step, workflow

    @workflow(name="archive_files")
    async def archive_files(ctx, input_data: dict):
        @step
        async def list_files(step_ctx):
            return [f"{input_data['folder']}/report.csv"]

        @step
        async def create_zip(step_ctx, files):
            return f"{files[0]}.zip"

        files = await list_files(ctx)
        zip_path = await create_zip(ctx, files)
        return {"zip_path": zip_path}
    ```

---

### 2. Enqueue API
Trigger a new workflow execution or enqueue a raw message payload.

=== "SQL API"

    Execute the SQL `pgqrs_enqueue` function:

    ```sql
    -- Enqueues the run input and returns the trigger message ID
    SELECT pgqrs_enqueue('archive_files', '{"folder": "/tmp/reports"}'::jsonb);
    ```

=== "Rust API"

    Trigger using the `pgqrs::enqueue` builder:

    ```rust
    let ids = pgqrs::enqueue()
        .message(&json!({"folder": "/tmp/reports"}))
        .to("archive_files")
        .execute(&store)
        .await?;
    ```

=== "Python API"

    Trigger using the PyO3 `produce` client:

    ```python
    message_id = await pgqrs.produce(store, "archive_files", {"folder": "/tmp/reports"})
    ```

---

### 3. Get API
Inspect overall workflow run progress, step-level outcomes, and status.

=== "SQL API"

    Query run status and cache records using helper functions (avoiding raw table access):

    ```sql
    -- 1. Get the overall run status (returns run_id, status, input, output, error)
    SELECT * FROM pgqrs_get_run(message_id);

    -- 2. Query individual step-level cached details
    SELECT * FROM pgqrs_get_steps(message_id);

    -- 3. Query the status of the underlying message queue item
    SELECT pgqrs_get_message_status(message_id);
    ```

=== "Rust API"

    Get the run record status and result from the store:

    ```rust
    let run = pgqrs::run().message(message_id).get(&store).await?;
    println!("Run status: {:?}", run.status);
    ```

=== "Python API"

    Get the run status and step results:

    ```python
    run = await store.get_run_by_message(message_id)
    print(f"Run status: {run.status}")
    ```

---

## Core Properties

* **Postgres-native:** Leverages PostgreSQL features (`SKIP LOCKED`, ACID transactions, schema namespaces).
* **Crash resilient:** Execution resumes from the exact failing step. Completed steps are never re-evaluated.
* **Unified Coordinator:** Runs natively inside the database as an extension (`pgqrs_extension`) or externally as a CLI daemon (`pgqrs admin`).

---

## Next Steps

* **[Installation](user-guide/getting-started/installation.md):** Setup the coordinator and workers.
* **[Quickstart](user-guide/getting-started/quickstart.md):** Write and run your first workflow in 5 minutes.
* **[Durable Workflows Guide](user-guide/guides/durable-workflows.md):** Deep dive on workflow behavior, retries, and errors.
