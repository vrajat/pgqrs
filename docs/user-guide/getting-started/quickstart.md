# Quickstart

This guide will walk you through creating, running, and triggering your first **Durable Workflow** in `pgqrs`. We will build a 3-step invoice processing flow that is completely crash-resilient and database-managed.

Before starting, ensure that you have configured and started the `pgqrs` coordinator and workers as described in the [Installation Guide](installation.md).

## Step 0: Create Sample Tables

Connect to your PostgreSQL database and set up the sample business tables for our quickstart:

```sql
CREATE TABLE IF NOT EXISTS invoice_runs (
    invoice_id INT PRIMARY KEY,
    amount NUMERIC,
    status TEXT
);

CREATE TABLE IF NOT EXISTS payment_log (
    id SERIAL PRIMARY KEY,
    invoice_id INT,
    amount NUMERIC,
    charged_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert a sample unpaid invoice
INSERT INTO invoice_runs (invoice_id, amount, status)
VALUES (42, 150.00, 'unpaid')
ON CONFLICT (invoice_id) DO NOTHING;
```

---

## Step 1: Define Your Workflow

Define the workflow steps using your chosen API language:

=== "SQL API"

    Use the declarative `pgqrs_workflow` SQL helper to register the workflow and its steps. Note the use of `:input` and `:step` placeholders:

    ```sql
    SELECT pgqrs_workflow(
        'invoice_processing',
        ARRAY[
            -- Step 1: Read the invoice amount
            pgqrs_step('fetch_invoice', 
                       'SELECT amount, status FROM invoice_runs WHERE invoice_id = (:input->>''invoice_id'')::int'),
            
            -- Step 2: Record a successful payment using the amount from step 1
            pgqrs_step('charge_user', 
                       'INSERT INTO payment_log (invoice_id, amount) VALUES ((:input->>''invoice_id'')::int, (:fetch_invoice->0->>''amount'')::numeric) RETURNING id'),
            
            -- Step 3: Update the status of the invoice to paid
            pgqrs_step('update_invoice', 
                       'UPDATE invoice_runs SET status = ''paid'' WHERE invoice_id = (:input->>''invoice_id'')::int')
         ]
    );
    ```

=== "Rust API"

    Define your workflow using async Rust code and the `#[pgqrs_workflow]` macro:

    ```rust
    use pgqrs;
    use serde_json::json;

    #[pgqrs::pgqrs_workflow(name = "invoice_processing")]
    async fn invoice_processing(
        run: &pgqrs::Run,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, pgqrs::Error> {
        let invoice_id = input["invoice_id"].as_i64().unwrap();

        // Step 1: Fetch Invoice
        let invoice = pgqrs::workflow_step(run, "fetch_invoice", || async {
            // Your DB query logic here: SELECT amount FROM invoice_runs WHERE invoice_id = ...
            Ok::<_, pgqrs::Error>(json!({ "amount": 150.00 }))
        }).await?;

        // Step 2: Charge User
        pgqrs::workflow_step(run, "charge_user", || async {
            // Your DB insert logic here: INSERT INTO payment_log ...
            Ok::<_, pgqrs::Error>(())
        }).await?;

        // Step 3: Update Invoice
        pgqrs::workflow_step(run, "update_invoice", || async {
            // Your DB update logic here: UPDATE invoice_runs ...
            Ok::<_, pgqrs::Error>(())
        }).await?;

        Ok(json!({ "status": "completed" }))
    }
    ```

=== "Python API"

    Define your workflow using decorated async Python functions:

    ```python
    import pgqrs
    from pgqrs.decorators import step, workflow

    @workflow(name="invoice_processing")
    async def invoice_processing(ctx, input_data: dict) -> dict:
        invoice_id = input_data["invoice_id"]

        @step
        async def fetch_invoice(step_ctx):
            # Your query logic here
            return {"amount": 150.00}

        @step
        async def charge_user(step_ctx, amount):
            # Your payment logic here
            return {"charged": True}

        @step
        async def update_invoice(step_ctx):
            # Your update logic here
            return {"status": "paid"}

        invoice = await fetch_invoice(ctx)
        await charge_user(ctx, invoice["amount"])
        await update_invoice(ctx)
        return {"status": "completed"}
    ```

---

## Step 2: Start the Workers

Start the worker processes to listen for execution jobs (SQL worker is already running as part of the core infrastructure):

=== "Rust API"

    Spawn the polling loop in your Rust application:

    ```rust
    let store = pgqrs::connect("postgresql://postgres:postgres@localhost:5432/postgres").await?;
    pgqrs::admin(&store).install().await?;
    pgqrs::workflow().name(invoice_processing).create().await?;

    let consumer = pgqrs::consumer("rust-worker", invoice_processing.name()).create(&store).await?;
    pgqrs::workflow()
        .name(invoice_processing)
        .consumer(&consumer)
        .poll(&store)
        .await?;
    ```

=== "Python API"

    Run the workflow consumer task in your Python application loop:

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://postgres:postgres@localhost:5432/postgres")
        await pgqrs.admin(store).install()
        await pgqrs.workflow().name("invoice_processing").store(store).create()

        consumer = await store.consumer("invoice_processing")
        await pgqrs.dequeue().worker(consumer).handle_workflow(invoice_processing).poll(store)

    asyncio.run(main())
    ```

---

## Step 3: Trigger the Workflow Run

Trigger your workflow by enqueuing a payload using your API's trigger/producer methods:

=== "SQL API"

    Execute the SQL `pgqrs_enqueue` function:

    ```sql
    -- Triggers the workflow and returns the enqueued message_id (e.g. 1)
    SELECT pgqrs_enqueue('invoice_processing', '{"invoice_id": 42}'::jsonb);
    ```

=== "Rust API"

    Trigger using the `pgqrs::enqueue` builder:

    ```rust
    let ids = pgqrs::enqueue()
        .message(&json!({"invoice_id": 42}))
        .to("invoice_processing")
        .execute(&store)
        .await?;
    ```

=== "Python API"

    Trigger using the PyO3 `produce` binding:

    ```python
    message_id = await pgqrs.produce(store, "invoice_processing", {"invoice_id": 42})
    ```

---

## Step 4: Monitor and Inspect Execution

Inspect the execution state safely using query APIs. Replace `1` with the enqueued `message_id` returned in Step 3:

=== "SQL API"

    Call the monitoring SQL functions to check the status of the run, steps, and message queue. Raw table access is not required:

    ```sql
    -- 1. View overall run execution status
    SELECT * FROM pgqrs_get_run(1);

    -- 2. View step results and cached inputs/outputs
    SELECT * FROM pgqrs_get_steps(1);

    -- 3. View the underlying queue message status (e.g. 'READY', 'PROCESSING', 'COMPLETED', 'DELAYED')
    SELECT pgqrs_get_message_status(1);

    -- 4. Verify your business tables updated successfully
    SELECT * FROM invoice_runs;
    SELECT * FROM payment_log;
    ```

=== "Rust API"

    Monitor programmatically using Rust bindings:

    ```rust
    // Query metrics using the admin client
    let metrics = pgqrs::admin(&store).all_queues_metrics().await?;
    ```

=== "Python API"

    Monitor programmatically using Python bindings:

    ```python
    # Query queue metrics
    admin = pgqrs.admin(store)
    queues = await admin.get_queues()
    metrics = await queues.list_metrics()
    ```

---

## Under the Hood: Crash Durability

If your worker crashes or the database disconnects halfway through execution (e.g., during Step 2), the coordinator automatically re-delivers the trigger message after the visibility timeout expires. When a new worker picks up the run, it queries the step cache, detects that Step 1 (`fetch_invoice`) completed successfully, and returns its cached output without re-executing it. Execution then resumes smoothly at Step 2, ensuring exactly-once execution semantics and complete database-managed durability.
