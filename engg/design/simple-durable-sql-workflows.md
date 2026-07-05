# Design Document: Simple Durable SQL Workflows (Workflow-as-Code)

**Status:** Proposed  
**Author:** Antigravity AI  
**Related Design Docs:**  
*   [pgqrs-sql-worker.md](file:///Users/rajatvenkatesh/code/pgqrs/engg/design/pgqrs-sql-worker.md)
*   [postgres-only-worker-protocol.md](file:///Users/rajatvenkatesh/code/pgqrs/engg/design/postgres-only-worker-protocol.md)

---

## 1. Context & Motivation

In `pgqrs`, we want to enable database-native workflow orchestration using PostgreSQL as the execution backend. 

To ensure **safety, validation, and strict database-level constraints** while keeping Git migrations as the source of truth, we store workflow steps in a normalized database table (`pgqrs_workflow_steps_def`). 

By leveraging PostgreSQL's native **`unnest() WITH ORDINALITY`** feature, we can implement declarative, loop-free CRUD helper functions that automatically manage step ordering (positions) and resolve the update/upsert complexities of a normalized layout.

---

## 2. Proposed Architecture

### Core Concepts
1.  **Linear Sequences:** A workflow is defined as a linear sequence of steps.
2.  **Commit-per-Step Durability:** Each step runs in its own database transaction. If a step fails, previously completed steps remain committed.
3.  **Normalized Step Definitions:** Steps are stored as rows in a dedicated table `pgqrs_workflow_steps_def` with strict foreign keys and unique constraints.
4.  **Implicit Ordering via Ordinality:** Positions are determined automatically by the order of elements in the migration array, preventing manual sequence management.

### Parameter Binding
Every SQL step statement is executed as a parameterized query with two standard parameters:
*   `$1` (`JSONB`): The main workflow run input payload.
*   `$2` (`JSONB`): A map of prior step outputs keyed by step name.
    *   *Format:* `{"step_name": [{"col": "val"}]}` (array of row objects).

### Native Conditionals (Using `WHERE`)
Branching/conditions are handled natively in the step's SQL statement via `WHERE` clauses. If a condition is false, the query affects 0 rows, returning an empty set, and the step completes successfully as a no-op.

---

## 3. Database Schema Changes

To store the step definitions for workflows, we introduce a new table, a custom composite type, and unique constraints.

```sql
-- 1. Custom type representing a step definition
CREATE TYPE pgqrs.step_definition_type AS (
    name VARCHAR(255),
    statement TEXT
);

-- 2. Table storing step definitions
CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps_def (
    id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL REFERENCES pgqrs_workflows(id) ON DELETE CASCADE,
    step_name VARCHAR(255) NOT NULL,
    statement TEXT NOT NULL,
    position INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure step names are unique within a workflow
    UNIQUE (workflow_id, step_name),
    -- Ensure positions are unique within a workflow to prevent collisions
    UNIQUE (workflow_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pgqrs_workflow_steps_def_wf ON pgqrs_workflow_steps_def(workflow_id);
```

---

## 4. Migration Helper APIs (Workflow-as-Code)

Developers define workflows in standard migration files using a clean SQL-first helper constructor.

### Step Definition Helper
A helper function `pgqrs.step_def` constructs the custom composite type:
```sql
CREATE OR REPLACE FUNCTION pgqrs.step_def(
    name VARCHAR(255),
    statement TEXT
) RETURNS pgqrs.step_definition_type AS $$
BEGIN
    RETURN (name, statement)::pgqrs.step_definition_type;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

### Idempotent Workflow Declarative Sync
The `pgqrs.define_workflow` function creates/updates the workflow metadata and performs a loop-free, declarative sync of the steps using `unnest() WITH ORDINALITY`:

```sql
CREATE OR REPLACE FUNCTION pgqrs.define_workflow(
    workflow_name VARCHAR(255),
    steps pgqrs.step_definition_type[]
) RETURNS VOID AS $$
DECLARE
    v_queue_id BIGINT;
    v_workflow_id BIGINT;
BEGIN
    -- 1. Ensure the matching queue exists
    INSERT INTO pgqrs_queues (queue_name)
    VALUES (workflow_name)
    ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name
    RETURNING id INTO v_queue_id;

    -- 2. Upsert workflow
    INSERT INTO pgqrs_workflows (name, queue_id)
    VALUES (workflow_name, v_queue_id)
    ON CONFLICT (name) DO UPDATE SET queue_id = EXCLUDED.queue_id
    RETURNING id INTO v_workflow_id;

    -- 3. Upsert step definitions and assign positions (1-indexed) based on array order
    INSERT INTO pgqrs_workflow_steps_def (workflow_id, step_name, statement, position, updated_at)
    SELECT 
        v_workflow_id, 
        s.name, 
        s.statement, 
        s.ordinality, -- automatically populated by WITH ORDINALITY
        NOW()
    FROM unnest(steps) WITH ORDINALITY s
    ON CONFLICT (workflow_id, step_name) DO UPDATE
    SET statement = EXCLUDED.statement,
        position = EXCLUDED.position,
        updated_at = NOW();

    -- 4. Delete step definitions that are no longer present in the array
    DELETE FROM pgqrs_workflow_steps_def
    WHERE workflow_id = v_workflow_id
      AND NOT (step_name = ANY(
          SELECT s.name FROM unnest(steps) s
      ));
END;
$$ LANGUAGE plpgsql;
```

### Deleting a Workflow
To completely retire a workflow definition:
```sql
CREATE OR REPLACE FUNCTION pgqrs.delete_workflow(
    workflow_name VARCHAR(255)
) RETURNS VOID AS $$
BEGIN
    -- This cascades to pgqrs_workflow_steps_def
    DELETE FROM pgqrs_workflows
    WHERE name = workflow_name;
END;
$$ LANGUAGE plpgsql;
```

---

## 5. Worker Execution Logic

When a worker (Rust daemon or extension bgworker) leases a message from a workflow queue:

1.  **Resolve Workflow:** Find the `workflow_id` from `pgqrs_workflows` matching the queue name.
2.  **Load Steps:** Load all step definitions from `pgqrs_workflow_steps_def` where `workflow_id = ?` ordered by `position ASC`.
3.  **Start/Resume Run:** Look up or create a run record in `pgqrs_workflow_runs` matching the `message_id`. Set status to `RUNNING`.
4.  **Sequential Execution:**
    *   Load all completed step execution logs from `pgqrs_workflow_steps` for this `run_id`.
    *   Assemble the initial `$2` (step outputs JSONB map) using completed steps.
    *   For each step loaded in step definitions:
        *   If the step is already marked `SUCCESS` in history, retrieve its output and proceed.
        *   If the step is not completed:
            *   Upsert step state in `pgqrs_workflow_steps` to `RUNNING`.
            *   Start a new database transaction.
            *   Execute the step's `statement` query, passing `$1` (workflow input) and `$2` (cumulative outputs map).
            *   If the statement returns rows, aggregate them into a JSONB array: `[{"col": "val"}]`. If no rows are returned, return an empty array `[]`.
            *   Update step status in `pgqrs_workflow_steps` to `SUCCESS` with the output.
            *   COMMIT the transaction.
            *   Add the output to the cumulative outputs map `$2`.
5.  **Complete Run:** Update `pgqrs_workflow_runs` to `SUCCESS` with the final `$2` outputs map as `output`. Archive the queue message.
6.  **Failures:** If a step fails, roll back the step transaction, update the step and run state to `ERROR`, and return the error to trigger queue retries.

---

## 6. Practical Examples

### Schema Migration Example (`0009_register_payment_flow.sql`)
```sql
SELECT pgqrs.define_workflow(
    workflow_name := 'process_payment_wf',
    steps := ARRAY[
        pgqrs.step_def(
            name := 'authorize_funds',
            statement := 'SELECT authorize_card(($1->>''card_number''), ($1->>''amount'')::numeric) AS auth_id'
        ),
        pgqrs.step_def(
            name := 'record_transaction',
            statement := 'INSERT INTO transactions (auth_id, user_id, status) VALUES (($2->''authorize_funds''->0->>''auth_id''), ($1->>''user_id'')::int, ''pending'') RETURNING id'
        ),
        pgqrs.step_def(
            name := 'capture_funds',
            statement := 'SELECT capture_payment(($2->''authorize_funds''->0->>''auth_id''))'
        ),
        pgqrs.step_def(
            name := 'mark_completed',
            statement := 'UPDATE transactions SET status = ''completed'' WHERE id = ($2->''record_transaction''->0->>''id'')::int'
        )
    ]
);
```

### Conditional Logic Example (SQL `WHERE` Clause)
Suppose we have a workflow `onboard_user_wf`.
*   **Step 1 (`check_premium`):** Checks if the user is a premium member.
*   **Step 2 (`charge_fee`):** Only charges a fee if the user is **not** premium.

```sql
SELECT pgqrs.define_workflow(
    workflow_name := 'onboard_user_wf',
    steps := ARRAY[
        pgqrs.step_def(
            name := 'check_premium',
            statement := 'SELECT is_premium FROM users WHERE id = ($1->>''user_id'')::int'
        ),
        pgqrs.step_def(
            name := 'charge_fee',
            statement := 'INSERT INTO billing_queue (user_id, amount) SELECT ($1->>''user_id'')::int, 15.00 WHERE (($2->''check_premium''->0->>''is_premium'')::boolean) IS NOT TRUE'
        )
    ]
);
```
If the user is premium, the `WHERE` clause in `charge_fee` resolves to `FALSE`. The `SELECT` returns 0 rows, the `INSERT` does nothing, and the step completes successfully as a no-op.
