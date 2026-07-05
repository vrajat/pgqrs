-- Migration: Simple Durable SQL Workflows (Workflow-as-Code)

-- 1. Create custom type representing a step definition
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type t 
        JOIN pg_namespace n ON t.typnamespace = n.oid 
        WHERE t.typname = 'step_definition_type' 
          AND n.nspname = current_schema()
    ) THEN
        CREATE TYPE step_definition_type AS (
            name TEXT,
            statement TEXT
        );
    END IF;
END $$;

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

-- 3. Step Definition Helper
CREATE OR REPLACE FUNCTION step_def(
    name TEXT,
    statement TEXT
) RETURNS step_definition_type AS $$
BEGIN
    RETURN (name, statement)::step_definition_type;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 4. Idempotent Workflow Declarative Sync
CREATE OR REPLACE FUNCTION define_workflow(
    workflow_name TEXT,
    steps step_definition_type[]
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
        s.ordinality::integer, -- automatically populated by WITH ORDINALITY
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

-- 5. Deleting a Workflow
CREATE OR REPLACE FUNCTION delete_workflow(
    workflow_name TEXT
) RETURNS VOID AS $$
BEGIN
    -- This cascades to pgqrs_workflow_steps_def
    DELETE FROM pgqrs_workflows
    WHERE name = workflow_name;
END;
$$ LANGUAGE plpgsql;
