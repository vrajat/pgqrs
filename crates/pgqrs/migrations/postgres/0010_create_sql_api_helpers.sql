-- Migration: Create SQL API helper functions for enqueuing and monitoring

-- 1. Create pgqrs_step custom type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type t 
        JOIN pg_namespace n ON t.typnamespace = n.oid 
        WHERE t.typname = 'pgqrs_step' 
          AND n.nspname = current_schema()
    ) THEN
        CREATE TYPE pgqrs_step AS (
            name TEXT,
            statement TEXT
        );
    END IF;
END $$;

-- 2. Step Definition Helper
CREATE OR REPLACE FUNCTION pgqrs_step(
    name TEXT,
    statement TEXT
) RETURNS pgqrs_step AS $$
BEGIN
    RETURN (name, statement)::pgqrs_step;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 3. Workflow sync helper
CREATE OR REPLACE FUNCTION pgqrs_workflow(
    workflow_name TEXT,
    steps pgqrs_step[]
) RETURNS VOID AS $$
DECLARE
    v_queue_id BIGINT;
    v_workflow_id BIGINT;
BEGIN
    -- Ensure the matching queue exists
    INSERT INTO pgqrs_queues (queue_name)
    VALUES (workflow_name)
    ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name
    RETURNING id INTO v_queue_id;

    -- Upsert workflow
    INSERT INTO pgqrs_workflows (name, queue_id)
    VALUES (workflow_name, v_queue_id)
    ON CONFLICT (name) DO UPDATE SET queue_id = EXCLUDED.queue_id
    RETURNING id INTO v_workflow_id;

    -- Upsert step definitions
    INSERT INTO pgqrs_workflow_steps_def (workflow_id, step_name, statement, position, updated_at)
    SELECT 
        v_workflow_id, 
        s.name, 
        s.statement, 
        s.ordinality::integer,
        NOW()
    FROM unnest(steps) WITH ORDINALITY s
    ON CONFLICT (workflow_id, step_name) DO UPDATE
    SET statement = EXCLUDED.statement,
        position = EXCLUDED.position,
        updated_at = NOW();

    -- Delete removed steps
    DELETE FROM pgqrs_workflow_steps_def
    WHERE workflow_id = v_workflow_id
      AND NOT (step_name = ANY(
          SELECT s.name FROM unnest(steps) s
      ));
END;
$$ LANGUAGE plpgsql;

-- 4. Raw enqueue helper
CREATE OR REPLACE FUNCTION pgqrs_enqueue_raw(
    p_queue_name TEXT,
    p_payload JSONB,
    p_delay_secs INT DEFAULT 0
) RETURNS BIGINT AS $$
DECLARE
    v_queue_id BIGINT;
    v_message_id BIGINT;
    v_vt TIMESTAMPTZ;
BEGIN
    INSERT INTO pgqrs_queues (queue_name)
    VALUES (p_queue_name)
    ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name
    RETURNING id INTO v_queue_id;

    IF p_delay_secs > 0 THEN
        v_vt := NOW() + make_interval(secs => p_delay_secs::double precision);
    ELSE
        v_vt := NOW();
    END IF;

    INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at)
    VALUES (v_queue_id, p_payload, v_vt, NOW())
    RETURNING id INTO v_message_id;

    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;

-- 5. Workflow enqueue (trigger) helper
CREATE OR REPLACE FUNCTION pgqrs_enqueue(
    p_workflow_name TEXT,
    p_input JSONB,
    p_delay_secs INT DEFAULT 0
) RETURNS BIGINT AS $$
BEGIN
    RETURN pgqrs_enqueue_raw(p_workflow_name, jsonb_build_object('input', p_input), p_delay_secs);
END;
$$ LANGUAGE plpgsql;

-- 6. Get message status helper
CREATE OR REPLACE FUNCTION pgqrs_get_message_status(
    p_message_id BIGINT
) RETURNS TEXT AS $$
DECLARE
    v_vt TIMESTAMPTZ;
    v_archived_at TIMESTAMPTZ;
    v_consumer_id BIGINT;
BEGIN
    SELECT vt, archived_at, consumer_worker_id
    INTO v_vt, v_archived_at, v_consumer_id
    FROM pgqrs_messages
    WHERE id = p_message_id;

    IF NOT FOUND THEN
        RETURN 'NOT_FOUND';
    ELSIF v_archived_at IS NOT NULL THEN
        RETURN 'COMPLETED';
    ELSIF v_consumer_id IS NOT NULL THEN
        RETURN 'PROCESSING';
    ELSIF v_vt > NOW() THEN
        RETURN 'DELAYED';
    ELSE
        RETURN 'READY';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 7. Get workflow run details
CREATE OR REPLACE FUNCTION pgqrs_get_run(
    p_message_id BIGINT
) RETURNS TABLE(
    run_id BIGINT,
    status TEXT,
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT r.id, r.status::text, r.input, r.output, r.error, r.started_at, r.completed_at
    FROM pgqrs_workflow_runs r
    WHERE r.message_id = p_message_id;
END;
$$ LANGUAGE plpgsql;

-- 8. Get workflow steps details
CREATE OR REPLACE FUNCTION pgqrs_get_steps(
    p_message_id BIGINT
) RETURNS TABLE(
    step_name VARCHAR(255),
    status TEXT,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT s.step_name, s.status::text, s.output, s.error, s.started_at, s.completed_at
    FROM pgqrs_workflow_steps s
    JOIN pgqrs_workflow_runs r ON s.run_id = r.id
    WHERE r.message_id = p_message_id
    ORDER BY s.id ASC;
END;
$$ LANGUAGE plpgsql;
