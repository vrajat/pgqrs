-- Migration: Create execute_sql_step helper function
CREATE OR REPLACE FUNCTION execute_sql_step(
    p_run_id BIGINT,
    p_step_name VARCHAR(255),
    p_statement TEXT,
    p_params JSONB DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
    v_step_id BIGINT;
    v_status pgqrs_workflow_status;
    v_output JSONB;
    v_error JSONB;
    v_result JSONB;
BEGIN
    -- 1. Check if step already exists and has succeeded or failed
    SELECT id, status, output, error
    INTO v_step_id, v_status, v_output, v_error
    FROM pgqrs_workflow_steps
    WHERE run_id = p_run_id AND step_name = p_step_name;

    IF FOUND THEN
        IF v_status = 'SUCCESS' THEN
            RETURN v_output;
        ELSIF v_status = 'ERROR' THEN
            RAISE EXCEPTION 'Step % already failed in run %: %', p_step_name, p_run_id, v_error;
        END IF;
        -- If it exists but is not completed, we reset status to RUNNING
        UPDATE pgqrs_workflow_steps
        SET status = 'RUNNING'::pgqrs_workflow_status,
            started_at = NOW(),
            updated_at = NOW()
        WHERE id = v_step_id;
    ELSE
        -- Insert new step record in RUNNING status
        INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at)
        VALUES (p_run_id, p_step_name, 'RUNNING'::pgqrs_workflow_status, NOW())
        RETURNING id INTO v_step_id;
    END IF;

    -- 2. Execute the dynamic statement
    BEGIN
        IF ltrim(upper(p_statement)) LIKE 'SELECT%' OR ltrim(upper(p_statement)) LIKE 'WITH%' THEN
            -- SELECT query: aggregate rows into JSONB
            IF p_params IS NOT NULL THEN
                EXECUTE format('SELECT coalesce(jsonb_agg(t), ''[]''::jsonb) FROM (%s) t', p_statement) USING p_params INTO v_result;
            ELSE
                EXECUTE format('SELECT coalesce(jsonb_agg(t), ''[]''::jsonb) FROM (%s) t', p_statement) INTO v_result;
            END IF;
        ELSE
            -- DML statement: execute directly
            IF p_params IS NOT NULL THEN
                EXECUTE p_statement USING p_params;
            ELSE
                EXECUTE p_statement;
            END IF;
            v_result := '{}'::jsonb;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Record error in step table
        v_error := jsonb_build_object(
            'message', SQLERRM,
            'code', SQLSTATE
        );
        UPDATE pgqrs_workflow_steps
        SET status = 'ERROR'::pgqrs_workflow_status,
            error = v_error,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE id = v_step_id;
        
        RAISE EXCEPTION 'Step % failed: %', p_step_name, SQLERRM;
    END;

    -- 3. Update step status to SUCCESS and cache the result
    v_output := COALESCE(v_result, '{}'::jsonb);
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS'::pgqrs_workflow_status,
        output = v_output,
        completed_at = NOW(),
        updated_at = NOW()
    WHERE id = v_step_id;

    RETURN v_output;
END;
$$ LANGUAGE plpgsql;
