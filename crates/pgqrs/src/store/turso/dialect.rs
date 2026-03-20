use crate::store::dialect::{SqlDialect, StepSql};

pub(crate) struct TursoDialect;

impl SqlDialect for TursoDialect {
    const STEP: StepSql = StepSql {
        acquire: r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE
    SET status = CASE
        WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'
        WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'
        ELSE 'RUNNING'
    END,
    started_at = CASE
        WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
        ELSE datetime('now')
    END
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        clear_retry: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE id = ?
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        complete: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS', output = ?2, completed_at = datetime('now')
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        fail: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR', error = ?2, completed_at = datetime('now'),
        retry_at = ?3, retry_count = ?4
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
    };
}
