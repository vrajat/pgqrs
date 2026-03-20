#[derive(Debug, Clone, Copy)]
pub(crate) struct StepSql {
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
}

pub(crate) trait SqlDialect {
    const STEP: StepSql;
}

pub(crate) const SQLITE_FAMILY_STEP_SQL: StepSql = StepSql {
    acquire: r#"
INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING', datetime('now'), 0)
ON CONFLICT (run_id, step_name) DO UPDATE
SET status = CASE
    WHEN status = 'SUCCESS' THEN 'SUCCESS'
    WHEN status = 'ERROR' THEN 'ERROR'
    ELSE 'RUNNING'
END,
started_at = CASE
    WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
    ELSE datetime('now')
END
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#,
    clear_retry: r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING', retry_at = NULL, error = NULL
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#,
    complete: r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS', output = $2, completed_at = datetime('now')
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#,
    fail: r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR', error = $2, completed_at = datetime('now'),
    retry_at = $3, retry_count = $4
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_count, retry_at, created_at, updated_at
"#,
};
