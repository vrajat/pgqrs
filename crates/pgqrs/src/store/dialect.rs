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
