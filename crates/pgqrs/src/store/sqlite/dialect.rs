use crate::store::dialect::{SqlDialect, StepSql, SQLITE_FAMILY_STEP_SQL};

pub(crate) struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    const STEP: StepSql = SQLITE_FAMILY_STEP_SQL;
}
