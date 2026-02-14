use crate::types::WorkflowRecord;
use async_trait::async_trait;
use sqlx::PgPool;

/// Handle for a durable workflow execution.
pub struct Workflow {
    record: WorkflowRecord,
    #[allow(dead_code)]
    pool: PgPool,
}

impl Workflow {
    /// Create a new workflow instance.
    pub fn new(record: WorkflowRecord, pool: PgPool) -> Self {
        Self { record, pool }
    }
}

#[async_trait]
impl crate::store::Workflow for Workflow {
    fn workflow_record(&self) -> &WorkflowRecord {
        &self.record
    }
}
