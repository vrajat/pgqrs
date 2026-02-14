use crate::types::WorkflowRecord;
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoWorkflow {
    record: WorkflowRecord,
    #[allow(dead_code)]
    db: Arc<Database>,
}

impl TursoWorkflow {
    /// Create a new workflow instance.
    pub fn new(record: WorkflowRecord, db: Arc<Database>) -> Self {
        Self { record, db }
    }
}

#[async_trait]
impl crate::store::Workflow for TursoWorkflow {
    fn workflow_record(&self) -> &WorkflowRecord {
        &self.record
    }
}
