use crate::types::WorkflowRecord;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct SqliteWorkflow {
    record: WorkflowRecord,
}

impl SqliteWorkflow {
    pub fn new(record: WorkflowRecord) -> Self {
        Self { record }
    }
}

#[async_trait]
impl crate::store::Workflow for SqliteWorkflow {
    fn workflow_record(&self) -> &WorkflowRecord {
        &self.record
    }
}
