use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

#[derive(Debug, Clone)]
pub struct TursoWorkflow {
    name: String,
    #[allow(dead_code)]
    db: Arc<Database>,
}

impl TursoWorkflow {
    /// Create a new workflow instance.
    pub fn new(db: Arc<Database>, name: &str) -> Self {
        Self {
            name: name.to_string(),
            db,
        }
    }
}

#[async_trait]
impl crate::store::Workflow for TursoWorkflow {
    fn name(&self) -> &str {
        &self.name
    }
}
