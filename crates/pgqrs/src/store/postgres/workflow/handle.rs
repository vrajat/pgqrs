use async_trait::async_trait;
use sqlx::PgPool;

/// Handle for a durable workflow execution.
pub struct Workflow {
    name: String,
    #[allow(dead_code)]
    pool: PgPool,
}

impl Workflow {
    /// Create a new workflow instance.
    pub fn new(pool: PgPool, name: &str) -> Self {
        Self {
            name: name.to_string(),
            pool,
        }
    }
}

#[async_trait]
impl crate::store::Workflow for Workflow {
    fn name(&self) -> &str {
        &self.name
    }
}
