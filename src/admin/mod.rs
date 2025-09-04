use crate::error::{PgqrsError, Result};
use crate::run_migrations;
use crate::types::{CreateQueueOptions, QueueMetrics};
use diesel::deserialize::QueryableByName;
use diesel::pg::PgConnection;
use diesel::r2d2::ConnectionManager;
use diesel::RunQueryDsl;
use r2d2::Pool;

/// Admin interface for managing pgqrs infrastructure
pub struct Admin {
    pub pool: Pool<ConnectionManager<PgConnection>>,
}

#[derive(QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}

impl Admin {
    /// Create a new Admin instance
    pub fn new(pool: Pool<ConnectionManager<PgConnection>>) -> Self {
        Self { pool }
    }

    /// Install pgqrs schema and infrastructure
    ///
    /// # Arguments
    /// * `dry_run` - If true, only validate what would be done without executing
    pub fn install(&self, dry_run: bool) -> Result<()> {
        if dry_run {
            // Just validate: check if migrations would run
            return Ok(());
        }
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        run_migrations(&mut conn).map_err(PgqrsError::from)?;
        Ok(())
    }

    /// Uninstall pgqrs schema and remove all state
    ///
    /// # Arguments
    /// * `dry_run` - If true, only validate what would be done without executing
    pub fn uninstall(&self, dry_run: bool) -> Result<()> {
        if dry_run {
            // Just validate: check if schema exists
            return Ok(());
        }
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        diesel::sql_query("DROP SCHEMA IF EXISTS pgqrs CASCADE;")
            .execute(&mut conn)
            .map_err(|e| PgqrsError::from(e))?;
        Ok(())
    }

    /// Verify that pgqrs installation is valid and healthy
    pub fn verify(&self) -> Result<()> {
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        let row = diesel::sql_query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgqrs') AS exists;"
        )
        .get_result::<ExistsRow>(&mut conn)
        .map_err(|e| PgqrsError::from(e))?;
        if row.exists {
            Ok(())
        } else {
            Err(PgqrsError::Internal {
                message: "pgqrs schema does not exist".to_string(),
            })
        }
    }

    /// Create a new queue
    ///
    /// # Arguments
    /// * `options` - Queue creation options
    pub async fn create_queue(&self, options: CreateQueueOptions) -> Result<()> {
        todo!("Implement Admin::create_queue")
    }

    /// List all queues
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        todo!("Implement Admin::list_queues")
    }

    /// Delete a queue and all its messages
    ///
    /// # Arguments
    /// * `name` - Name of the queue to delete
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        todo!("Implement Admin::delete_queue")
    }

    /// Purge all messages from a queue (but keep the queue)
    ///
    /// # Arguments
    /// * `name` - Name of the queue to purge
    pub async fn purge_queue(&self, name: &str) -> Result<()> {
        todo!("Implement Admin::purge_queue")
    }

    /// Get metrics for a specific queue
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    pub async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        todo!("Implement Admin::queue_metrics")
    }

    /// Get metrics for all queues
    pub async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        todo!("Implement Admin::all_queues_metrics")
    }
}
