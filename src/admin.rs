use crate::constants::{
    CREATE_QUEUE_STATEMENT, DELETE_QUEUE_METADATA, DROP_QUEUE_STATEMENT, INSERT_QUEUE_METADATA,
    PGQRS_SCHEMA, PURGE_QUEUE_STATEMENT, QUEUE_PREFIX, SCHEMA_EXISTS_QUERY, UNINSTALL_STATEMENT,
};
use crate::error::{PgqrsError, Result};
use crate::queue::Queue;
use crate::run_migrations;
use crate::schema::pgqrs::meta;
use crate::types::MetaResult;
use crate::types::QueueMetrics;
use crate::Config;
use diesel::deserialize::QueryableByName;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::{Connection, RunQueryDsl};
use r2d2::Pool;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
pub struct PgqrsAdmin {
    pub pool: Pool<ConnectionManager<PgConnection>>,
    config: Config,
}

#[derive(QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}

impl PgqrsAdmin {
    /// Create a new Admin instance
    pub fn new(config: &Config) -> Self {
        let pool = Pool::builder()
            .max_size(config.max_connections)
            .build(ConnectionManager::<PgConnection>::new(&config.dsn))
            .expect("Failed to create connection pool");
        Self {
            pool,
            config: config.clone(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
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
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        let uninstall_statement = UNINSTALL_STATEMENT.replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA);
        if dry_run {
            tracing::info!("Uninstall statement (dry run): {}", uninstall_statement);
            // Just validate: check if schema exists
            return Ok(());
        }
        tracing::debug!("Executing uninstall statement: {}", uninstall_statement);
        diesel::sql_query(&uninstall_statement)
            .execute(&mut conn)
            .map_err(|e| PgqrsError::from(e))?;
        Ok(())
    }

    /// Verify that pgqrs installation is valid and healthy
    pub fn verify(&self) -> Result<()> {
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        let schema_exists_statement = SCHEMA_EXISTS_QUERY.replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA);
        tracing::debug!(
            "Executing schema exists statement: {}",
            schema_exists_statement
        );
        let row = diesel::sql_query(&schema_exists_statement)
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
    pub async fn create_queue(&self, name: &String) -> Result<Queue> {
        let create_statement = CREATE_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &name);

        let insert_meta = INSERT_QUEUE_METADATA
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{name}", &name);

        tracing::debug!("{}", create_statement);
        tracing::debug!("{}", insert_meta);
        // Execute both statements in a transaction
        self.run_statements_in_transaction(vec![create_statement, insert_meta])?;
        Ok(Queue::new(self.pool.clone(), name))
    }

    /// List all queues
    pub async fn list_queues(&self) -> Result<Vec<MetaResult>> {
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        meta::table
            .select(MetaResult::as_select())
            .load::<MetaResult>(&mut conn)
            .map_err(PgqrsError::from)
    }

    /// Delete a queue and all its messages
    ///
    /// # Arguments
    /// * `name` - Name of the queue to delete
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        let drop_statement = DROP_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);

        let delete_meta = DELETE_QUEUE_METADATA
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{name}", name);
        tracing::debug!("Executing delete metadata statement: {}", delete_meta);
        tracing::debug!("Executing drop queue statement: {}", drop_statement);
        self.run_statements_in_transaction(vec![drop_statement, delete_meta])
    }

    /// Purge all messages from a queue (but keep the queue)
    ///
    /// # Arguments
    /// * `name` - Name of the queue to purge
    pub async fn purge_queue(&self, name: &str) -> Result<()> {
        let purge_statement = PURGE_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);
        tracing::debug!("Executing purge queue statement: {}", purge_statement);
        self.run_statements_in_transaction(vec![purge_statement])
    }

    /// Get metrics for a specific queue
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    pub async fn queue_metrics(&self, _name: &str) -> Result<QueueMetrics> {
        todo!("Implement Admin::queue_metrics")
    }

    /// Get metrics for all queues
    pub async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        todo!("Implement Admin::all_queues_metrics")
    }

    pub async fn get_queue(&self, name: &str) -> Result<Queue> {
        Ok(Queue::new(self.pool.clone(), name))
    }

    fn run_statements_in_transaction(&self, statements: Vec<String>) -> Result<()> {
        let mut conn = self.pool.get().map_err(PgqrsError::from)?;
        conn.transaction::<_, PgqrsError, _>(|conn| {
            for stmt in &statements {
                diesel::sql_query(stmt)
                    .execute(conn)
                    .map_err(PgqrsError::from)?;
            }
            Ok(())
        })
    }
}
