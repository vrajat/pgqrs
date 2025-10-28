//! Administrative interface for managing pgqrs infrastructure.
//!
//! This module provides the [`PgqrsAdmin`] struct and related functions for installing, uninstalling, verifying, and managing queues in a PostgreSQL-backed job queue system.
//!
//! ## What
//!
//! - [`PgqrsAdmin`] allows you to create, delete, purge, and list queues, as well as install and uninstall the schema.
//! - Provides metrics and access to individual queues.
//!
//! ## How
//!
//! Use [`PgqrsAdmin`] to set up and administer your queue infrastructure. See function docs for usage details.
//!
//! ### Example
//!
//! ```no_run
//! use pgqrs::admin::PgqrsAdmin;
//! use pgqrs::config::Config;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::default();
//!     let admin = PgqrsAdmin::new(&config);
//!     admin.install(false)?;
//!     admin.create_queue(&"jobs".to_string(), false).await?;
//!     Ok(())
//! }
//! ```
use crate::config::Config;
use crate::constants::{
    CREATE_META_TABLE_STATEMENT, CREATE_QUEUE_STATEMENT, CREATE_SCHEMA_STATEMENT,
    DELETE_QUEUE_METADATA, DROP_QUEUE_STATEMENT, INSERT_QUEUE_METADATA, PGQRS_SCHEMA,
    PURGE_QUEUE_STATEMENT, QUEUE_PREFIX, SCHEMA_EXISTS_QUERY, UNINSTALL_STATEMENT,
};
use crate::error::{PgqrsError, Result};
use crate::queue::Queue;
use crate::types::MetaResult;
use crate::types::QueueMetrics;
use sqlx::PgPool;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
pub struct PgqrsAdmin {
    pub pool: PgPool,
    config: Config,
}

impl PgqrsAdmin {
    /// Create a new admin interface for managing pgqrs infrastructure.
    ///
    /// # Arguments
    /// * `config` - Configuration for database connection and queue options
    ///
    /// # Returns
    /// A new `PgqrsAdmin` instance.
    pub async fn new(config: &Config) -> Result<Self> {
        let pool = PgPool::connect(&config.dsn)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(Self {
            pool,
            config: config.clone(),
        })
    }

    /// Get the configuration used by this admin instance.
    ///
    /// # Returns
    /// Reference to the [`Config`] struct.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Install pgqrs schema and infrastructure in the database.
    ///
    /// # Arguments
    /// * `dry_run` - If true, only validate what would be done without executing
    ///
    /// # Returns
    /// Ok if installation (or validation) succeeds, error otherwise.
    pub async fn install(&self, dry_run: bool) -> Result<()> {
        if dry_run {
            // Just validate: check if schemas would run
            return Ok(());
        }

        // Create schema
        sqlx::query(CREATE_SCHEMA_STATEMENT)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        // Create meta table
        sqlx::query(CREATE_META_TABLE_STATEMENT)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Uninstall pgqrs schema and remove all state from the database.
    ///
    /// # Arguments
    /// * `dry_run` - If true, only validate what would be done without executing
    ///
    /// # Returns
    /// Ok if uninstall (or validation) succeeds, error otherwise.
    pub async fn uninstall(&self, dry_run: bool) -> Result<()> {
        let uninstall_statement = UNINSTALL_STATEMENT.replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA);
        if dry_run {
            tracing::info!("Uninstall statement (dry run): {}", uninstall_statement);
            // Just validate: check if schema exists
            return Ok(());
        }
        tracing::debug!("Executing uninstall statement: {}", uninstall_statement);
        sqlx::query(&uninstall_statement)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(())
    }

    /// Verify that pgqrs installation is valid and healthy.
    ///
    /// # Returns
    /// Ok if installation is valid, error otherwise.
    pub async fn verify(&self) -> Result<()> {
        let schema_exists_statement = SCHEMA_EXISTS_QUERY.replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA);
        tracing::debug!(
            "Executing schema exists statement: {}",
            schema_exists_statement
        );
        let exists: bool = sqlx::query_scalar(&schema_exists_statement)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        if exists {
            Ok(())
        } else {
            Err(PgqrsError::Internal {
                message: "pgqrs schema does not exist".to_string(),
            })
        }
    }

    /// Create a new queue in the database.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to create
    /// * `unlogged` - If true, create an unlogged queue (faster, but less durable)
    ///
    /// # Returns
    /// The created [`Queue`] instance.
    pub async fn create_queue(&self, name: &String, unlogged: bool) -> Result<Queue> {
        let create_statement = if unlogged {
            CREATE_QUEUE_STATEMENT.replace("{UNLOGGED}", "UNLOGGED")
        } else {
            CREATE_QUEUE_STATEMENT.replace("{UNLOGGED}", "")
        };
        let create_statement = create_statement
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &name);

        let insert_meta = INSERT_QUEUE_METADATA
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{name}", &name)
            .replace("{unlogged}", if unlogged { "TRUE" } else { "FALSE" });

        tracing::debug!("{}", create_statement);
        tracing::debug!("{}", insert_meta);
        // Execute both statements in a transaction
        self.run_statements_in_transaction(vec![create_statement, insert_meta])
            .await?;
        Ok(Queue::new(self.pool.clone(), name))
    }

    /// List all queues managed by pgqrs.
    ///
    /// # Returns
    /// Vector of [`MetaResult`] describing each queue.
    pub async fn list_queues(&self) -> Result<Vec<MetaResult>> {
        let sql = "SELECT queue_name, created_at, unlogged FROM pgqrs.meta";
        let results = sqlx::query_as::<_, MetaResult>(sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(results)
    }

    /// Delete a queue and all its messages from the database.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to delete
    ///
    /// # Returns
    /// Ok if deletion succeeds, error otherwise.
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
            .await
    }

    /// Purge all messages from a queue, but keep the queue itself.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to purge
    ///
    /// # Returns
    /// Ok if purge succeeds, error otherwise.
    pub async fn purge_queue(&self, name: &str) -> Result<()> {
        let purge_statement = PURGE_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);
        tracing::debug!("Executing purge queue statement: {}", purge_statement);
        self.run_statements_in_transaction(vec![purge_statement])
            .await
    }

    /// Get metrics for a specific queue.
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    ///
    /// # Returns
    /// [`QueueMetrics`] for the queue.
    pub async fn queue_metrics(&self, _name: &str) -> Result<QueueMetrics> {
        todo!("Implement Admin::queue_metrics")
    }

    /// Get metrics for all queues managed by pgqrs.
    ///
    /// # Returns
    /// Vector of [`QueueMetrics`] for all queues.
    pub async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        todo!("Implement Admin::all_queues_metrics")
    }

    /// Get a [`Queue`] instance for a given queue name.
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    ///
    /// # Returns
    /// [`Queue`] instance for the queue.
    pub async fn get_queue(&self, name: &str) -> Result<Queue> {
        Ok(Queue::new(self.pool.clone(), name))
    }

    async fn run_statements_in_transaction(&self, statements: Vec<String>) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        for stmt in &statements {
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| PgqrsError::Connection {
                    message: e.to_string(),
                })?;
        }

        tx.commit().await.map_err(|e| PgqrsError::Connection {
            message: e.to_string(),
        })?;

        Ok(())
    }
}
