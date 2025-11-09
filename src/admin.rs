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
//!     let config = Config::from_dsn("postgresql://user:pass@localhost/db");
//!     let admin = PgqrsAdmin::new(&config).await?;
//!     admin.install().await?;
//!     admin.create_queue(&"jobs".to_string(), false).await?;
//!     Ok(())
//! }
//! ```
use crate::config::Config;
use crate::constants::{
    CREATE_ARCHIVE_INDEX_ARCHIVED_AT, CREATE_ARCHIVE_INDEX_ENQUEUED_AT, CREATE_ARCHIVE_TABLE,
    CREATE_QUEUE_INFO_TABLE_STATEMENT, CREATE_QUEUE_STATEMENT, CREATE_WORKERS_INDEX_HEARTBEAT,
    CREATE_WORKERS_INDEX_QUEUE_STATUS, CREATE_WORKERS_TABLE, CREATE_WORKER_STATUS_ENUM,
    DELETE_QUEUE_METADATA, DROP_ARCHIVE_TABLE, DROP_QUEUE_REPOSITORY, DROP_QUEUE_STATEMENT,
    DROP_WORKER_REPOSITORY, DROP_WORKER_STATUS_ENUM, INSERT_QUEUE_METADATA,
    LOCK_QUEUE_REPOSITORY_ACCESS_SHARE, LOCK_QUEUE_REPOSITORY_EXCLUSIVE, PURGE_ARCHIVE_TABLE,
    PURGE_QUEUE_STATEMENT, QUEUE_PREFIX,
};
use crate::error::{PgqrsError, Result};
use crate::queue::Queue;
use crate::types::QueueMetrics;
use crate::types::{QueueInfo, WorkerInfo};
use crate::WorkerStatus;
use chrono::{Duration, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
pub struct PgqrsAdmin {
    pub pool: PgPool,
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
        // Create the search_path setting
        let search_path_sql = format!("SET search_path = \"{}\"", config.schema);

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .after_connect(move |conn, _meta| {
                let sql = search_path_sql.clone();
                Box::pin(async move {
                    sqlx::query(&sql).execute(conn).await?;
                    Ok(())
                })
            })
            .connect(&config.dsn)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(Self { pool })
    }

    /// Install pgqrs schema and infrastructure in the database.
    ///
    /// **Important**: The schema must be created before running install.
    /// Use your preferred method to create the schema, for example:
    /// ```sql
    /// CREATE SCHEMA IF NOT EXISTS my_schema;
    /// ```
    ///
    /// # Returns
    /// Ok if installation (or validation) succeeds, error otherwise.
    pub async fn install(&self) -> Result<()> {
        let statements = vec![
            CREATE_QUEUE_INFO_TABLE_STATEMENT.to_string(),
            CREATE_WORKER_STATUS_ENUM.to_string(),
            CREATE_WORKERS_TABLE.to_string(),
            CREATE_WORKERS_INDEX_QUEUE_STATUS.to_string(),
            CREATE_WORKERS_INDEX_HEARTBEAT.to_string(),
        ];

        self.run_statements_in_transaction(statements).await
    }

    /// Uninstall pgqrs schema and remove all state from the database.
    ///
    /// **Warning**: This will permanently delete all pgqrs data including:
    /// - All queue tables (q_*)
    /// - All archive tables (archive_*)
    /// - Queue repository table
    /// - Worker repository table
    /// - Worker status enum type
    ///
    /// # Returns
    /// Ok if uninstallation succeeds, error otherwise.
    pub async fn uninstall(&self) -> Result<()> {
        // First, get list of all queues to drop their tables
        let queues = self.list_queues().await?;

        // Build all DROP statements in the correct order
        let mut statements = Vec::new();

        // Drop all queue tables first (they have foreign keys to worker_repository)
        for queue_info in &queues {
            let queue_name = &queue_info.queue_name;
            let drop_queue_sql = DROP_QUEUE_STATEMENT
                .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
                .replace("{queue_name}", queue_name);
            statements.push(drop_queue_sql);
        }

        // Drop all archive tables
        for queue_info in &queues {
            let queue_name = &queue_info.queue_name;
            let drop_archive_sql = DROP_ARCHIVE_TABLE.replace("{queue_name}", queue_name);
            statements.push(drop_archive_sql);
        }

        // Now drop the repository tables (order matters due to dependencies)
        statements.push(DROP_QUEUE_REPOSITORY.to_string());
        statements.push(DROP_WORKER_REPOSITORY.to_string());

        // Drop the enum type last (after all tables that use it)
        statements.push(DROP_WORKER_STATUS_ENUM.to_string());

        // Execute all statements in a single transaction
        self.run_statements_in_transaction(statements).await
    }

    /// Verify that pgqrs installation is valid and healthy.
    ///
    /// This method checks that all required infrastructure is in place:
    /// - QueueRepository table exists
    /// - WorkerRepository table exists
    /// - All queues in queue repository have corresponding queue and archive tables
    ///
    /// # Returns
    /// Ok if installation is valid, error otherwise.
    pub async fn verify(&self) -> Result<()> {
        // Begin a read-only transaction for all verification queries
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to begin transaction: {}", e),
            })?;

        // Check if queue_repository table exists
        let queue_repo_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'queue_repository'
            )",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| PgqrsError::Connection {
            message: format!("Failed to check queue_repository existence: {}", e),
        })?;

        if !queue_repo_exists {
            return Err(PgqrsError::Connection {
                message: "queue_repository table does not exist".to_string(),
            });
        }

        // Check if worker_repository table exists
        let worker_repo_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'worker_repository'
            )",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| PgqrsError::Connection {
            message: format!("Failed to check worker_repository existence: {}", e),
        })?;

        if !worker_repo_exists {
            return Err(PgqrsError::Connection {
                message: "worker_repository table does not exist".to_string(),
            });
        }

        // Lock queue_repository table in ACCESS SHARE mode
        // This allows other reads but blocks writes, ensuring consistent metadata view
        sqlx::query(LOCK_QUEUE_REPOSITORY_ACCESS_SHARE)
            .execute(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to acquire lock on queue_repository: {}", e),
            })?;

        // Get all queues from queue_repository and verify their tables exist
        let queues: Vec<QueueInfo> = sqlx::query_as(
            "SELECT queue_name, unlogged, created_at FROM queue_repository ORDER BY queue_name",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| PgqrsError::Connection {
            message: format!("Failed to list queues: {}", e),
        })?;

        for queue_info in &queues {
            let queue_name = &queue_info.queue_name;

            // Check if queue table exists
            let queue_table_name = format!("{}_{}", QUEUE_PREFIX, queue_name);
            let queue_table_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = $1
                )",
            )
            .bind(&queue_table_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!(
                    "Failed to check queue table '{}' existence: {}",
                    queue_table_name, e
                ),
            })?;

            if !queue_table_exists {
                return Err(PgqrsError::Connection {
                    message: format!(
                        "Queue table '{}' does not exist for queue '{}'",
                        queue_table_name, queue_name
                    ),
                });
            }

            // Check if archive table exists
            let archive_table_name = format!("archive_{}", queue_name);
            let archive_table_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = $1
                )",
            )
            .bind(&archive_table_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!(
                    "Failed to check archive table '{}' existence: {}",
                    archive_table_name, e
                ),
            })?;

            if !archive_table_exists {
                return Err(PgqrsError::Connection {
                    message: format!(
                        "Archive table '{}' does not exist for queue '{}'",
                        archive_table_name, queue_name
                    ),
                });
            }
        }

        // Commit the transaction (though it's read-only, this ensures consistency)
        tx.commit().await.map_err(|e| PgqrsError::Connection {
            message: format!("Failed to commit transaction: {}", e),
        })?;

        Ok(())
    }

    /// Create a new queue in the database.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to create
    /// * `unlogged` - If true, create an unlogged queue (faster, but less durable)
    ///
    /// # Returns
    /// The created [`Queue`] instance.
    pub async fn create_queue(&self, name: &str, unlogged: bool) -> Result<Queue> {
        let create_statement = if unlogged {
            CREATE_QUEUE_STATEMENT.replace("{UNLOGGED}", "UNLOGGED")
        } else {
            CREATE_QUEUE_STATEMENT.replace("{UNLOGGED}", "")
        };
        let create_statement = create_statement
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);

        let insert_meta = INSERT_QUEUE_METADATA
            .replace("{name}", name)
            .replace("{unlogged}", if unlogged { "TRUE" } else { "FALSE" });

        // Create archive table for message archiving
        let create_archive_statement = CREATE_ARCHIVE_TABLE.replace("{queue_name}", name);

        let create_archive_index1 = CREATE_ARCHIVE_INDEX_ARCHIVED_AT.replace("{queue_name}", name);

        let create_archive_index2 = CREATE_ARCHIVE_INDEX_ENQUEUED_AT.replace("{queue_name}", name);

        tracing::debug!("Queue statement: {}", create_statement);
        tracing::debug!("Meta statement: {}", insert_meta);
        tracing::debug!("Archive statement: {}", create_archive_statement);
        tracing::debug!("Archive index 1: {}", create_archive_index1);
        tracing::debug!("Archive index 2: {}", create_archive_index2);

        // Execute all statements in a transaction with table-level locking
        // Lock statement must be first to acquire exclusive access to queue_repository
        self.run_statements_in_transaction(vec![
            LOCK_QUEUE_REPOSITORY_EXCLUSIVE.to_string(),
            create_statement,
            create_archive_statement,
            create_archive_index1,
            create_archive_index2,
            insert_meta,
        ])
        .await?;

        Ok(Queue::new(self.pool.clone(), name))
    }

    /// List all queues managed by pgqrs.
    ///
    /// # Returns
    /// Vector of [`MetaResult`] describing each queue.
    pub async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let results = sqlx::query_as::<_, QueueInfo>(crate::constants::LIST_QUEUE_INFO)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;
        Ok(results)
    }

    /// Delete a queue and all its messages from the database.
    /// This also deletes the corresponding archive table.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to delete
    ///
    /// # Returns
    /// Ok if deletion succeeds, error otherwise.
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        let drop_statement = DROP_QUEUE_STATEMENT
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);

        let drop_archive_statement = DROP_ARCHIVE_TABLE.replace("{queue_name}", name);

        let delete_meta = DELETE_QUEUE_METADATA.replace("{name}", name);

        tracing::debug!("Executing delete metadata statement: {}", delete_meta);
        tracing::debug!("Executing drop queue statement: {}", drop_statement);
        tracing::debug!(
            "Executing drop archive statement: {}",
            drop_archive_statement
        );

        // Execute all statements in a transaction with table-level locking
        // Lock statement must be first to acquire exclusive access to queue_repository
        self.run_statements_in_transaction(vec![
            LOCK_QUEUE_REPOSITORY_EXCLUSIVE.to_string(),
            drop_statement,
            drop_archive_statement,
            delete_meta,
        ])
        .await
    }

    /// Purge all messages from a queue, but keep the queue itself.
    /// Note: This only purges the active queue, not the archive table.
    /// Use `purge_archive` to purge archived messages.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to purge
    ///
    /// # Returns
    /// Ok if purge succeeds, error otherwise.
    pub async fn purge_queue(&self, name: &str) -> Result<()> {
        let purge_statement = PURGE_QUEUE_STATEMENT
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", name);
        tracing::debug!("Executing purge queue statement: {}", purge_statement);
        self.run_statements_in_transaction(vec![purge_statement])
            .await
    }

    /// Purge all archived messages from a queue's archive table.
    /// The queue and archive table structure are preserved.
    ///
    /// # Arguments
    /// * `name` - Name of the queue whose archive to purge
    ///
    /// # Returns
    /// Ok if purge succeeds, error otherwise.
    pub async fn purge_archive(&self, name: &str) -> Result<()> {
        let purge_archive_statement = PURGE_ARCHIVE_TABLE.replace("{queue_name}", name);
        tracing::debug!(
            "Executing purge archive statement: {}",
            purge_archive_statement
        );
        self.run_statements_in_transaction(vec![purge_archive_statement])
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

    /// Get a [`Queue`] instance for a given queue name.
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    ///
    /// # Returns
    /// [`Queue`] instance for the queue.
    pub async fn get_queue_by_name(&self, queue_name: &str) -> Result<Queue> {
        let queue_name: String = sqlx::query_scalar(crate::constants::GET_QUEUE_INFO_BY_NAME)
            .bind(queue_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|_e| PgqrsError::QueueNotFound {
                name: queue_name.to_string(),
            })?;

        Ok(Queue::new(self.pool.clone(), &queue_name))
    }

    ///
    ///
    ///
    /// Execute multiple SQL statements in a single transaction.
    ///
    /// This method ensures that either all statements succeed or all are rolled back,
    /// providing atomicity for operations that require multiple SQL commands.
    ///
    /// # Arguments
    /// * `statements` - Vector of SQL statements to execute
    ///
    /// # Returns
    /// Ok if all statements executed successfully, error otherwise.
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

    /// Create and register a new worker for a queue
    ///
    /// # Arguments
    /// * `queue` - The queue this worker will process
    /// * `hostname` - Hostname where the worker is running
    /// * `port` - Port number for the worker
    ///
    /// # Returns
    /// A new `Worker` instance registered in the database
    ///
    /// # Errors
    /// Returns `PgqrsError` if database operations fail or if hostname+port
    /// combination is already registered by another active worker
    pub async fn register(
        &self,
        queue_name: String,
        hostname: String,
        port: i32,
    ) -> Result<WorkerInfo> {
        let queue = self.get_queue(&queue_name).await?;
        let now = Utc::now();

        // Insert the worker into the database and get the generated ID
        let worker_id: i64 = sqlx::query_scalar(crate::constants::INSERT_WORKER)
            .bind(&hostname)
            .bind(port)
            .bind(&queue.queue_name)
            .bind(now)
            .bind(now)
            .bind(WorkerStatus::Ready)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        let worker = WorkerInfo {
            id: worker_id,
            hostname: hostname.clone(),
            port,
            queue_name: queue.queue_name.clone(),
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        };

        Ok(worker)
    }

    /// Update this worker's heartbeat timestamp
    ///
    /// Should be called periodically to indicate the worker is still alive
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn heartbeat(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();

        let sql = crate::constants::UPDATE_WORKER_HEARTBEAT;

        sqlx::query(sql)
            .bind(now)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Get all workers across all queues
    ///
    /// # Returns
    /// Vector of all workers in the system
    pub async fn list_all_workers(&self) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(crate::constants::LIST_ALL_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(workers)
    }

    /// Get a specific worker by ID
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to retrieve
    ///
    /// # Returns
    /// The worker with the specified ID, or error if not found
    ///
    /// # Errors
    /// Returns `PgqrsError` if database query fails or worker not found
    pub async fn get_worker_by_id(&self, worker_id: i64) -> Result<WorkerInfo> {
        let worker = sqlx::query_as::<_, WorkerInfo>(crate::constants::GET_WORKER_BY_ID)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(worker)
    }

    /// Get messages being processed by a worker
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker
    ///
    /// # Returns
    /// Vector of messages being processed by the worker
    pub async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        let worker = self.get_worker_by_id(worker_id).await?;
        let sql = crate::constants::GET_WORKER_MESSAGES
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", &worker.queue_name);

        let messages = sqlx::query_as::<_, crate::types::QueueMessage>(&sql)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(messages)
    }

    /// Get workers for a specific queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    ///
    /// # Returns
    /// Vector of workers processing the specified queue
    pub async fn list_queue_workers(&self, queue_name: &str) -> Result<Vec<WorkerInfo>> {
        let workers = sqlx::query_as::<_, WorkerInfo>(crate::constants::LIST_QUEUE_WORKERS)
            .bind(queue_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(workers)
    }

    /// Release messages from a specific worker (for shutdown)
    ///
    /// # Arguments
    /// * `worker_id` - The worker whose messages should be released
    ///
    /// # Returns
    /// Number of messages released
    ///
    /// # Errors
    /// Returns `PgqrsError` if database operations fail
    pub async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        let worker = self.get_worker_by_id(worker_id).await?;
        let sql = crate::constants::RELEASE_WORKER_MESSAGES
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", &worker.queue_name);

        let result = sqlx::query(&sql)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(result.rows_affected())
    }

    /// Mark this worker as shutting down gracefully
    ///
    /// This signals that the worker is beginning shutdown process
    /// and should not receive new message assignments
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn begin_shutdown(&self, worker_id: i64) -> Result<()> {
        let now = Utc::now();

        sqlx::query(crate::constants::UPDATE_WORKER_SHUTDOWN)
            .bind(now)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Mark this worker as stopped (final state)
    ///
    /// This is the final step in worker lifecycle
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn mark_stopped(&self, worker_id: i64) -> Result<()> {
        sqlx::query(crate::constants::UPDATE_WORKER_STOPPED)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Check if this worker is healthy based on heartbeat age
    ///
    /// # Arguments
    /// * `max_age` - Maximum allowed age for the last heartbeat
    ///
    /// # Returns
    /// `true` if the worker's last heartbeat is within the max_age threshold
    pub async fn is_healthy(&self, worker_id: i64, max_age: Duration) -> Result<bool> {
        let now = Utc::now();
        let worker = self.get_worker_by_id(worker_id).await?;
        let heartbeat_age = now.signed_duration_since(worker.heartbeat_at);

        Ok(heartbeat_age <= max_age)
    }

    /// Remove stopped workers older than specified duration
    ///
    /// # Arguments
    /// * `older_than` - Duration threshold for worker removal
    ///
    /// # Returns
    /// Number of workers removed
    pub async fn purge_old_workers(&self, older_than: std::time::Duration) -> Result<u64> {
        let threshold = chrono::Utc::now()
            - chrono::Duration::from_std(older_than).map_err(|e| PgqrsError::InvalidConfig {
                field: "older_than".to_string(),
                message: format!("Invalid duration: {}", e),
            })?;

        let result = sqlx::query(crate::constants::PURGE_OLD_WORKERS)
            .bind(threshold)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(result.rows_affected())
    }

    /// Get worker statistics
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to get stats for
    ///
    /// # Returns
    /// Worker statistics for the queue
    pub async fn worker_stats(&self, queue_name: &str) -> Result<crate::types::WorkerStats> {
        let workers = self.list_queue_workers(queue_name).await?;

        let total_workers = workers.len() as u32;
        let ready_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::Ready)
            .count() as u32;
        let shutting_down_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::ShuttingDown)
            .count() as u32;
        let stopped_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::Stopped)
            .count() as u32;

        // Get message counts per worker
        let mut total_messages = 0u64;

        for worker in &workers {
            let messages = self.get_worker_messages(worker.id).await?;
            total_messages += messages.len() as u64;
        }

        let average_messages_per_worker = if total_workers > 0 {
            total_messages as f64 / total_workers as f64
        } else {
            0.0
        };

        let now = chrono::Utc::now();
        let oldest_worker_age = workers
            .iter()
            .map(|w| now.signed_duration_since(w.started_at))
            .max()
            .unwrap_or(chrono::Duration::zero())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);

        let newest_heartbeat_age = workers
            .iter()
            .map(|w| now.signed_duration_since(w.heartbeat_at))
            .min()
            .unwrap_or(chrono::Duration::zero())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);

        Ok(crate::types::WorkerStats {
            total_workers,
            ready_workers,
            shutting_down_workers,
            stopped_workers,
            average_messages_per_worker,
            oldest_worker_age,
            newest_heartbeat_age,
        })
    }
}
