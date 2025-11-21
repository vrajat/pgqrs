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
//!     admin.create_queue("jobs").await?;
//!     Ok(())
//! }
//! ```
use crate::config::Config;
use crate::error::{PgqrsError, Result};
use crate::tables::{PgqrsArchive, PgqrsMessages, PgqrsQueues, PgqrsWorkers, Table};
use crate::types::QueueMetrics;
use crate::types::{QueueInfo, WorkerInfo};
use chrono::{Duration, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

// Verification queries
const CHECK_TABLE_EXISTS: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = $1
    )
"#;

const CHECK_ORPHANED_MESSAGES: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_messages m
    LEFT OUTER JOIN pgqrs_queues q ON m.queue_id = q.id
    WHERE q.id IS NULL
"#;

const CHECK_ORPHANED_MESSAGE_WORKERS: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_messages m
    LEFT OUTER JOIN pgqrs_workers w ON m.worker_id = w.id
    WHERE m.worker_id IS NOT NULL AND w.id IS NULL
"#;

const CHECK_ORPHANED_ARCHIVE_QUEUES: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_archive a
    LEFT OUTER JOIN pgqrs_queues q ON a.queue_id = q.id
    WHERE q.id IS NULL
"#;

const CHECK_ORPHANED_ARCHIVE_WORKERS: &str = r#"
    SELECT COUNT(*)
    FROM pgqrs_archive a
    LEFT OUTER JOIN pgqrs_workers w ON a.worker_id = w.id
    WHERE a.worker_id IS NOT NULL AND w.id IS NULL
"#;

pub const DLQ_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE read_ct >= $1
        RETURNING id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
pub struct PgqrsAdmin {
    pub pool: PgPool,
    pub config: Config,
    pub queues: PgqrsQueues,
    pub messages: PgqrsMessages,
    pub archive: PgqrsArchive,
    pub workers: PgqrsWorkers,
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

        let workers = PgqrsWorkers::new(pool.clone());
        let queues = PgqrsQueues::new(pool.clone());
        let messages = PgqrsMessages::new(pool.clone());
        let archive = PgqrsArchive::new(pool.clone());

        Ok(Self {
            pool,
            config: config.clone(),
            queues,
            messages,
            archive,
            workers,
        })
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
            // Create enum types first
            crate::constants::CREATE_WORKER_STATUS_ENUM.to_string(),
            // Create repository tables
            crate::constants::CREATE_QUEUE_INFO_TABLE_STATEMENT.to_string(),
            crate::constants::CREATE_WORKERS_TABLE.to_string(),
            // Create unified tables with foreign key dependencies
            crate::constants::CREATE_PGQRS_MESSAGES_TABLE.to_string(),
            crate::constants::CREATE_PGQRS_ARCHIVE_TABLE.to_string(),
            // Create indexes for performance
            crate::constants::CREATE_WORKERS_INDEX_QUEUE_STATUS.to_string(),
            crate::constants::CREATE_WORKERS_INDEX_HEARTBEAT.to_string(),
            // Create individual indexes for messages table
            crate::constants::CREATE_PGQRS_MESSAGES_INDEX_QUEUE_VT.to_string(),
            crate::constants::CREATE_PGQRS_MESSAGES_INDEX_WORKER_ID.to_string(),
            // Create individual indexes for archive table
            crate::constants::CREATE_PGQRS_ARCHIVE_INDEX_QUEUE_ID.to_string(),
            crate::constants::CREATE_PGQRS_ARCHIVE_INDEX_ARCHIVED_AT.to_string(),
            crate::constants::CREATE_PGQRS_ARCHIVE_INDEX_ORIGINAL_MSG_ID.to_string(),
        ];

        self.run_statements_in_transaction(statements).await
    }

    /// Uninstall pgqrs from the database.
    ///
    /// This method removes all pgqrs infrastructure from the database:
    /// - Unified messages table (pgqrs_messages)
    /// - Unified archive table (pgqrs_archive)
    /// - Queue repository table (pgqrs_queues)
    /// - Worker repository table (pgqrs_workers)
    /// - Worker status enum type
    ///
    /// # Returns
    /// Ok if uninstallation succeeds, error otherwise.
    pub async fn uninstall(&self) -> Result<()> {
        // Build DROP statements in the correct order (considering foreign key dependencies)
        let statements = vec![
            // Drop tables with foreign keys first
            crate::constants::DROP_PGQRS_MESSAGES_TABLE.to_string(),
            crate::constants::DROP_PGQRS_ARCHIVE_TABLE.to_string(),
            // Drop repository tables
            crate::constants::DROP_QUEUE_REPOSITORY.to_string(),
            crate::constants::DROP_WORKER_REPOSITORY.to_string(),
            // Drop enum type last
            crate::constants::DROP_WORKER_STATUS_ENUM.to_string(),
        ];

        // Execute all statements in a single transaction
        self.run_statements_in_transaction(statements).await
    }

    /// Verify that pgqrs installation is valid and healthy.
    ///
    /// This method checks that all required infrastructure is in place:
    /// - All unified tables exist (pgqrs_queues, pgqrs_workers, pgqrs_messages, pgqrs_archive)
    /// - Referential integrity is maintained (all foreign keys are valid)
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

        // Category 1: Check existence of all required tables
        let required_tables = [
            ("pgqrs_queues", "Queue repository table"),
            ("pgqrs_workers", "Worker repository table"),
            ("pgqrs_messages", "Unified messages table"),
            ("pgqrs_archive", "Unified archive table"),
        ];

        for (table_name, description) in &required_tables {
            let table_exists = sqlx::query_scalar::<_, bool>(CHECK_TABLE_EXISTS)
                .bind(table_name)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| PgqrsError::Connection {
                    message: format!("Failed to check {} existence: {}", description, e),
                })?;

            if !table_exists {
                return Err(PgqrsError::Connection {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        // Category 2: Validate referential integrity using left outer joins

        // Check that all messages have valid queue_id references
        let orphaned_messages = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to check message referential integrity: {}", e),
            })?;

        if orphaned_messages > 0 {
            return Err(PgqrsError::Connection {
                message: format!(
                    "Found {} messages with invalid queue_id references",
                    orphaned_messages
                ),
            });
        }

        // Check that all messages with worker_id have valid worker references
        let orphaned_message_workers = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_MESSAGE_WORKERS)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!(
                    "Failed to check message worker referential integrity: {}",
                    e
                ),
            })?;

        if orphaned_message_workers > 0 {
            return Err(PgqrsError::Connection {
                message: format!(
                    "Found {} messages with invalid worker_id references",
                    orphaned_message_workers
                ),
            });
        }

        // Check that all archived messages have valid queue_id references
        let orphaned_archive_queues = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_ARCHIVE_QUEUES)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to check archive referential integrity: {}", e),
            })?;

        if orphaned_archive_queues > 0 {
            return Err(PgqrsError::Connection {
                message: format!(
                    "Found {} archived messages with invalid queue_id references",
                    orphaned_archive_queues
                ),
            });
        }

        // Check that all archived messages with worker_id have valid worker references
        let orphaned_archive_workers = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_ARCHIVE_WORKERS)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!(
                    "Failed to check archive worker referential integrity: {}",
                    e
                ),
            })?;

        if orphaned_archive_workers > 0 {
            return Err(PgqrsError::Connection {
                message: format!(
                    "Found {} archived messages with invalid worker_id references",
                    orphaned_archive_workers
                ),
            });
        }

        // Commit the transaction (ensures consistency of all checks)
        tx.commit().await.map_err(|e| PgqrsError::Connection {
            message: format!("Failed to commit transaction: {}", e),
        })?;

        Ok(())
    }

    /// Create a new queue in the database.
    ///
    /// This method creates a new queue by inserting a record into the pgqrs_queues table.
    /// All messages for this queue will be stored in the unified pgqrs_messages table.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to create
    ///
    /// # Returns
    /// The created [`QueueInfo`] instance.
    pub async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
        // Create new queue data
        use crate::tables::NewQueue;
        let new_queue = NewQueue {
            queue_name: name.to_string(),
        };

        // Use the queues table insert function
        self.queues.insert(new_queue).await
    }

    /// Get a [`QueueInfo`] instance for a given queue name.
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    ///
    /// # Returns
    /// [`QueueInfo`] instance for the queue.
    pub async fn get_queue(&self, name: &str) -> Result<QueueInfo> {
        // Get queue info to get the queue_id
        self.queues.get_by_name(name).await
    }

    /// Delete a queue from the database.
    ///
    /// A queue can only be deleted if there are no references in messages or archive.
    /// This prevents data orphaning by ensuring referential integrity is maintained.
    /// The operation runs in a transaction with row-level locking to ensure atomicity
    /// and prevent concurrent modifications during the deletion process.
    ///
    /// # Arguments
    /// * `name` - Name of the queue to delete
    ///
    /// # Returns
    /// Ok if deletion succeeds, error if queue has messages/archives or other error.
    pub async fn delete_queue(&self, queue_info: &QueueInfo) -> Result<()> {
        // Start a transaction for atomic deletion with row locking
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to begin transaction: {}", e),
            })?;

        // Lock the queue row for exclusive access and get queue_id
        let queue_id: Option<i64> = sqlx::query_scalar(crate::constants::LOCK_QUEUE_FOR_DELETE)
            .bind(&queue_info.queue_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to lock queue '{}': {}", queue_info.queue_name, e),
            })?;

        let queue_id = queue_id.ok_or_else(|| crate::error::PgqrsError::QueueNotFound {
            name: queue_info.queue_name.clone(),
        })?;

        // Check for active workers assigned to this queue
        let active_workers = self.workers.count_active_for_queue(queue_info.id).await?;

        if active_workers > 0 {
            return Err(crate::error::PgqrsError::Connection {
                message: format!(
                    "Cannot delete queue '{}': {} active worker(s) are still assigned to this queue. Stop workers first.",
                    queue_info.queue_name, active_workers
                ),
            });
        }

        // Check referential integrity using table methods
        let messages_count = self.messages.count_for_fk(queue_id, &mut tx).await?;
        let archive_count = self.archive.count_for_fk(queue_id, &mut tx).await?;
        let total_references = messages_count + archive_count;

        if total_references > 0 {
            return Err(crate::error::PgqrsError::Connection {
                message: format!(
                    "Cannot delete queue '{}': {} references exist in messages/archive tables. Purge data first.",
                    queue_info.queue_name, total_references
                ),
            });
        }

        // Safe to delete - no references exist and we have exclusive lock
        PgqrsQueues::delete_by_name_tx(&queue_info.queue_name, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to commit queue deletion: {}", e),
            })?;

        tracing::debug!("Successfully deleted queue '{}'", queue_info.queue_name);
        Ok(())
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
        // Start a transaction for atomic purge with row locking
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to begin transaction: {}", e),
            })?;

        // Lock the queue row for data modification and get queue_id
        let queue_id: Option<i64> = sqlx::query_scalar(crate::constants::LOCK_QUEUE_FOR_UPDATE)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to lock queue '{}': {}", name, e),
            })?;

        let queue_id = queue_id.ok_or_else(|| crate::error::PgqrsError::QueueNotFound {
            name: name.to_string(),
        })?;

        self.messages.delete_by_fk(queue_id, &mut tx).await?;
        self.archive.delete_by_fk(queue_id, &mut tx).await?;
        self.workers.delete_by_fk(queue_id, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| crate::error::PgqrsError::Connection {
                message: format!("Failed to commit queue purge: {}", e),
            })?;

        tracing::debug!("Successfully purged queue '{}'", name);
        Ok(())
    }

    pub async fn dlq(&self) -> Result<Vec<i64>> {
        let result: Vec<i64> = sqlx::query_scalar(DLQ_BATCH)
            .bind(self.config.max_read_ct)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to move messages to DLQ: {}", e),
            })?;

        Ok(result)
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
        // First, verify the queue exists and get its info
        let queue_info = self.get_queue(&queue_name).await?;

        // Create new worker data
        use crate::tables::pgqrs_workers::NewWorker;
        let new_worker = NewWorker {
            hostname: hostname.clone(),
            port,
            queue_id: queue_info.id,
        };

        // Use the workers table insert function
        let worker = self.workers.insert(new_worker).await?;

        tracing::debug!(
            "Successfully registered worker {} for queue '{}'",
            worker.id,
            queue_name
        );
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
        self.workers.list().await
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
        self.workers.get(worker_id).await
    }

    /// Get messages currently being processed by a worker
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
        let messages =
            sqlx::query_as::<_, crate::types::QueueMessage>(crate::constants::GET_WORKER_MESSAGES)
                .bind(worker_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| PgqrsError::Connection {
                    message: format!("Failed to get messages for worker {}: {}", worker_id, e),
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
        let queue_info = self.get_queue(queue_name).await?;
        self.workers.filter_by_fk(queue_info.id).await
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
        let result = sqlx::query(crate::constants::RELEASE_WORKER_MESSAGES)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to release messages for worker {}: {}", worker_id, e),
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

    /// Check if a worker has any associated messages or archives
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to check
    ///
    /// # Returns
    /// Number of associated records (messages + archives)
    pub async fn check_worker_references(&self, worker_id: i64) -> Result<i64> {
        let row = sqlx::query_scalar::<_, i64>(crate::constants::CHECK_WORKER_REFERENCES)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(row)
    }

    /// Delete a specific worker if it has no associated messages or archives
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to delete
    ///
    /// # Returns
    /// Error if worker has references, otherwise number of workers deleted (0 or 1)
    pub async fn delete_worker(&self, worker_id: i64) -> Result<u64> {
        // First check if worker has any references
        let reference_count = self.check_worker_references(worker_id).await?;

        if reference_count > 0 {
            return Err(PgqrsError::SchemaValidation {
                message: format!(
                    "Cannot delete worker {}: worker has {} associated messages/archives. Purge messages first.",
                    worker_id, reference_count
                ),
            });
        }

        self.workers.delete(worker_id).await
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
