//! Administrative interface for managing pgqrs infrastructure.
//!
//! This module provides the [`PgqrsAdmin`] struct and related functions for installing, uninstalling, verifying, and managing queues in a PostgreSQL-backed job queue system.
//!
//! ## What
//!
//! - [`PgqrsAdmin`] allows you to create, delete, purge, and list queues, as well as install and uninstall the schema.
//! - Provides metrics and access to individual queues.
//! - Implements the [`Worker`] trait for lifecycle management.
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
use crate::types::{QueueInfo, WorkerInfo, WorkerStatus};
use crate::worker::{Worker, WorkerLifecycle};
use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

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
    LEFT OUTER JOIN pgqrs_workers pw ON m.producer_worker_id = pw.id
    LEFT OUTER JOIN pgqrs_workers cw ON m.consumer_worker_id = cw.id
    WHERE (m.producer_worker_id IS NOT NULL AND pw.id IS NULL)
       OR (m.consumer_worker_id IS NOT NULL AND cw.id IS NULL)
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
    LEFT OUTER JOIN pgqrs_workers pw ON a.producer_worker_id = pw.id
    LEFT OUTER JOIN pgqrs_workers cw ON a.consumer_worker_id = cw.id
    WHERE (a.producer_worker_id IS NOT NULL AND pw.id IS NULL)
       OR (a.consumer_worker_id IS NOT NULL AND cw.id IS NULL)
"#;

pub const DLQ_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE read_ct >= $1
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
///
/// Implements the [`Worker`] trait for lifecycle management.
/// Call `register()` after `install()` to register Admin as a worker.
pub struct PgqrsAdmin {
    pub pool: PgPool,
    pub config: Config,
    pub queues: PgqrsQueues,
    pub messages: PgqrsMessages,
    pub archive: PgqrsArchive,
    pub workers: PgqrsWorkers,
    /// Worker info for this Admin instance (set after calling register())
    worker_info: Option<WorkerInfo>,
    /// Worker lifecycle manager
    lifecycle: WorkerLifecycle,
}

impl PgqrsAdmin {
    /// Create a new admin interface for managing pgqrs infrastructure.
    ///
    /// Call `register()` after `install()` to register this Admin as a worker.
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
        let lifecycle = WorkerLifecycle::new(pool.clone());

        Ok(Self {
            pool,
            config: config.clone(),
            queues,
            messages,
            archive,
            workers,
            worker_info: None,
            lifecycle,
        })
    }

    /// Register this Admin instance as a worker.
    ///
    /// Register this Admin as a worker in the database.
    ///
    /// Call this after `install()` to register the Admin as a worker.
    /// This enables the Worker trait methods (suspend, resume, shutdown).
    ///
    /// # Arguments
    /// * `hostname` - Hostname identifier for this admin instance
    /// * `port` - Port identifier for this admin instance
    ///
    /// # Returns
    /// The WorkerInfo for this Admin.
    pub async fn register(&mut self, hostname: String, port: i32) -> Result<WorkerInfo> {
        if let Some(ref worker_info) = self.worker_info {
            return Ok(worker_info.clone());
        }

        use crate::tables::pgqrs_workers::NewWorker;
        let new_worker = NewWorker {
            hostname: hostname.clone(),
            port,
            queue_id: None,
        };

        let worker_info = self.workers.insert(new_worker).await?;

        tracing::debug!(
            "Registered Admin as worker {} (hostname: {}, port: {})",
            worker_info.id,
            hostname,
            port
        );

        self.worker_info = Some(worker_info.clone());
        Ok(worker_info)
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
        // Run migrations using sqlx
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Migration failed: {}", e),
            })?;
        Ok(())
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

        // Check that all messages with producer_worker_id or consumer_worker_id have valid worker references
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
                    "Found {} messages with invalid producer_worker_id or consumer_worker_id references",
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

        // Check that all archived messages with producer_worker_id or consumer_worker_id have valid worker references
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
                    "Found {} archived messages with invalid producer_worker_id or consumer_worker_id references",
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

    const GET_QUEUE_METRICS: &str = r#"
        WITH queue_counts AS (
            SELECT
                COUNT(*) as total_messages,
                COUNT(*) FILTER (WHERE vt <= NOW()) as pending_messages,
                COUNT(*) FILTER (WHERE vt > NOW()) as locked_messages,
                MIN(enqueued_at) FILTER (WHERE vt <= NOW()) as oldest_pending_message,
                MAX(enqueued_at) as newest_message
            FROM pgqrs_messages
            WHERE queue_id = $1
        ),
        archive_counts AS (
            SELECT COUNT(*) as archived_messages
            FROM pgqrs_archive
            WHERE queue_id = $1
        )
        SELECT
            $2 as name,
            COALESCE(qc.total_messages, 0) as total_messages,
            COALESCE(qc.pending_messages, 0) as pending_messages,
            COALESCE(qc.locked_messages, 0) as locked_messages,
            COALESCE(ac.archived_messages, 0) as archived_messages,
            qc.oldest_pending_message,
            qc.newest_message
        FROM queue_counts qc
        CROSS JOIN archive_counts ac;
    "#;

    const GET_ALL_QUEUES_METRICS: &str = r#"
        WITH queue_counts AS (
            SELECT
                queue_id,
                COUNT(*) as total_messages,
                COUNT(*) FILTER (WHERE vt <= NOW()) as pending_messages,
                COUNT(*) FILTER (WHERE vt > NOW()) as locked_messages,
                MIN(enqueued_at) FILTER (WHERE vt <= NOW()) as oldest_pending_message,
                MAX(enqueued_at) as newest_message
            FROM pgqrs_messages
            GROUP BY queue_id
        ),
        archive_counts AS (
            SELECT queue_id, COUNT(*) as archived_messages
            FROM pgqrs_archive
            GROUP BY queue_id
        )
        SELECT
            q.queue_name as name,
            COALESCE(qc.total_messages, 0) as total_messages,
            COALESCE(qc.pending_messages, 0) as pending_messages,
            COALESCE(qc.locked_messages, 0) as locked_messages,
            COALESCE(ac.archived_messages, 0) as archived_messages,
            qc.oldest_pending_message,
            qc.newest_message
        FROM pgqrs_queues q
        LEFT JOIN queue_counts qc ON q.id = qc.queue_id
        LEFT JOIN archive_counts ac ON q.id = ac.queue_id;
    "#;

    /// Get metrics for a specific queue.
    ///
    /// # Arguments
    /// * `name` - Name of the queue
    ///
    /// # Returns
    /// [`QueueMetrics`] for the queue.
    pub async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        let queue_info = self.queues.get_by_name(name).await?;

        let metrics = sqlx::query_as::<_, QueueMetrics>(Self::GET_QUEUE_METRICS)
            .bind(queue_info.id)
            .bind(name) // Pass name to be returned in the struct
            .fetch_one(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to get metrics for queue {}: {}", name, e),
            })?;

        Ok(metrics)
    }

    /// Get metrics for all queues managed by pgqrs.
    ///
    /// # Returns
    /// Vector of [`QueueMetrics`] for all queues.
    pub async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let metrics = sqlx::query_as::<_, QueueMetrics>(Self::GET_ALL_QUEUES_METRICS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to get all queues metrics: {}", e),
            })?;

        Ok(metrics)
    }

    /// Get messages currently being processed by a worker
    ///
    /// Only valid for queue workers (producers/consumers), not admin workers.
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker
    ///
    /// # Returns
    /// Vector of messages being processed by the worker
    ///
    /// # Errors
    /// Returns `PgqrsError::InvalidWorkerType` if called on an admin worker
    pub async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        // First validate the worker is not an admin (admin workers have queue_id = None)
        let worker = self.workers.get(worker_id).await?;
        if worker.queue_id.is_none() {
            return Err(PgqrsError::InvalidWorkerType {
                message: format!(
                    "Cannot get messages for admin worker {}. Admin workers do not process messages.",
                    worker_id
                ),
            });
        }

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

    /// Release messages from a specific worker (for shutdown)
    ///
    /// Only valid for queue workers (producers/consumers), not admin workers.
    ///
    /// # Arguments
    /// * `worker_id` - The worker whose messages should be released
    ///
    /// # Returns
    /// Number of messages released
    ///
    /// # Errors
    /// Returns `PgqrsError::InvalidWorkerType` if called on an admin worker
    /// Returns `PgqrsError` if database operations fail
    pub async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        // Validate the worker is not an admin
        let worker = self.workers.get(worker_id).await?;
        if worker.queue_id.is_none() {
            return Err(PgqrsError::InvalidWorkerType {
                message: format!(
                    "Cannot release messages for admin worker {}. Admin workers do not hold messages.",
                    worker_id
                ),
            });
        }

        let result = sqlx::query(crate::constants::RELEASE_WORKER_MESSAGES)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: format!("Failed to release messages for worker {}: {}", worker_id, e),
            })?;

        Ok(result.rows_affected())
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
    async fn check_worker_references(&self, worker_id: i64) -> Result<i64> {
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
        let queue_id = self.queues.get_by_name(queue_name).await?.id;
        let workers = self.workers.filter_by_fk(queue_id).await?;

        let total_workers = workers.len() as u32;
        let ready_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::Ready)
            .count() as u32;
        let stopped_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::Stopped)
            .count() as u32;
        let suspended_workers = workers
            .iter()
            .filter(|w| w.status == crate::types::WorkerStatus::Suspended)
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
            suspended_workers,
            stopped_workers,
            average_messages_per_worker,
            oldest_worker_age,
            newest_heartbeat_age,
        })
    }
}

#[async_trait]
impl Worker for PgqrsAdmin {
    fn worker_id(&self) -> i64 {
        self.worker_info
            .as_ref()
            .expect("Admin must be registered before using Worker methods. Call register() first.")
            .id
    }

    async fn heartbeat(&self) -> Result<()> {
        let worker_id = self.worker_id();
        self.lifecycle.heartbeat(worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        self.lifecycle.is_healthy(self.worker_id(), max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        let worker_id = self.worker_id();
        self.lifecycle.get_status(worker_id).await
    }

    async fn suspend(&self) -> Result<()> {
        let worker_id = self.worker_id();
        self.lifecycle.suspend(worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        let worker_id = self.worker_id();
        self.lifecycle.resume(worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        let worker_id = self.worker_id();
        self.lifecycle.shutdown(worker_id).await
    }
}

impl PgqrsAdmin {
    /// Delete this admin worker from the database.
    ///
    /// Admin workers can delete themselves since they don't hold messages.
    /// The admin must be stopped before calling this method.
    ///
    /// # Returns
    /// Ok if deletion succeeds
    ///
    /// # Errors
    /// Returns `PgqrsError` if the admin is not registered or deletion fails
    pub async fn delete_self(&self) -> Result<()> {
        let worker_id = self.worker_id();
        self.workers.delete(worker_id).await?;
        Ok(())
    }
}
