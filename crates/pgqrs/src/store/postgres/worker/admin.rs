//! Administrative interface for managing pgqrs infrastructure.
//!
//! This module provides the [`Admin`] struct and related functions for installing, uninstalling, verifying, and managing queues in a PostgreSQL-backed job queue system.
//!
//! ## What
//!
//! - [`Admin`] allows you to create, delete, purge, and list queues, as well as install and uninstall the schema.
//! - Provides metrics and access to individual queues.
//! - Implements the [`Worker`] trait for lifecycle management.
//!
//! ## How
//!
//! Use [`Admin`] to set up and administer your queue infrastructure. See function docs for usage details.
//!
//! ### Example
//!
//! ```no_run
//! # use pgqrs::{Config, Admin};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::from_dsn("postgresql://user:pass@localhost/db");
//! let store = pgqrs::connect_with_config(&config).await?;
//! pgqrs::admin(&store).install().await?;
//! pgqrs::admin(&store).create_queue("jobs").await?;
//! # Ok(())
//! # }
//! ```
use crate::config::Config;
use crate::error::Result;
use crate::store::{QueueTable, WorkerTable};
use crate::types::QueueMessage;

use crate::store::postgres::tables::pgqrs_archive::Archive;
use crate::store::postgres::tables::pgqrs_messages::Messages;
use crate::store::postgres::tables::pgqrs_queues::Queues;
use crate::store::postgres::tables::pgqrs_workers::Workers;
use crate::types::QueueMetrics;
use crate::types::{QueueInfo, SystemStats, WorkerHealthStats, WorkerInfo, WorkerStatus};
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

const RELEASE_ZOMBIE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET consumer_worker_id = NULL,
        vt = NOW(),
        dequeued_at = NULL
    WHERE consumer_worker_id = $1
"#;

const SHUTDOWN_ZOMBIE_WORKER: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped',
        shutdown_at = NOW()
    WHERE id = $1
"#;

/// Get messages assigned to a specific worker
const GET_WORKER_MESSAGES: &str = r#"
    SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
    FROM pgqrs_messages
    WHERE consumer_worker_id = $1
    ORDER BY id;
"#;

/// Release messages assigned to a worker (set worker_id to NULL and reset vt)
const RELEASE_WORKER_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NULL, consumer_worker_id = NULL
    WHERE consumer_worker_id = $1;
"#;

/// Lock queue row for exclusive access during deletion
const LOCK_QUEUE_FOR_DELETE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR UPDATE;
"#;

/// Lock queue row for data modification operations (purge, etc.)
const LOCK_QUEUE_FOR_UPDATE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR NO KEY UPDATE;
"#;

/// Check if worker has any associated messages or archives
const CHECK_WORKER_REFERENCES: &str = r#"
    SELECT COUNT(*) as total_references FROM (
        SELECT 1 FROM pgqrs_messages WHERE producer_worker_id = $1 OR consumer_worker_id = $1
        UNION ALL
        SELECT 1 FROM pgqrs_archive WHERE producer_worker_id = $1 OR consumer_worker_id = $1
    ) refs
"#;

/// Delete old stopped workers (only those without references)
const PURGE_OLD_WORKERS: &str = r#"
    DELETE FROM pgqrs_workers
    WHERE status = 'stopped'
      AND heartbeat_at < $1
      AND id NOT IN (
          SELECT DISTINCT worker_id
          FROM (
              SELECT producer_worker_id as worker_id FROM pgqrs_messages WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL
              UNION
              SELECT producer_worker_id as worker_id FROM pgqrs_archive WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id as worker_id FROM pgqrs_archive WHERE consumer_worker_id IS NOT NULL
          ) refs
      )
"#;

const GET_QUEUE_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL) as pending_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id) as archived_messages,
        MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    WHERE q.id = $1
    GROUP BY q.id, q.queue_name
"#;

const GET_ALL_QUEUES_METRICS: &str = r#"
    SELECT
        q.queue_name as name,
        COUNT(m.id) as total_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL) as pending_messages,
        COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive a WHERE a.queue_id = q.id) as archived_messages,
        MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL) as oldest_pending_message,
        MAX(m.enqueued_at) as newest_message
    FROM pgqrs_queues q
    LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
    GROUP BY q.id, q.queue_name
"#;

const GET_SYSTEM_STATS: &str = r#"
    SELECT
        (SELECT COUNT(*) FROM pgqrs_queues) as total_queues,
        (SELECT COUNT(*) FROM pgqrs_workers) as total_workers,
        (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready') as active_workers,
        (SELECT COUNT(*) FROM pgqrs_messages) as total_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL) as pending_messages,
        (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL) as locked_messages,
        (SELECT COUNT(*) FROM pgqrs_archive) as archived_messages,
        '0.5.0' as schema_version
"#;

const GET_WORKER_HEALTH_GLOBAL: &str = r#"
    SELECT
        'Global' as queue_name,
        COUNT(*) as total_workers,
        COUNT(*) FILTER (WHERE status = 'ready') as ready_workers,
        COUNT(*) FILTER (WHERE status = 'suspended') as suspended_workers,
        COUNT(*) FILTER (WHERE status = 'stopped') as stopped_workers,
        COUNT(*) FILTER (WHERE status = 'ready' AND heartbeat_at < NOW() - $1::interval) as stale_workers
    FROM pgqrs_workers
"#;

const GET_WORKER_HEALTH_BY_QUEUE: &str = r#"
    SELECT
        COALESCE(q.queue_name, 'Admin') as queue_name,
        COUNT(w.id) as total_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'ready') as ready_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'suspended') as suspended_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'stopped') as stopped_workers,
        COUNT(w.id) FILTER (WHERE w.status = 'ready' AND w.heartbeat_at < NOW() - $1::interval) as stale_workers
    FROM pgqrs_workers w
    LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
    GROUP BY q.queue_name
"#;

#[derive(Debug)]
/// Admin interface for managing pgqrs infrastructure
///
/// Implements the [`Worker`] trait for lifecycle management.
/// Call `register()` after `install()` to register Admin as a worker.
pub struct Admin {
    pub pool: PgPool,
    pub config: Config,
    pub queues: Queues,
    pub messages: Messages,
    pub archive: Archive,
    pub workers: Workers,
    /// Worker info for this Admin instance (set after calling register())
    worker_info: Option<WorkerInfo>,
}

impl Admin {
    /// Create a new admin interface for managing pgqrs infrastructure.
    ///
    /// Call `register()` after `install()` to register this Admin as a worker.
    ///
    /// # Arguments
    /// * `config` - Configuration for database connection and queue options
    ///
    /// # Returns
    /// A new `Admin` instance.
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
            .map_err(|e| crate::error::Error::ConnectionFailed {
                source: e,
                context: "Failed to connect to postgres".into(),
            })?;

        let workers = Workers::new(pool.clone());
        let queues = Queues::new(pool.clone());
        let messages = Messages::new(pool.clone());
        let archive = Archive::new(pool.clone());

        Ok(Self {
            pool,
            config: config.clone(),
            queues,
            messages,
            archive,
            workers,
            worker_info: None,
        })
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
    /// Returns `crate::error::Error::InvalidWorkerType` if called on an admin worker
    pub async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        // First validate the worker is not an admin (admin workers have queue_id = None)
        let worker = self.workers.get(worker_id).await?;
        if worker.queue_id.is_none() {
            return Err(crate::error::Error::InvalidWorkerType {
                message: format!(
                    "Cannot get messages for admin worker {}. Admin workers do not process messages.",
                    worker_id
                ),
            });
        }

        let messages = sqlx::query_as::<_, crate::types::QueueMessage>(GET_WORKER_MESSAGES)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_WORKER_MESSAGES".into(),
                source: e,
                context: format!("Failed to get messages for worker {}", worker_id),
            })?;

        Ok(messages)
    }

    /// Check if a worker has any associated messages or archives
    ///
    /// # Arguments
    /// * `worker_id` - ID of the worker to check
    ///
    /// # Returns
    /// Number of associated records (messages + archives)
    async fn check_worker_references(&self, worker_id: i64) -> Result<i64> {
        let row = sqlx::query_scalar::<_, i64>(CHECK_WORKER_REFERENCES)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_WORKER_REFERENCES".into(),
                source: e,
                context: format!("Failed to check worker references for worker {}", worker_id),
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
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Cannot delete worker {}: worker has {} associated messages/archives. Purge messages first.",
                    worker_id, reference_count
                ),
            });
        }

        self.workers.delete(worker_id).await
    }

    /// Get the worker ID, returning an error if not registered.
    /// Use this in async methods that can propagate errors.
    fn try_worker_id(&self) -> Result<i64> {
        self.worker_info.as_ref().map(|w| w.id).ok_or_else(|| {
            crate::error::Error::WorkerNotRegistered {
                message:
                    "Admin must be registered before using Worker methods. Call register() first."
                        .to_string(),
            }
        })
    }
}

#[async_trait]
impl crate::store::Worker for Admin {
    fn worker_id(&self) -> i64 {
        // Return -1 as sentinel value if not registered.
        // Async methods should use try_worker_id() which returns Result.
        self.worker_info.as_ref().map(|w| w.id).unwrap_or(-1)
    }

    async fn heartbeat(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.heartbeat(worker_id).await
    }

    async fn is_healthy(&self, max_age: chrono::Duration) -> Result<bool> {
        let worker_id = self.try_worker_id()?;
        self.workers.is_healthy(worker_id, max_age).await
    }

    async fn status(&self) -> Result<WorkerStatus> {
        let worker_id = self.try_worker_id()?;
        self.workers.get_status(worker_id).await
    }

    async fn suspend(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.suspend(worker_id).await
    }

    async fn resume(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.resume(worker_id).await
    }

    async fn shutdown(&self) -> Result<()> {
        let worker_id = self.try_worker_id()?;
        self.workers.shutdown(worker_id).await
    }
}

#[async_trait]
impl crate::store::Admin for Admin {
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
    async fn install(&self) -> Result<()> {
        // Run migrations using sqlx
        MIGRATOR.run(&self.pool).await?;
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
    async fn verify(&self) -> Result<()> {
        // Begin a read-only transaction for all verification queries
        let mut tx =
            self.pool
                .begin()
                .await
                .map_err(|e| crate::error::Error::TransactionFailed {
                    source: e,
                    context: "Failed to begin transaction for verification".into(),
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
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("CHECK_TABLE_EXISTS ({})", table_name),
                    source: e,
                    context: format!("Failed to check if table {} exists", table_name),
                })?;

            if !table_exists {
                return Err(crate::error::Error::SchemaValidation {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        // Category 2: Validate referential integrity using left outer joins

        // Check that all messages have valid queue_id references
        let orphaned_messages = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_MESSAGES".into(),
                source: e,
                context: "Failed to check message referential integrity".into(),
            })?;

        if orphaned_messages > 0 {
            return Err(crate::error::Error::SchemaValidation {
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_MESSAGE_WORKERS".into(),
                source: e,
                context: "Failed to check message worker referential integrity".into(),
            })?;

        if orphaned_message_workers > 0 {
            return Err(crate::error::Error::SchemaValidation {
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_ARCHIVE_QUEUES".into(),
                source: e,
                context: "Failed to check archive queue referential integrity".into(),
            })?;

        if orphaned_archive_queues > 0 {
            return Err(crate::error::Error::SchemaValidation {
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
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "CHECK_ORPHANED_ARCHIVE_WORKERS".into(),
                source: e,
                context: "Failed to check archive worker referential integrity".into(),
            })?;

        if orphaned_archive_workers > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Found {} archived messages with invalid producer_worker_id or consumer_worker_id references",
                    orphaned_archive_workers
                ),
            });
        }

        // Commit the transaction (ensures consistency of all checks)
        tx.commit()
            .await
            .map_err(|e| crate::error::Error::TransactionFailed {
                source: e,
                context: "Failed to commit verification transaction".into(),
            })?;

        Ok(())
    }

    async fn reclaim_messages(
        &self,
        queue_id: i64,
        older_than: Option<chrono::Duration>,
    ) -> Result<u64> {
        let timeout = older_than
            .unwrap_or_else(|| chrono::Duration::seconds(self.config.heartbeat_interval as i64));

        // Start a transaction for the entire reclamation process
        let mut tx =
            self.pool
                .begin()
                .await
                .map_err(|e| crate::error::Error::TransactionFailed {
                    source: e,
                    context: "Failed to begin transaction for message reclamation".into(),
                })?;

        // 1. Find zombie workers using the transaction
        let zombies = self
            .workers
            .list_zombies_for_queue_tx(queue_id, timeout, &mut tx)
            .await?;

        if zombies.is_empty() {
            tx.commit()
                .await
                .map_err(|e| crate::error::Error::TransactionFailed {
                    source: e,
                    context: "Failed to commit empty transaction during message reclamation".into(),
                })?;
            return Ok(0);
        }

        let mut total_released = 0;

        // Process each zombie
        for zombie in zombies {
            tracing::info!(
                "Reclaiming messages from zombie worker {} (last heartbeat: {:?})",
                zombie.id,
                zombie.heartbeat_at
            );

            // 2. Release messages
            let result = sqlx::query(RELEASE_ZOMBIE_MESSAGES)
                .bind(zombie.id)
                .execute(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "RELEASE_ZOMBIE_MESSAGES".into(),
                    source: e,
                    context: format!("Failed to release messages for zombie worker {}", zombie.id),
                })?;

            let released = result.rows_affected();
            total_released += released;

            // 3. Mark worker as stopped
            sqlx::query(SHUTDOWN_ZOMBIE_WORKER)
                .bind(zombie.id)
                .execute(&mut *tx)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "SHUTDOWN_ZOMBIE_WORKER".into(),
                    source: e,
                    context: format!("Failed to stop zombie worker {}", zombie.id),
                })?;

            if released > 0 {
                tracing::info!(
                    "Released {} messages from zombie worker {}",
                    released,
                    zombie.id
                );
            }
        }

        tx.commit()
            .await
            .map_err(|e| crate::error::Error::TransactionFailed {
                source: e,
                context: "Failed to commit zombie cleanup".into(),
            })?;

        Ok(total_released)
    }

    async fn worker_stats(&self, queue_name: &str) -> Result<crate::types::WorkerStats> {
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
            .unwrap_or(chrono::Duration::zero());

        let newest_heartbeat_age = workers
            .iter()
            .map(|w| now.signed_duration_since(w.heartbeat_at))
            .min()
            .unwrap_or(chrono::Duration::zero());

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

    async fn purge_old_workers(&self, older_than: chrono::Duration) -> Result<u64> {
        let threshold = chrono::Utc::now() - older_than;

        let result = sqlx::query(PURGE_OLD_WORKERS)
            .bind(threshold)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "PURGE_OLD_WORKERS".into(),
                source: e,
                context: "Failed to purge old workers".into(),
            })?;

        Ok(result.rows_affected())
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
    /// Returns `crate::error::Error::InvalidWorkerType` if called on an admin worker
    /// Returns `crate::error::Error` if database operations fail
    async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        let result = sqlx::query(RELEASE_WORKER_MESSAGES)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_WORKER_MESSAGES".into(),
                source: e,
                context: format!("Failed to release messages for worker {}", worker_id),
            })?;
        Ok(result.rows_affected())
    }

    /// Register this Admin instance as a worker.
    async fn register(&mut self, hostname: String, port: i32) -> Result<WorkerInfo> {
        if let Some(ref worker_info) = self.worker_info {
            return Ok(worker_info.clone());
        }

        // Use WorkerTable::register (via Workers struct methods) to handle state machine
        let worker_info = self.workers.register(None, &hostname, port).await?;

        tracing::debug!(
            "Registered Admin as worker {} (hostname: {}, port: {})",
            worker_info.id,
            hostname,
            port
        );

        self.worker_info = Some(worker_info.clone());
        Ok(worker_info)
    }

    /// Create a new queue in the database.
    async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
        // Create new queue data
        use crate::types::NewQueue;
        let new_queue = NewQueue {
            queue_name: name.to_string(),
        };

        // Use the queues table insert function
        self.queues.insert(new_queue).await
    }

    /// Get a [`QueueInfo`] instance for a given queue name.
    async fn get_queue(&self, name: &str) -> Result<QueueInfo> {
        // Get queue info to get the queue_id
        self.queues.get_by_name(name).await
    }

    /// Delete a queue from the database.
    async fn delete_queue(&self, queue_info: &QueueInfo) -> Result<()> {
        // Start a transaction for atomic deletion with row locking
        let mut tx =
            self.pool
                .begin()
                .await
                .map_err(|e| crate::error::Error::ConnectionFailed {
                    source: e,
                    context: "Failed to begin transaction".into(),
                })?;

        // Lock the queue row for exclusive access and get queue_id
        let queue_id: Option<i64> = sqlx::query_scalar(LOCK_QUEUE_FOR_DELETE)
            .bind(&queue_info.queue_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LOCK_QUEUE_FOR_DELETE".into(),
                source: e,
                context: format!("Failed to lock queue '{}'", queue_info.queue_name),
            })?;

        let queue_id = queue_id.ok_or_else(|| crate::error::Error::QueueNotFound {
            name: queue_info.queue_name.clone(),
        })?;

        // Check for active workers assigned to this queue
        let ready_workers = self
            .workers
            .count_for_queue(queue_info.id, WorkerStatus::Ready)
            .await?;
        let suspended_workers = self
            .workers
            .count_for_queue(queue_info.id, WorkerStatus::Suspended)
            .await?;
        let active_workers = ready_workers + suspended_workers;

        if active_workers > 0 {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
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
            return Err(crate::error::Error::ValidationFailed {
                reason: format!(
                    "Cannot delete queue '{}': {} references exist in messages/archive tables. Purge data first.",
                    queue_info.queue_name, total_references
                ),
            });
        }

        // Safe to delete - no references exist and we have exclusive lock
        Queues::delete_by_name_tx(&queue_info.queue_name, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| crate::error::Error::TransactionFailed {
                source: e,
                context: "Failed to commit queue deletion transaction".into(),
            })?;

        tracing::debug!("Successfully deleted queue '{}'", queue_info.queue_name);
        Ok(())
    }

    /// Purge all messages from a queue, but keep the queue itself.
    async fn purge_queue(&self, name: &str) -> Result<()> {
        // Start a transaction for atomic purge with row locking
        let mut tx =
            self.pool
                .begin()
                .await
                .map_err(|e| crate::error::Error::TransactionFailed {
                    source: e,
                    context: "Failed to begin transaction for queue purge".into(),
                })?;

        // Lock the queue row for data modification and get queue_id
        let queue_id: Option<i64> = sqlx::query_scalar(LOCK_QUEUE_FOR_UPDATE)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LOCK_QUEUE_FOR_UPDATE".into(),
                source: e,
                context: format!("Failed to lock queue '{}'", name),
            })?;

        let queue_id = queue_id.ok_or_else(|| crate::error::Error::QueueNotFound {
            name: name.to_string(),
        })?;

        self.messages.delete_by_fk(queue_id, &mut tx).await?;
        self.archive.delete_by_fk(queue_id, &mut tx).await?;
        self.workers.delete_by_fk(queue_id, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| crate::error::Error::TransactionFailed {
                source: e,
                context: "Failed to commit queue purge transaction".into(),
            })?;

        tracing::debug!("Successfully purged queue '{}'", name);
        Ok(())
    }

    async fn dlq(&self) -> Result<Vec<i64>> {
        let result: Vec<i64> = sqlx::query_scalar(DLQ_BATCH)
            .bind(self.config.max_read_ct)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DLQ_BATCH".into(),
                source: e,
                context: "Failed to move messages to DLQ".into(),
            })?;

        Ok(result)
    }

    /// Get metrics for a specific queue.
    async fn queue_metrics(&self, name: &str) -> Result<QueueMetrics> {
        let queue_info = self.queues.get_by_name(name).await?;

        let metrics = sqlx::query_as::<_, QueueMetrics>(GET_QUEUE_METRICS)
            .bind(queue_info.id)
            .bind(name) // Pass name to be returned in the struct
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_QUEUE_METRICS".into(),
                source: e,
                context: format!("Failed to get metrics for queue {}", name),
            })?;

        Ok(metrics)
    }

    /// Get metrics for all queues managed by pgqrs.
    async fn all_queues_metrics(&self) -> Result<Vec<QueueMetrics>> {
        let metrics = sqlx::query_as::<_, QueueMetrics>(GET_ALL_QUEUES_METRICS)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_ALL_QUEUES_METRICS".into(),
                source: e,
                context: "Failed to get all queues metrics".into(),
            })?;

        Ok(metrics)
    }

    /// Get system-wide statistics.
    async fn system_stats(&self) -> Result<SystemStats> {
        let stats = sqlx::query_as::<_, SystemStats>(GET_SYSTEM_STATS)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_SYSTEM_STATS".into(),
                source: e,
                context: "Failed to get system stats".into(),
            })?;

        Ok(stats)
    }

    async fn worker_health_stats(
        &self,
        heartbeat_timeout: chrono::Duration,
        group_by_queue: bool,
    ) -> Result<Vec<WorkerHealthStats>> {
        // Convert heartbeat_timeout to a string for interval (e.g., '60 seconds')
        let interval_str = format!("{} seconds", heartbeat_timeout.num_seconds());

        let stats = if group_by_queue {
            sqlx::query_as::<_, WorkerHealthStats>(GET_WORKER_HEALTH_BY_QUEUE)
                .bind(interval_str)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, WorkerHealthStats>(GET_WORKER_HEALTH_GLOBAL)
                .bind(interval_str)
                .fetch_all(&self.pool)
                .await
        };

        stats.map_err(|e| crate::error::Error::QueryFailed {
            query: "GET_WORKER_HEALTH".into(),
            source: e,
            context: "Failed to get worker health stats".into(),
        })
    }

    async fn delete_worker(&self, worker_id: i64) -> Result<u64> {
        // First check if worker has any references
        let reference_count = self.check_worker_references(worker_id).await?;

        if reference_count > 0 {
            return Err(crate::error::Error::SchemaValidation {
                message: format!(
                    "Cannot delete worker {}: worker has {} associated messages/archives. Purge messages first.",
                    worker_id, reference_count
                ),
            });
        }

        self.workers.delete(worker_id).await
    }

    async fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        // Delegate to the workers table
        self.workers.list().await
    }

    async fn get_worker_messages(&self, worker_id: i64) -> Result<Vec<QueueMessage>> {
        // First validate the worker exists
        let worker = self.workers.get(worker_id).await?;
        if worker.queue_id.is_none() {
            return Err(crate::error::Error::ValidationFailed {
                reason: "Cannot get messages for admin worker".to_string(),
            });
        }

        // Get all messages held by this worker (locked by consumer_worker_id)
        let messages = sqlx::query_as::<_, QueueMessage>(
            "SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
             FROM pgqrs_messages
             WHERE consumer_worker_id = $1
             ORDER BY id"
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::error::Error::QueryFailed {
            query: "GET_WORKER_MESSAGES_RAW".into(),
            source: e,
            context: format!("Failed to get messages for worker {}", worker_id),
        })?;

        Ok(messages)
    }
}
