//! Postgres implementation of the Store trait.

use crate::store::{Store, QueueStore, MessageStore, WorkerStore, ArchiveStore};
use crate::error::Error;
use sqlx::PgPool;
use std::sync::Arc;

pub mod queues;
pub mod messages;
pub mod workers;
pub mod workflows;
pub mod archive;


#[derive(Clone, Debug)]
pub struct PostgresStore {
    queues: Arc<queues::PostgresQueueStore>,
    messages: Arc<messages::PostgresMessageStore>,
    workers: Arc<workers::PostgresWorkerStore>,
    archive: Arc<archive::PostgresArchiveStore>,
    workflows: Arc<workflows::PostgresWorkflowStore>,
    pool: PgPool,
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

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

impl PostgresStore {
    pub fn new(pool: PgPool, max_read_ct: u32) -> Self {
        Self {
            queues: Arc::new(queues::PostgresQueueStore::new(pool.clone())),
            messages: Arc::new(messages::PostgresMessageStore::new(pool.clone(), max_read_ct)),
            workers: Arc::new(workers::PostgresWorkerStore::new(pool.clone())),
            archive: Arc::new(archive::PostgresArchiveStore::new(pool.clone())),
            workflows: Arc::new(workflows::PostgresWorkflowStore::new(pool.clone())),
            pool,
        }
    }
}

#[async_trait::async_trait]
impl Store for PostgresStore {
    type Error = Error;

    type QueueStore = queues::PostgresQueueStore;
    type MessageStore = messages::PostgresMessageStore;
    type WorkerStore = workers::PostgresWorkerStore;
    type ArchiveStore = archive::PostgresArchiveStore;
    type WorkflowStore = workflows::PostgresWorkflowStore;

    async fn install(&self) -> Result<(), Self::Error> {
        // Run migrations using sqlx
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Connection {
                message: format!("Migration failed: {}", e),
            })?;
        Ok(())
    }

    async fn verify(&self) -> Result<(), Self::Error> {
        // Begin a read-only transaction for all verification queries
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Connection {
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
                .map_err(|e| Error::Connection {
                    message: format!("Failed to check {} existence: {}", description, e),
                })?;

            if !table_exists {
                return Err(Error::Connection {
                    message: format!("{} ('{}') does not exist", description, table_name),
                });
            }
        }

        // Category 2: Validate referential integrity using left outer joins

        // Check that all messages have valid queue_id references
        let orphaned_messages = sqlx::query_scalar::<_, i64>(CHECK_ORPHANED_MESSAGES)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to check message referential integrity: {}", e),
            })?;

        if orphaned_messages > 0 {
            return Err(Error::Connection {
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
            .map_err(|e| Error::Connection {
                message: format!(
                    "Failed to check message worker referential integrity: {}",
                    e
                ),
            })?;

        if orphaned_message_workers > 0 {
            return Err(Error::Connection {
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
            .map_err(|e| Error::Connection {
                message: format!("Failed to check archive referential integrity: {}", e),
            })?;

        if orphaned_archive_queues > 0 {
            return Err(Error::Connection {
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
            .map_err(|e| Error::Connection {
                message: format!(
                    "Failed to check archive worker referential integrity: {}",
                    e
                ),
            })?;

        if orphaned_archive_workers > 0 {
            return Err(Error::Connection {
                message: format!(
                    "Found {} archived messages with invalid producer_worker_id or consumer_worker_id references",
                    orphaned_archive_workers
                ),
            });
        }

        // Commit the transaction (ensures consistency of all checks)
        tx.commit()
            .await
            .map_err(|e| Error::Connection {
                message: format!("Failed to commit transaction: {}", e),
            })?;

        Ok(())
    }

    fn queues(&self) -> &Self::QueueStore {
        &self.queues
    }

    fn messages(&self) -> &Self::MessageStore {
        &self.messages
    }

    fn workers(&self) -> &Self::WorkerStore {
        &self.workers
    }

    fn archive(&self) -> &Self::ArchiveStore {
        &self.archive
    }

    fn workflows(&self) -> &Self::WorkflowStore {
        &self.workflows
    }
}
