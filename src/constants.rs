//! SQL constants and configuration values for pgqrs.
//!
//! This module contains all SQL statement templates, schema constants, and default values
//! used throughout the pgqrs system.
//!
//! ## What
//!
//! - Database schema and table names
//! - SQL statement templates for queue operations
//! - Default timeout and configuration values
//!
//! ## How
//!
//! These constants are used internally by the admin and queue modules to generate
//! dynamic SQL statements with proper schema and table names.

/// Default visibility timeout in seconds for locked messages
pub const VISIBILITY_TIMEOUT: u32 = 5;

// Parameterized SQL for unified pgqrs_messages table operations
// Note: Basic CRUD operations moved to tables/pgqrs_messages.rs

/// Get count of pending messages in queue
// Note: Moved to tables/pgqrs_messages.rs as count_pending()

/// Update message visibility timeout (extend lock)
// Note: Moved to tables/pgqrs_messages.rs as extend_visibility()

/// Delete batch of messages
pub const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1)
    RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct;
"#;

// Parameterized SQL for unified pgqrs_archive table operations

/// Archive single message (atomic operation)
pub const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_messages
        WHERE id = $1
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msg
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at;
"#;

/// Archive batch of messages (efficient batch operation)
pub const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE id = ANY($1)
        RETURNING id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

// Worker operations with unified tables

/// Get messages assigned to a specific worker
pub const GET_WORKER_MESSAGES: &str = r#"
    SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct
    FROM pgqrs_messages
    WHERE consumer_worker_id = $1
    ORDER BY id;
"#;

/// Release messages assigned to a worker (set worker_id to NULL and reset vt)
pub const RELEASE_WORKER_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NOW(), consumer_worker_id = NULL
    WHERE consumer_worker_id = $1;
"#;

/// Lock queue row for exclusive access during deletion
pub const LOCK_QUEUE_FOR_DELETE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR UPDATE;
"#;

/// Lock queue row for data modification operations (purge, etc.)
pub const LOCK_QUEUE_FOR_UPDATE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR NO KEY UPDATE;
"#;

// Worker Management SQL Templates

/// Update worker heartbeat timestamp
pub const UPDATE_WORKER_HEARTBEAT: &str = r#"
    UPDATE pgqrs_workers
    SET heartbeat_at = $1
    WHERE id = $2
"#;

/// Update worker status to shutting down
pub const UPDATE_WORKER_SHUTDOWN: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'shutting_down', shutdown_at = $1
    WHERE id = $2
"#;

/// Update worker status to stopped
pub const UPDATE_WORKER_STOPPED: &str = r#"
    UPDATE pgqrs_workers
    SET status = 'stopped'
    WHERE id = $1
"#;

/// Check if worker has any associated messages or archives
pub const CHECK_WORKER_REFERENCES: &str = r#"
    SELECT COUNT(*) as total_references FROM (
        SELECT 1 FROM pgqrs_messages WHERE producer_worker_id = $1 OR consumer_worker_id = $1
        UNION ALL
        SELECT 1 FROM pgqrs_archive WHERE producer_worker_id = $1 OR consumer_worker_id = $1
    ) refs
"#;

/// Delete old stopped workers (only those without references)
pub const PURGE_OLD_WORKERS: &str = r#"
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
              SELECT producer_worker_id FROM pgqrs_archive WHERE producer_worker_id IS NOT NULL
              UNION
              SELECT consumer_worker_id FROM pgqrs_archive WHERE consumer_worker_id IS NOT NULL
          ) refs
      )
"#;
