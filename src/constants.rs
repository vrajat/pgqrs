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

pub const CREATE_QUEUE_INFO_TABLE_STATEMENT: &str = r#"
    CREATE TABLE IF NOT EXISTS pgqrs_queues (
        id BIGSERIAL PRIMARY KEY,
        queue_name VARCHAR UNIQUE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
    );
"#;

pub const LIST_QUEUE_INFO: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues;
"#;

// Parameterized SQL for unified pgqrs_messages table operations

/// Insert message into unified messages table
pub const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id;
"#;

/// Select message by ID from unified messages table
pub const SELECT_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, worker_id, payload, vt, enqueued_at, read_ct, dequeued_at
    FROM pgqrs_messages
    WHERE id = $1
"#;

/// Read available messages from queue (with SKIP LOCKED)
/// Select messages for worker with lock
pub const DEQUEUE_MESSAGES: &str = r#"
    UPDATE pgqrs_messages t 
    SET worker_id = $4, vt = NOW() + make_interval(secs => $3::double precision), read_ct = read_ct + 1, dequeued_at = NOW()
    FROM (
        SELECT id 
        FROM pgqrs_messages 
        WHERE queue_id = $1 AND (vt IS NULL OR vt <= NOW()) AND worker_id IS NULL 
        ORDER BY id ASC
        LIMIT $2 
        FOR UPDATE SKIP LOCKED
    ) selected 
    WHERE t.id = selected.id
    RETURNING t.id, t.queue_id, t.worker_id, t.payload, t.vt, t.enqueued_at, t.read_ct, t.dequeued_at;
"#;

/// Get count of pending messages in queue
pub const PENDING_COUNT: &str = r#"
    SELECT COUNT(*) AS count
    FROM pgqrs_messages
    WHERE queue_id = $1 AND vt <= $2;
"#;

/// Update message visibility timeout (extend lock)
pub const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages 
    SET vt = vt + make_interval(secs => $1::double precision)
    WHERE id = $2
    RETURNING id, queue_id, worker_id, payload, vt, enqueued_at, read_ct, dequeued_at;
"#;

/// Delete batch of messages
pub const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ANY($1)
    RETURNING id, queue_id, worker_id, payload, vt, enqueued_at, read_ct;
"#;

/// Purge all messages from a specific queue
pub const PURGE_QUEUE_MESSAGES_UNIFIED: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE queue_id = $1;
"#;

// Parameterized SQL for unified pgqrs_archive table operations

/// Archive single message (atomic operation)
pub const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM pgqrs_messages
        WHERE id = $1
        RETURNING id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msg
    RETURNING id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at;
"#;

/// Archive batch of messages (efficient batch operation)
pub const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM pgqrs_messages
        WHERE id = ANY($1)
        RETURNING id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    )
    INSERT INTO pgqrs_archive
        (original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at)
    SELECT
        id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, dequeued_at
    FROM archived_msgs
    RETURNING original_msg_id;
"#;

/// Select single archived message by original message ID
pub const ARCHIVE_SELECT_BY_ID: &str = r#"
    SELECT id, original_msg_id, queue_id, worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE original_msg_id = $1
"#;

// Worker operations with unified tables

/// Get messages assigned to a specific worker
pub const GET_WORKER_MESSAGES: &str = r#"
    SELECT id, queue_id, worker_id, payload, vt, enqueued_at, read_ct
    FROM pgqrs_messages
    WHERE worker_id = $1
    ORDER BY id;
"#;

/// Release messages assigned to a worker (set worker_id to NULL and reset vt)
pub const RELEASE_WORKER_MESSAGES: &str = r#"
    UPDATE pgqrs_messages
    SET vt = NOW(), worker_id = NULL
    WHERE worker_id = $1;
"#;

pub const INSERT_QUEUE_METADATA: &str = r#"
    INSERT INTO pgqrs_queues (queue_name)
    VALUES ($1)
    RETURNING id;
"#;

pub const GET_QUEUE_INFO_BY_NAME: &str = r#"
    SELECT id, queue_name, created_at
    FROM pgqrs_queues
    WHERE queue_name = $1;
"#;

pub const DELETE_QUEUE_METADATA: &str = r#"
        DELETE FROM pgqrs_queues
        WHERE queue_name = $1;
"#;

/// Lock queue row for exclusive access during deletion
pub const LOCK_QUEUE_FOR_DELETE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR UPDATE;
"#;

/// Check referential integrity for queue deletion (single query)
pub const CHECK_QUEUE_REFERENCES: &str = r#"
    SELECT
        (SELECT COUNT(*) FROM pgqrs_messages WHERE queue_id = $1) +
        (SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = $1) as total_references;
"#;

/// Lock queue row for data modification operations (purge, etc.)
pub const LOCK_QUEUE_FOR_UPDATE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR NO KEY UPDATE;
"#;

/// Lock queue row for reading during worker registration
pub const LOCK_QUEUE_FOR_KEY_SHARE: &str = r#"
    SELECT id FROM pgqrs_queues
    WHERE queue_name = $1
    FOR KEY SHARE;
"#;

/// Create index on worker table for efficient worker lookups
pub const CREATE_WORKERS_INDEX_QUEUE_STATUS: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_workers_queue_status
    ON pgqrs_workers(queue_name, status);
"#;

/// Create index on worker table for heartbeat monitoring
pub const CREATE_WORKERS_INDEX_HEARTBEAT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_workers_heartbeat
    ON pgqrs_workers(heartbeat_at);
"#;

// Worker Management SQL Templates

/// Create worker status enum type
pub const CREATE_WORKER_STATUS_ENUM: &str = r#"
    DO $$
    BEGIN
        CREATE TYPE worker_status AS ENUM ('ready', 'shutting_down', 'stopped');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$;
"#;

/// Create workers table
pub const CREATE_WORKERS_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS pgqrs_workers (
        id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        hostname TEXT NOT NULL,
        port INTEGER NOT NULL,
        queue_name TEXT NOT NULL,
        started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        heartbeat_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        shutdown_at TIMESTAMP WITH TIME ZONE,
        status worker_status NOT NULL DEFAULT 'ready'::worker_status,

        UNIQUE(hostname, port)
    );
"#;

/// Create unified messages table
pub const CREATE_PGQRS_MESSAGES_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS pgqrs_messages (
        id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        queue_id BIGINT NOT NULL,
        worker_id BIGINT,
        payload JSONB NOT NULL,
        vt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        read_ct INT DEFAULT 0,
        dequeued_at TIMESTAMP WITH TIME ZONE,

        CONSTRAINT fk_messages_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
        CONSTRAINT fk_messages_worker_id FOREIGN KEY (worker_id) REFERENCES pgqrs_workers(id)
    );
"#;

/// Create unified archive table
pub const CREATE_PGQRS_ARCHIVE_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS pgqrs_archive (
        id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        original_msg_id BIGINT NOT NULL,
        queue_id BIGINT NOT NULL,
        worker_id BIGINT,
        payload JSONB NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        read_ct INT NOT NULL,
        archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        dequeued_at TIMESTAMP WITH TIME ZONE,

        CONSTRAINT fk_archive_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
        CONSTRAINT fk_archive_worker_id FOREIGN KEY (worker_id) REFERENCES pgqrs_workers(id)
    );
"#;

/// Create indexes for unified messages table
pub const CREATE_PGQRS_MESSAGES_INDEX_QUEUE_VT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_messages_queue_vt ON pgqrs_messages (queue_id, vt);
"#;

pub const CREATE_PGQRS_MESSAGES_INDEX_WORKER_ID: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_messages_worker_id ON pgqrs_messages (worker_id);
"#;

/// Create indexes for unified archive table
pub const CREATE_PGQRS_ARCHIVE_INDEX_QUEUE_ID: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_archive_queue_id ON pgqrs_archive (queue_id);
"#;

pub const CREATE_PGQRS_ARCHIVE_INDEX_ARCHIVED_AT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_archive_archived_at ON pgqrs_archive (archived_at);
"#;

pub const CREATE_PGQRS_ARCHIVE_INDEX_ORIGINAL_MSG_ID: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_archive_original_msg_id ON pgqrs_archive (original_msg_id);
"#;

/// Insert new worker registration
pub const INSERT_WORKER: &str = r#"
    INSERT INTO pgqrs_workers (hostname, port, queue_name, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id
"#;

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

/// List workers for a specific queue
pub const LIST_QUEUE_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE queue_name = $1
    ORDER BY started_at DESC
"#;

/// List all workers in the system
pub const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    ORDER BY started_at DESC
"#;

/// Get a specific worker by ID
pub const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_name, started_at, heartbeat_at, shutdown_at, status
    FROM pgqrs_workers
    WHERE id = $1
"#;

/// Check if worker has any associated messages or archives
pub const CHECK_WORKER_REFERENCES: &str = r#"
    SELECT COUNT(*) as total_references FROM (
        SELECT 1 FROM pgqrs_messages WHERE worker_id = $1
        UNION ALL
        SELECT 1 FROM pgqrs_archive WHERE worker_id = $1
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
              SELECT worker_id FROM pgqrs_messages WHERE worker_id IS NOT NULL
              UNION
              SELECT worker_id FROM pgqrs_archive WHERE worker_id IS NOT NULL
          ) refs
      )
"#;

/// Drop the queue repository table
pub const DROP_QUEUE_REPOSITORY: &str = r#"
    DROP TABLE IF EXISTS pgqrs_queues CASCADE;
"#;

/// Drop the worker repository table
pub const DROP_WORKER_REPOSITORY: &str = r#"
    DROP TABLE IF EXISTS pgqrs_workers CASCADE;
"#;

/// Drop the unified messages table
pub const DROP_PGQRS_MESSAGES_TABLE: &str = r#"
    DROP TABLE IF EXISTS pgqrs_messages CASCADE;
"#;

/// Drop the unified archive table
pub const DROP_PGQRS_ARCHIVE_TABLE: &str = r#"
    DROP TABLE IF EXISTS pgqrs_archive CASCADE;
"#;

/// Drop the worker status enum type
pub const DROP_WORKER_STATUS_ENUM: &str = r#"
    DROP TYPE IF EXISTS worker_status CASCADE;
"#;
