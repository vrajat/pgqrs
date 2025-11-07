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

/// Prefix for queue table names in the database
pub const QUEUE_PREFIX: &str = r#"q"#;
/// Name of the pgqrs schema in PostgreSQL
pub const PGQRS_SCHEMA: &str = "pgqrs";
/// Default visibility timeout in seconds for locked messages
pub const VISIBILITY_TIMEOUT: i32 = 5;

pub const CREATE_SCHEMA_STATEMENT: &str = r#"CREATE SCHEMA IF NOT EXISTS {PGQRS_SCHEMA};"#;

pub const CREATE_META_TABLE_STATEMENT: &str = r#"
    CREATE TABLE IF NOT EXISTS {PGQRS_SCHEMA}.meta (
        queue_name VARCHAR UNIQUE NOT NULL PRIMARY KEY,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        unlogged BOOLEAN DEFAULT FALSE
    );
"#;
pub const CREATE_QUEUE_STATEMENT: &str = r#"
    CREATE {UNLOGGED} TABLE IF NOT EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        worker_id BIGINT REFERENCES {PGQRS_SCHEMA}.pgqrs_workers(id)
    );"#;

pub const DROP_QUEUE_STATEMENT: &str = r#"
    DROP TABLE IF EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} CASCADE;
"#;

pub const INSERT_QUEUE_METADATA: &str = r#"
    INSERT INTO {PGQRS_SCHEMA}.meta (queue_name, unlogged)
    VALUES ('{name}', {unlogged})
    ON CONFLICT (queue_name)
    DO UPDATE SET unlogged = EXCLUDED.unlogged;
"#;

pub const DELETE_QUEUE_METADATA: &str = r#"
        DELETE FROM {PGQRS_SCHEMA}.meta
        WHERE queue_name = '{name}';
"#;

pub const PURGE_QUEUE_STATEMENT: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name};
"#;

pub const INSERT_MESSAGE: &str = r#"
    INSERT INTO {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (read_ct, enqueued_at, vt, message)
    VALUES ($1, $2, $3, $4)
    RETURNING msg_id;
"#;

pub const SELECT_MESSAGE_BY_ID: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message, worker_id
    FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE msg_id = $1;
"#;

pub const READ_MESSAGES: &str = r#"
    WITH cte AS
        (
            SELECT msg_id
            FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT {limit}
            FOR UPDATE SKIP LOCKED
        )
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} t
    SET
        vt = clock_timestamp() + interval '{vt} seconds',
        read_ct = read_ct + 1
    FROM cte
    WHERE t.msg_id=cte.msg_id
    RETURNING *;
"#;

pub const PENDING_COUNT: &str = r#"
    SELECT COUNT(*) AS count
    FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE vt <= $1;
"#;

pub const DEQUEUE_MESSAGE: &str = r#"
    DELETE from {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE msg_id = $1
    RETURNING *;
"#;

pub const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    SET vt = vt + interval '$1 seconds'
    WHERE msg_id = $2
    RETURNING *;
"#;

pub const DELETE_MESSAGE_BATCH: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE msg_id = ANY($1)
    RETURNING *;
"#;

pub const SCHEMA_EXISTS_QUERY: &str = r#"
    SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{PGQRS_SCHEMA}') AS exists;
"#;

pub const UNINSTALL_STATEMENT: &str = r#"
    DROP SCHEMA IF EXISTS {PGQRS_SCHEMA} CASCADE;
"#;

/// Create archive table for queue
pub const CREATE_ARCHIVE_TABLE: &str = r#"
    CREATE UNLOGGED TABLE IF NOT EXISTS {PGQRS_SCHEMA}.archive_{queue_name} (
        msg_id BIGINT NOT NULL,
        message JSONB NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        read_ct INTEGER NOT NULL,

        -- Archive-specific tracking columns
        archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        processing_duration BIGINT,

        PRIMARY KEY (msg_id)
    )
"#;

/// Create indexes for archive table
pub const CREATE_ARCHIVE_INDEX_ARCHIVED_AT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_archive_{queue_name}_archived_at ON {PGQRS_SCHEMA}.archive_{queue_name}(archived_at)
"#;

pub const CREATE_ARCHIVE_INDEX_ENQUEUED_AT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_archive_{queue_name}_enqueued_at ON {PGQRS_SCHEMA}.archive_{queue_name}(enqueued_at)
"#;

/// Archive single message (atomic operation)
pub const ARCHIVE_MESSAGE: &str = r#"
    WITH archived_msg AS (
        DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
        WHERE msg_id = $1
        RETURNING msg_id, message, enqueued_at, vt, read_ct
    )
    INSERT INTO {PGQRS_SCHEMA}.archive_{queue_name}
        (msg_id, message, enqueued_at, vt, read_ct, processing_duration)
    SELECT
        msg_id, message, enqueued_at, vt, read_ct,
        EXTRACT(EPOCH FROM (NOW() - enqueued_at)) * 1000 as processing_duration
    FROM archived_msg
    RETURNING (msg_id IS NOT NULL);
"#;

/// Archive batch messages (efficient batch operation)
pub const ARCHIVE_BATCH: &str = r#"
    WITH archived_msgs AS (
        DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
        WHERE msg_id = ANY($1)
        RETURNING msg_id, message, enqueued_at, vt, read_ct
    )
    INSERT INTO {PGQRS_SCHEMA}.archive_{queue_name}
        (msg_id, message, enqueued_at, vt, read_ct, processing_duration)
    SELECT
        msg_id, message, enqueued_at, vt, read_ct,
        EXTRACT(EPOCH FROM (NOW() - enqueued_at)) * 1000 as processing_duration
    FROM archived_msgs
    RETURNING msg_id;
"#;

/// Select messages from archive table
pub const ARCHIVE_LIST: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message, archived_at, processing_duration
    FROM {PGQRS_SCHEMA}.archive_{queue_name}
    ORDER BY archived_at DESC
    LIMIT $1 OFFSET $2;
"#;

/// Select single message from archive table by ID
pub const ARCHIVE_SELECT_BY_ID: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message, archived_at, processing_duration
    FROM {PGQRS_SCHEMA}.archive_{queue_name}
    WHERE msg_id = $1
    ORDER BY archived_at DESC
    LIMIT 1;
"#;

/// Drop the archive table for a queue
pub const DROP_ARCHIVE_TABLE: &str = r#"
    DROP TABLE IF EXISTS {PGQRS_SCHEMA}.archive_{queue_name} CASCADE;
"#;

/// Purge all messages from archive table
pub const PURGE_ARCHIVE_TABLE: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.archive_{queue_name};
"#;

/// Create index on worker table for efficient worker lookups
pub const CREATE_WORKERS_INDEX_QUEUE_STATUS: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_workers_queue_status
    ON {PGQRS_SCHEMA}.pgqrs_workers(queue_id, status);
"#;

/// Create index on worker table for heartbeat monitoring
pub const CREATE_WORKERS_INDEX_HEARTBEAT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_pgqrs_workers_heartbeat
    ON {PGQRS_SCHEMA}.pgqrs_workers(heartbeat_at);
"#;

// Worker Management SQL Templates

/// Create workers table
pub const CREATE_WORKERS_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS {PGQRS_SCHEMA}.pgqrs_workers (
        id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        hostname TEXT NOT NULL,
        port INTEGER NOT NULL,
        queue_id TEXT NOT NULL,
        started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        heartbeat_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        shutdown_at TIMESTAMP WITH TIME ZONE,
        status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'shutting_down', 'stopped')),

        UNIQUE(hostname, port)
    )
"#;

/// Insert new worker registration
pub const INSERT_WORKER: &str = r#"
    INSERT INTO {PGQRS_SCHEMA}.pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id
"#;

/// Update worker heartbeat timestamp
pub const UPDATE_WORKER_HEARTBEAT: &str = r#"
    UPDATE {PGQRS_SCHEMA}.pgqrs_workers
    SET heartbeat_at = $1
    WHERE id = $2
"#;

/// Update worker status to shutting down
pub const UPDATE_WORKER_SHUTDOWN: &str = r#"
    UPDATE {PGQRS_SCHEMA}.pgqrs_workers
    SET status = 'shutting_down', shutdown_at = $1
    WHERE id = $2
"#;

/// Update worker status to stopped
pub const UPDATE_WORKER_STOPPED: &str = r#"
    UPDATE {PGQRS_SCHEMA}.pgqrs_workers
    SET status = 'stopped'
    WHERE id = $1
"#;

/// List workers for a specific queue
pub const LIST_QUEUE_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM {PGQRS_SCHEMA}.pgqrs_workers
    WHERE queue_id = $1
    ORDER BY started_at DESC
"#;

/// List all workers in the system
pub const LIST_ALL_WORKERS: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM {PGQRS_SCHEMA}.pgqrs_workers
    ORDER BY started_at DESC
"#;

/// Get a specific worker by ID
pub const GET_WORKER_BY_ID: &str = r#"
    SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
    FROM {PGQRS_SCHEMA}.pgqrs_workers
    WHERE id = $1
"#;

/// Delete old stopped workers
pub const PURGE_OLD_WORKERS: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.pgqrs_workers
    WHERE status = 'stopped' AND heartbeat_at < $1
"#;

/// Read messages with worker assignment (with worker_id)
pub const READ_MESSAGES_WITH_WORKER: &str = r#"
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    SET vt = $1, read_ct = read_ct + 1, worker_id = $3
    WHERE msg_id IN (
        SELECT msg_id FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
        WHERE vt < NOW()
        ORDER BY msg_id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
    RETURNING msg_id, read_ct, enqueued_at, vt, message, worker_id
"#;

/// Read messages without worker assignment (without worker_id)
pub const READ_MESSAGES_WITHOUT_WORKER: &str = r#"
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    SET vt = $1, read_ct = read_ct + 1
    WHERE msg_id IN (
        SELECT msg_id FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
        WHERE vt < NOW()
        ORDER BY msg_id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
    RETURNING msg_id, read_ct, enqueued_at, vt, message, worker_id
"#;

/// Release messages assigned to a worker
pub const RELEASE_WORKER_MESSAGES: &str = r#"
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    SET vt = NOW(), worker_id = NULL
    WHERE worker_id = $1
"#;

/// Get messages assigned to a specific worker
pub const GET_WORKER_MESSAGES: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message, worker_id FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE worker_id = $1
    ORDER BY msg_id
"#;
