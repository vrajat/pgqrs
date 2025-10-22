pub const PGQRS_SCHEMA: &str = "pgqrs";
pub const QUEUE_PREFIX: &str = "q";
pub const PENDING_COUNT: &str = r#"
    SELECT COUNT(*) as count FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE vt <= $1;
"#;

pub const CREATE_QUEUE_STATEMENT: &str = r#"
    CREATE {UNLOGGED} TABLE IF NOT EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (
        msg_id BIGSERIAL PRIMARY KEY,
        read_ct INTEGER NOT NULL DEFAULT 0,
        enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        vt TIMESTAMPTZ NOT NULL,
        message JSONB NOT NULL
    );
"#;

pub const INSERT_QUEUE_METADATA: &str = r#"
    INSERT INTO {PGQRS_SCHEMA}.meta (queue_name, unlogged)
    VALUES ($1, $2)
    ON CONFLICT (queue_name) DO UPDATE SET unlogged = EXCLUDED.unlogged;
"#;

pub const DROP_QUEUE_STATEMENT: &str = r#"
    DROP TABLE IF EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} CASCADE;
"#;

pub const DELETE_QUEUE_METADATA: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.meta WHERE queue_name = $1;
"#;

pub const PURGE_QUEUE_STATEMENT: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name};
"#;

pub const SELECT_QUEUE_META: &str = r#"
    SELECT queue_name, created_at, unlogged FROM {PGQRS_SCHEMA}.meta WHERE queue_name = $1;
"#;

pub const LIST_QUEUES_META: &str = r#"
    SELECT queue_name, created_at, unlogged FROM {PGQRS_SCHEMA}.meta ORDER BY created_at ASC;
"#;

pub const CREATE_INDEX_STATEMENT: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_{queue_name}_vt ON {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (vt);
"#;

pub const INSERT_MESSAGE: &str = r#"
    INSERT INTO {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (read_ct, enqueued_at, vt, message)
    VALUES ($1, $2, $3, $4)
    RETURNING msg_id, read_ct, enqueued_at, vt, message;
"#;

pub const SELECT_MESSAGE_BY_ID: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE msg_id = $1;
"#;

pub const READ_MESSAGES: &str = r#"
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE vt <= $1
    ORDER BY msg_id ASC
    LIMIT $2;
"#;

pub const DELETE_MESSAGE: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    WHERE msg_id = $1
    RETURNING msg_id, read_ct, enqueued_at, vt, message;
"#;

pub const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
    SET vt = $1
    WHERE msg_id = $2
    RETURNING msg_id, read_ct, enqueued_at, vt, message;
"#;

pub const UNINSTALL_STATEMENT: &str = r#"
    DROP SCHEMA IF EXISTS {PGQRS_SCHEMA} CASCADE;
"#;
