pub const QUEUE_PREFIX: &str = r#"q"#;
pub const PGQRS_SCHEMA: &str = "pgqrs";
pub const VISIBILITY_TIMEOUT: i32 = 5;
pub const CREATE_QUEUE_STATEMENT: &str = r#"
    CREATE {UNLOGGED} TABLE IF NOT EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
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
    SELECT msg_id, read_ct, enqueued_at, vt, message
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
