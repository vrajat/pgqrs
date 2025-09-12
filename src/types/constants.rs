pub const QUEUE_PREFIX: &str = r#"q"#;
pub const PGQRS_SCHEMA: &str = "pgqrs";
pub const VISIBILITY_TIMEOUT: i32 = 5;
pub const CREATE_QUEUE_STATEMENT: &str = r#"
    CREATE TABLE IF NOT EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (
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
        INSERT INTO {PGQRS_SCHEMA}.meta (queue_name)
        VALUES ('{name}')
        ON CONFLICT
        DO NOTHING;
"#;

pub const DELETE_QUEUE_METADATA: &str = r#"
        DELETE FROM {PGQRS_SCHEMA}.meta
        WHERE queue_name = '{name}';
        "#;

pub const PURGE_QUEUE_STATEMENT: &str = r#"
    DELETE FROM {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name};
"#;
