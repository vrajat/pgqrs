pub const PGQRS_SCHEMA: &str = "pgqrs";
pub const QUEUE_PREFIX: &str = "q";

pub const CREATE_QUEUE_STATEMENT: &str = r#"
    CREATE {UNLOGGED} TABLE IF NOT EXISTS {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (
        msg_id BIGSERIAL PRIMARY KEY,
        read_ct INTEGER NOT NULL DEFAULT 0,
        enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        vt TIMESTAMPTZ NOT NULL,
        message JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_{queue_name}_vt ON {PGQRS_SCHEMA}.{QUEUE_PREFIX}_{queue_name} (vt);
"#;
