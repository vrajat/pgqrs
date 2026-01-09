/// Turso database schema definitions
pub const SCHEMA: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_queues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_workers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hostname TEXT NOT NULL,
        port INTEGER NOT NULL,
        queue_id INTEGER,
        started_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        shutdown_at TEXT,
        status TEXT NOT NULL,
        FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id) ON DELETE CASCADE
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_id INTEGER NOT NULL,
        payload TEXT NOT NULL,
        read_ct INTEGER NOT NULL DEFAULT 0,
        enqueued_at TEXT NOT NULL,
        vt TEXT,
        dequeued_at TEXT,
        producer_worker_id INTEGER,
        consumer_worker_id INTEGER,
        FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id) ON DELETE CASCADE
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_archive (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_msg_id INTEGER NOT NULL,
        queue_id INTEGER NOT NULL,
        producer_worker_id INTEGER,
        consumer_worker_id INTEGER,
        payload TEXT NOT NULL,
        enqueued_at TEXT NOT NULL,
        vt TEXT,
        read_ct INTEGER NOT NULL,
        dequeued_at TEXT,
        archived_at TEXT NOT NULL,
        FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id) ON DELETE CASCADE
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_workflows (
        workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        input TEXT,
        output TEXT,
        error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        executor_id TEXT
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
        workflow_id INTEGER NOT NULL,
        step_key TEXT NOT NULL,
        status TEXT NOT NULL,
        output TEXT,
        error TEXT,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        PRIMARY KEY (workflow_id, step_key),
        FOREIGN KEY (workflow_id) REFERENCES pgqrs_workflows(workflow_id) ON DELETE CASCADE
    );
    "#,
];
