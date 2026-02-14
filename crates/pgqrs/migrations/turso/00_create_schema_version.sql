CREATE TABLE IF NOT EXISTS pgqrs_schema_version (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
);

INSERT OR IGNORE INTO pgqrs_schema_version (version, applied_at, description)
VALUES ('0.5.0', datetime('now'), 'Initial schema');
