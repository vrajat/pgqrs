-- Init DB: create three queue tables
\connect pgbench pgbench
-- Common queue schema
CREATE TABLE IF NOT EXISTS queue_template (
  msg_id bigserial primary key,
  payload jsonb not null,
  vt timestamptz not null,
  read_ct int not null default 0
);

-- Logged, naive (no skip locked)
DROP TABLE IF EXISTS queue_logged_naive;
CREATE TABLE queue_logged_naive (LIKE queue_template INCLUDING ALL);

-- Logged with skip locked
DROP TABLE IF EXISTS queue_logged_skiplocked;
CREATE TABLE queue_logged_skiplocked (LIKE queue_template INCLUDING ALL);

-- Unlogged with skip locked
DROP TABLE IF EXISTS queue_unlogged_skiplocked;
CREATE UNLOGGED TABLE queue_unlogged_skiplocked (LIKE queue_template INCLUDING ALL);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_logged_naive_vt_msgid ON queue_logged_naive (vt ASC, msg_id ASC);
CREATE INDEX IF NOT EXISTS idx_logged_skip_vt_msgid ON queue_logged_skiplocked (vt ASC, msg_id ASC);
CREATE INDEX IF NOT EXISTS idx_unlogged_skip_vt_msgid ON queue_unlogged_skiplocked (vt ASC, msg_id ASC);
