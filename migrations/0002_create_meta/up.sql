-- Table where queues and metadata about them is stored
CREATE TABLE IF NOT EXISTS pgqrs.meta
(
    queue_name VARCHAR UNIQUE NOT NULL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);
