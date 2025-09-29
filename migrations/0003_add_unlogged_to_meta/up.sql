ALTER TABLE pgqrs.meta ADD COLUMN unlogged BOOLEAN DEFAULT FALSE;
-- Populate unlogged for existing queues (assume all are logged)
UPDATE pgqrs.meta SET unlogged = FALSE;