DO $$
BEGIN
	CREATE TYPE worker_status AS ENUM ('ready', 'shutting_down', 'stopped');
EXCEPTION
	WHEN duplicate_object THEN null;
END $$;