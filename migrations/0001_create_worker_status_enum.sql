DO $$
BEGIN
	CREATE TYPE worker_status AS ENUM ('ready', 'suspended', 'stopped');
EXCEPTION
	WHEN duplicate_object THEN null;
END $$;