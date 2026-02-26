DO $$
BEGIN
	CREATE TYPE worker_status AS ENUM ('ready', 'polling', 'suspended', 'interrupted', 'stopped');
EXCEPTION
	WHEN duplicate_object THEN null;
END $$;