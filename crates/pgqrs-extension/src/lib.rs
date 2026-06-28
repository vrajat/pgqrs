use pgrx::prelude::*;

pub mod bgworker;

pub use bgworker::pgqrs_coordinator_main;

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    bgworker::init_gucs();

    if bgworker::COORDINATOR_ENABLED.get() {
        pgrx::bgworkers::BackgroundWorkerBuilder::new("pgqrs coordinator")
            .set_function("pgqrs_coordinator_main")
            .set_library("pgqrs_extension")
            .enable_spi_access()
            .set_start_time(pgrx::bgworkers::BgWorkerStartTime::RecoveryFinished)
            .load();
    }
}

/// Get the compiled version of pgqrs-extension
#[pg_extern]
fn pgqrs_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Simple health check function
#[pg_extern]
fn pgqrs_health_check() -> bool {
    true
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any extra postgresql.conf settings required by the tests
        vec![]
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pgqrs_version() {
        let version = crate::pgqrs_version();
        assert_eq!(version, "0.1.0");
    }

    #[pg_test]
    fn test_pgqrs_health_check() {
        assert!(crate::pgqrs_health_check());
    }

    fn setup_test_tables(client: &mut pgrx::spi::SpiClient<'_>) -> Result<(), pgrx::spi::Error> {
        let ddl = r#"
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'worker_status') THEN
                    CREATE TYPE worker_status AS ENUM ('ready', 'polling', 'suspended', 'interrupted', 'stopped');
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pgqrs_workflow_status') THEN
                    CREATE TYPE pgqrs_workflow_status AS ENUM ('QUEUED', 'RUNNING', 'SUCCESS', 'ERROR');
                END IF;
            END$$;

            CREATE TABLE IF NOT EXISTS pgqrs_workers (
                id BIGSERIAL PRIMARY KEY,
                status worker_status NOT NULL,
                heartbeat_at TIMESTAMPTZ NOT NULL,
                shutdown_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS pgqrs_queues (
                id BIGSERIAL PRIMARY KEY,
                queue_name TEXT UNIQUE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pgqrs_messages (
                id BIGSERIAL PRIMARY KEY,
                queue_id BIGINT NOT NULL,
                payload JSONB,
                vt TIMESTAMPTZ NOT NULL,
                archived_at TIMESTAMPTZ,
                consumer_worker_id BIGINT,
                enqueued_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pgqrs_schedules (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                cron_expression TEXT NOT NULL,
                workflow_name TEXT NOT NULL,
                input JSONB,
                status TEXT NOT NULL DEFAULT 'active',
                next_fire_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS pgqrs_workflow_runs (
                id BIGSERIAL PRIMARY KEY,
                status pgqrs_workflow_status NOT NULL,
                error JSONB,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL,
                status pgqrs_workflow_status NOT NULL,
                error JSONB,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        "#;
        client.update(ddl, None, None)?;
        Ok(())
    }

    #[pg_test]
    fn test_coordinator_maintenance_sweep() {
        Spi::connect(|mut client| {
            setup_test_tables(&mut client).unwrap();

            // Clean slate
            client
                .update("TRUNCATE pgqrs_workers, pgqrs_messages CASCADE", None, None)
                .unwrap();

            // 1. Insert stale worker (heartbeat 60 seconds ago)
            let worker_id: i64 = client.select(
                "INSERT INTO pgqrs_workers (status, heartbeat_at) VALUES ('polling'::worker_status, NOW() - interval '60 seconds') RETURNING id",
                None, None
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 2. Insert leased message assigned to this stale worker (vt is in future, i.e. leased)
            let msg_id: i64 = client.select(
                "INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at, consumer_worker_id) VALUES (1, '{}'::jsonb, NOW() + interval '30 seconds', NOW(), $1) RETURNING id",
                None, Some(vec![(PgBuiltInOids::INT8OID.oid(), worker_id.into_datum())])
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 3. Run maintenance sweep with 30s timeout
            crate::bgworker::run_maintenance_sweep(30.0, 3600.0).unwrap();

            // 4. Verify worker status is now 'stopped'
            let worker_status: String = client
                .select(
                    "SELECT status::text FROM pgqrs_workers WHERE id = $1",
                    None,
                    Some(vec![(PgBuiltInOids::INT8OID.oid(), worker_id.into_datum())]),
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name("status")
                .unwrap()
                .unwrap();
            assert_eq!(worker_status, "stopped");

            // 5. Verify message lease was reclaimed (vt reset to <= NOW(), consumer_worker_id set to NULL)
            let row = client.select(
                "SELECT vt <= NOW() as due, consumer_worker_id FROM pgqrs_messages WHERE id = $1",
                None, Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
            ).unwrap().next().unwrap();
            let due: bool = row.get_by_name("due").unwrap().unwrap();
            let consumer_id: Option<i64> = row.get_by_name("consumer_worker_id").unwrap();

            assert!(due);
            assert!(consumer_id.is_none());
        });
    }

    #[pg_test]
    fn test_coordinator_schedule_scanner() {
        Spi::connect(|mut client| {
            setup_test_tables(&mut client).unwrap();

            // Clean slate
            client
                .update(
                    "TRUNCATE pgqrs_schedules, pgqrs_queues, pgqrs_messages CASCADE",
                    None,
                    None,
                )
                .unwrap();

            // 1. Insert schedule due for firing
            let schedule_id: i64 = client.select(
                "INSERT INTO pgqrs_schedules (name, cron_expression, workflow_name, input, next_fire_at) VALUES ('test-cron', '*/5 * * * *', 'test-wf', '{\"x\": 1}'::jsonb, NOW() - interval '1 minute') RETURNING id",
                None, None
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 2. Run schedule scanner
            let triggered = crate::bgworker::scan_schedules_once().unwrap();
            assert!(triggered);

            // 3. Verify queue was created
            let queue_exists: bool = client.select(
                "SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = 'test-wf') as exists",
                None, None
            ).unwrap().next().unwrap().get_by_name("exists").unwrap().unwrap();
            assert!(queue_exists);

            // 4. Verify trigger message was enqueued
            let msg_count: i64 = client
                .select("SELECT COUNT(*) FROM pgqrs_messages", None, None)
                .unwrap()
                .next()
                .unwrap()
                .get_by_name("count")
                .unwrap()
                .unwrap();
            assert_eq!(msg_count, 1);

            // 5. Verify next_fire_at on the schedule was updated to a future time
            let next_fire_due: bool = client
                .select(
                    "SELECT next_fire_at <= NOW() as due FROM pgqrs_schedules WHERE id = $1",
                    None,
                    Some(vec![(
                        PgBuiltInOids::INT8OID.oid(),
                        schedule_id.into_datum(),
                    )]),
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name("due")
                .unwrap()
                .unwrap();
            assert!(!next_fire_due);
        });
    }
}
