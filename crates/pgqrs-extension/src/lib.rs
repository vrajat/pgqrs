#![allow(unexpected_cfgs)]
use pgrx::prelude::*;

pub mod bgworker;
pub mod builtins;

pub use bgworker::pgqrs_coordinator_main;

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    bgworker::init_gucs();
    builtins::init_gucs();

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
                name TEXT UNIQUE NOT NULL,
                queue_id BIGINT,
                status worker_status NOT NULL,
                heartbeat_at TIMESTAMPTZ NOT NULL,
                shutdown_at TIMESTAMPTZ,
                started_at TIMESTAMPTZ DEFAULT NOW()
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
                producer_worker_id BIGINT,
                read_ct INTEGER NOT NULL DEFAULT 0,
                dequeued_at TIMESTAMPTZ,
                enqueued_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pgqrs_cron (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                queue_id BIGINT NOT NULL UNIQUE,
                cron_expression TEXT NOT NULL,
                input JSONB,
                status TEXT NOT NULL DEFAULT 'active',
                trigger_state TEXT NOT NULL DEFAULT 'idle',
                next_fire_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS pgqrs_workflows (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                queue_id BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pgqrs_workflow_runs (
                id BIGSERIAL PRIMARY KEY,
                workflow_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL UNIQUE,
                status pgqrs_workflow_status NOT NULL,
                input JSONB,
                output JSONB,
                error JSONB,
                worker_id BIGINT,
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
                "INSERT INTO pgqrs_workers (name, status, heartbeat_at) VALUES ('stale-worker-test', 'polling'::worker_status, NOW() - interval '60 seconds') RETURNING id",
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
    fn test_coordinator_cron_scanner() {
        Spi::connect(|mut client| {
            setup_test_tables(&mut client).unwrap();

            // Clean slate
            client
                .update(
                    "TRUNCATE pgqrs_cron, pgqrs_queues, pgqrs_messages CASCADE",
                    None,
                    None,
                )
                .unwrap();

            // 1. Insert backing queue first
            let queue_id: i64 = client
                .select(
                    "INSERT INTO pgqrs_queues (queue_name) VALUES ('test-wf') RETURNING id",
                    None,
                    None,
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name("id")
                .unwrap()
                .unwrap();

            // 2. Insert cron due for firing referencing the queue_id
            let cron_id: i64 = client.select(
                "INSERT INTO pgqrs_cron (name, queue_id, cron_expression, input, next_fire_at) VALUES ('test-cron', $1, '*/5 * * * *', '{\"x\": 1}'::jsonb, NOW() - interval '1 minute') RETURNING id",
                None, Some(vec![(PgBuiltInOids::INT8OID.oid(), queue_id.into_datum())])
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 3. Run cron scanner
            let triggered = crate::bgworker::scan_cron_once().unwrap();
            assert!(triggered);

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

            // 5. Verify next_fire_at on the cron was updated to a future time
            let next_fire_due: bool = client
                .select(
                    "SELECT next_fire_at <= NOW() as due FROM pgqrs_cron WHERE id = $1",
                    None,
                    Some(vec![(PgBuiltInOids::INT8OID.oid(), cron_id.into_datum())]),
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

    #[pg_test]
    fn test_builtins_inspection() {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT capability, version FROM pgqrs_builtins()",
                    None,
                    None,
                )
                .unwrap();
            let mut caps = Vec::new();
            for row in table {
                let cap: String = row.get_by_name("capability").unwrap().unwrap();
                caps.push(cap);
            }
            assert!(caps.contains(&"sql".to_string()));
            assert!(caps.contains(&"timer".to_string()));
            assert!(caps.contains(&"maintenance".to_string()));
            assert!(caps.contains(&"metrics".to_string()));
        });
    }

    #[pg_test]
    fn test_builtin_sql_executor() {
        Spi::connect(|mut client| {
            setup_test_tables(&mut client).unwrap();

            // Clean slate
            client
                .update(
                    "TRUNCATE pgqrs_queues, pgqrs_messages, pgqrs_workers CASCADE",
                    None,
                    None,
                )
                .unwrap();

            // 1. Resolve queue_id
            let queue_id: i64 = client.select(
                "INSERT INTO pgqrs_queues (queue_name) VALUES ('test-builtin-sql') RETURNING id",
                None, None
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 2. Enqueue standalone SQL job (insert a new queue named 'dynamically-added-queue')
            let payload = serde_json::json!({
                "statement": "INSERT INTO pgqrs_queues (queue_name) VALUES ($1)",
                "params": ["dynamically-added-queue"]
            });
            let payload_str = serde_json::to_string(&payload).unwrap();

            let msg_id: i64 = client.select(
                "INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at) VALUES ($1, $2::jsonb, NOW(), NOW()) RETURNING id",
                None, Some(vec![
                    (PgBuiltInOids::INT8OID.oid(), queue_id.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), payload_str.into_datum()),
                ])
            ).unwrap().next().unwrap().get_by_name("id").unwrap().unwrap();

            // 3. Process the queue using the builtin executor
            crate::builtins::process_builtin_queue_once("test-builtin-sql");

            // 4. Verify that the SQL statement executed (i.e. 'dynamically-added-queue' exists in pgqrs_queues)
            let queue_exists: bool = client.select(
                "SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = 'dynamically-added-queue') as exists",
                None, None
            ).unwrap().next().unwrap().get_by_name("exists").unwrap().unwrap();
            assert!(queue_exists);

            // 5. Verify message was archived
            let archived: bool = client
                .select(
                    "SELECT archived_at IS NOT NULL as archived FROM pgqrs_messages WHERE id = $1",
                    None,
                    Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())]),
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name("archived")
                .unwrap()
                .unwrap();
            assert!(archived);
        });
    }
}
