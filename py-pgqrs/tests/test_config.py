import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_config_properties(postgres_dsn):
    # Test valid DSN creation
    c = pgqrs.Config.from_dsn(postgres_dsn)

    # Test Schema Defaults and Setter
    assert c.get_schema() == "public"
    c.set_schema("custom_schema")
    assert c.get_schema() == "custom_schema"

    # Test Max Connections Defaults and Setter
    assert c.get_max_connections() == 16
    c.set_max_connections(42)
    assert c.get_max_connections() == 42

    # Test Timeout Defaults and Setter
    assert c.get_connection_timeout_seconds() == 30
    c.set_connection_timeout_seconds(60)
    assert c.get_connection_timeout_seconds() == 60

@pytest.mark.asyncio
async def test_config_integration(postgres_dsn, schema):
    # This test verifies that we can pass a Config object to clients
    # and they respect the settings (specifically schema).

    config = pgqrs.Config.from_dsn(postgres_dsn)
    config.set_schema(schema)
    config.set_max_connections(5)

    # Test Admin with Config
    admin = pgqrs.Admin(config)
    await admin.install()

    queue_name = "test_config_queue"
    await admin.create_queue(queue_name)

    # Test Producer with Config
    producer = pgqrs.Producer(config, queue_name, "prod_conf", 1)
    await producer.enqueue({"key": "val"})

    # Test Consumer with Config
    consumer = pgqrs.Consumer(config, queue_name, "cons_conf", 1)
    messages = await consumer.dequeue()

    assert len(messages) == 1
    assert messages[0].payload == {"key": "val"}

@pytest.mark.asyncio
async def test_legacy_string_dsn(postgres_dsn, schema):
    # Verify backward compatibility (passing string DSN still works)
    # Note: Using schema fixture implies the schema exists, but default config uses "public".
    # So we must pass dsn string, but if we want to use the test schema,
    # the existing `Config::from_dsn` defaults to public.
    # To test legacy string support effectively in this test environment (which uses random schemas),
    # we might need to rely on the fact that `public` schema usually exists.
    # But `PgqrsAdmin(dsn)` will use `public` schema. If we run install, it installs to public.
    # Setup: We should assume public is safe to verify at least connectivity.

    # However, to avoid polluting public in shared db, maybe we skip full install
    # and just assert it doesn't crash on instantiation.

    admin = pgqrs.Admin(postgres_dsn)
    # Just creating it proves constructor accepted the string.
    assert isinstance(admin, pgqrs.Admin)
