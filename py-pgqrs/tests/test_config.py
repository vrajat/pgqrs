import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_config_properties(postgres_dsn):
    # Test valid DSN creation
    c = pgqrs.Config.from_dsn(postgres_dsn)

    # Test Schema Defaults and Setter
    # Uses property valid syntax
    # c.schema = "custom_schema"
    # assert c.schema == "custom_schema"

    # Test Max Connections Defaults and Setter
    c.max_connections = 42
    assert c.max_connections == 42

    # Test Timeout Defaults and Setter
    c.connection_timeout_seconds = 60
    assert c.connection_timeout_seconds == 60

@pytest.mark.asyncio
async def test_config_integration(postgres_dsn, schema):
    # This test verifies that we can pass a Config object to Admin

    config = pgqrs.Config.from_dsn(postgres_dsn)
    config.schema = schema
    config.max_connections = 5

    # Test Admin with Config
    admin = pgqrs.Admin(config)
    await admin.install()

    queue_name = "test_config_queue"
    await admin.create_queue(queue_name)

    # Test Producer with Config -> MUST use Admin now
    producer = pgqrs.Producer(admin, queue_name, "prod_conf", 1)
    await producer.enqueue({"key": "val"})

    # Test Consumer with Config -> MUST use Admin now
    consumer = pgqrs.Consumer(admin, queue_name, "cons_conf", 1)
    messages = await consumer.dequeue()

    assert len(messages) == 1
    assert messages[0].payload == {"key": "val"}

@pytest.mark.asyncio
async def test_legacy_string_dsn(postgres_dsn, schema):
    # Verify backward compatibility (passing string DSN to Admin)

    # We use schema arg to support test isolation
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    assert isinstance(admin, pgqrs.Admin)
