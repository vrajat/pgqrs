import pytest
import pgqrs
import asyncio
import pytest_asyncio

@pytest.mark.asyncio
async def test_worker_lifecycle(postgres_dsn, schema):
    # Use Admin correctly
    # Note: postgres_dsn fixture is bare DSN. schema fixture is string.
    # Admin(dsn, schema=...) handles schema isolation.

    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()

    queue = "worker_test_queue_legacy"
    await admin.create_queue(queue)

    # 1. Create Consumer (which is a Worker)
    # MUST pass admin, not dsn string
    consumer = pgqrs.Consumer(admin, queue, "worker_host", 1)

    # 2. Check Status
    workers = await admin.get_workers()
    count = await workers.count()
    assert count == 1
