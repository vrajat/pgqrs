import pytest
import pgqrs
import asyncio
import pytest_asyncio

@pytest.mark.asyncio
async def test_worker_lifecycle(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.Admin(dsn)
    await admin.install()

    queue = "worker_test_queue"
    await admin.create_queue(queue)

    # 1. Create Consumer (which is a Worker)
    consumer = pgqrs.Consumer(dsn, queue, "worker_host", 1)

    # 2. Check Status (Should be 'On' or similar, depending on enum exposure)
    # Note: WorkerStatus might be an enum or string. Let's inspect what we get.
    # If it's an enum, we need to know its values. Usually: Active, Suspended, etc.
    # Based on rust code: Active, Suspended, Stopping, Stopped.

    # Verify we can access the worker table via Admin
    workers = await admin.get_workers()
    count = await workers.count()
    assert count == 1
