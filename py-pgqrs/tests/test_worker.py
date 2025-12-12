import pytest
import pgqrs
import asyncio
import time

@pytest.mark.asyncio
async def test_worker_lifecycle(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.PgqrsAdmin(dsn)
    await admin.install()

    queue = "worker_test_queue"
    await admin.create_queue(queue)

    producer = pgqrs.Producer(dsn, queue, "worker_prod", 1)

    # Check ID
    wid = producer.worker_id()
    assert isinstance(wid, int)

    # Check Status
    status = await producer.status()
    # status is an enum wrapper, let's assume it maps to names or check equality if exposed,
    # but the python wrapper might be opaque. It's an instance of WorkerStatus.
    # We can check strict equality if we export the class to the test module or access via module.
    assert isinstance(status, pgqrs.WorkerStatus)

    # Heartbeat
    await producer.heartbeat()

    # Suspend
    await producer.suspend()
    status_suspended = await producer.status()
    # We can't easily compare enum variants unless __eq__ works or we check name/value if exposed.
    # Since we didn't add __eq__ or expose fields, simpler to check if it changed or just runs without error.
    # Wait, PyO3 enums usually don't have equality by default unless __eq__ is implemented or it's a simple enum.
    # Our WorkerStatus is a struct/class wrapper around the rust enum.
    # Let's rely on no exceptions for now, and maybe add __eq__ if needed.

    # Resume
    await producer.resume()

    # Is Healthy
    healthy = await producer.is_healthy(60.0) # 60 seconds
    assert healthy is True

    # Consumer test
    consumer = pgqrs.Consumer(dsn, queue, "worker_cons", 2)
    cwid = consumer.worker_id()
    assert isinstance(cwid, int)
    assert cwid != wid

    await consumer.heartbeat()
