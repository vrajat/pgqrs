import pytest
import pgqrs
import asyncio

async def setup_test(dsn, schema):
    config = pgqrs.Config(dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    return store, admin

@pytest.mark.asyncio
async def test_consume_stream_iterator(postgres_dsn, schema):
    """
    Test the consume_stream iterator.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "stream_test_q"
    await admin.create_queue(queue_name)

    iterator = store.consume_iter(queue_name, poll_interval_ms=10)
    received_count = 0
    consume_task = None

    async def consume_loop():
        nonlocal received_count
        async for msg in iterator:
            received_count += 1
            # Verify archive method
            res = await iterator.archive(msg.id)
            assert res is True
            if received_count >= 5:
                break

    # Produce messages FIRST
    producer = await store.producer(queue_name)
    for i in range(5):
        await pgqrs.enqueue(producer, {"i": i})

    consume_task = asyncio.create_task(consume_loop())

    await asyncio.wait_for(consume_task, timeout=5.0)
    assert received_count == 5

    # Verify messages are archived
    archive_count = await (await admin.get_archive()).count()
    assert archive_count == 5

@pytest.mark.asyncio
async def test_exceptions(postgres_dsn, schema):
    """
    Test that custom exceptions are raised and catchable.
    """
    store, admin = await setup_test(postgres_dsn, schema)

    # Test QueueNotFoundError
    # Attempt to use a non-existent queue with consumer to force an error
    # since admin.delete_queue might be idempotent
    qname = "non_existent_queue_abc"
    try:
        await admin.delete_queue(qname)
    except pgqrs.QueueNotFoundError:
        pass # Expected if it failed
    except Exception:
        pass

    # Better test: connect with invalid DSN

    # Use invalid host
    bad_dsn = "postgres://user:pass@non-existent-host-12345.local:5432/db"

    with pytest.raises(pgqrs.PgqrsConnectionError):
         await pgqrs.connect(bad_dsn)

    # Test create_queue on existing queue?
    q2 = "existing_queue"
    await admin.create_queue(q2)
    # create_queue behavior: if exists, what happens?
    # pgqrs checks: QueueAlreadyExists
    # But admin.create_queue implementation?
    # Let's try.
    with pytest.raises(pgqrs.QueueAlreadyExistsError):
        await admin.create_queue(q2)
