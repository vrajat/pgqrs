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
    queue = "stream_test_q"
    await admin.create_queue(queue)

    # Produce some messages
    producer = await store.producer(queue)
    for i in range(5):
        await pgqrs.enqueue(producer, {"i": i})

    # Consume using iterator
    # Note: Our iterator loops forever, so we need to break manually or run with timeout
    received = []

    async def consume_loop():
        async for msg in store.consume_stream(queue):
            received.append(msg)
            if len(received) >= 5:
                break

    await asyncio.wait_for(consume_loop(), timeout=5.0)

    assert len(received) == 5
    assert received[0].payload == {"i": 0}
    assert received[4].payload == {"i": 4}

@pytest.mark.asyncio
async def test_exceptions(postgres_dsn, schema):
    """
    Test that custom exceptions are raised and catchable.
    """
    store, admin = await setup_test(postgres_dsn, schema)

    # Test QueueNotFoundError
    # First ensure it doesn't exist
    qname = "non_existent_queue_abc"
    try:
        await admin.delete_queue(qname)
    except pgqrs.QueueNotFoundError:
        pass # Expected if it failed
    except Exception:
        # If delete_queue is idempotent, we might need another way to trigger generic error
        # Try getting messages from it?
        # But get_messages returns a Messages object, not async.
        pass

    # Better test: connect with invalid DSN
    print(f"DEBUG: postgres_dsn={postgres_dsn}")

    # Use invalid host
    bad_dsn = "postgres://user:pass@non-existent-host-12345.local:5432/db"

    with pytest.raises(pgqrs.ConnectionError):
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
