import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_enqueue_delayed(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.Admin(postgres_dsn)
    await admin.install()
    queue_name = "test_delayed_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(postgres_dsn, queue_name, "host1", 123)
    consumer = pgqrs.Consumer(dsn, queue_name, "cons", 2)

    payload = {"foo": "bar"}
    # Enqueue with 2 seconds delay
    msg_id = await producer.enqueue_delayed(payload, 2)
    assert msg_id > 0

    # Try immediate dequeue
    msgs = await consumer.dequeue()
    # It might take a few ms for visibility check, but 2s is long enough
    assert len(msgs) == 0, "Message should not be visible yet"

    # Wait 2.5 seconds
    await asyncio.sleep(2.5)

    # Try dequeue again
    msgs = await consumer.dequeue()
    assert len(msgs) == 1, "Message should be visible now"
    assert msgs[0].id == msg_id
