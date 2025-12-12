import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_consumer_delete(postgres_dsn, schema):
    admin = pgqrs.Admin(postgres_dsn)
    await admin.install()

    queue_name = "test_delete_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(postgres_dsn, queue_name, "host1", 123)
    consumer = pgqrs.Consumer(postgres_dsn, queue_name, "cons", 2)

    # Enqueue a message
    payload = {"foo": "bar"}
    msg_id = await producer.enqueue(payload)

    # Dequeue it
    msgs = await consumer.dequeue()
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg.id == msg_id

    # Delete it
    deleted = await consumer.delete(msg.id)
    assert deleted is True, f"Delete failed for message {msg.id}"

    # Verify it's gone from active messages
    active_count = await (await admin.get_messages()).count()
    assert active_count == 0
