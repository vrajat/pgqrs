import pytest
import pgqrs
import asyncio
import os

# Constants for ports as requested
PRODUCER_PORT = 8100
CONSUMER_PORT = 8200

@pytest.mark.asyncio
async def test_basic_workflow(postgres_dsn, schema):
    """
    Test basic workflow: Install -> Create Queue -> Produce -> Consume -> Archive
    From original test_basic.py
    """
    # Append search_path options to DSN so all operations happen in the isolated schema
    dsn_with_schema = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"

    admin = pgqrs.Admin(dsn_with_schema)

    # 1. Install & Verify Schema
    await admin.install()
    await admin.verify()

    # 2. Create Queue
    queue_name = "test_queue_1"
    await admin.create_queue(queue_name)

    # 3. Verify Queue Exists
    queues = await admin.get_queues()
    assert await queues.count() == 1

    # 4. Produce Message
    test_name = "test_basic_workflow"
    producer = pgqrs.Producer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_producer",
        port=PRODUCER_PORT
    )
    payload = {"data": "test_payload", "id": 123}
    msg_id = await producer.enqueue(payload)
    assert msg_id > 0

    # 5. Verify Message Count
    messages = await admin.get_messages()
    assert await messages.count() == 1

    # 6. Consume Message
    consumer = pgqrs.Consumer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_consumer",
        port=CONSUMER_PORT
    )

    # Poll for message
    received_msgs = []
    for _ in range(10):
        batch = await consumer.dequeue()
        if batch:
            received_msgs.extend(batch)
            break
        await asyncio.sleep(0.1)

    assert len(received_msgs) == 1
    msg = received_msgs[0]
    assert msg.id == msg_id
    assert msg.payload == payload

    # 7. Archive Message
    await consumer.archive(msg.id)

    # 8. Verify Archive
    archive_msgs = await admin.get_archive()
    assert await archive_msgs.count() == 1

    # Verify Active Messages is 0
    assert await messages.count() == 0


@pytest.mark.asyncio
async def test_consumer_delete(postgres_dsn, schema):
    """
    Test consumer delete functionality, including ownership checks.
    From original test_consumer_delete.py
    """
    # Use DSN with schema if consistently needed, or rely on search_path being set by fixture/env?
    # Original test_consumer_delete used raw postgres_dsn but passed `schema` fixture (which does logic?)
    # Actually the fixture `schema` in conftest typically creates a schema and sets it?
    # No, usually standard pg fixture just gives DSN. Let's start sticking to dsn_with_schema pattern
    # OR rely on admin.install() if it uses the passed DSN.
    # The `test_basic.py` was explicit about `dsn_with_schema`.
    # Let's use `dsn_with_schema` to be safe and consistent with "Admin needs valid connection to schema".
    dsn_with_schema = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"

    admin = pgqrs.Admin(dsn_with_schema)
    await admin.install()

    queue_name = "test_delete_queue"
    await admin.create_queue(queue_name)

    test_name = "test_consumer_delete"
    producer = pgqrs.Producer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_producer",
        port=PRODUCER_PORT
    )
    consumer = pgqrs.Consumer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_consumer",
        port=CONSUMER_PORT
    )

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

    # Test deleting non-existent message
    deleted = await consumer.delete(999999)
    assert deleted is False, "Delete should return False for non-existent message"

    # Test deleting message not owned by consumer
    msg_id2 = await producer.enqueue({"foo": "bar2"})
    # Do NOT dequeue it. Consumer does not own it.
    deleted = await consumer.delete(msg_id2)
    assert deleted is False, "Delete should return False for message not owned/locked by consumer"


@pytest.mark.asyncio
async def test_enqueue_delayed(postgres_dsn, schema):
    """
    Test delayed message enqueueing.
    From original test_delayed.py
    """
    dsn_with_schema = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"

    admin = pgqrs.Admin(dsn_with_schema)
    await admin.install()
    queue_name = "test_delayed_queue"
    await admin.create_queue(queue_name)

    test_name = "test_enqueue_delayed"
    producer = pgqrs.Producer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_producer",
        port=PRODUCER_PORT
    )
    consumer = pgqrs.Consumer(
        dsn_with_schema,
        queue_name,
        hostname=f"{test_name}_consumer",
        port=CONSUMER_PORT
    )

    payload = {"foo": "bar"}
    # Enqueue with 2 seconds delay
    msg_id = await producer.enqueue_delayed(payload, 2)
    assert msg_id > 0

    # Try immediate dequeue
    msgs = await consumer.dequeue()
    assert len(msgs) == 0, "Message should not be visible yet"

    # Wait 2.5 seconds
    await asyncio.sleep(2.5)

    # Try dequeue again
    msgs = await consumer.dequeue()
    assert len(msgs) == 1, "Message should be visible now"
    assert msgs[0].id == msg_id
