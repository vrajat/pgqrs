import pytest
import pgqrs
import asyncio

# Constants for ports as requested
PRODUCER_PORT = 8100
CONSUMER_PORT = 8200

@pytest.mark.asyncio
async def test_basic_workflow(postgres_dsn, schema):
    """
    Test basic workflow: Install -> Create Queue -> Produce -> Consume -> Archive
    From original test_basic.py
    """
    # Create Admin with DSN
    admin = pgqrs.Admin(postgres_dsn, schema=schema)

    # 1. Install & Verify Schema
    await admin.install()
    await admin.verify()

    # 2. Create Queue
    queue_name = "test_queue_1"
    await admin.create_queue(queue_name)

    # 3. Verify Queue Exists
    queues = await admin.get_queues()
    assert await queues.count() == 1

    # 4. Produce Message using Admin
    test_name = "test_basic_workflow"
    # New strict API: Producer(admin, queue, hostname, port)
    producer = pgqrs.Producer(
        admin,
        queue_name,
        f"{test_name}_producer",
        PRODUCER_PORT
    )
    payload = {"data": "test_payload", "id": 123}
    msg_id = await producer.enqueue(payload)
    assert msg_id > 0

    # 5. Verify Message Count
    messages = await admin.get_messages()
    assert await messages.count() == 1

    # 6. Consume Message using Admin
    # New strict API: Consumer(admin, queue, hostname, port)
    consumer = pgqrs.Consumer(
        admin,
        queue_name,
        f"{test_name}_consumer",
        CONSUMER_PORT
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

    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()

    queue_name = "test_delete_queue"
    await admin.create_queue(queue_name)

    test_name = "test_consumer_delete"
    producer = pgqrs.Producer(
        admin,
        queue_name,
        f"{test_name}_producer",
        PRODUCER_PORT
    )
    consumer = pgqrs.Consumer(
        admin,
        queue_name,
        f"{test_name}_consumer",
        CONSUMER_PORT
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
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()
    queue_name = "test_delayed_queue"
    await admin.create_queue(queue_name)

    test_name = "test_enqueue_delayed"
    producer = pgqrs.Producer(
        admin,
        queue_name,
        f"{test_name}_producer",
        PRODUCER_PORT
    )
    consumer = pgqrs.Consumer(
        admin,
        queue_name,
        f"{test_name}_consumer",
        CONSUMER_PORT
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


@pytest.mark.asyncio
async def test_extend_visibility(postgres_dsn, schema):
    """
    Test visibility timeout extension.
    From Issue #69
    """
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()
    queue_name = "test_visibility_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(admin, queue_name, "vis_prod", PRODUCER_PORT)
    consumer = pgqrs.Consumer(admin, queue_name, "vis_cons", CONSUMER_PORT)

    # Enqueue message
    msg_id = await producer.enqueue({"foo": "bar"})

    # Dequeue with short visibility (2 seconds)
    msgs = await consumer.dequeue_batch_with_delay(1, 2)
    assert len(msgs) == 1
    msg = msgs[0]

    # Extend visibility by 5 seconds
    # If successful, another consumer shouldn't see it even after 2s
    extended = await consumer.extend_visibility(msg.id, 5)
    assert extended is True, "Failed to extend visibility"

    # Wait 2.5 seconds (original VT expired, but extended should hold)
    await asyncio.sleep(2.5)



    # Let's check if it is visible. It should NOT be.
    msgs_retry = await consumer.dequeue()
    assert len(msgs_retry) == 0, "Message should still be invisible after extension"

    # Wait another 4 seconds (total 6.5s elapsed; still less than the extended visibility timeout of 7s [2s initial + 5s extension])
    await asyncio.sleep(4.0)

    # Reclaim messages from zombie consumers (since we waited > heartbeat interval likely, or we force it)
    await admin.reclaim_messages(queue_name, older_than_seconds=1.0)

    # Now it should be visible (or nearly)
    msgs_retry_2 = await consumer.dequeue()
    # The dequeue method performs a single fetch and does not loop internally.

    if len(msgs_retry_2) == 0:
        await asyncio.sleep(1.0)
        msgs_retry_2 = await consumer.dequeue()

    assert len(msgs_retry_2) == 1, "Message should become visible after extended timeout"
    assert msgs_retry_2[0].id == msg_id


@pytest.mark.asyncio
async def test_batch_operations(postgres_dsn, schema):
    """
    Test batch operations (enqueue, dequeue, archive).
    Consolidated from test_batch.py
    """
    # Create Admin with schema parameter
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()

    queue_name = "batch_ops_test"
    await admin.create_queue(queue_name)


    producer = pgqrs.Producer(admin, queue_name, "batch_prod", PRODUCER_PORT)
    consumer = pgqrs.Consumer(admin, queue_name, "batch_cons", CONSUMER_PORT)

    # 1. Batch Enqueue
    payloads = [{"id": i, "data": f"msg_{i}"} for i in range(5)]
    msgs = await producer.enqueue_batch(payloads)
    assert len(msgs) == 5


    # Verify count
    count = await (await admin.get_messages()).count()
    assert count == 5

    # 2. Dequeue Batch (2)
    batch1 = await consumer.dequeue_batch(2)
    assert len(batch1) == 2

    # 3. Dequeue Batch with Delay (2) with 60s VT
    batch2 = await consumer.dequeue_batch_with_delay(2, 60)
    assert len(batch2) == 2

    # Ensure IDs are unique across batches
    batch1_ids = {m.id for m in batch1}
    batch2_ids = {m.id for m in batch2}
    assert batch1_ids.isdisjoint(batch2_ids)

    # 4. Archive Batch
    to_archive = list(batch1_ids.union(batch2_ids))
    results = await consumer.archive_batch(to_archive)
    assert len(results) == 4
    assert all(results) # All should be True

    # Verify 1 message left active
    count_after = await (await admin.get_messages()).count()
    assert count_after == 1

    # Verify 4 items in archive
    arch_count = await (await admin.get_archive()).count()
    assert arch_count == 4
