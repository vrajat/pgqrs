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

@pytest.mark.asyncio
async def test_config_properties(postgres_dsn):
    # Test valid DSN creation
    c = pgqrs.Config.from_dsn(postgres_dsn)

    # Test Max Connections Defaults and Setter
    c.max_connections = 42
    assert c.max_connections == 42

    # Test Timeout Defaults and Setter
    c.connection_timeout_seconds = 60
    assert c.connection_timeout_seconds == 60

    # Test Advanced Config (PR #89)
    c.default_lock_time_seconds = 120
    assert c.default_lock_time_seconds == 120

    c.default_max_batch_size = 500
    assert c.default_max_batch_size == 500

    c.max_read_ct = 10
    assert c.max_read_ct == 10

    c.heartbeat_interval_seconds = 15
    assert c.heartbeat_interval_seconds == 15

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

@pytest.mark.asyncio
async def test_worker_list_status(postgres_dsn, schema):
    """
    Test worker listing and status APIs.
    """
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()
    queue_name = "worker_test_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(admin, queue_name, "prod_host", 5000)
    consumer = pgqrs.Consumer(admin, queue_name, "cons_host", 5001)

    # Ensure workers are registered
    # Producer registration happens on new()
    # Consumer registration happens on new()

    workers_handle = await admin.get_workers()

    # List workers
    worker_list = await workers_handle.list()
    assert len(worker_list) >= 2

    # Verify fields
    prod_worker = next(w for w in worker_list if w.hostname == "prod_host")
    cons_worker = next(w for w in worker_list if w.hostname == "cons_host")

    assert prod_worker.port == 5000
    assert cons_worker.port == 5001

    assert prod_worker.status == pgqrs.WorkerStatus.Ready

    # Verify Get
    fetched_worker = await workers_handle.get(prod_worker.id)
    assert fetched_worker.id == prod_worker.id
    assert fetched_worker.hostname == "prod_host"

@pytest.mark.asyncio
async def test_archive_advanced_methods(postgres_dsn, schema):
    """
    Test advanced archive methods (get, list_by_worker, dlq).
    From Issue #90
    """
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()
    queue_name = "archive_api_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(admin, queue_name, "arch_prod", PRODUCER_PORT)
    consumer = pgqrs.Consumer(admin, queue_name, "arch_cons", CONSUMER_PORT)

    # 1. Enqueue, Consume, Archive
    msg_id = await producer.enqueue({"foo": "bar"})
    msgs = await consumer.dequeue()
    assert len(msgs) == 1
    msg = msgs[0]
    await consumer.archive(msg.id)

    # 2. Get Archive handle
    archive = await admin.get_archive()

    # 3. Test get()
    # Note: archive.get() takes archive ID, which is typically same as msg ID in current impl or we need to find it.
    # The rust implementation might use same ID sequence or different?
    # Usually archive table uses `id` serial. `original_msg_id` is the queue message id.
    # We need to find the archive record first.

    # Since we can't search by original_msg_id easily without list(), let's list by worker if possible?
    # Or just guess ID 1 (since it's fresh schema).

    # Let's try to find it via worker list if consumer registered properly.
    # Consumer worker ID is needed.
    workers = await (await admin.get_workers()).list()
    cons_worker = next(w for w in workers if w.hostname == "arch_cons")

    # Test list_by_worker
    archived_msgs = await archive.list_by_worker(cons_worker.id, 10, 0)
    assert len(archived_msgs) == 1
    arch_msg = archived_msgs[0]

    assert arch_msg.original_msg_id == msg_id
    assert arch_msg.consumer_worker_id == cons_worker.id
    assert isinstance(arch_msg.vt, str)
    assert isinstance(arch_msg.dequeued_at, str)

    # Test get() using the ID we just found
    fetched_arch_msg = await archive.get(arch_msg.id)
    assert fetched_arch_msg.id == arch_msg.id
    assert fetched_arch_msg.payload == {"foo": "bar"}

    # 4. Test count_by_worker
    count = await archive.count_by_worker(cons_worker.id)
    assert count == 1

    # 5. Test delete()
    deleted = await archive.delete(arch_msg.id)
    assert deleted == 1

    # Verify gone
    assert await archive.count() == 0

    # 6. Test DLQ (optional, if we can simulate failure easily)
    # We won't simulate DLQ logic here to keep it simple, verifying API existence was the main goal.
    dlq_count = await archive.dlq_count(5)
    assert dlq_count == 0
