import pytest
import pgqrs
import asyncio

# Constants
PRODUCER_PORT = 9100
CONSUMER_PORT = 9200

@pytest.mark.asyncio
async def test_batch_operations(postgres_dsn, schema):
    # Setup isolated schema DSN
    dsn_with_schema = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"

    # Create Admin
    admin = pgqrs.Admin(dsn_with_schema)
    await admin.install()

    queue_name = "batch_ops_test"
    await admin.create_queue(queue_name)

    # Use Shared Admin API
    producer = pgqrs.Producer(admin, queue_name, "batch_prod", PRODUCER_PORT)
    consumer = pgqrs.Consumer(admin, queue_name, "batch_cons", CONSUMER_PORT)

    # 1. Batch Enqueue
    payloads = [{"id": i, "data": f"msg_{i}"} for i in range(5)]
    msgs = await producer.enqueue_batch(payloads)
    assert len(msgs) == 5
    ids = [m.id for m in msgs]

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
