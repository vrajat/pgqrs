import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_batch_operations(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.PgqrsAdmin(dsn)
    await admin.install()

    queue = "batch_ops_test"
    await admin.create_queue(queue)

    producer = pgqrs.Producer(dsn, queue, "prod", 1)
    consumer = pgqrs.Consumer(dsn, queue, "cons", 2)

    # 1. Batch Enqueue
    payloads = [{"id": i, "data": f"msg_{i}"} for i in range(5)]
    msgs = await producer.enqueue_batch(payloads)
    assert len(msgs) == 5
    ids = [m.id for m in msgs]

    # Verify count
    count = await (await admin.get_messages()).count()
    assert count == 5

    # 2. Dequeue Many (2)
    batch1 = await consumer.dequeue_many(2)
    assert len(batch1) == 2

    # 3. Dequeue Many with Delay (2) with 60s VT
    batch2 = await consumer.dequeue_many_with_delay(2, 60)
    assert len(batch2) == 2

    # Ensure IDs are unique across batches
    batch1_ids = {m.id for m in batch1}
    batch2_ids = {m.id for m in batch2}
    assert batch1_ids.isdisjoint(batch2_ids)

    # 4. Archive Many
    to_archive = list(batch1_ids.union(batch2_ids))
    results = await consumer.archive_many(to_archive)
    assert len(results) == 4
    assert all(results) # All should be True

    # Verify 1 message left active
    count_after = await (await admin.get_messages()).count()
    assert count_after == 1

    # Verify 4 items in archive
    arch_count = await (await admin.get_archive()).count()
    assert arch_count == 4
