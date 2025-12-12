import pytest
import pgqrs
import asyncio
import pytest_asyncio

@pytest.mark.asyncio
async def test_archive_access(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.Admin(dsn)
    await admin.install()

    queue = "archive_test_queue"
    q_info = await admin.create_queue(queue)

    # 1. Archive should be empty initially
    archive = await admin.get_archive()
    count = await archive.count()
    assert count == 0

    # 2. Produce and Archive a message
    producer = pgqrs.Producer(dsn, queue, "arch_prod", 1)
    msg_id = await producer.enqueue({"foo": "bar"})

    consumer = pgqrs.Consumer(dsn, queue, "arch_cons", 1)
    # Dequeue to get the message (so we have a claim)
    msgs = await consumer.dequeue()
    assert len(msgs) == 1
    assert msgs[0].id == msg_id

    # Archive it
    await consumer.archive(msg_id)

    # 3. Verify count increases
    count_after = await archive.count()
    assert count_after == 1
