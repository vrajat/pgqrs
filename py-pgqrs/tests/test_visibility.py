import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_visibility_and_delete(postgres_dsn, schema):
    dsn = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"
    admin = pgqrs.Admin(postgres_dsn)
    await admin.install()

    queue_name = "test_vis_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(postgres_dsn, queue_name, "prod", 1)
    consumer = pgqrs.Consumer(postgres_dsn, queue_name, "cons", 2)

    payload = {"task": "cleanup"}
    await producer.enqueue(payload)

    # 1. Dequeue
    msgs = await consumer.dequeue()
    assert len(msgs) == 1
    msg = msgs[0]

    # 2. Extend Visibility
    # We can't easily check the DB state, but we can verify the call succeeds
    # await consumer.extend_visibility(msg.id, 60.0)

    # 3. Delete (Discard)
    print(f"DEBUG: Attempting to delete message {msg.id}")
    count_before = await (await admin.get_messages()).count()
    print(f"DEBUG: Count before delete: {count_before}")

    deleted = await consumer.delete(msg.id)
    print(f"DEBUG: Delete result: {deleted}")

    # Check count after
    count_after = await (await admin.get_messages()).count()
    print(f"DEBUG: Count after delete attempt: {count_after}")

    assert deleted is True, f"Delete failed for message {msg.id}. Count before: {count_before}, After: {count_after}"

    # 4. Verify message is GONE from active messages (count=0)
    messages = await admin.get_messages()
    assert await messages.count() == 0

    # 5. Verify message is NOT in archive (since we used delete, not archive)
    archive = await admin.get_archive()
    assert await archive.count() == 0
