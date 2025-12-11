import pytest
import pgqrs
import asyncio
import os

@pytest.mark.asyncio
async def test_admin_install_and_queue_workflow(postgres_dsn, schema):
    # Initialize Admin with schema configuration if supported by DSN/Config
    # Currently Config might default to public schema or take it from search_path?
    # Our Rust Config parses DSN. Does it support `search_path` param?
    # Usually sqlx/postgres supports ?search_path=schema in DSN or we execute "SET search_path"

    # We need to construct DSN with search_path or manually configure it?
    # Let's assume we can append options to DSN.
    dsn_with_schema = f"{postgres_dsn}?options=-c%20search_path%3D{schema}"

    # Or rely on pgqrs Config supporting schema?
    # Rust `Config` struct doesn't seem to have explicit schema field, it relies on DB connection.
    # But `PgqrsAdmin::install()` creates tables. If search_path is set, they go to that schema.

    admin = pgqrs.PgqrsAdmin(dsn_with_schema)

    # 1. Install Schema
    await admin.install()

    # 2. Verify Schema
    await admin.verify()

    # 3. Create Queue
    queue_name = "test_queue_1"
    try:
        await admin.create_queue(queue_name)
    except Exception as e:
        # Should not fail in a fresh schema
        pytest.fail(f"Failed to create queue: {e}")

    # 4. Verify Queue Exists (via getters)
    queues = await admin.get_queues()
    count = await queues.count()
    assert count == 1

    # 5. Produce Message
    # Provide unique hostname/port to avoid collision
    producer = pgqrs.Producer(dsn_with_schema, queue_name, hostname="producer_host", port=1001)
    payload = {"data": "test_payload", "id": 123}
    msg_id = await producer.enqueue(payload)
    assert msg_id > 0

    # 6. Verify Message Count
    messages = await admin.get_messages()
    msg_count = await messages.count()
    assert msg_count == 1

    # 7. Consume Message
    # Different port/hostname
    consumer = pgqrs.Consumer(dsn_with_schema, queue_name, hostname="consumer_host", port=2001)

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
    # Ensure payload matches (converted back from JSON)
    assert msg.payload == payload

    # 8. Archive Message
    await consumer.archive(msg.id)

    # 9. Verify Archive
    archive_msgs = await admin.get_archive()
    # Archive should have 1 item now? or Consumer.archive moves it?
    # Yes, usually archive moves from active to archive.
    # Wait, check rust impl. consumer.archive(id) calls logic to move.

    # Wait for archive to settle if async? It is async await.
    arch_count = await archive_msgs.count()
    assert arch_count == 1

    # Verify Active Messages is 0
    msg_count_after = await messages.count()
    assert msg_count_after == 0
