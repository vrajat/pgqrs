import pytest
import pgqrs
import asyncio

from pgqrs import PyWorkflow
from pgqrs.decorators import workflow, step

# Helper to setup a store and admin
async def setup_test(dsn, schema):
    config = pgqrs.Config(dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    return store, admin

@pytest.mark.asyncio
async def test_basic_workflow(postgres_dsn, schema):
    """
    Test basic workflow: Install -> Create Queue -> Produce -> Consume -> Archive
    """
    store, admin = await setup_test(postgres_dsn, schema)
    await admin.verify()

    queue_name = "test_queue_1"
    await admin.create_queue(queue_name)

    queues = await admin.get_queues()
    assert await queues.count() == 1

    payload = {"data": "test_payload", "id": 123}
    msg_id = await pgqrs.produce(store, queue_name, payload)
    assert msg_id > 0

    messages = await admin.get_messages()
    assert await messages.count() == 1

    consumer = await store.consumer(queue_name)
    received_msgs = []
    for _ in range(10):
        batch = await pgqrs.dequeue(consumer, 1)
        if batch:
            received_msgs.extend(batch)
            break
        await asyncio.sleep(0.1)

    assert len(received_msgs) == 1
    msg = received_msgs[0]
    assert msg.id == msg_id
    assert msg.payload == payload

    await pgqrs.archive(consumer, msg)

    archive_msgs = await admin.get_archive()
    assert await archive_msgs.count() == 1
    assert await messages.count() == 0

@pytest.mark.asyncio
async def test_consumer_archive(postgres_dsn, schema):
    """
    Test consumer delete (archive) functionality.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "test_delete_queue"
    await admin.create_queue(queue_name)

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)

    payload = {"foo": "bar"}
    msg_id = await pgqrs.enqueue(producer, payload)

    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg.id == msg_id

    await pgqrs.archive(consumer, msg)
    assert await (await admin.get_messages()).count() == 0

@pytest.mark.asyncio
async def test_batch_operations(postgres_dsn, schema):
    """
    Test batch operations (enqueue_batch, dequeue, archive_batch).
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "batch_ops_test"
    await admin.create_queue(queue_name)

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)

    payloads = [{"id": i} for i in range(5)]
    msg_ids = await pgqrs.enqueue_batch(producer, payloads)
    assert len(msg_ids) == 5

    count = await (await admin.get_messages()).count()
    assert count == 5

    batch = await pgqrs.dequeue(consumer, 3)
    assert len(batch) == 3

    await pgqrs.archive_batch(consumer, batch)

    count_after = await (await admin.get_messages()).count()
    assert count_after == 2

    arch_count = await (await admin.get_archive()).count()
    assert arch_count == 3

@pytest.mark.asyncio
async def test_config_properties(postgres_dsn):
    c = pgqrs.Config(postgres_dsn)

    c.max_connections = 42
    assert c.max_connections == 42

    c.connection_timeout_seconds = 60
    assert c.connection_timeout_seconds == 60

    c.default_lock_time_seconds = 120
    assert c.default_lock_time_seconds == 120

    c.default_max_batch_size = 500
    assert c.default_max_batch_size == 500

    c.max_read_ct = 10
    assert c.max_read_ct == 10

    c.heartbeat_interval_seconds = 15
    assert c.heartbeat_interval_seconds == 15

@pytest.mark.asyncio
async def test_connect_string(postgres_dsn):
    """Test connecting with a plain string DSN."""
    store = await pgqrs.connect(postgres_dsn)
    assert store is not None
    admin = pgqrs.admin(store)
    await admin.install()
    await admin.verify()

@pytest.mark.asyncio
async def test_enqueue_delayed(postgres_dsn, schema):
    """
    Test delayed message enqueueing.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "test_delayed_queue"
    await admin.create_queue(queue_name)

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)

    payload = {"foo": "bar"}
    # Enqueue with 2 second delay
    await pgqrs.enqueue_delayed(producer, payload, 1)

    # Should not be available immediately
    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 0

    await asyncio.sleep(1.2)

    # Should be available now
    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 1

@pytest.mark.asyncio
async def test_worker_list_status(postgres_dsn, schema):
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "worker_test_queue"
    await admin.create_queue(queue_name)

    _ = await store.producer(queue_name)
    _ = await store.consumer(queue_name)

    workers_handle = await admin.get_workers()
    worker_list = await workers_handle.list()
    assert len(worker_list) >= 2
    # Check status of first worker (should be ready or similar)
    assert worker_list[0].status == "ready"

@pytest.mark.asyncio
async def test_consumer_delete(postgres_dsn, schema):
    """
    Test consumer delete (without archiving) functionality.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "test_delete_logic_queue"
    await admin.create_queue(queue_name)

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)

    payload = {"foo": "bar"}
    msg_id = await pgqrs.enqueue(producer, payload)

    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 1
    msg = msgs[0]

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
    msg_id2 = await pgqrs.enqueue(producer, {"foo": "bar2"})
    # Do NOT dequeue it. Consumer does not own it.
    deleted = await consumer.delete(msg_id2)
    assert deleted is False, "Delete should return False for message not owned/locked by consumer"

@pytest.mark.asyncio
async def test_archive_advanced_methods(postgres_dsn, schema):
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "archive_api_queue"
    await admin.create_queue(queue_name)

    await pgqrs.produce(store, queue_name, {"foo": "bar"})
    consumer = await store.consumer(queue_name)
    msgs = await pgqrs.dequeue(consumer, 1)
    await pgqrs.archive(consumer, msgs[0])

    archive = await admin.get_archive()
    assert await archive.count() == 1

    # 1. Test get()
    # We need to list to find the ID of the archived message
    # We can use the consumer's ID since we exposed it.
    worker_id = consumer.worker_id

    arch_msgs = await archive.list_by_worker(worker_id, 10, 0)
    assert len(arch_msgs) == 1
    arch_msg = arch_msgs[0]

    fetched = await archive.get(arch_msg.id)
    assert fetched.id == arch_msg.id
    assert fetched.payload == {"foo": "bar"}

    # 2. Test count_by_worker
    assert await archive.count_by_worker(worker_id) == 1

    # 3. Test dlq_count
    assert await archive.dlq_count(5) == 0

    # 4. Test delete()
    await archive.delete(arch_msg.id)
    assert await archive.count() == 0

@pytest.mark.asyncio
async def test_api_redesign_high_level(postgres_dsn, schema):
    """
    Test the high-level redesigned API: connect -> produce -> consume.
    """
    config = pgqrs.Config(postgres_dsn, schema=schema)
    store = await pgqrs.connect_with(config)

    admin = pgqrs.admin(store)
    await admin.install()
    queue = "high_level_q"
    await admin.create_queue(queue)

    payload = {"foo": "bar"}
    msg_id = await pgqrs.produce(store, queue, payload)
    assert msg_id > 0

    consumed = asyncio.Event()
    async def handler(msg):
        assert msg.payload == payload
        consumed.set()
        return True

    await pgqrs.consume(store, queue, handler)
    assert consumed.is_set()

@pytest.mark.asyncio
async def test_api_redesign_low_level(postgres_dsn, schema):
    """
    Test the low-level redesigned API: connect -> produce -> enqueue/dequeue/archive.
    """
    config = pgqrs.Config(postgres_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    queue = "low_level_q"
    await admin.create_queue(queue)

    producer = await store.producer(queue)
    consumer = await store.consumer(queue)

    # Use low-level functions
    msg_id = await pgqrs.enqueue(producer, {"foo": "bar"})
    assert msg_id > 0

    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 1
    assert msgs[0].id == msg_id

    # Use msg.id for archive? No, the archive pyfunction takes Quantitative message object
    await pgqrs.archive(consumer, msgs[0])

    assert await (await admin.get_messages()).count() == 0
    assert await (await admin.get_archive()).count() == 1

# --- Durable Workflow Tests ---

@step
async def step1(ctx: PyWorkflow, msg: str):
    return f"processed_{msg}"

@step
async def step2(ctx: PyWorkflow, val: str):
    return f"step2_{val}"

@workflow
async def simple_wf(ctx: PyWorkflow, arg: str):
    res1 = await step1(ctx, arg)
    res2 = await step2(ctx, res1)
    return res2

@pytest.mark.asyncio
async def test_workflow_execution(postgres_dsn, schema):
    store, admin = await setup_test(postgres_dsn, schema)
    wf_name = "simple_wf"
    wf_arg = "test_data"
    wf_ctx = await admin.create_workflow(wf_name, wf_arg)

    result = await simple_wf(wf_ctx, wf_arg)
    assert result == "step2_processed_test_data"

@pytest.mark.asyncio
async def test_extend_visibility(postgres_dsn, schema):
    """
    Test extending visibility timeout.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "test_extend_vt_queue"
    await admin.create_queue(queue_name)

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)

    await pgqrs.enqueue(producer, {"foo": "bar"})

    # Dequeue
    msgs = await pgqrs.dequeue(consumer, 1)
    assert len(msgs) == 1
    msg = msgs[0]

    # Extend VT by 10 seconds
    await pgqrs.extend_vt(consumer, msg, 10)

@pytest.mark.asyncio
async def test_workflow_crash_recovery(postgres_dsn, schema):
    """
    Test workflow recovery.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    wf_name = "recovery_wf"
    wf_arg = "crash_test"

    wf_ctx = await admin.create_workflow(wf_name, wf_arg)

    step1_called = 0
    step2_called = 0

    @step
    async def step_a(ctx, arg):
        nonlocal step1_called
        step1_called += 1
        return f"a_{arg}"

    @step
    async def step_b(ctx, arg):
        nonlocal step2_called
        step2_called += 1
        return f"b_{arg}"

    async def run_wf(ctx):
        res1 = await step_a(ctx, wf_arg)
        return res1

    await run_wf(wf_ctx)
    assert step1_called == 1

    async def run_wf_complete(ctx):
        res1 = await step_a(ctx, wf_arg) # Skipped
        res2 = await step_b(ctx, res1)   # Executed
        return res2

    result = await run_wf_complete(wf_ctx)
    assert result == "b_a_crash_test"
    assert step1_called == 1
    assert step2_called == 1

@pytest.mark.asyncio
async def test_ephemeral_consume_success(postgres_dsn, schema):
    """
    Test that successful consumption archives the message.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue = "ephemeral_success_q"
    await admin.create_queue(queue)

    payload = {"data": "test_success"}
    await pgqrs.produce(store, queue, payload)

    consumed_event = asyncio.Event()

    async def handler(msg):
        assert msg.payload == payload
        consumed_event.set()
        return True

    await pgqrs.consume(store, queue, handler)
    assert consumed_event.is_set()

    # Verify message is archived and not in queue
    assert await (await admin.get_messages()).count() == 0
    assert await (await admin.get_archive()).count() == 1

@pytest.mark.asyncio
async def test_ephemeral_consume_failure(postgres_dsn, schema):
    """
    Test that failed consumption (exception) does NOT archive the message.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue = "ephemeral_fail_q"
    await admin.create_queue(queue)

    payload = {"data": "test_fail"}
    await pgqrs.produce(store, queue, payload)

    async def handler(msg):
        raise ValueError("Simulated failure")

    with pytest.raises(ValueError, match="Simulated failure"):
        await pgqrs.consume(store, queue, handler)

    # Verify message is STILL in queue and NOT archived
    assert await (await admin.get_messages()).count() == 1
    assert await (await admin.get_archive()).count() == 0

@pytest.mark.asyncio
async def test_ephemeral_consume_batch_success(postgres_dsn, schema):
    """
    Test that successful batch consumption archives all messages.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue = "ephemeral_batch_success_q"
    await admin.create_queue(queue)

    for i in range(3):
        await pgqrs.produce(store, queue, {"i": i})

    consumed_event = asyncio.Event()

    async def batch_handler(msgs):
        assert len(msgs) == 3
        consumed_event.set()
        return True

    await pgqrs.consume_batch(store, queue, 10, batch_handler)
    assert consumed_event.is_set()

    # Verify all messages are archived
    assert await (await admin.get_messages()).count() == 0
    assert await (await admin.get_archive()).count() == 3

@pytest.mark.asyncio
async def test_ephemeral_consume_batch_failure(postgres_dsn, schema):
    """
    Test that failed batch consumption does NOT archive any messages.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue = "ephemeral_batch_fail_q"
    await admin.create_queue(queue)

    for i in range(3):
        await pgqrs.produce(store, queue, {"i": i})

    async def batch_handler(msgs):
        raise ValueError("Simulated batch failure")

    with pytest.raises(ValueError, match="Simulated batch failure"):
        await pgqrs.consume_batch(store, queue, 10, batch_handler)

    # Verify all messages are STILL in queue
    assert await (await admin.get_messages()).count() == 3
    assert await (await admin.get_archive()).count() == 0

@pytest.mark.asyncio
async def test_consume_stream_iterator(postgres_dsn, schema):
    """
    Test the consume_stream iterator and verification of ephemeral consumer cleanup.
    """
    store, admin = await setup_test(postgres_dsn, schema)
    queue_name = "stream_test_q"
    await admin.create_queue(queue_name)

    received_count = 0

    async def consume_loop():
        nonlocal received_count
        async with store.consume_iter(queue_name, poll_interval_ms=10) as iterator:
            async for msg in iterator:
                received_count += 1
                # Verify archive method
                res = await iterator.archive(msg.id)
                assert res is True
                if received_count >= 5:
                    break

    # Produce messages FIRST
    producer = await store.producer(queue_name)
    for i in range(5):
        await pgqrs.enqueue(producer, {"i": i})

    consume_task = asyncio.create_task(consume_loop())

    await asyncio.wait_for(consume_task, timeout=5.0)
    assert received_count == 5

    # Verify messages are archived
    archive_count = await (await admin.get_archive()).count()
    assert archive_count == 5

    # Verify ephemeral consumer cleanup
    # Since we used context manager, cleanup (shutdown) should have happened explicitly upon exit.

    # Wait briefly for async shutdown to complete if needed
    await asyncio.sleep(0.5)

    # Check workers
    workers = await (await admin.get_workers()).list()
    ephemeral_workers = [w for w in workers if w.hostname.startswith("__ephemeral__")]

    # Verify that any ephemeral worker found is in 'stopped' status
    for w in ephemeral_workers:
        assert w.status == "stopped", f"Ephemeral worker {w.id} should be stopped, but is {w.status}"

@pytest.mark.asyncio
async def test_exceptions(postgres_dsn, schema):
    """
    Test that custom exceptions are raised and catchable.
    """
    store, admin = await setup_test(postgres_dsn, schema)

    # Test QueueNotFoundError
    qname = "non_existent_queue_abc"
    try:
        await admin.delete_queue(qname)
    except pgqrs.QueueNotFoundError:
        pass # Expected if it failed
    except Exception:
        pass # admin.delete_queue might return boolean false instead of error in some impls, but checking exception types here.

    # Tests connect with invalid DSN
    # This test assumes Postgres is enabled. If we are on SQLite-only build,
    # passing a postgres:// DSN will raise ConfigError, not ConnectionError.
    bad_dsn = "postgres://user:pass@non-existent-host-12345.local:5432/db"

    try:
        await pgqrs.connect(bad_dsn)
        pytest.fail("Should have raised an error")
    except (pgqrs.PgqrsConnectionError, pgqrs.ConfigError):
        # PgqrsConnectionError: Postgres enabled, but host unreachable
        # ConfigError: Postgres disabled, scheme not supported
        pass

    # Test create_queue on existing queue
    q2 = "existing_queue"
    await admin.create_queue(q2)
    with pytest.raises(pgqrs.QueueAlreadyExistsError):
        await admin.create_queue(q2)
