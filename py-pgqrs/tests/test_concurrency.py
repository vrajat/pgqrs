import pytest
from datetime import datetime, timedelta, timezone

import pytest
import pgqrs
from pgqrs import Run


async def setup_test(dsn, schema):
    config = pgqrs.Config(dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    await admin.verify()
    return store, admin


async def dequeue_one(consumer):
    msgs = await consumer.dequeue(batch_size=1)
    assert len(msgs) == 1
    return msgs[0]


async def execute_step(run: Run, name: str, current_time, action):
    step_result = await run.acquire_step(name, current_time=current_time)
    if step_result.status == "SKIPPED":
        return step_result.value
    return await action(step_result.guard)


async def step_success(guard):
    await guard.success("step_success")
    return "step_success"


async def step1(guard):
    await guard.success("step1_done")
    return "step1_done"


async def step2(guard):
    await guard.success("step2_done")
    return "step2_done"


@pytest.mark.asyncio
async def test_zombie_consumer_race_condition(test_dsn, schema):
    """
    Tests that a consumer cannot delete or archive a message if it has lost the lease.
    Matches Rust test_zombie_consumer_race_condition.
    """
    store, admin = await setup_test(test_dsn, schema)
    queue_name = "race_condition_queue"
    await store.queue(queue_name)

    producer = await store.producer(queue_name)
    consumer_a = await store.consumer(queue_name)
    consumer_b = await store.consumer(queue_name)

    payload = {"task": "slow_process"}
    msg_id = await producer.enqueue(payload)

    # Consumer A dequeues with short visibility timeout
    msgs_a = await consumer_a.dequeue_many_with_delay(1, 1)
    assert len(msgs_a) == 1
    assert msgs_a[0].id == msg_id

    # Simulate lease reclamation for Consumer A
    released = await admin.release_worker_messages(consumer_a.worker_id)
    assert released == 1

    # Consumer B dequeues the SAME message (stealing the lock)
    msgs_b = await consumer_b.dequeue(batch_size=1)
    assert len(msgs_b) == 1
    assert msgs_b[0].id == msg_id

    # Consumer A tries to DELETE -> Should FAIL (return false)
    deleted_a = await consumer_a.delete(msg_id)
    assert deleted_a is False

    # Consumer A tries to ARCHIVE -> Should FAIL
    archived_a = await consumer_a.archive(msg_id)
    assert archived_a is False

    # Consumer B completes work and DELETES -> Should SUCCEED
    deleted_b = await consumer_b.delete(msg_id)
    assert deleted_b is True


@pytest.mark.asyncio
async def test_zombie_consumer_batch_ops(test_dsn, schema):
    """
    Tests that a consumer cannot delete_many or archive_many messages if it has lost the lease.
    Matches Rust test_zombie_consumer_batch_ops.
    """
    store, admin = await setup_test(test_dsn, schema)
    queue_name = "batch_race_queue"
    await store.queue(queue_name)

    producer = await store.producer(queue_name)
    consumer_a = await store.consumer(queue_name)
    consumer_b = await store.consumer(queue_name)

    # Enqueue 2 messages
    msg1_id = await producer.enqueue(1)
    msg2_id = await producer.enqueue(2)

    # A dequeues both with short visibility timeout
    future_time = datetime.now(timezone.utc) + timedelta(seconds=1)
    msgs_a = await consumer_a.dequeue_at(2, 1, future_time.isoformat())
    assert len(msgs_a) == 2

    # Simulate reclamation of messages from A
    released = await admin.release_worker_messages(consumer_a.worker_id)
    assert released == 2

    # B dequeues both
    msgs_b = await consumer_b.dequeue(batch_size=2)
    assert len(msgs_b) == 2

    # A tries delete_many -> Should return [False, False]
    results_a = await consumer_a.delete_many([msg1_id, msg2_id])
    assert results_a == [False, False]

    # A tries archive_many -> Should return [False, False]
    arch_results_a = await consumer_a.archive_many([msg1_id, msg2_id])
    assert arch_results_a == [False, False]

    # B deletes -> [True, True]
    results_b = await consumer_b.delete_many([msg1_id, msg2_id])
    assert results_b == [True, True]


@pytest.mark.asyncio
async def test_concurrent_visibility_extension(test_dsn, schema):
    """
    Tests that only the owner of a message can extend its visibility.
    Matches Rust test_concurrent_visibility_extension.
    """
    store, admin = await setup_test(test_dsn, schema)
    queue_name = "concurrent_vis_queue"
    await store.queue(queue_name)

    producer = await store.producer(queue_name)
    consumer_a = await store.consumer(queue_name)
    consumer_b = await store.consumer(queue_name)

    msg_id = await producer.enqueue({"foo": "bar"})

    # Consumer A dequeues
    msgs_a = await consumer_a.dequeue(batch_size=1)
    assert len(msgs_a) == 1
    assert msgs_a[0].id == msg_id

    # Consumer B tries to extend visibility -> SHOULD FAIL
    extended_by_b = await consumer_b.extend_vt(msg_id, 10)
    assert extended_by_b is False

    # Consumer A tries to extend visibility -> SHOULD SUCCEED
    extended_by_a = await consumer_a.extend_vt(msg_id, 10)
    assert extended_by_a is True


@pytest.mark.asyncio
async def test_workflow_scenario_success(test_dsn, schema):
    store, admin = await setup_test(test_dsn, schema)
    wf_name = "scenario_success"
    await pgqrs.workflow().name(wf_name).store(store).create()

    consumer = await store.consumer(wf_name)
    message = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "success"})
        .execute()
    )

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()
    await execute_step(run, "step1", run.current_time, step_success)
    await run.complete({"done": True})

    archived = await consumer.archive(msg.id)
    assert archived is True

    archived_entries = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived_entries) == 1

    workflow = await (await store.get_workflows()).get_by_name(wf_name)
    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "SUCCESS"

    steps = await (await store.get_workflow_steps()).list()
    step_entry = next(entry for entry in steps if entry.run_id == run_entry.id)
    assert step_entry.status == "SUCCESS"


@pytest.mark.asyncio
async def test_workflow_scenario_permanent_error(test_dsn, schema):
    store, admin = await setup_test(test_dsn, schema)
    wf_name = "scenario_permanent_error"
    await pgqrs.workflow().name(wf_name).store(store).create()

    consumer = await store.consumer(wf_name)
    message = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "perm_error"})
        .execute()
    )

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()

    async def permanent_error_step(guard):
        await guard.fail("File not found")
        return None

    await execute_step(run, "download", run.current_time, permanent_error_step)
    await run.fail("File not found")

    archived = await consumer.archive(msg.id)
    assert archived is True

    archived_entries = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived_entries) == 1

    workflow = await (await store.get_workflows()).get_by_name(wf_name)
    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "ERROR"
    assert "File not found" in str(run_entry.error)

    steps = await (await store.get_workflow_steps()).list()
    step_entry = next(entry for entry in steps if entry.run_id == run_entry.id)
    assert step_entry.status == "ERROR"
    assert "File not found" in str(step_entry.error)


@pytest.mark.asyncio
async def test_workflow_scenario_crash_recovery(test_dsn, schema):
    store, admin = await setup_test(test_dsn, schema)
    wf_name = "scenario_crash_recovery"
    await pgqrs.workflow().name(wf_name).store(store).create()

    consumer = await store.consumer(wf_name)
    message = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "crash_recovery"})
        .execute()
    )

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()
    await execute_step(run, "step1", run.current_time, step1)

    with pytest.raises(RuntimeError, match="TestCrash"):
        raise RuntimeError("TestCrash")

    archived = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived) == 0

    workflow = await (await store.get_workflows()).get_by_name(wf_name)
    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "RUNNING"

    steps = await (await store.get_workflow_steps()).list()
    step_entries = [entry for entry in steps if entry.run_id == run_entry.id]
    assert len(step_entries) == 1
    assert step_entries[0].step_name == "step1"
    assert step_entries[0].status == "SUCCESS"

    await admin.release_worker_messages(consumer.worker_id)

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()
    await execute_step(run, "step1", run.current_time, step1)
    await execute_step(run, "step2", run.current_time, step2)
    await run.complete({"done": True})
    assert await consumer.archive(msg.id) is True

    archived = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived) == 1

    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "SUCCESS"

    steps = await (await store.get_workflow_steps()).list()
    step_count = len([entry for entry in steps if entry.run_id == run_entry.id])
    assert step_count == 2


@pytest.mark.asyncio
async def test_workflow_scenario_transient_error(test_dsn, schema):
    store, admin = await setup_test(test_dsn, schema)
    wf_name = "scenario_transient_error"
    await pgqrs.workflow().name(wf_name).store(store).create()

    consumer = await store.consumer(wf_name)
    message = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "transient_error"})
        .execute()
    )

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()

    async def transient_step(guard):
        await guard.fail_transient("TIMEOUT", "Connection timed out", retry_after=30.0)
        raise pgqrs.TransientStepError("Connection timed out")

    with pytest.raises(pgqrs.TransientStepError):
        await execute_step(run, "api_call", run.current_time, transient_step)

    archived = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived) == 0

    workflow = await (await store.get_workflows()).get_by_name(wf_name)
    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "RUNNING"

    steps = await (await store.get_workflow_steps()).list()
    step_entry = next(entry for entry in steps if entry.run_id == run_entry.id)
    assert step_entry.status == "ERROR"
    assert step_entry.error.get("is_transient") is True
    assert step_entry.error.get("code") == "TIMEOUT"
    assert step_entry.retry_at is not None


@pytest.mark.asyncio
async def test_workflow_scenario_pause(test_dsn, schema):
    store, admin = await setup_test(test_dsn, schema)
    wf_name = "scenario_pause"
    await pgqrs.workflow().name(wf_name).store(store).create()

    consumer = await store.consumer(wf_name)
    message = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "pause"})
        .execute()
    )

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()

    async def pause_step(guard):
        await guard.fail_transient("PAUSED", "Waiting for approval", retry_after=60.0)
        await run.pause("Waiting for approval", 60)
        raise pgqrs.StepNotReadyError("Waiting for approval")

    with pytest.raises(pgqrs.StepNotReadyError):
        await execute_step(run, "step1", run.current_time, pause_step)

    archived = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived) == 0

    workflow = await (await store.get_workflows()).get_by_name(wf_name)
    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "PAUSED"
    assert "Waiting for approval" in str(run_entry.error)

    steps = await (await store.get_workflow_steps()).list()
    step_entry = next(entry for entry in steps if entry.run_id == run_entry.id)
    assert step_entry.status == "ERROR"
    assert step_entry.error.get("code") == "PAUSED"
    assert step_entry.error.get("is_transient") is True
    assert step_entry.retry_at is not None

    await admin.release_worker_messages(consumer.worker_id)

    msg = await dequeue_one(consumer)
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()

    with pytest.raises(pgqrs.StepNotReadyError):
        await execute_step(run, "step1", run.current_time, pause_step)

    resume_time = datetime.now(timezone.utc) + timedelta(seconds=61)
    run = run.with_time(resume_time.isoformat())

    await execute_step(run, "step1", run.current_time, step1)
    await run.complete({"done": True})
    assert await consumer.archive(msg.id) is True

    archived = await (await store.get_archive()).filter_by_fk(message.queue_id)
    assert len(archived) == 1

    runs = await (await store.get_workflow_runs()).list()
    run_entry = next(entry for entry in runs if entry.workflow_id == workflow.id)
    assert run_entry.status == "SUCCESS"

    steps = await (await store.get_workflow_steps()).list()
    step_entry = next(entry for entry in steps if entry.run_id == run_entry.id)
    assert step_entry.status == "SUCCESS"
