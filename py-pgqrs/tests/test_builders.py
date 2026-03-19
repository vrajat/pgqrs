import asyncio
from datetime import datetime, timezone

import pytest

import pgqrs
from pgqrs.decorators import WorkflowDef, workflow, step


async def wait_for_worker(store, worker_id: int, expected_status: str):
    workers = await store.get_workers()
    for _ in range(80):
        worker = await workers.get(worker_id)
        if worker.status == expected_status:
            return worker
        await asyncio.sleep(0.05)
    raise AssertionError(f"worker {worker_id} did not reach status {expected_status}")


async def wait_for_heartbeat_advance(store, worker_id: int, after: str):
    workers = await store.get_workers()
    after_dt = datetime.fromisoformat(after)
    for _ in range(80):
        worker = await workers.get(worker_id)
        if datetime.fromisoformat(worker.heartbeat_at) > after_dt:
            return worker
        await asyncio.sleep(0.05)
    raise AssertionError(f"worker {worker_id} heartbeat did not advance")


@pytest.mark.asyncio
async def test_builder_api(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # 1. Test WorkflowBuilder
    wf_name = "builder_test_wf"
    wf = await pgqrs.workflow().name(wf_name).store(store).create()
    assert wf.name == wf_name
    assert wf.id > 0

    # 2. Test WorkflowTriggerBuilder
    input_data = {"foo": "bar"}
    msg = (
        await pgqrs.workflow().name(wf_name).store(store).trigger(input_data).execute()
    )
    assert msg.id > 0
    # The message payload for a workflow trigger contains the input
    assert msg.payload == input_data

    # 3. Test RunBuilder
    run = await pgqrs.run().message(msg).store(store).execute()
    assert run.id() > 0

    # 4. Test StepBuilder
    step_name = "step_1"
    step_res = await pgqrs.step().run(run).name(step_name).execute()
    assert step_res.status == "EXECUTE"
    assert step_res.guard is not None

    # Complete step via guard
    await step_res.guard.success({"result": "ok"})

    # Verify idempotency (should be SKIPPED now)
    step_res_retry = await pgqrs.step().run(run).name(step_name).execute()
    assert step_res_retry.status == "SKIPPED"
    assert step_res_retry.value == {"result": "ok"}

    # 5. Test manual step completion on Run
    step_2_name = "step_2"
    step_res_2 = await pgqrs.step().run(run).name(step_2_name).execute()
    assert step_res_2.status == "EXECUTE"

    await run.complete_step(step_2_name, {"manual": "ok"})

    step_res_2_retry = await pgqrs.step().run(run).name(step_2_name).execute()
    assert step_res_2_retry.status == "SKIPPED"
    assert step_res_2_retry.value == {"manual": "ok"}


@pytest.mark.asyncio
async def test_workflow_decorators_with_builders(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    wf_name = "decorated_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    step_called = 0

    @step
    async def my_step(ctx, val):
        nonlocal step_called
        step_called += 1
        return f"echo_{val}"

    @workflow(name="decorated_wf")
    async def my_workflow(ctx, val):
        return await my_step(ctx, val)

    # Trigger and get run handle
    msg = await pgqrs.workflow().name(wf_name).store(store).trigger("hello").execute()
    assert msg.payload == "hello"
    run = await pgqrs.run().message(msg).store(store).execute()

    # Execute workflow
    result = await my_workflow(run, "hello")
    assert result == "echo_hello"
    assert step_called == 1

    # Re-run (should skip step)
    result2 = await my_workflow(run, "hello")
    assert result2 == "echo_hello"
    assert step_called == 1  # Still 1


@pytest.mark.asyncio
async def test_dequeue_builder_single_handler_rejects_batch_gt_one(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "builder_single_handler_batch_validation"
    await store.queue(queue_name)
    consumer = await store.consumer(queue_name)

    async def handler(_msg):
        return True

    with pytest.raises(pgqrs.ValidationError):
        await pgqrs.dequeue().worker(consumer).batch(2).handle(handler).poll(store)


@pytest.mark.asyncio
async def test_dequeue_builder_poll_updates_heartbeat_while_idle(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    config.heartbeat_interval_seconds = 1
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "builder_poll_heartbeat"
    await store.queue(queue_name)
    consumer = await store.consumer(queue_name)
    fixed_now = datetime.now(timezone.utc).isoformat()

    async def handler(_msg):
        return True

    task = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer)
        .batch(1)
        .at(fixed_now)
        .poll_interval(2000)
        .handle(handler)
        .poll(store)
    )

    worker_before = await wait_for_worker(store, consumer.worker_id, "polling")
    worker_after = await wait_for_heartbeat_advance(
        store, consumer.worker_id, worker_before.heartbeat_at
    )

    assert datetime.fromisoformat(worker_after.heartbeat_at) > datetime.fromisoformat(
        worker_before.heartbeat_at
    )

    try:
        await consumer.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(task, timeout=5)
