import asyncio
import asyncio

from typing import Any, Callable

import pytest
import pgqrs

from .conftest import requires_backend, TestBackend

Predicate = Callable[[Any], bool]
POLL_INTERVAL = 0.025


# --8<-- [start:wait_for_archived_count]
async def wait_for_archived_count(
    messages_table, queue_id: int, expected: int, timeout: float = 5.0
):
    async def poll():
        while True:
            archived = await messages_table.list_archived_by_queue(queue_id)
            if len(archived) == expected:
                return archived
            await asyncio.sleep(POLL_INTERVAL)

    return await asyncio.wait_for(poll(), timeout=timeout)


# --8<-- [end:wait_for_archived_count]


# --8<-- [start:wait_for_archived_message]
async def wait_for_archived_message(
    messages_table, queue_id: int, predicate: Predicate, timeout: float = 5.0
):
    async def poll():
        while True:
            archived = await messages_table.list_archived_by_queue(queue_id)
            for msg in archived:
                if predicate(msg):
                    return msg
            await asyncio.sleep(POLL_INTERVAL)

    return await asyncio.wait_for(poll(), timeout=timeout)


# --8<-- [end:wait_for_archived_message]


# --8<-- [start:basic_queue_setup]
async def basic_queue_setup(test_dsn, schema, queue_name: str):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_info = await store.queue(queue_name)
    queue_id = queue_info.id

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)
    messages_table = await store.get_messages()

    return store, queue_id, producer, consumer, messages_table


# --8<-- [end:basic_queue_setup]


# --8<-- [start:basic_queue_enqueue_one]
async def basic_queue_enqueue_one(producer, payload: dict):
    return await pgqrs.enqueue(producer, payload)


# --8<-- [end:basic_queue_enqueue_one]


# --8<-- [start:basic_queue_consumer_poll]
async def basic_queue_consumer_poll(store, consumer, handler):
    return asyncio.create_task(
        pgqrs.dequeue().worker(consumer).batch(1).handle(handler).poll(store)
    )


# --8<-- [end:basic_queue_consumer_poll]


# --8<-- [start:basic_queue_py_poll_and_interrupt]
# Assumes `store` and `consumer` already exist.
async def basic_queue_py_poll_and_interrupt(store, consumer, handler):
    task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).batch(1).handle(handler).poll(store)
    )
    await consumer.interrupt()
    return task


# --8<-- [end:basic_queue_py_poll_and_interrupt]


# --8<-- [start:basic_queue_single_consumer_poll]
@pytest.mark.asyncio
async def test_basic_queue_single_consumer_handler_poll(test_dsn, schema):
    queue_name = "guide_basic_queue_single"
    store, queue_id, producer, consumer, messages_table = await basic_queue_setup(
        test_dsn, schema, queue_name
    )

    async def handler(_msg):
        return True

    consumer_task = await basic_queue_consumer_poll(store, consumer, handler)

    payload = {"k": "v"}
    msg_id = await basic_queue_enqueue_one(producer, payload)

    archived = await wait_for_archived_message(
        messages_table,
        queue_id,
        lambda msg: msg.id == msg_id and msg.consumer_worker_id == consumer.worker_id,
    )
    assert archived.payload == payload
    assert archived.consumer_worker_id == consumer.worker_id

    await consumer.interrupt()
    with pytest.raises(Exception):
        await asyncio.wait_for(consumer_task, timeout=5)

    assert await consumer.status() == pgqrs.WorkerStatus.Suspended


# --8<-- [end:basic_queue_single_consumer_poll]


@pytest.mark.asyncio
async def test_basic_queue_two_consumers_poll_batch_handoff(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "guide_basic_queue_handoff"
    queue_info = await store.queue(queue_name)
    queue_id = queue_info.id

    producer = await store.producer(queue_name)
    consumer_a = await store.consumer(queue_name)
    consumer_b = await store.consumer(queue_name)
    messages_table = await store.get_messages()

    async def handle_batch(_msgs):
        return True

    # --8<-- [start:basic_queue_py_handoff_start_consumer_a]
    task_a = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_a)
        .batch(5)
        .handle_batch(handle_batch)
        .poll(store)
    )
    # --8<-- [end:basic_queue_py_handoff_start_consumer_a]

    id1 = await pgqrs.enqueue(producer, {"n": 1})
    archived_a = await wait_for_archived_message(
        messages_table,
        queue_id,
        lambda msg: msg.id == id1 and msg.consumer_worker_id == consumer_a.worker_id,
    )
    assert archived_a.payload == {"n": 1}

    # --8<-- [start:basic_queue_py_handoff_interrupt_consumer_a]
    await consumer_a.interrupt()
    with pytest.raises(Exception):
        await asyncio.wait_for(task_a, timeout=5)
    # --8<-- [end:basic_queue_py_handoff_interrupt_consumer_a]

    # --8<-- [start:basic_queue_py_handoff_start_consumer_b]
    task_b = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_b)
        .batch(5)
        .handle_batch(handle_batch)
        .poll(store)
    )
    # --8<-- [end:basic_queue_py_handoff_start_consumer_b]

    id2 = await pgqrs.enqueue(producer, {"n": 2})
    archived_b = await wait_for_archived_message(
        messages_table,
        queue_id,
        lambda msg: msg.id == id2 and msg.consumer_worker_id == consumer_b.worker_id,
    )
    assert archived_b.payload == {"n": 2}

    # --8<-- [start:basic_queue_py_handoff_interrupt_consumer_b]
    await consumer_b.interrupt()
    with pytest.raises(Exception):
        await asyncio.wait_for(task_b, timeout=5)
    # --8<-- [end:basic_queue_py_handoff_interrupt_consumer_b]


@pytest.mark.asyncio
async def test_basic_queue_two_consumers_continuous_handler_poll_interrupt(
    test_dsn, schema
):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "guide_basic_queue_continuous"
    queue_info = await store.queue(queue_name)
    queue_id = queue_info.id

    producer = await store.producer(queue_name)
    consumer_a = await store.consumer(queue_name)
    consumer_b = await store.consumer(queue_name)
    messages_table = await store.get_messages()

    async def handle_batch(_msgs):
        return True

    # --8<-- [start:basic_queue_py_continuous_start_two_consumers]
    task_a = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_a)
        .batch(10)
        .handle_batch(handle_batch)
        .poll(store)
    )
    task_b = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_b)
        .batch(10)
        .handle_batch(handle_batch)
        .poll(store)
    )
    # --8<-- [end:basic_queue_py_continuous_start_two_consumers]

    for idx in range(40):
        await pgqrs.enqueue(producer, {"i": idx})

    archived = await wait_for_archived_count(messages_table, queue_id, 40, timeout=10)
    assert len(archived) == 40

    # --8<-- [start:basic_queue_py_continuous_interrupt_two_consumers]
    try:
        await consumer_a.interrupt()
    except pgqrs.StateTransitionError:
        pass

    try:
        await consumer_b.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(task_a, timeout=5)
    with pytest.raises(Exception):
        await asyncio.wait_for(task_b, timeout=5)

    assert await consumer_a.status() == pgqrs.WorkerStatus.Suspended
    assert await consumer_b.status() == pgqrs.WorkerStatus.Suspended
    # --8<-- [end:basic_queue_py_continuous_interrupt_two_consumers]

from pgqrs import run as pgqrs_run, step as pgqrs_step, workflow as pgqrs_workflow
from pgqrs.decorators import workflow as workflow_def, step as step_def

# --8<-- [start:wait_for_workflow_complete]
async def wait_for_workflow_complete(store, message_id: int, timeout: float = 10.0):
    async def poll():
        while True:
            try:
                runs = await (await store.get_workflow_runs()).list()
                record = next((r for r in runs if r.id == message_id), None)
                if not record:
                    # In this version workflows list run by passing the trigger msg_id
                    # Actually getting run_id via run().result() is easier.
                    continue
                if record and record.status in ("SUCCESS", "ERROR", "PAUSED"):
                    return record
            except Exception:
                pass
            await asyncio.sleep(POLL_INTERVAL)

    return await asyncio.wait_for(poll(), timeout=timeout)
# --8<-- [end:wait_for_workflow_complete]

@pytest.mark.asyncio
async def test_basic_workflow_ephemeral_trigger(test_dsn, schema):
    # --8<-- [start:basic_workflow_setup]
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # --8<-- [start:basic_workflow_define]
    @workflow_def(name="process_task")
    async def process_task_wf(ctx, input_data: dict) -> dict:
        # --8<-- [start:basic_workflow_step_call]
        @step_def
        async def process_item_step(step_ctx):
            return {"processed": "task_data", "status": "done"}

        result = await process_item_step(ctx)
        # --8<-- [end:basic_workflow_step_call]
        return result
    # --8<-- [end:basic_workflow_define]

    await pgqrs.workflow().name("process_task").store(store).create()
    # --8<-- [end:basic_workflow_setup]

    # --8<-- [start:basic_workflow_trigger_ephemeral]
    input_data = {"id": 1, "payload": "Hello pgqrs"}
    msg_trigger = pgqrs.workflow().name("process_task").store(store).trigger(input_data)
    msg = await msg_trigger.execute()
    # --8<-- [end:basic_workflow_trigger_ephemeral]

    assert msg.id > 0
    assert msg.payload == input_data

    # --8<-- [start:basic_workflow_consumer_start]
    consumer = await store.consumer("process_task")
    consumer_task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).handle_workflow(process_task_wf).poll(store)
    )
    # --8<-- [end:basic_workflow_consumer_start]

    # --8<-- [start:basic_workflow_get_result]
    result = await pgqrs.run().message(msg).store(store).result()
    print(f"Workflow Result: {result}")
    # --8<-- [end:basic_workflow_get_result]

    assert result["processed"] == "task_data"

    try:
        await consumer.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(consumer_task, timeout=5)

@pytest.mark.asyncio
async def test_durable_workflow_crash_recovery(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    crash_state = {"has_crashed": False}

    # --8<-- [start:durable_workflow_crash_define]
    @workflow_def(name="crash_recovery_wf")
    async def crash_recovery_wf(run_ctx, input_data: dict) -> dict:
        @step_def
        async def step1(step_ctx):
            return {"data": "from step 1"}

        await step1(run_ctx)

        if not crash_state["has_crashed"]:
            crash_state["has_crashed"] = True
            raise BaseException("TestCrash")

        @step_def
        async def step2(step_ctx):
            return "step2_done"

        await step2(run_ctx)

        return {"done": True}
    # --8<-- [end:durable_workflow_crash_define]

    await pgqrs.workflow().name("crash_recovery_wf").store(store).create()
    consumer = await store.consumer("crash_recovery_wf")

    # --8<-- [start:durable_workflow_crash_first_run]
    consumer_task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).handle_workflow(crash_recovery_wf).poll(store)
    )
    # --8<-- [end:durable_workflow_crash_first_run]

    # --8<-- [start:durable_workflow_crash_trigger]
    input_data = {"test": True}
    msg_trigger = pgqrs.workflow().name("crash_recovery_wf").store(store).trigger(input_data)
    msg = await msg_trigger.execute()
    # --8<-- [end:durable_workflow_crash_trigger]

    # --8<-- [start:durable_workflow_crash_simulate]
    async def wait_for_step1_success():
        while True:
            steps = await (await store.get_workflow_steps()).list()
            step = next(
                (
                    s
                    for s in steps
                    if s.step_name == "step1"
                    and s.status == pgqrs.WorkflowStatus.Success
                ),
                None,
            )
            if step:
                return step
            await asyncio.sleep(POLL_INTERVAL)

    await asyncio.wait_for(wait_for_step1_success(), timeout=10.0)

    try:
        await consumer.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(BaseException, match="TestCrash"):
         await asyncio.wait_for(consumer_task, timeout=5)

    await pgqrs.admin(store).release_worker_messages(consumer.worker_id)
    # --8<-- [end:durable_workflow_crash_simulate]

    await asyncio.sleep(0.1)

    # --8<-- [start:durable_workflow_crash_recovery]
    consumer_2 = await store.consumer("crash_recovery_wf")
    consumer_task_2 = asyncio.create_task(
        pgqrs.dequeue().worker(consumer_2).handle_workflow(crash_recovery_wf).poll(store)
    )

    result = await pgqrs.run().message(msg).store(store).result()
    # --8<-- [end:durable_workflow_crash_recovery]

    assert result["done"] is True

    try:
        await consumer_2.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(consumer_task_2, timeout=5)

@pytest.mark.asyncio
async def test_durable_workflow_transient_error(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # --8<-- [start:durable_workflow_transient_define]
    @workflow_def(name="transient_error_wf")
    async def transient_error_wf(run_ctx, input_data: dict) -> dict:
        @step_def
        async def api_call(step_ctx):
            if True:
                err = pgqrs.TransientStepError("Connection timed out")
                err.code = "TIMEOUT"
                err.retry_after = 30.0
                raise err

        await api_call(run_ctx)
        return {"done": True}
    # --8<-- [end:durable_workflow_transient_define]

    await pgqrs.workflow().name("transient_error_wf").store(store).create()
    consumer = await store.consumer("transient_error_wf")

    msg_trigger = pgqrs.workflow().name("transient_error_wf").store(store).trigger(True)
    msg = await msg_trigger.execute()

    # --8<-- [start:durable_workflow_transient_run]
    consumer_task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).handle_workflow(transient_error_wf).poll(store)
    )
    # --8<-- [end:durable_workflow_transient_run]

    async def wait_for_api_call_error():
        while True:
            steps = await (await store.get_workflow_steps()).list()
            step = next(
                (
                    s
                    for s in steps
                    if s.step_name == "api_call"
                    and s.status == pgqrs.WorkflowStatus.Error
                ),
                None,
            )
            if step:
                return step
            await asyncio.sleep(POLL_INTERVAL)

    await asyncio.wait_for(wait_for_api_call_error(), timeout=15.0)

    try:
        await consumer.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(consumer_task, timeout=5)

    # --8<-- [start:durable_workflow_transient_inspect]
    workflow = await (await store.get_workflows()).get_by_name("transient_error_wf")
    runs = await (await store.get_workflow_runs()).list()
    run_rec = next((r for r in runs if r.workflow_id == workflow.id), None)

    assert run_rec.status == pgqrs.WorkflowStatus.Running

    steps = await (await store.get_workflow_steps()).list()
    step_rec = next((s for s in steps if s.run_id == run_rec.id), None)

    assert step_rec.status == pgqrs.WorkflowStatus.Error
    assert step_rec.retry_at is not None
    # --8<-- [end:durable_workflow_transient_inspect]

    archived = await (await store.get_messages()).list_archived_by_queue(msg.queue_id)
    assert len(archived) == 0

@pytest.mark.asyncio
async def test_durable_workflow_pause(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # --8<-- [start:durable_workflow_pause_define]
    @workflow_def(name="pause_wf")
    async def pause_wf(run_ctx, input_data: dict) -> dict:
        @step_def
        async def step1(step_ctx):
                # We can achieve pause by using the run context pause
                await run_ctx.pause("Waiting for approval", 60)
                raise pgqrs.PausedError("Waiting for approval")

        await step1(run_ctx)
        return {"done": True}
    # --8<-- [end:durable_workflow_pause_define]

    await pgqrs.workflow().name("pause_wf").store(store).create()
    consumer = await store.consumer("pause_wf")

    msg_trigger = pgqrs.workflow().name("pause_wf").store(store).trigger(True)
    msg = await msg_trigger.execute()

    # --8<-- [start:durable_workflow_pause_run]
    consumer_task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).handle_workflow(pause_wf).poll(store)
    )
    # --8<-- [end:durable_workflow_pause_run]

    async def wait_for_pause():
        while True:
            runs = await (await store.get_workflow_runs()).list()
            run_rec = next((r for r in runs if r.id == msg.id), None)
            if run_rec and run_rec.status == pgqrs.WorkflowStatus.Paused:
                return run_rec
            await asyncio.sleep(POLL_INTERVAL)

    await asyncio.wait_for(wait_for_pause(), timeout=15.0)

    try:
        await consumer.interrupt()
    except pgqrs.StateTransitionError:
        pass

    with pytest.raises(Exception):
        await asyncio.wait_for(consumer_task, timeout=5)

    # --8<-- [start:durable_workflow_pause_inspect]
    workflow = await (await store.get_workflows()).get_by_name("pause_wf")
    runs = await (await store.get_workflow_runs()).list()
    run_rec = next((r for r in runs if r.workflow_id == workflow.id), None)

    assert run_rec.status == pgqrs.WorkflowStatus.Paused
    # --8<-- [end:durable_workflow_pause_inspect]
