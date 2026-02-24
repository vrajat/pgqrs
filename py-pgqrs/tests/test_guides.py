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
@requires_backend(TestBackend.SQLITE)
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

    assert await consumer.status() == "SUSPENDED"


# --8<-- [end:basic_queue_single_consumer_poll]


@pytest.mark.asyncio
@requires_backend(TestBackend.SQLITE)
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
@requires_backend(TestBackend.SQLITE)
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
    await consumer_a.interrupt()
    await consumer_b.interrupt()

    with pytest.raises(Exception):
        await asyncio.wait_for(task_a, timeout=5)
    with pytest.raises(Exception):
        await asyncio.wait_for(task_b, timeout=5)

    assert await consumer_a.status() == "SUSPENDED"
    assert await consumer_b.status() == "SUSPENDED"
    # --8<-- [end:basic_queue_py_continuous_interrupt_two_consumers]
