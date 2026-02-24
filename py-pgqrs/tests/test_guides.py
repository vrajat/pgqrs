import asyncio
from typing import Any, Callable

import pytest
import pgqrs

from .conftest import requires_backend, TestBackend

Predicate = Callable[[Any], bool]
POLL_INTERVAL = 0.025


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


@pytest.mark.asyncio
@requires_backend(TestBackend.SQLITE)
async def test_basic_queue_single_consumer_handler_poll(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "guide_basic_queue_single"
    queue_info = await store.queue(queue_name)
    queue_id = queue_info.id

    producer = await store.producer(queue_name)
    consumer = await store.consumer(queue_name)
    messages_table = await store.get_messages()

    async def handler(_msg):
        return True

    consumer_task = asyncio.create_task(
        pgqrs.dequeue().worker(consumer).batch(1).handle(handler).poll(store)
    )

    payload = {"k": "v"}
    msg_id = await pgqrs.enqueue(producer, payload)

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

    task_a = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_a)
        .batch(5)
        .handle_batch(handle_batch)
        .poll(store)
    )

    id1 = await pgqrs.enqueue(producer, {"n": 1})
    archived_a = await wait_for_archived_message(
        messages_table,
        queue_id,
        lambda msg: msg.id == id1 and msg.consumer_worker_id == consumer_a.worker_id,
    )
    assert archived_a.payload == {"n": 1}

    await consumer_a.interrupt()
    with pytest.raises(Exception):
        await asyncio.wait_for(task_a, timeout=5)

    task_b = asyncio.create_task(
        pgqrs.dequeue()
        .worker(consumer_b)
        .batch(5)
        .handle_batch(handle_batch)
        .poll(store)
    )

    id2 = await pgqrs.enqueue(producer, {"n": 2})
    archived_b = await wait_for_archived_message(
        messages_table,
        queue_id,
        lambda msg: msg.id == id2 and msg.consumer_worker_id == consumer_b.worker_id,
    )
    assert archived_b.payload == {"n": 2}

    await consumer_b.interrupt()
    with pytest.raises(Exception):
        await asyncio.wait_for(task_b, timeout=5)


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

    for idx in range(40):
        await pgqrs.enqueue(producer, {"i": idx})

    archived = await wait_for_archived_count(messages_table, queue_id, 40, timeout=10)
    assert len(archived) == 40

    await consumer_a.interrupt()
    await consumer_b.interrupt()

    with pytest.raises(Exception):
        await asyncio.wait_for(task_a, timeout=5)
    with pytest.raises(Exception):
        await asyncio.wait_for(task_b, timeout=5)

    assert await consumer_a.status() == "SUSPENDED"
    assert await consumer_b.status() == "SUSPENDED"
