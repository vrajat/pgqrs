import pytest
import pgqrs
import asyncio

@pytest.mark.asyncio
async def test_worker_list_status(postgres_dsn, schema):
    """
    Test worker listing and status APIs.
    """
    admin = pgqrs.Admin(postgres_dsn, schema=schema)
    await admin.install()
    queue_name = "worker_test_queue"
    await admin.create_queue(queue_name)

    producer = pgqrs.Producer(admin, queue_name, "prod_host", 5000)
    consumer = pgqrs.Consumer(admin, queue_name, "cons_host", 5001)

    # Ensure workers are registered
    # Producer registration happens on new()
    # Consumer registration happens on new()

    workers_handle = await admin.get_workers()

    # List workers
    worker_list = await workers_handle.list()
    assert len(worker_list) >= 2

    # Verify fields
    prod_worker = next(w for w in worker_list if w.hostname == "prod_host")
    cons_worker = next(w for w in worker_list if w.hostname == "cons_host")

    assert prod_worker.port == 5000
    assert cons_worker.port == 5001

    assert prod_worker.status == pgqrs.WorkerStatus.Ready

    # Verify Get
    fetched_worker = await workers_handle.get(prod_worker.id)
    assert fetched_worker.id == prod_worker.id
    assert fetched_worker.hostname == "prod_host"
