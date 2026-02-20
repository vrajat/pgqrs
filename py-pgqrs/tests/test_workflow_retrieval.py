import pytest
import asyncio
import pgqrs

async def setup_test(dsn, schema):
    config = pgqrs.Config(dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    await admin.verify()
    return store, admin

@pytest.mark.asyncio
async def test_workflow_result_get_non_blocking(test_dsn, schema):
    """
    Tests the non-blocking get() API.
    Returns None while running/queued, and result once finished.
    """
    store, admin = await setup_test(test_dsn, schema)

    # Create definition
    wf_name = "get_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    input_data = {"msg": "get_test"}
    msg = await pgqrs.workflow().name(wf_name).store(store).trigger(input_data).execute()

    # 1. Check immediately - should be None (not created yet)
    res = await pgqrs.run().store(store).message(msg).get()
    assert res is None

    # 2. Start the run
    run = await pgqrs.run().message(msg).store(store).execute()
    await run.start()

    # 3. Check again - still None (Running)
    res = await pgqrs.run().store(store).message(msg).get()
    assert res is None

    # 4. Complete the run
    await run.success({"msg": "finished"})

    # 5. Check again - should be dict
    res = await pgqrs.run().store(store).message(msg).get()
    assert res == {"msg": "finished"}

@pytest.mark.asyncio
async def test_workflow_result_polling(test_dsn, schema):
    """
    Tests the polling result() API for successful workflows.
    """
    store, admin = await setup_test(test_dsn, schema)

    wf_name = "polling_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    input_data = {"msg": "poll_me"}
    msg = await pgqrs.workflow().name(wf_name).store(store).trigger(input_data).execute()

    async def worker_sim():
        await asyncio.sleep(1)
        run = await pgqrs.run().message(msg).store(store).execute()
        await run.start()
        await run.success({"msg": "polled_success"})

    asyncio.create_task(worker_sim())

    # Use the polling API
    actual_output = await pgqrs.run().store(store).message(msg).result()
    assert actual_output == {"msg": "polled_success"}

@pytest.mark.asyncio
async def test_workflow_error_polling(test_dsn, schema):
    """
    Tests the result() API handling of workflow failures.
    Should raise PgqrsError with execution failure details.
    """
    store, admin = await setup_test(test_dsn, schema)

    wf_name = "error_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    msg = await pgqrs.workflow().name(wf_name).store(store).trigger({"msg": "fail"}).execute()

    async def worker_sim():
        await asyncio.sleep(1)
        run = await pgqrs.run().message(msg).store(store).execute()
        await run.start()
        await run.fail("intentional failure")

    asyncio.create_task(worker_sim())

    # Use the polling API and expect error
    with pytest.raises(pgqrs.PgqrsError) as excinfo:
        await pgqrs.run().store(store).message(msg).result()

    # The error message should contain the failure detail
    assert "intentional failure" in str(excinfo.value)
