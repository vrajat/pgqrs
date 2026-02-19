import pytest
import asyncio
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


@pytest.mark.asyncio
async def test_workflow_success_lifecycle(test_dsn, schema):
    """
    Tests workflow.start(), step.execute(), workflow.complete(),
    and that start() fails if already SUCCESS.
    Matches Rust test_workflow_success_lifecycle.
    """
    store, admin = await setup_test(test_dsn, schema)

    # Create definition
    wf_name = "test_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    input_data = {"msg": "start"}
    msg = (
        await pgqrs.workflow().name(wf_name).store(store).trigger(input_data).execute()
    )

    run = await pgqrs.run().message(msg).store(store).execute()

    # Start
    await run.start()

    # Step 1
    step_name = "step1"
    step_res = await pgqrs.step().run(run).name(step_name).execute()
    assert step_res.status == "EXECUTE"
    await step_res.guard.success({"msg": "done"})

    # Finish Workflow
    await run.success({"msg": "all_done"})

    # Restart Workflow (should fail if currently SUCCESS)
    with pytest.raises(pgqrs.ValidationError):
        await run.start()


@pytest.mark.asyncio
async def test_workflow_failure_lifecycle(test_dsn, schema):
    """
    Tests workflow.fail() and that start() fails if already ERROR.
    Matches Rust test_workflow_failure_lifecycle.
    """
    store, admin = await setup_test(test_dsn, schema)

    wf_name = "fail_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    msg = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "fail"})
        .execute()
    )
    run = await pgqrs.run().message(msg).store(store).execute()

    await run.start()
    await run.fail("failed")

    # Restart Failed Workflow (should fail because ERROR is terminal)
    with pytest.raises(pgqrs.ValidationError):
        await run.start()


@pytest.mark.asyncio
async def test_workflow_pause_resume_lifecycle(test_dsn, schema):
    """
    Tests workflow.pause() and workflow.start() to resume.
    Matches Rust test_workflow_pause_resume_lifecycle.
    """
    store, admin = await setup_test(test_dsn, schema)

    wf_name = "pause_wf"
    await pgqrs.workflow().name(wf_name).store(store).create()

    msg = (
        await pgqrs.workflow()
        .name(wf_name)
        .store(store)
        .trigger({"msg": "pause"})
        .execute()
    )
    run = await pgqrs.run().message(msg).store(store).execute()

    await run.start()
    # Pause for 30 seconds
    await run.pause("wait", 30)

    # Resume
    await run.start()
    # If it doesn't raise, it resumed.

    await run.success({"msg": "done"})
