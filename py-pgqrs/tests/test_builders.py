import pytest
import pgqrs
import asyncio
from pgqrs.decorators import workflow, step


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
    # The message payload for a workflow trigger is wrapped: {"workflow_id": id, "input": input}
    assert msg.payload["input"] == input_data
    assert msg.payload["workflow_id"] == wf.id

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

    @workflow
    async def my_workflow(ctx, val):
        return await my_step(ctx, val)

    # Trigger and get run handle
    msg = await pgqrs.workflow().name(wf_name).store(store).trigger("hello").execute()
    run = await pgqrs.run().message(msg).store(store).execute()

    # Execute workflow
    result = await my_workflow(run, "hello")
    assert result == "echo_hello"
    assert step_called == 1

    # Re-run (should skip step)
    result2 = await my_workflow(run, "hello")
    assert result2 == "echo_hello"
    assert step_called == 1  # Still 1
