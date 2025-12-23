import asyncio
from pgqrs import Admin
from pgqrs.decorators import workflow, step
from pgqrs import PyWorkflow

# Global flag to control failure simulation
FAIL_ONCE = True

@step
async def step1(ctx: PyWorkflow, msg: str):
    print(f"  [Step 1] Executing with msg: {msg}")
    return f"processed_{msg}"

@step
async def step2(ctx: PyWorkflow, val: str):
    print(f"  [Step 2] Executing with val: {val}")
    if FAIL_ONCE:
        print("  [Step 2] SIMULATING FAILURE!")
        raise RuntimeError("Simulated failure in step 2")
    return f"step2_{val}"

@workflow
async def my_workflow(ctx: PyWorkflow, arg: str):
    print(f"[Workflow] Starting with arg: {arg}")
    res1 = await step1(ctx, arg)
    print(f"[Workflow] Step 1 result: {res1}")
    res2 = await step2(ctx, res1)
    print(f"[Workflow] Step 2 result: {res2}")
    return res2

async def run_resumable_workflow(dsn):
    global FAIL_ONCE

    admin = Admin(dsn, None)
    await admin.install()

    wf_name = "resumable_workflow"
    wf_arg = "data"

    print("\n--- RUN 1: Expecting Failure ---")
    wf_ctx = await admin.create_workflow(wf_name, wf_arg)
    print(f"Created workflow ID: {wf_ctx.id()}")

    try:
        await my_workflow(wf_ctx, wf_arg)
    except Exception as e:
        print(f"Run 1 failed as expected: {e}")

    print("\n--- RUN 2: Resuming (Expecting Success) ---")
    FAIL_ONCE = False # Disable failure

    # In a real scenario, we would reload the workflow by ID.
    # But here we pass the SAME context (or create new handle for same ID if possible).
    # Since `create_workflow` creates a NEW row, we can't use it to 'resume'.
    # We need `load_workflow`?
    # Admin doesn't strictly have `load_workflow` in Python bindings yet?
    # Wait, `Admin` has `get_workflow`? No.
    # But we can reuse the `wf_ctx` handle! The handle has the ID.
    # When we call `my_workflow(wf_ctx, ...)` again, logic is:
    # 1. Start -> Idempotent (transitions to RUNNING if not running).
    # 2. Step 1 -> Checks DB. Should be SUCCESS. Returns SKIPPED.
    # 3. Step 2 -> Checks DB. PENDING/ERROR? If ERROR, acquire resets to RUNNING.
    # 4. Step 2 Logic -> Runs (FAIL_ONCE=False) -> Success.

    # However, `my_workflow` calls `ctx.start()`.
    # `handle.rs`: `start()` checks status. If ERROR, it fails with "ValidationFailed".
    # If the workflow itself is failed (because step failed?), we need to reset it?
    # `StepGuard` failure DOES mark step as ERROR.
    # Does it mark workflow as ERROR?
    # The decorator `@workflow` catches exception and calls `ctx.fail()`.
    # So workflow status becomes ERROR.
    # Resuming an ERROR workflow requires manual intervention or specific API?
    # `start()` implementation fails if status is ERROR.

    # So to resume, we probably need to clear the error status?
    # Or `retry` logic?
    # `pgqrs` currently handles crashes (process death).
    # If process dies, workflow stays RUNNING (stale).
    # `start()` on RUNNING is idempotent.
    # So creating a "Crash" means NOT calling `ctx.fail()`.

    # Let's simulate CRASH (incomplete execution) rather than logic error.
    # Run 1 will raise SystemExit or something caught but NOT calling ctx.fail?
    # No, raising exception in `retry_logic_wrapper`...
    # I can just return early or break?
    pass

    # Let's override the decorator behavior for testing? No.
    # I will modify Step 2 to throw exception, but I will catch it OUTSIDE the decorator?
    # Wait, decorator catches exception and calls fail.
    # If I want to simulate process crash, I should throw an exception that decorator doesn't catch?
    # BaseException?
    # Or just Mock the fail call?
    pass

async def test_crash_recovery(dsn):
    admin = Admin(dsn, None)
    await admin.install()

    print("\n--- Setting up ---")
    wf_ctx = await admin.create_workflow("crash_test", "input")
    # wf_id = wf_ctx.id()

    # We need to manually simulate the state after a "crash" at step 1 completion.
    # Since we can't easily kill process in this single script without losing state context (unless using DB persistence),
    # we rely on the fact that we reuse the same DB container.

    # Approach:
    # Run a func that runs Step 1 then "crashes" (raises Skip/Exit without failing workflow).

    print("\n--- RUN 1: Crashing after Step 1 ---")
    try:
        await crashing_workflow(wf_ctx, "input")
    except ZeroDivisionError:
        print("Crashed (simulated)!")

    print("\n--- RUN 2: Resuming ---")
    # Workflow status should be RUNNING (stuck).
    # Step 1 should be SUCCESS.
    # Step 2 should be PENDING.

    res = await full_workflow(wf_ctx, "input")
    print(f"Result: {res}")
    assert res == "step2_processed_input"

# Renamed to avoid redefinition error
@step
async def step1_v2(ctx: PyWorkflow, msg: str):
    print("  [Step 1] Executing")
    return f"processed_{msg}"

@step
async def step2_v2(ctx: PyWorkflow, val: str):
    print("  [Step 2] Executing")
    return f"step2_{val}"

# A generic runner that mimics the workflow decorator but crashes
async def crashing_workflow(ctx: PyWorkflow, arg: str):
    await ctx.start()
    res1 = await step1_v2(ctx, arg)
    print(f"Step 1 done: {res1}")
    print("Simulating crash now...")
    raise ZeroDivisionError("Crash")

# Full workflow using normal decorator (but reusing context)
@workflow
async def full_workflow(ctx: PyWorkflow, arg: str):
    # This wrapper calls start(). If stuck in RUNNING, start() is OK.
    res1 = await step1_v2(ctx, arg) # Should SKIP
    res2 = await step2_v2(ctx, res1) # Should EXECUTE
    return res2

def main():
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        return

    with PostgresContainer("postgres:15") as postgres:
        dsn = postgres.get_connection_url().replace("psycopg2", "postgresql")
        asyncio.run(test_crash_recovery(dsn))

if __name__ == "__main__":
    main()
