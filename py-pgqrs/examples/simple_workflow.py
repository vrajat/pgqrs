import asyncio

from pgqrs import Admin
from pgqrs.decorators import workflow, step
from pgqrs import PyWorkflow

@step
async def step1(ctx: PyWorkflow, msg: str):
    print(f"  [Step 1] Executing with msg: {msg}")
    return f"processed_{msg}"

@step
async def step2(ctx: PyWorkflow, val: str):
    print(f"  [Step 2] Executing with val: {val}")
    return f"step2_{val}"

@workflow
async def my_workflow(ctx: PyWorkflow, arg: str):
    print(f"[Workflow] Starting with arg: {arg}")
    res1 = await step1(ctx, arg)
    print(f"[Workflow] Step 1 result: {res1}")
    res2 = await step2(ctx, res1)
    print(f"[Workflow] Step 2 result: {res2}")
    return res2

async def run_workflow(dsn):
    try:
        admin = Admin(dsn, None)
        # Install schema
        print("Installing schema...")
        await admin.install()
    except Exception as e:
        print(f"Failed to connect/install: {e}")
        return

    wf_name = "my_workflow"
    wf_arg = "test_data"

    print(f"Creating workflow '{wf_name}'...")
    try:
        wf_ctx = await admin.create_workflow(wf_name, wf_arg)
        print(f"Created workflow ID: {wf_ctx.id()}")

        # Run workflow
        result = await my_workflow(wf_ctx, wf_arg)
        print(f"Workflow completed successfully. Result: {result}")
        assert result == "step2_processed_test_data"

    except Exception as e:
        print(f"Workflow execution failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        print("testcontainers not installed, skipping full DB test")
        return

    print("Starting Postgres container...")
    with PostgresContainer("postgres:15") as postgres:
        dsn = postgres.get_connection_url().replace("psycopg2", "postgresql")
        print(f"DSN: {dsn}")
        asyncio.run(run_workflow(dsn))

if __name__ == "__main__":
    main()
