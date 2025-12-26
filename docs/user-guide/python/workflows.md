# Python Workflow API

This page documents the Python API for durable workflows in pgqrs.

## Overview

The Python workflow API provides a decorator-based approach that feels natural to Python developers while leveraging the underlying Rust implementation for durability.

## Core Components

### Decorators

| Decorator | Description |
|-----------|-------------|
| `@workflow` | Marks a function as a workflow entry point |
| `@step` | Marks a function as a durable step |

### Classes

| Class | Description |
|-------|-------------|
| `PyWorkflow` | Workflow context passed to steps |
| `Admin` | Used to create and manage workflows |

## Quick Start

```python
import asyncio
from pgqrs import Admin, PyWorkflow
from pgqrs.decorators import workflow, step

@step
async def my_step(ctx: PyWorkflow, data: str) -> str:
    return f"processed_{data}"

@workflow
async def my_workflow(ctx: PyWorkflow, input_data: str):
    result = await my_step(ctx, input_data)
    return result

async def main():
    admin = Admin("postgresql://localhost/mydb", None)
    await admin.install()

    ctx = await admin.create_workflow("my_workflow", "test")
    result = await my_workflow(ctx, "test")
    print(result)

asyncio.run(main())
```

## The `@step` Decorator

The `@step` decorator transforms a function into a durable step.

### Signature

```python
from pgqrs.decorators import step

@step
async def step_name(ctx: PyWorkflow, *args, **kwargs) -> ReturnType:
    ...
```

### Requirements

1. **First argument**: Must be a `PyWorkflow` instance
2. **Async function**: Steps must be `async def`
3. **JSON-serializable return**: Return values are persisted as JSON

### Behavior

1. **Checks completion**: Queries DB for step status
2. **If completed**: Returns cached result immediately (no execution)
3. **If not completed**: Executes function, persists result

### Example

```python
from pgqrs import PyWorkflow
from pgqrs.decorators import step

@step
async def fetch_user(ctx: PyWorkflow, user_id: int) -> dict:
    """Fetch user data - only executes once per workflow run."""
    print(f"Fetching user {user_id}")

    # This API call only happens once
    response = await http_client.get(f"/users/{user_id}")
    return response.json()

@step
async def process_user(ctx: PyWorkflow, user_data: dict) -> dict:
    """Process user data."""
    return {
        "id": user_data["id"],
        "name": user_data["name"].upper(),
        "processed": True
    }
```

### Step ID

By default, the step ID is the function name. This means:

- Each step function must have a unique name within a workflow
- The same function called multiple times uses the same step ID

!!! warning "Step Deduplication"
    When the same step function is called multiple times, subsequent calls return the cached result from the first call. This is **intentional behavior** for crash recovery - it ensures idempotency.

```python
@step
async def transform(ctx: PyWorkflow, data: str) -> str:
    return data.upper()

@workflow
async def my_workflow(ctx: PyWorkflow, data: str):
    # Both calls share the same step_id "transform"
    result1 = await transform(ctx, "hello")  # Executes, returns "HELLO"
    result2 = await transform(ctx, "world")  # Skipped! Returns cached "HELLO"
    # result2 == "HELLO" because the step was already completed
```

To process multiple items, use a loop within a single step or create separate step functions.

## The `@workflow` Decorator

The `@workflow` decorator marks a function as a workflow entry point.

### Signature

```python
from pgqrs.decorators import workflow

@workflow
async def workflow_name(ctx: PyWorkflow, *args, **kwargs) -> ReturnType:
    ...
```

### Requirements

1. **First argument**: Must be a `PyWorkflow` instance
2. **Async function**: Workflows must be `async def`

### Behavior

1. **Starts workflow**: Transitions status to `RUNNING`
2. **Executes function**: Runs the workflow logic
3. **On success**: Marks workflow as `SUCCESS`, persists return value
4. **On exception**: Marks workflow as `ERROR`, persists error

### Example

```python
from pgqrs import PyWorkflow
from pgqrs.decorators import workflow, step

@step
async def step_one(ctx: PyWorkflow, data: str) -> str:
    return f"step1_{data}"

@step
async def step_two(ctx: PyWorkflow, data: str) -> str:
    return f"step2_{data}"

@workflow
async def my_pipeline(ctx: PyWorkflow, input_data: str):
    """A durable pipeline that survives crashes."""
    result1 = await step_one(ctx, input_data)
    result2 = await step_two(ctx, result1)
    return result2
```

## The `PyWorkflow` Class

The workflow context provides access to workflow state and database operations.

### Creating a Workflow

```python
from pgqrs import Admin

async def create_workflow():
    admin = Admin("postgresql://localhost/mydb", None)

    # Create new workflow
    ctx = await admin.create_workflow(
        name="my_workflow",    # Workflow type name
        input_arg="some data"  # Initial input
    )

    print(f"Workflow ID: {ctx.id()}")
    return ctx
```

### Methods

| Method | Description |
|--------|-------------|
| `id()` | Returns the workflow ID |
| `start()` | Transitions to `RUNNING` state |
| `success(output)` | Marks workflow as `SUCCESS` |
| `fail(error)` | Marks workflow as `ERROR` |
| `acquire_step(step_id)` | Low-level step acquisition |

### Low-Level Step Access

For advanced use cases, you can use `acquire_step` directly:

```python
async def manual_step(ctx: PyWorkflow, data: str) -> str:
    step_result = await ctx.acquire_step("my_manual_step")

    if step_result.status == "SKIPPED":
        return step_result.value

    elif step_result.status == "EXECUTE":
        guard = step_result.guard
        try:
            result = process(data)
            await guard.success(result)
            return result
        except Exception as e:
            await guard.fail(str(e))
            raise
```

## The `Admin` Class

Used for workflow creation and schema management.

### Connection

```python
from pgqrs import Admin

# Basic connection
admin = Admin("postgresql://user:pass@localhost:5432/db", None)

# With schema
admin = Admin("postgresql://localhost/db", "custom_schema")
```

### Methods

| Method | Description |
|--------|-------------|
| `install()` | Install pgqrs schema (including workflow tables) |
| `create_workflow(name, input)` | Create a new workflow instance |

### Example

```python
from pgqrs import Admin

async def setup():
    admin = Admin("postgresql://localhost/mydb", None)

    # Install schema (idempotent)
    await admin.install()

    # Create workflow
    ctx = await admin.create_workflow("data_pipeline", "input_data")
    return ctx
```

## Complete Example

```python
import asyncio
from pgqrs import Admin, PyWorkflow
from pgqrs.decorators import workflow, step

# Define steps
@step
async def validate_input(ctx: PyWorkflow, data: str) -> dict:
    """Validate and parse input data."""
    print(f"[validate] Checking: {data}")

    if not data:
        raise ValueError("Empty input")

    return {"valid": True, "data": data, "length": len(data)}

@step
async def transform_data(ctx: PyWorkflow, validated: dict) -> dict:
    """Transform the validated data."""
    print(f"[transform] Processing: {validated}")

    return {
        "original": validated["data"],
        "transformed": validated["data"].upper(),
        "doubled": validated["data"] * 2
    }

@step
async def save_result(ctx: PyWorkflow, transformed: dict) -> str:
    """Save the transformed data."""
    print(f"[save] Storing: {transformed}")

    # Simulate database write
    await asyncio.sleep(0.1)

    return f"Saved: {transformed['transformed']}"

# Define workflow
@workflow
async def data_pipeline(ctx: PyWorkflow, raw_data: str):
    """A complete data processing pipeline."""
    print(f"[workflow] Starting with: {raw_data}")

    validated = await validate_input(ctx, raw_data)
    transformed = await transform_data(ctx, validated)
    result = await save_result(ctx, transformed)

    print(f"[workflow] Completed: {result}")
    return result

# Run workflow
async def main():
    dsn = "postgresql://localhost:5432/mydb"
    admin = Admin(dsn, None)

    # Install schema
    await admin.install()

    # Create and run workflow
    ctx = await admin.create_workflow("data_pipeline", "hello world")
    print(f"Created workflow: {ctx.id()}")

    result = await data_pipeline(ctx, "hello world")
    print(f"Final result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Output

```
Created workflow: 1
[workflow] Starting with: hello world
[validate] Checking: hello world
[transform] Processing: {'valid': True, 'data': 'hello world', 'length': 11}
[save] Storing: {'original': 'hello world', 'transformed': 'HELLO WORLD', 'doubled': 'hello worldhello world'}
[workflow] Completed: Saved: HELLO WORLD
Final result: Saved: HELLO WORLD
```

## Error Handling

### Step Errors

Exceptions in steps are caught, persisted, and re-raised:

```python
@step
async def risky_step(ctx: PyWorkflow, data: str) -> str:
    if data == "bad":
        raise ValueError("Invalid data")
    return f"ok_{data}"

@workflow
async def error_demo(ctx: PyWorkflow, data: str):
    try:
        result = await risky_step(ctx, data)
        return result
    except ValueError as e:
        print(f"Step failed: {e}")
        raise  # Re-raise to mark workflow as ERROR
```

### Workflow Errors

Unhandled exceptions in workflows are persisted:

```python
@workflow
async def may_fail(ctx: PyWorkflow, data: str):
    if data == "crash":
        raise RuntimeError("Workflow crashed")
    return "success"

# Usage
try:
    await may_fail(ctx, "crash")
except RuntimeError:
    # Workflow is now in ERROR state
    pass
```

### Terminal Error State

Once a step is in `ERROR` state, re-running the workflow will fail:

```python
# First run - step fails
try:
    await my_workflow(ctx, "bad_data")
except Exception:
    pass

# Second run - cannot proceed past failed step
try:
    await my_workflow(ctx, "bad_data")  # Fails immediately
except Exception as e:
    print(f"Step is terminal: {e}")
```

## Crash Recovery

The primary benefit of durable workflows is automatic crash recovery.

### Scenario

```python
CRASH_ON_STEP_TWO = True

@step
async def step_one(ctx: PyWorkflow, data: str) -> str:
    print("[step_one] Executing")
    return f"s1_{data}"

@step
async def step_two(ctx: PyWorkflow, data: str) -> str:
    global CRASH_ON_STEP_TWO
    print("[step_two] Executing")

    if CRASH_ON_STEP_TWO:
        CRASH_ON_STEP_TWO = False
        raise RuntimeError("Crash!")

    return f"s2_{data}"

@workflow
async def crash_demo(ctx: PyWorkflow, data: str):
    r1 = await step_one(ctx, data)
    r2 = await step_two(ctx, r1)
    return r2
```

### Run 1 (Crash)

```
[step_one] Executing
[step_two] Executing
RuntimeError: Crash!
```

### Run 2 (Resume)

```
[step_two] Executing    # step_one skipped!
Result: s2_s1_test
```

## Best Practices

### 1. Unique Step Functions

Each step function should have a unique name:

```python
# Good
@step
async def fetch_user_profile(ctx, user_id): ...

@step
async def fetch_user_orders(ctx, user_id): ...

# Bad - naming collision risk
@step
async def fetch(ctx, thing): ...  # Too generic
```

### 2. JSON-Serializable Returns

Ensure all return values can be serialized:

```python
# Good
@step
async def get_data(ctx) -> dict:
    return {"id": 1, "name": "test"}

# Bad - datetime not JSON serializable by default
@step
async def get_timestamp(ctx):
    return datetime.now()  # Will fail

# Fixed
@step
async def get_timestamp(ctx) -> str:
    return datetime.now().isoformat()
```

### 3. Idempotent External Calls

For side effects, use idempotency keys:

```python
@step
async def send_notification(ctx: PyWorkflow, user_id: str) -> str:
    # Use workflow ID for idempotency
    await notification_service.send(
        user_id=user_id,
        idempotency_key=f"workflow-{ctx.id()}-notify-{user_id}"
    )
    return "sent"
```

### 4. Error Boundaries

Decide where errors should be terminal:

```python
@step
async def robust_step(ctx: PyWorkflow, data: str) -> dict:
    try:
        result = await external_call(data)
        return {"success": True, "data": result}
    except TransientError:
        # Re-raise to retry on next run
        raise
    except PermanentError as e:
        # Return error as data (step succeeds, error is recorded)
        return {"success": False, "error": str(e)}
```

## Testing

### Unit Testing Steps

```python
import pytest
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture
def mock_ctx():
    ctx = MagicMock()
    ctx.acquire_step = AsyncMock()
    return ctx

@pytest.mark.asyncio
async def test_step_logic():
    # Test the underlying logic without durability
    result = process_data("test")
    assert result == expected
```

### Integration Testing

```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture
async def db():
    with PostgresContainer("postgres:15") as postgres:
        dsn = postgres.get_connection_url()
        admin = Admin(dsn, None)
        await admin.install()
        yield admin

@pytest.mark.asyncio
async def test_workflow(db):
    ctx = await db.create_workflow("test_wf", "input")
    result = await my_workflow(ctx, "input")
    assert result == "expected"
```

## See Also

- [Durable Workflows Concepts](../concepts/durable-workflows.md)
- [Durable Workflows Guide](../guides/durable-workflows.md)
- [Rust Workflow API](../rust/workflows.md)
