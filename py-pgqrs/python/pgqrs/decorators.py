import functools
from ._pgqrs import PyWorkflow

def workflow(func):
    """
    Decorator to mark a function as a durable workflow entry point.

    The decorated function must accept a `PyWorkflow` instance as its first argument.
    The decorator handles:
    1. Starting the workflow (transitioning to RUNNING state).
    2. Executing the function body.
    3. Marking the workflow as SUCCESS (with return value) or ERROR (with exception).
    """
    @functools.wraps(func)
    async def wrapper(ctx, *args, **kwargs):
        if not isinstance(ctx, PyWorkflow):
             raise TypeError(f"First argument to a workflow must be a PyWorkflow instance, got {type(ctx)}")

        # Start workflow
        await ctx.start()

        try:
            result = await func(ctx, *args, **kwargs)
            # Success
            await ctx.success(result)
            return result
        except Exception as e:
            # Failure
            await ctx.fail(str(e))
            raise

    return wrapper

def step(func):
    """
    Decorator to mark a function as a durable workflow step.

    The decorated function must accept a `PyWorkflow` instance as its first argument.
    The decorator handles:
    1. Checking if the step has already completed (idempotency).
    2. If completed, returning the cached result immediately (skipping execution).
    3. If not, executing the function body.
    4. Persisting the success/failure result.
    """
    @functools.wraps(func)
    async def wrapper(ctx, *args, **kwargs):
        if not isinstance(ctx, PyWorkflow):
             raise TypeError(f"First argument to a step must be a PyWorkflow instance, got {type(ctx)}")

        step_id = func.__name__

        # Acquire step
        step_result = await ctx.acquire_step(step_id)

        if step_result.status == "SKIPPED":
            return step_result.value

        elif step_result.status == "EXECUTE":
            guard = step_result.guard
            try:
                result = await func(ctx, *args, **kwargs)
                await guard.success(result)
                return result
            except Exception as e:
                await guard.fail(str(e))
                raise
        else:
            raise RuntimeError(f"Unknown step status: {step_result.status}")

    return wrapper
