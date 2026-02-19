import functools
from ._pgqrs import Run
import pgqrs


def workflow(func):
    """
    Decorator to mark a function as a durable workflow entry point.

    The decorated function must accept a `Run` instance as its first argument.
    The decorator handles:
    1. Starting the workflow (transitioning to RUNNING state).
    2. Executing the function body.
    3. Marking the workflow as SUCCESS (with return value) or ERROR (with exception).
    """

    @functools.wraps(func)
    async def wrapper(ctx, *args, **kwargs):
        if not isinstance(ctx, Run):
            raise TypeError(
                f"First argument to a workflow must be a Run instance, got {type(ctx)}"
            )

        # Start workflow
        try:
            await ctx.start()
        except pgqrs.ValidationError as e:
            if "terminal SUCCESS" not in str(e):
                raise

        try:
            result = await func(ctx, *args, **kwargs)
            # Success
            await ctx.success(result)
            return result
        except pgqrs.TransientStepError:
            # Propagate transient errors without marking workflow as failed
            # The step is already marked with retry scheduled
            raise
        except (pgqrs.StepNotReadyError, pgqrs.RetriesExhaustedError):
            # Propagate retry control-flow exceptions
            raise
        except Exception as e:
            # Failure
            await ctx.fail(str(e))
            raise

    return wrapper


def step(func):
    """
    Decorator to mark a function as a durable workflow step.

    The decorated function must accept a `Run` instance as its first argument.
    The decorator handles:
    1. Checking if the step has already completed (idempotency).
    2. If completed, returning the cached result immediately (skipping execution).
    3. If not, executing the function body.
    4. Persisting the success/failure result.
    """

    @functools.wraps(func)
    async def wrapper(ctx, *args, **kwargs):
        if not isinstance(ctx, Run):
            raise TypeError(
                f"First argument to a step must be a Run instance, got {type(ctx)}"
            )

        step_name = func.__name__

        # Acquire step (uses system time by default, or current_time parameter in tests)
        current_time = getattr(ctx, "current_time", None)
        step_result = await ctx.acquire_step(step_name, current_time=current_time)

        if step_result.status == "SKIPPED":
            return step_result.value

        elif step_result.status == "EXECUTE":
            guard = step_result.guard
            try:
                result = await func(ctx, *args, **kwargs)
                await guard.success(result)
                return result
            except pgqrs.TransientStepError as e:
                # Handle transient failures - schedule retry and propagate
                await guard.fail_transient("TRANSIENT_ERROR", str(e))
                raise
            except (pgqrs.StepNotReadyError, pgqrs.RetriesExhaustedError):
                # Propagate retry control-flow exceptions
                raise
            except Exception as e:
                await guard.fail(str(e))
                raise
        else:
            raise RuntimeError(f"Unknown step status: {step_result.status}")

    return wrapper
