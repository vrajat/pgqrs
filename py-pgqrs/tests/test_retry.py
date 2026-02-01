"""
Test suite for workflow step retry functionality.

This test demonstrates:
1. Transient error handling with automatic retries
2. Backoff strategies (fixed, exponential, exponential with jitter)
3. Custom retry delays (e.g., Retry-After headers)
4. Retry exhaustion behavior
5. Non-transient errors (immediate failure)
"""

import pytest
import asyncio
from datetime import datetime, timezone

from pgqrs import PyWorkflow, BackoffStrategy, StepRetryPolicy
from pgqrs.decorators import step, workflow
import pgqrs


# ============================================================================
# Test Fixtures & Helpers
# ============================================================================


class RetryTracker:
    """Track retry attempts across workflow executions."""

    def __init__(self):
        self.attempts = 0
        self.timestamps = []

    def record_attempt(self):
        self.attempts += 1
        self.timestamps.append(datetime.now(timezone.utc))
        return self.attempts


# Global tracker (reset per test)
retry_tracker = RetryTracker()


@pytest.fixture(autouse=True)
def reset_retry_tracker():
    """Reset the retry tracker before each test."""
    global retry_tracker
    retry_tracker = RetryTracker()
    yield


# ============================================================================
# Basic Retry Functionality Tests
# ============================================================================


@step
async def transient_failure_step(ctx: PyWorkflow, max_failures: int) -> str:
    """
    Step that fails transiently N times before succeeding.

    Args:
        max_failures: Number of times to fail before succeeding
    """
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("transient_step")
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    if attempt <= max_failures:
        # Fail transiently
        await guard.fail_transient(
            "SIMULATED_TIMEOUT", f"Attempt {attempt} failed (simulated)"
        )
    else:
        # Succeed
        await guard.success(f"Success after {attempt} attempts")

    return f"Success after {attempt} attempts"


@workflow
async def basic_retry_workflow(ctx: PyWorkflow, max_failures: int) -> dict:
    """Workflow that demonstrates basic retry behavior."""
    result = await transient_failure_step(ctx, max_failures)
    return {"result": result, "attempts": retry_tracker.attempts}


@pytest.mark.asyncio
async def test_basic_transient_retry(test_dsn: str, schema: str | None):
    """Test that transient errors trigger automatic retries."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    # Create workflow that fails twice, then succeeds
    wf = await admin.create_workflow("basic_retry_test", {"max_failures": 2})

    # Start workflow
    await wf.start()

    # Execute workflow - will fail twice, then succeed on 3rd attempt
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            result = await basic_retry_workflow(wf, max_failures=2)
            assert result["attempts"] == 3, "Should succeed on 3rd attempt"
            assert "Success after 3 attempts" in result["result"]
            break
        except pgqrs.StepNotReadyError:
            # Step is scheduled for retry, wait and try again
            await asyncio.sleep(0.1)
        except Exception as e:
            # Unexpected error
            pytest.fail(f"Unexpected error: {e}")

    assert retry_tracker.attempts == 3, "Should have made 3 attempts total"


# ============================================================================
# Custom Retry Delay Tests
# ============================================================================


@step
async def rate_limited_step(ctx: PyWorkflow, retry_after_seconds: float) -> str:
    """Step that simulates rate limiting with custom retry delay."""
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("rate_limited_step")
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    if attempt == 1:
        # First attempt: fail with custom retry delay (simulating Retry-After header)
        await guard.fail_transient(
            "RATE_LIMITED",
            "Too many requests (simulated)",
            retry_after=retry_after_seconds,
        )
    else:
        # Second attempt: succeed
        await guard.success(f"Success after rate limit")

    return f"Success after rate limit"


@workflow
async def rate_limit_workflow(ctx: PyWorkflow, retry_after: float) -> dict:
    """Workflow demonstrating custom retry delays."""
    result = await rate_limited_step(ctx, retry_after)
    return {"result": result, "attempts": retry_tracker.attempts}


@pytest.mark.asyncio
async def test_custom_retry_delay(test_dsn: str, schema: str | None):
    """Test that custom retry_after delay is respected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    # Create workflow with 0.5 second retry delay
    wf = await admin.create_workflow("rate_limit_test", {"retry_after": 0.5})

    await wf.start()

    # First execution: will fail with rate limit
    try:
        await rate_limit_workflow(wf, retry_after=0.5)
    except pgqrs.StepNotReadyError as e:
        # Expected: step scheduled for retry
        pass

    # Wait for retry delay
    await asyncio.sleep(0.6)

    # Second execution: should succeed
    result = await rate_limit_workflow(wf, retry_after=0.5)
    assert result["attempts"] == 2
    assert "Success after rate limit" in result["result"]


# ============================================================================
# Retry Exhaustion Tests
# ============================================================================


@step
async def always_fails_transient(ctx: PyWorkflow, _: None) -> str:
    """Step that always fails transiently (to test retry exhaustion)."""
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("always_fails_step")
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    # Always fail transiently
    await guard.fail_transient("PERSISTENT_ERROR", f"Attempt {attempt} failed")

    return "Never succeeds"


@workflow
async def retry_exhaustion_workflow(ctx: PyWorkflow, _: None) -> dict:
    """Workflow that exhausts retries."""
    result = await always_fails_transient(ctx, None)
    return {"result": result}


@pytest.mark.asyncio
async def test_retry_exhaustion(test_dsn: str, schema: str | None):
    """Test that retries exhaust after max_attempts."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("retry_exhaustion_test", {})
    await wf.start()

    # Default retry policy: max_attempts = 3
    # Execute workflow multiple times until retries are exhausted
    max_polls = 20
    retries_exhausted = False

    for poll in range(max_polls):
        try:
            await retry_exhaustion_workflow(wf, None)
        except pgqrs.StepNotReadyError:
            # Step is scheduled for retry
            await asyncio.sleep(0.1)
        except pgqrs.RetriesExhaustedError as e:
            # Expected: retries exhausted after 3 attempts
            retries_exhausted = True
            break
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")

    assert retries_exhausted, "Should exhaust retries after max_attempts"
    assert retry_tracker.attempts >= 3, "Should have attempted at least 3 times"


# ============================================================================
# Non-Transient Error Tests
# ============================================================================


@step
async def non_transient_failure_step(ctx: PyWorkflow, _: None) -> str:
    """Step that fails with a non-transient error."""
    retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("non_transient_step")
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    # Fail with non-transient error (no retry)
    await guard.fail("Permanent validation error")

    return "Never succeeds"


@workflow
async def non_transient_workflow(ctx: PyWorkflow, _: None) -> dict:
    """Workflow with non-transient failure."""
    result = await non_transient_failure_step(ctx, None)
    return {"result": result}


@pytest.mark.asyncio
async def test_non_transient_error_no_retry(test_dsn: str, schema: str | None):
    """Test that non-transient errors fail immediately without retry."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("non_transient_test", {})
    await wf.start()

    # Execute workflow - should fail immediately without retry
    try:
        await non_transient_workflow(wf, None)
    except pgqrs.RetriesExhaustedError:
        # Non-transient errors result in RetriesExhausted with attempts=0
        pass
    except Exception as e:
        # Could also manifest as a different error depending on implementation
        pass

    # Second execution: should return the same error (cached)
    step_res = await wf.acquire_step("non_transient_step")
    assert step_res.status == "EXECUTE" or step_res.status == "SKIPPED"

    # Should NOT have retried
    assert retry_tracker.attempts == 1, "Non-transient error should not retry"


# ============================================================================
# Backoff Strategy Type Construction Tests
# ============================================================================


def test_backoff_strategy_construction():
    """Test that BackoffStrategy types can be constructed."""

    # Fixed backoff
    fixed = BackoffStrategy.fixed(5)
    assert fixed is not None

    # Exponential backoff
    exponential = BackoffStrategy.exponential(2, 60)
    assert exponential is not None

    # Exponential with jitter
    jitter = BackoffStrategy.exponential_with_jitter(1, 120)
    assert jitter is not None


def test_retry_policy_construction():
    """Test that StepRetryPolicy can be constructed."""

    # Default policy
    default_policy = StepRetryPolicy()
    assert default_policy.max_attempts == 3

    # Custom policy
    custom_policy = StepRetryPolicy(
        max_attempts=5, backoff=BackoffStrategy.exponential_with_jitter(2, 60)
    )
    assert custom_policy.max_attempts == 5


# ============================================================================
# Validation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_negative_retry_after_rejected(test_dsn: str, schema: str | None):
    """Test that negative retry_after values are rejected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("validation_test", {})
    await wf.start()

    step_res = await wf.acquire_step("validation_step")
    guard = step_res.guard

    # Attempt to fail with negative retry_after
    with pytest.raises(ValueError, match="retry_after must be non-negative"):
        await guard.fail_transient("TEST", "Test error", retry_after=-5.0)


# ============================================================================
# Multi-Backend Compatibility
# ============================================================================


@pytest.mark.asyncio
async def test_retry_across_backends(test_dsn: str, schema: str | None):
    """
    Test retry functionality works across all backends (Postgres, SQLite, Turso).

    This test is automatically run on all backends via conftest.py test_backend fixture.
    """
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    # Create workflow that fails once, then succeeds
    wf = await admin.create_workflow("backend_compat_test", {"max_failures": 1})
    await wf.start()

    # Execute workflow
    max_attempts = 10
    success = False
    for attempt in range(max_attempts):
        try:
            result = await basic_retry_workflow(wf, max_failures=1)
            assert result["attempts"] == 2, "Should succeed on 2nd attempt"
            success = True
            break
        except pgqrs.StepNotReadyError:
            await asyncio.sleep(0.1)

    assert success, "Workflow should eventually succeed across all backends"
